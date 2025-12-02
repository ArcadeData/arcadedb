/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.select.SelectIterator;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;

import static com.arcadedb.schema.LocalSchema.STATISTICS_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class RecordRecyclingTest {
  private final static int    TOT_RECORDS  = 100_000;
  private final static String VERTEX_TYPE  = "Product";
  private final static String EDGE_TYPE    = "LinkedTo";
  private static final int    CYCLES       = 10;
  private final static int    TOT_VERTICES = 1000;

  @Test
  @Tag("slow")
  void createAndDeleteGraph() {
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/RecordRecyclingTest")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      final Database database = databaseFactory.create();
      database.getSchema().createVertexType(VERTEX_TYPE, 1);
      database.getSchema().createEdgeType(EDGE_TYPE, 1);
      database.close();

      RID maxVertexRID = null;
      RID maxEdgeRID = null;

      for (int i = 0; i < CYCLES; i++) {
        final Database db = databaseFactory.open();

        try {
          db.transaction(() -> {
            final MutableVertex root = db.newVertex(VERTEX_TYPE)//
                .set("id", 0)//
                .save();

            for (int k = 1; k < TOT_RECORDS; k++) {
              final MutableVertex v = db.newVertex(VERTEX_TYPE)//
                  .set("id", k)//
                  .save();

              root.newEdge(EDGE_TYPE, v, "something", k);
            }
          });

          // CHECK RECORDS HAVE BEEN RECYCLED MORE THAN 80% OF THE SPACE
          final RID maxVertex = db.query("sql", "select from " + VERTEX_TYPE + " order by @rid desc").next().getIdentity().get();
          if (maxVertexRID != null)
            assertThat(maxVertex.getPosition()).isLessThan((long) (maxVertexRID.getPosition() * 1.2));
          maxVertexRID = maxVertex;

          final RID maxEdge = db.query("sql", "select from " + EDGE_TYPE + " order by @rid desc").next().getIdentity().get();
          if (maxEdgeRID != null)
            assertThat(maxEdge.getPosition()).isLessThan((long) (maxEdgeRID.getPosition() * 1.2));
          maxEdgeRID = maxEdge;

          db.transaction(() -> {
            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(TOT_RECORDS);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(TOT_RECORDS - 1);

            db.command("sql", "delete from " + VERTEX_TYPE);

            assertThat(db.countType(VERTEX_TYPE, true)).isEqualTo(0);
            assertThat(db.countType(EDGE_TYPE, true)).isEqualTo(0);

            assertThat(db.countBucket(db.getSchema().getBucketById(1).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(2).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(3).getName())).isEqualTo(0);
            assertThat(db.countBucket(db.getSchema().getBucketById(4).getName())).isEqualTo(0);
          });

          final ResultSet result = db.command("sql", "check database");
          // System.out.println(result.nextIfAvailable().toJSON());
        } finally {
          db.close();
          new File(databaseFactory.getDatabasePath() + File.separator + STATISTICS_FILE_NAME).delete();
          assertThat(databaseFactory.getActiveDatabaseInstances()).isEmpty();
        }
      }

      if (databaseFactory.exists())
        databaseFactory.open().drop();

    } finally {

      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.reset();
    }
  }

  /**
   * Inserts a graph with a root node, then update other nodes with an increasing large content (to stress
   * page reallocation).
   * Then tests the deletion of a vertex and its edges, then check that the records are recycled correctly.
   * It uses 2 ways of deleting vertices: 1) from the last inserted and 2) from the first connected.
   * Deletion is done in batches of 20 vertices at a time or 1 at the end.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  void edgeLinkedLists() {
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/RecordRecyclingTest")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.getSchema().createVertexType(VERTEX_TYPE, 1);
        db.getSchema().createEdgeType(EDGE_TYPE, 1);

        db.transaction(() -> {
          final MutableVertex root = db.newVertex(VERTEX_TYPE)//
              .set("id", 0)//
              .save();
        });

        String largeContent = "";
        for (int i = 0; i < 100; i++)
          largeContent += i;

        for (int i = 0; i < CYCLES; i++) {
          String finalLargeContent = largeContent;

          db.transaction(() -> {
            Vertex root = db.select().fromType(VERTEX_TYPE).limit(1).vertices().next();

            for (int k = 1; k < TOT_VERTICES; k++) {
              final MutableVertex v = db.newVertex(VERTEX_TYPE)//
                  .set("id", k)//
                  .set("largeContent", finalLargeContent)//
                  .save();

              root.newEdge(EDGE_TYPE, v, "something", k);
            }
          });

          // UPDATE ALL THE VERTICES WITH A LARGE CONTENT TO FORCE REALLOCATION ON AVAILABLE PAGES
          db.transaction(() -> {
            String largeContent2 = finalLargeContent;
            for (int j = 0; j < 100; j++)
              largeContent2 += j;

            for (Vertex v : db.select().fromType(VERTEX_TYPE).vertices().toList()) {
              v.modify().set("largeContent2", largeContent2).save();
            }
          });

          for (int k = 1; k < TOT_VERTICES; ) {
            final int deleteRecords = TOT_VERTICES - k > 20 ? 20 : 1;
            // DELETE THE VERTEX IN 2 DIFFERENT WAYS
            if (k % 2 == 0)
              deleteFirstConnectedVertex(db, deleteRecords);
            else
              deleteLastVertex(db, deleteRecords);

            k += deleteRecords;

            final int idx = k;

            db.transaction(() -> {
              final SelectIterator<Vertex> cursor = db.select().fromType(VERTEX_TYPE).vertices();

              int loaded = 0;
              while (cursor.hasNext()) {
                ++loaded;
                cursor.next().toJSON();  // CHECK FULL LOADING
              }

              assertThat(loaded).isEqualTo(TOT_VERTICES - idx + 1);
            });
          }
        }

        final ResultSet result = db.command("sql", "check database");
        assertThat((Collection) result.next().getProperty("corruptedRecords")).isEmpty();
      } finally {
        if (databaseFactory.exists())
          databaseFactory.open().drop();
      }
    } finally {
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.reset();
    }
  }

  private void deleteLastVertex(Database db, int deleteRecords) {
    db.transaction(() -> {
      for (int i = 0; i < deleteRecords; i++) {
        db.query("sql", "select from " + VERTEX_TYPE + " order by @rid desc limit 1").toVertices().getFirst().delete();
      }
    });
  }

  private void deleteFirstConnectedVertex(Database db, int deleteRecords) {
    db.transaction(() -> {
      for (int i = 0; i < deleteRecords; i++) {
        Vertex root = db.select().fromType(VERTEX_TYPE).limit(1).vertices().next();
        Vertex connected = root.getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE).iterator().next();
        connected.delete();
      }
    });
  }
}
