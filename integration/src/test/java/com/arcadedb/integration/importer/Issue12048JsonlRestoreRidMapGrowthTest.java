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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12048: the JSONL importer maps the RID of every record it restores to its new RID in a {@code CompressedRID2RIDIndex} it
 * builds with 1,000 hash slots, and that table never grew, so a restore took time quadratic in the number of records. The table
 * now doubles as it fills; this restores more records than it starts with slots, spread over several buckets, and checks that
 * every edge still lands on the vertices it joined in the source, which is what the rebuilt chains have to keep answering.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12048JsonlRestoreRidMapGrowthTest {
  private static final String SOURCE_PATH  = "target/databases/issue12048-source";
  private static final String RESTORE_PATH = "target/databases/issue12048-restore";
  private static final String FILE         = "target/issue12048-export.jsonl.tgz";
  private static final String VERTEX_TYPE  = "V12048";
  private static final String EDGE_TYPE    = "E12048";
  private static final int    BUCKETS      = 4;
  // MORE THAN TWO DOUBLINGS OF THE 1,000 SLOTS THE IMPORTER STARTS WITH
  private static final int    VERTICES     = 4_500;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PATH));
    FileUtils.deleteRecursively(new File(FILE));
  }

  @Test
  void aRestoreOfMoreRecordsThanTheInitialSlotsKeepsEveryEdgeOnItsEndpoints() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      final VertexType type = source.getSchema().buildVertexType().withName(VERTEX_TYPE).withTotalBuckets(BUCKETS).create();
      type.createProperty("id", Type.INTEGER);
      source.getSchema().buildEdgeType().withName(EDGE_TYPE).create();

      source.transaction(() -> {
        final RID[] rids = new RID[VERTICES];
        for (int i = 0; i < VERTICES; i++)
          rids[i] = source.newVertex(VERTEX_TYPE).set("id", i).save().getIdentity();

        // A RING: THE EDGE OF VERTEX i GOES TO i + 1, SO A WRONG RE-MAPPING OF EITHER ENDPOINT SHOWS
        for (int i = 0; i < VERTICES; i++) {
          final MutableVertex from = rids[i].asVertex().modify();
          from.newEdge(EDGE_TYPE, rids[(i + 1) % VERTICES], "seq", i).save();
        }
      });
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE)).exists();

    try (final Database restored = new DatabaseFactory(RESTORE_PATH).create()) {
      // THE IMPORT CREATES EACH TYPE WITH THE DESTINATION'S DEFAULT BUCKET COUNT
      restored.getConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, BUCKETS);
      restored.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database restored = new DatabaseFactory(RESTORE_PATH).open()) {
      assertThat(restored.countType(VERTEX_TYPE, false)).isEqualTo(VERTICES);
      assertThat(restored.countType(EDGE_TYPE, false)).isEqualTo(VERTICES);

      int checked = 0;
      for (final Iterator<Vertex> it = restored.iterateType(VERTEX_TYPE, false); it.hasNext(); ) {
        final Vertex vertex = it.next();
        final int id = vertex.getInteger("id");

        final Iterator<Edge> edges = vertex.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE).iterator();
        assertThat(edges.hasNext()).as("vertex %d must keep its outgoing edge", id).isTrue();
        final Edge edge = edges.next();
        assertThat(edges.hasNext()).as("vertex %d must have exactly one outgoing edge", id).isFalse();

        assertThat(edge.getInteger("seq")).isEqualTo(id);
        assertThat(edge.getOutVertex().getInteger("id")).isEqualTo(id);
        assertThat(edge.getInVertex().getInteger("id")).isEqualTo((id + 1) % VERTICES);
        ++checked;
      }
      assertThat(checked).isEqualTo(VERTICES);
    }
  }
}
