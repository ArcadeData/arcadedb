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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Word2Vec importer using LSM Vector index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Word2VecImporterLSMIT {
  @Test
  void importDocumentsWithLSMVector() {
    final String databasePath = "target/databases/test-word2vec-lsm";

    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      // Import using LSMVector
      db.command("sql", "import database file://src/test/resources/importer-word2vec.txt "  //
          + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
          + "vertexType = Word, vectorProperty = vector, idProperty = name" //
      );

      // Verify 10 words were imported
      assertThat(db.countType("Word", true)).isEqualTo(10);

      // Verify the LSM_VECTOR index was created
      final Index index = db.getSchema().getIndexByName("Word[vector]");
      assertThat(index).isNotNull();
      assertThat(index).isInstanceOf(TypeIndex.class);

      final TypeIndex typeIndex = (TypeIndex) index;
      // Verify it's an LSMVectorIndex by checking the class name
      assertThat(typeIndex.getIndexesOnBuckets()[0].getClass().getSimpleName()).isEqualTo("LSMVectorIndex");

      // Verify index entry count
      assertThat(typeIndex.countEntries()).isEqualTo(10);

      // Verify we can query records
      final ResultSet results = db.query("sql", "SELECT name FROM Word LIMIT 5");

      int count = 0;
      while (results.hasNext()) {
        results.next();
        count++;
      }
      results.close();

      assertThat(count).isEqualTo(5);

    } finally {
      db.drop();
      TestHelper.checkActiveDatabases();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }

  @Test
  void importAndUpdateWithLSMVector() {
    final String databasePath = "target/databases/test-word2vec-lsm-update";

    FileUtils.deleteRecursively(new File(databasePath));

    try (final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath)) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      final Database db = databaseFactory.create();
      try {
        // Import using LSMVector
        db.command("sql", "import database file://src/test/resources/importer-word2vec.txt "  //
            + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
            + "vertexType = Word, vectorProperty = vector, idProperty = name" //
        );

        assertThat(db.countType("Word", true)).isEqualTo(10);

        final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Word[vector]");
        final long initialCount = typeIndex.countEntries();
        assertThat(initialCount).isEqualTo(10);

        // Update a vector (LSM append-only semantics)
        db.transaction(() -> {
          final ResultSet rs = db.query("sql", "SELECT FROM Word WHERE name = '<user>' LIMIT 1");
          if (rs.hasNext()) {
            final var vertex = rs.next().toElement().asVertex().modify();
            final float[] newVector = new float[100];
            for (int i = 0; i < 100; i++) {
              newVector[i] = (float) Math.random();
            }
            vertex.set("vector", newVector);
            vertex.save();
          }
          rs.close();
        });

        // Count should still be 10 (last-write-wins in LSM)
        final long countAfterUpdate = typeIndex.countEntries();
        assertThat(countAfterUpdate).isEqualTo(10);

        // Delete a vertex
        db.transaction(() -> {
          final ResultSet rs = db.query("sql", "SELECT FROM Word WHERE name = '<url>' LIMIT 1");
          if (rs.hasNext()) {
            rs.next().toElement().asVertex().delete();
          }
          rs.close();
        });

        // Count should be 9
        final long countAfterDelete = typeIndex.countEntries();
        assertThat(countAfterDelete).isEqualTo(9);

      } finally {
        db.drop();
        TestHelper.checkActiveDatabases();
        FileUtils.deleteRecursively(new File(databasePath));
      }
    }
  }

  @Test
  void compactionAfterImport() {
    final String databasePath = "target/databases/test-word2vec-lsm-compact";

    FileUtils.deleteRecursively(new File(databasePath));

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      // Import using LSMVector
      db.command("sql", "import database file://src/test/resources/importer-word2vec.txt "  //
          + "with distanceFunction = cosine, m = 16, beamWidth = 100, " //
          + "vertexType = Word, vectorProperty = vector, idProperty = name" //
      );

      assertThat(db.countType("Word", true)).isEqualTo(10);

      final Index index = db.getSchema().getIndexByName("Word[vector]");
      final TypeIndex typeIndex = (TypeIndex) index;

      // Perform multiple updates to create LSM pages
      for (int batch = 0; batch < 3; batch++) {
        final int batchNum = batch; // Make it final for lambda
        db.transaction(() -> {
          final ResultSet rs = db.query("sql", "SELECT FROM Word");
          while (rs.hasNext()) {
            final var vertex = rs.next().toElement().asVertex().modify();
            final float[] newVector = new float[100];
            for (int i = 0; i < 100; i++) {
              newVector[i] = (float) (Math.random() + batchNum);
            }
            vertex.set("vector", newVector);
            vertex.save();
          }
          rs.close();
        });
      }

      // Verify index still has 10 entries (LSM last-write-wins)
      assertThat(typeIndex.countEntries()).isEqualTo(10);

      // Verify queries still work after multiple updates
      final ResultSet results = db.query("sql", "SELECT name FROM Word LIMIT 5");

      int count = 0;
      while (results.hasNext()) {
        results.next();
        count++;
      }
      results.close();

      assertThat(count).isEqualTo(5);

    } finally {
      db.drop();
      TestHelper.checkActiveDatabases();
      FileUtils.deleteRecursively(new File(databasePath));
    }
  }
}
