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
package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5069: {@code IMPORT DATABASE} crashes when the JSONL dump contains an
 * {@code LSM_VECTOR} index in the schema line.
 * <p>
 * Also pins the shape of the definition the importer restores from (issue #5639): the dump carries the whole persisted
 * index definition, including structural keys and the metric under its persisted name {@code similarityFunction}, so the
 * importer must read it as a persisted definition and not as a user {@code METADATA} clause - and every configured
 * setting has to come out the other side.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorIndexExportImportIT {

  private final static String SOURCE_PATH  = "target/databases/vector-export-source";
  private final static String RESTORE_PATH = "target/databases/vector-export-restore";
  private final static String FILE         = "target/vector-export.jsonl.tgz";
  private final static int    DIMENSIONS   = 4;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORE_PATH));
    FileUtils.deleteRecursively(new File(FILE));
  }

  @Test
  void exportAndImportDatabaseWithVectorIndex() throws Exception {
    // 1. Create a source database with a vector index and some embeddings
    try (final Database db = new DatabaseFactory(SOURCE_PATH).create()) {
      db.transaction(() -> {
        db.getSchema().createVertexType("Test");
        db.getSchema().getType("Test").createProperty("name", Type.STRING);
        db.getSchema().getType("Test").createProperty("embedding", Type.ARRAY_OF_FLOATS);
        // Every setting away from its default, so a knob lost in the round trip fails here instead of silently
        // reverting: the metric especially, since COSINE would pass whether or not it was restored at all.
        db.command("sql",
            "CREATE INDEX ON Test (embedding) LSM_VECTOR METADATA "
                + "{dimensions: " + DIMENSIONS + ", similarity: 'EUCLIDEAN', idPropertyName: 'name', "
                + "maxConnections: 24, beamWidth: 150, efSearch: 250, mutationsBeforeRebuild: 4242}");
      });

      db.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final float[] embedding = new float[DIMENSIONS];
          for (int j = 0; j < DIMENSIONS; j++)
            embedding[j] = (i + 1) * 0.1f + j;
          db.newVertex("Test").set("name", "v" + i).set("embedding", embedding).save();
        }
      });

      assertThat(db.countType("Test", true)).isEqualTo(5);
    }

    // 2. Export it to JSONL
    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    // 3. Import the dump into a fresh database (this used to crash with issue #5069)
    try (final Database db2 = new DatabaseFactory(RESTORE_PATH).create()) {
      db2.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    // 4. Verify the restored database has the schema, the vector index and the data
    try (final Database db2 = new DatabaseFactory(RESTORE_PATH).open()) {
      final Schema schema = db2.getSchema();

      assertThat(schema.getType("Test")).isNotNull();
      assertThat(db2.countType("Test", true)).isEqualTo(5);

      final Index vectorIndex = schema.getIndexByName("Test[embedding]");
      assertThat(vectorIndex).as("Vector index should be restored").isNotNull();
      assertThat(vectorIndex.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);

      final LSMVectorIndexMetadata metadata = ((LSMVectorIndex) ((TypeIndex) vectorIndex).getIndexesOnBuckets()[0])
          .getMetadata();
      assertThat(metadata.dimensions).isEqualTo(DIMENSIONS);
      assertThat(metadata.similarityFunction.name()).isEqualTo("EUCLIDEAN");
      assertThat(metadata.idPropertyName).isEqualTo("name");
      assertThat(metadata.maxConnections).isEqualTo(24);
      assertThat(metadata.beamWidth).isEqualTo(150);
      assertThat(metadata.efSearch).isEqualTo(250);
      assertThat(metadata.mutationsBeforeRebuild).isEqualTo(4242);

      // The restored index must be populated with all imported embeddings (rebuilt via the put hook)
      assertThat(vectorIndex.countEntries()).as("Vector index should have all entries").isEqualTo(5);

      // Embedding data must survive intact
      db2.iterateType("Test", true).forEachRemaining(r -> {
        final Object embedding = r.asVertex().get("embedding");
        assertThat(embedding).isNotNull();
        assertThat(((float[]) embedding).length).isEqualTo(DIMENSIONS);
      });

      // The restored index must be searchable: a neighbor query should return the imported vertices
      final var result = db2.query("sql", "SELECT `vector.neighbors`('Test[embedding]', 'v0', 3) as neighbors");
      assertThat(result.hasNext()).as("Vector search should return results after import").isTrue();
    }
  }
}
