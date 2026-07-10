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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4359: TRUNCATE TYPE resets LSM_VECTOR index dimension to 0.
 * <p>
 * The fix for #4352 drops and recreates the indexes on the type around the truncation. The
 * recreate path used to ignore the per-index metadata, so type-specific configuration
 * (LSM_VECTOR dimensions/similarity, FULL_TEXT analyzers) was lost. After TRUNCATE the
 * index existed with dimensions=0 and the next INSERT failed with "index dimension 0".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TruncateTypeVectorIndexBug4359Test extends TestHelper {

  @Test
  void truncateTypePreservesLSMVectorMetadata() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR
        METADATA { dimensions: 4, similarity: 'COSINE' }""");

    database.transaction(() -> database.command("sql",
        "INSERT INTO Doc SET embedding = [0.1, 0.2, 0.3, 0.4]"));

    assertVectorMetadata("Doc[embedding]", 4, "COSINE");

    database.transaction(() -> database.command("sql", "TRUNCATE TYPE Doc UNSAFE"));

    // After truncate the index must still carry the original dimensions and similarity:
    // a regression would surface here as dimensions == 0.
    assertVectorMetadata("Doc[embedding]", 4, "COSINE");

    // and the next insert of the same shape vector must succeed
    database.transaction(() -> database.command("sql",
        "INSERT INTO Doc SET embedding = [0.5, 0.6, 0.7, 0.8]"));

    final ResultSet count = database.query("sql", "SELECT count(*) AS cnt FROM Doc");
    assertThat(count.next().<Long>getProperty("cnt")).isEqualTo(1L);
    count.close();
  }

  @Test
  void truncateTypePreservesLSMVectorMetadataWithExtraOptions() {
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (name) UNIQUE");
    database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR
        METADATA {
          dimensions: 3,
          similarity: 'COSINE',
          idPropertyName: 'name',
          maxConnections: 24,
          beamWidth: 150
        }""");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET name = 'a', embedding = [1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Doc SET name = 'b', embedding = [0.0, 1.0, 0.0]");
    });

    final LSMVectorIndexMetadata before = lookupVectorMetadata("Doc[embedding]");
    assertThat(before.dimensions).isEqualTo(3);
    assertThat(before.similarityFunction.name()).isEqualTo("COSINE");
    assertThat(before.idPropertyName).isEqualTo("name");
    assertThat(before.maxConnections).isEqualTo(24);
    assertThat(before.beamWidth).isEqualTo(150);

    database.transaction(() -> database.command("sql", "TRUNCATE TYPE Doc UNSAFE"));

    final LSMVectorIndexMetadata after = lookupVectorMetadata("Doc[embedding]");
    assertThat(after.dimensions).isEqualTo(3);
    assertThat(after.similarityFunction.name()).isEqualTo("COSINE");
    assertThat(after.idPropertyName).isEqualTo("name");
    assertThat(after.maxConnections).isEqualTo(24);
    assertThat(after.beamWidth).isEqualTo(150);

    // unique index on name keeps working after truncate
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET name = 'a', embedding = [1.0, 0.0, 0.0]");
      database.command("sql", "INSERT INTO Doc SET name = 'b', embedding = [0.0, 1.0, 0.0]");
    });
  }

  private void assertVectorMetadata(final String indexName, final int dimensions, final String similarity) {
    final LSMVectorIndexMetadata meta = lookupVectorMetadata(indexName);
    assertThat(meta.dimensions).as("index '%s' dimensions", indexName).isEqualTo(dimensions);
    assertThat(meta.similarityFunction.name()).as("index '%s' similarity", indexName).isEqualTo(similarity);
  }

  private LSMVectorIndexMetadata lookupVectorMetadata(final String indexName) {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(index).as("vector index '%s' must exist", indexName).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);
    return (LSMVectorIndexMetadata) index.getMetadata();
  }
}
