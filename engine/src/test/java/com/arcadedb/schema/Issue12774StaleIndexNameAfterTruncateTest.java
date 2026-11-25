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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12774: {@code REBUILD INDEX} hands the derived name of a {@code TypeIndex} back to the builder as the
 * {@code typeIndexName} of the bucket-level index metadata. A rename of the type moved the {@code TypeIndex} and left that
 * name at the old derived one, so the {@code TRUNCATE TYPE} that re-creates the index from the metadata saw a custom name
 * which is not the one derived from the current type name, took it for a manual name, and minted the index under the name of
 * the old type (and persisted it, so it survived a reopen and blocked a new type with the old name from indexing the same
 * properties).
 */
class Issue12774StaleIndexNameAfterTruncateTest extends TestHelper {

  @Test
  void sparseIndexKeepsDerivedNameAfterRebuildRenameTruncate() {
    createTypeWithSparseIndex("D", "");
    insertSparse("D");
    database.command("sql", "REBUILD INDEX *");
    database.command("sql", "ALTER TYPE D NAME Dren");
    database.command("sql", "TRUNCATE TYPE Dren");

    assertThat(indexes()).containsExactly("Dren[tk,wt]@Dren");

    reopenDatabase();

    assertThat(indexes()).containsExactly("Dren[tk,wt]@Dren");

    // the freed name is usable by a new type that indexes the same properties
    createTypeWithSparseIndex("D", "");
    assertThat(indexes()).containsExactlyInAnyOrder("Dren[tk,wt]@Dren", "D[tk,wt]@D");

    insertSparse("Dren");
    assertThat(sparseNeighbors("Dren[tk,wt]")).isEqualTo(1);
    assertThat(sparseNeighbors("D[tk,wt]")).isEqualTo(0);

    database.command("sql", "DROP TYPE Dren");
    assertThat(indexes()).containsExactly("D[tk,wt]@D");
  }

  @Test
  void sparseIndexNamedByTheUserKeepsItsNameAfterRebuildRenameTruncate() {
    createTypeWithSparseIndex("D", "myname ");
    insertSparse("D");
    database.command("sql", "REBUILD INDEX *");
    database.command("sql", "ALTER TYPE D NAME Dren");
    database.command("sql", "TRUNCATE TYPE Dren");

    assertThat(indexes()).containsExactly("myname@Dren");

    reopenDatabase();

    assertThat(indexes()).containsExactly("myname@Dren");

    insertSparse("Dren");
    assertThat(sparseNeighbors("myname")).isEqualTo(1);
  }

  @Test
  void indexNamedLikeTheDerivedOneFollowsTheRenameAfterTruncate() {
    // CREATE INDEX `D[tk,wt]` spells the derived name out: it is no manual name, so the rename has to move it, truncate or not
    createTypeWithSparseIndex("D", "`D[tk,wt]` ");
    database.command("sql", "ALTER TYPE D NAME Dren");
    database.command("sql", "TRUNCATE TYPE Dren");

    assertThat(indexes()).containsExactly("Dren[tk,wt]@Dren");

    reopenDatabase();

    assertThat(indexes()).containsExactly("Dren[tk,wt]@Dren");
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE", "FULL_TEXT", "GEOSPATIAL", "LSM_VECTOR", "LSM_SPARSE_VECTOR" })
  void everyKindKeepsDerivedNameAfterRebuildRenameTruncate(final String kind) {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    final String create;
    switch (kind) {
    case "FULL_TEXT" -> {
      database.command("sql", "CREATE PROPERTY D.p STRING");
      create = "CREATE INDEX ON D (p) FULL_TEXT";
    }
    case "GEOSPATIAL" -> {
      database.command("sql", "CREATE PROPERTY D.p STRING");
      create = "CREATE INDEX ON D (p) GEOSPATIAL";
    }
    case "LSM_VECTOR" -> {
      database.command("sql", "CREATE PROPERTY D.p ARRAY_OF_FLOATS");
      create = "CREATE INDEX ON D (p) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }";
    }
    case "LSM_SPARSE_VECTOR" -> {
      database.command("sql", "CREATE PROPERTY D.p ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY D.q ARRAY_OF_FLOATS");
      create = "CREATE INDEX ON D (p, q) LSM_SPARSE_VECTOR METADATA { dimensions: 8 }";
    }
    default -> {
      database.command("sql", "CREATE PROPERTY D.p STRING");
      create = "CREATE INDEX ON D (p) " + kind;
    }
    }
    database.command("sql", create);

    database.command("sql", "REBUILD INDEX *");
    database.command("sql", "ALTER TYPE D NAME Dren");
    database.command("sql", "TRUNCATE TYPE Dren");

    final String expected = "Dren[" + ("LSM_SPARSE_VECTOR".equals(kind) ? "p,q" : "p") + "]@Dren";
    assertThat(indexes()).containsExactly(expected);

    reopenDatabase();

    assertThat(indexes()).containsExactly(expected);
  }

  private void createTypeWithSparseIndex(final String type, final String indexName) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".tk ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY " + type + ".wt ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX " + indexName + "ON " + type + " (tk, wt) LSM_SPARSE_VECTOR METADATA { dimensions: 8 }");
  }

  private void insertSparse(final String type) {
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET tk = [1,2], wt = [1.0, 2.0]"));
  }

  private int sparseNeighbors(final String indexName) {
    int count = 0;
    try (final ResultSet rs = database.query("sql", "SELECT expand(vectorSparseNeighbors('" + indexName + "', [1], [1.0], 5))")) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  /** {@code name@typeName} of every index the catalog lists, the bucket indexes under them left out. */
  private List<String> indexes() {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, typeName FROM schema:indexes WHERE associatedBucketId IS NULL")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.add(r.<String>getProperty("name") + "@" + r.<String>getProperty("typeName"));
      }
    }
    return result;
  }
}
