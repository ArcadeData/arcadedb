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
package com.arcadedb.query.search;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7312: Studio's vector search panel lists the searchable indexes from {@code schema:types}, and it can only
 * catch a query vector of the wrong length before the round trip if that listing carries the dimension count. The
 * scoring direction is listed alongside, so the panel can say "lower is better" before the first search is run.
 * <p>
 * Both values must be the ones the search itself reports, not a second derivation of them: each test compares the
 * listing with the {@code scoring} field of a real search response over the same index.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7312">issue #7312</a>
 */
class Issue7312SchemaTypesDescribesVectorIndexesTest extends TestHelper {

  @Test
  void aDenseIndexIsListedWithItsDimensionsAndTheScoringTheSearchReports() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc7312");
      database.command("sql", "CREATE PROPERTY Doc7312.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Doc7312 (embedding) LSM_VECTOR METADATA { dimensions: 3, similarity: 'EUCLIDEAN' }");
      database.newVertex("Doc7312").set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
    });

    final Result listed = indexEntry("Doc7312", "Doc7312[embedding]");
    assertThat(listed.<Object>getProperty("dimensions")).isEqualTo(3);

    final JSONObject response = VectorSearch.search(database, new JSONObject()
        .put("indexName", "Doc7312[embedding]")
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 1));
    assertThat(listed.<String>getProperty("scoring"))
        .isEqualTo(response.getString("scoring"))
        .isEqualTo("distance_lower_is_better:EUCLIDEAN");
  }

  @Test
  void aSparseIndexIsListedWithItsDimensionsAndTheScoringTheSearchReports() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Sparse7312");
      database.command("sql", "CREATE PROPERTY Sparse7312.dims ARRAY_OF_INTEGERS");
      database.command("sql", "CREATE PROPERTY Sparse7312.weights ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Sparse7312 (dims, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 128, modifier: 'IDF' }");
      database.command("sql", "INSERT INTO Sparse7312 SET dims = [1, 5], weights = [0.5, 0.25]");
    });

    final Result listed = indexEntry("Sparse7312", "Sparse7312[dims,weights]");
    assertThat(listed.<Object>getProperty("dimensions")).isEqualTo(128);

    final JSONObject response = VectorSearch.search(database, new JSONObject()
        .put("indexName", "Sparse7312[dims,weights]")
        .put("sparse", true)
        .put("queryIndices", new JSONArray(new Object[] { 1 }))
        .put("queryVector", new JSONArray(new Object[] { 1.0f }))
        .put("k", 1));
    assertThat(listed.<String>getProperty("scoring"))
        .isEqualTo(response.getString("scoring"))
        .isEqualTo("score_higher_is_better:idf_weighted_dot_product");
  }

  /** Only a vector index has a dimension count or a scoring direction; every other index keeps its old shape. */
  @Test
  void aNonVectorIndexCarriesNeitherField() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Plain7312");
      database.command("sql", "CREATE PROPERTY Plain7312.name STRING");
      database.command("sql", "CREATE PROPERTY Plain7312.body STRING");
      database.command("sql", "CREATE INDEX ON Plain7312 (name) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Plain7312 (body) FULL_TEXT");
    });

    for (final String indexName : List.of("Plain7312[name]", "Plain7312[body]")) {
      final Result listed = indexEntry("Plain7312", indexName);
      assertThat(listed.hasProperty("dimensions")).as(indexName).isFalse();
      assertThat(listed.hasProperty("scoring")).as(indexName).isFalse();
    }
  }

  private Result indexEntry(final String typeName, final String indexName) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = ?", typeName)) {
      assertThat(rs.hasNext()).as("type " + typeName + " listed").isTrue();
      final List<Result> indexes = rs.next().getProperty("indexes");
      for (final Result index : indexes)
        if (indexName.equals(index.getProperty("name")))
          return index;
    }
    throw new AssertionError("index " + indexName + " not listed under " + typeName);
  }
}
