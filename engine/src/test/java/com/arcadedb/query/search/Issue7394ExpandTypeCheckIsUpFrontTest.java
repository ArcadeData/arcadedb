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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7394 item 5: with {@code expand}, a full-text index declared on a non-vertex type used to be
 * rejected only once the text query happened to match something.
 * <p>
 * The check sat behind {@code if (!fullTextLeg.rows().isEmpty())}, so the same request - the same index, the
 * same {@code expand} - reported success or failure depending on the corpus. A configuration error that
 * passes whenever the query matches nothing is a configuration error that ships.
 * <p>
 * Both type checks now run before the searches they used to follow: the vector index's type is checked
 * before the vector leg spends a search, and the full-text index's type as soon as the index resolves and
 * before the text search runs. A rejected request does no retrieval I/O at all.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
 */
class Issue7394ExpandTypeCheckIsUpFrontTest extends TestHelper {

  private static final String VERTEX_TYPE   = "Doc7394";
  private static final String DOCUMENT_TYPE = "Note7394";
  private static final String VECTOR_INDEX  = VERTEX_TYPE + "[embedding]";
  private static final String TEXT_INDEX    = DOCUMENT_TYPE + "[body]";

  /**
   * A vertex type carrying the vector index, and a plain document type carrying the full-text index - the
   * shape the issue describes. The corpus is seeded so that "zebra" matches nothing and "alpha" matches.
   */
  private void buildSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE " + VERTEX_TYPE);
      database.command("sql", "CREATE PROPERTY " + VERTEX_TYPE + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + VERTEX_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");

      database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);
      database.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".body STRING");
      database.command("sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (body) FULL_TEXT");

      database.newVertex(VERTEX_TYPE).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
      database.newVertex(VERTEX_TYPE).set("embedding", new float[] { 0.0f, 1.0f, 0.0f }).save();
      database.newDocument(DOCUMENT_TYPE).set("body", "alpha beta").save();
    });
  }

  private JSONObject argsWithExpand(final String fulltextQuery) {
    return new JSONObject()
        .put("vectorIndexName", VECTOR_INDEX)
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5)
        .put("fulltextIndexName", TEXT_INDEX)
        .put("fulltextQuery", fulltextQuery)
        .put("expand", new JSONObject().put("maxDepth", 1));
  }

  /** The regression: the text query matches nothing, so the invalid configuration used to pass. */
  @Test
  void aNonVertexFullTextIndexWithExpandIsRefusedEvenWhenTheTextQueryMatchesNothing() {
    buildSchema();

    assertThatThrownBy(() -> HybridSearch.search(database, argsWithExpand("zebra")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Graph expansion requires a vertex type")
        .hasMessageContaining(DOCUMENT_TYPE);
  }

  /** The case that was already caught, pinned so the fix does not trade one gate for the other. */
  @Test
  void aNonVertexFullTextIndexWithExpandIsStillRefusedWhenTheTextQueryMatches() {
    buildSchema();

    assertThatThrownBy(() -> HybridSearch.search(database, argsWithExpand("alpha")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Graph expansion requires a vertex type")
        .hasMessageContaining(DOCUMENT_TYPE);
  }

  /**
   * The vector index's own type is checked before the vector leg runs, not after. A document-typed vector
   * index with {@code expand} is refused without a search being spent on it.
   */
  @Test
  void aNonVertexVectorIndexWithExpandIsRefused() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + DOCUMENT_TYPE);
      database.command("sql", "CREATE PROPERTY " + DOCUMENT_TYPE + ".embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + DOCUMENT_TYPE + " (embedding) LSM_VECTOR "
          + "METADATA { dimensions: 3, similarity: 'COSINE' }");
      database.newDocument(DOCUMENT_TYPE).set("embedding", new float[] { 1.0f, 0.0f, 0.0f }).save();
    });

    final JSONObject args = new JSONObject()
        .put("vectorIndexName", DOCUMENT_TYPE + "[embedding]")
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5)
        .put("expand", new JSONObject().put("maxDepth", 1));

    assertThatThrownBy(() -> HybridSearch.search(database, args))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Graph expansion requires a vertex type")
        .hasMessageContaining(DOCUMENT_TYPE);
  }

  /**
   * Without {@code expand} the same document-typed full-text index is a perfectly ordinary request: the type
   * check must be conditional on the expansion leg, not on the index.
   */
  @Test
  void aNonVertexFullTextIndexWithoutExpandIsFine() {
    buildSchema();

    final JSONObject args = new JSONObject()
        .put("vectorIndexName", VECTOR_INDEX)
        .put("queryVector", new JSONArray(new Object[] { 1.0f, 0.0f, 0.0f }))
        .put("k", 5)
        .put("fulltextIndexName", TEXT_INDEX)
        .put("fulltextQuery", "alpha");

    final JSONObject result = HybridSearch.search(database, args);
    assertThat(result.getJSONObject("legs").has("fulltext")).isTrue();
  }
}
