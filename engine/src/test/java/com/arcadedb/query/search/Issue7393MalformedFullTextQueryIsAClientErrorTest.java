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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A full-text query the Lucene parser rejects is the caller's mistake, and every protocol surface maps an
 * {@link IllegalArgumentException} raised by the search services to a client error (HTTP 400, gRPC
 * INVALID_ARGUMENT). The full-text leg of {@link HybridSearch} was the one stage that let the parser's
 * {@code IndexException} through untouched, so an unbalanced quote or a dangling operator surfaced as an
 * "Internal error" with a stack trace in the server log; the standalone {@link FullTextQuery} surface had the
 * same gap (issue #7393).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7393MalformedFullTextQueryIsAClientErrorTest extends TestHelper {
  private static final String TYPE       = "Doc";
  private static final String VECTOR_IDX = "Doc[embedding]";
  private static final String TEXT_IDX   = "Doc[title]";
  private static final int    DIM        = 4;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("title", Type.STRING);
      type.createProperty("embedding", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex(TYPE, new String[] { "embedding" })
          .withLSMVectorType().withDimensions(DIM).withSimilarity("COSINE").create();
      database.command("sql", "CREATE INDEX ON " + TYPE + " (title) FULL_TEXT");

      for (int i = 0; i < 5; i++) {
        final MutableDocument doc = database.newDocument(TYPE);
        doc.set("title", "gearbox manual " + i);
        doc.set("embedding", new float[] { 1f, i * 0.1f, 0f, 0f });
        doc.save();
      }
    });
  }

  @Test
  void aMalformedFullTextQueryInAHybridSearchIsReportedAsAnInvalidArgument() {
    for (final String malformed : new String[] { "title:(foo AND", "\"unbalanced", "gearbox AND", "~", "^2" }) {
      assertThatThrownBy(() -> HybridSearch.search(database, hybridArgs(malformed)))
          .as("fulltextQuery <%s>", malformed)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("full-text leg")
          .hasMessageContaining(malformed);
    }
  }

  @Test
  void anExecutionTimeIndexFaultIsNotFiledAsAClientError() {
    // The parser's exception is the only one re-typed: a plain IndexException from the same call is a server fault.
    assertThat(new FullTextQueryParseException("x", null)).isInstanceOf(IndexException.class);
    assertThat(IndexException.class.isAssignableFrom(FullTextQueryParseException.class)).isTrue();
    assertThat(new IndexException("Error on tokenizer")).isNotInstanceOf(FullTextQueryParseException.class);
  }

  @Test
  void aWellFormedFullTextQueryStillRunsTheLeg() {
    final JSONObject result = HybridSearch.search(database, hybridArgs("gearbox"));
    assertThat(result.getJSONObject("legs").getJSONObject("fulltext").getInt("count")).isGreaterThan(0);
  }

  @Test
  void aMalformedQueryOnTheStandaloneFullTextSurfaceIsReportedAsAnInvalidArgument() {
    assertThatThrownBy(() -> FullTextQuery.search(database,
        new JSONObject().put("indexName", TEXT_IDX).put("queryText", "title:(foo AND")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("title:(foo AND");
  }

  private static JSONObject hybridArgs(final String fulltextQuery) {
    final JSONArray vector = new JSONArray();
    for (int i = 0; i < DIM; i++)
      vector.put(i == 0 ? 1f : 0f);
    return new JSONObject()
        .put("vectorIndexName", VECTOR_IDX)
        .put("queryVector", vector)
        .put("k", 3)
        .put("fulltextIndexName", TEXT_IDX)
        .put("fulltextQuery", fulltextQuery);
  }
}
