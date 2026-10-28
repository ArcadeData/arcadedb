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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10898: every row of {@code vector.neighbors}, {@code vector.fuse} and {@code vector.rerank} held the record's properties
 * flattened AND the whole record again under a {@code record} key, so {@code SELECT expand(...)} and the JSON of the rows carried
 * every property twice - embeddings and passages, the largest ones, included. The key is no longer enumerated: a row lists the
 * properties, {@code @rid}, {@code @type} and the score once, and whoever asks for {@code record} still gets the record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10898VectorRowRecordKeyTest extends TestHelper {
  private static final float[] QUERY = { 1.0f, 0.0f, 0.0f };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.content STRING");
    database.command("sql", "CREATE PROPERTY Doc.dense ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT");
    database.command("sql", "CREATE INDEX ON Doc (dense) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }");
    database.transaction(() -> {
      final Object[][] docs = { { "apple", new float[] { 1.0f, 0.0f, 0.0f } }, { "apple pie", new float[] { 0.9f, 0.1f, 0.0f } },
          { "banana", new float[] { 0.0f, 1.0f, 0.0f } }, { "cherry", new float[] { 0.0f, 0.0f, 1.0f } } };
      for (final Object[] doc : docs)
        database.newDocument("Doc").set("content", doc[0], "dense", doc[1]).save();
    });
  }

  @Test
  void anExpandedNeighborsRowCarriesEachPropertyOnce() {
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.neighbors`('Doc[dense]', ?, 3))", (Object) QUERY)) {
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("content", "dense", "@rid", "@type", "distance");
        assertNoRecordKey(row);
        assertCarriesTheRecord(row);
        rows++;
      }
      assertThat(rows).isEqualTo(3);
    }
  }

  /** The report's query, verbatim but for the parameter: neighbours and a full-text search fused, then re-ranked. */
  @Test
  void theReportedFuseRerankChainCarriesEachPropertyOnce() {
    try (final ResultSet rs = database.query("sql", """
        SELECT expand(`vector.rerank`(
          `vector.fuse`(`vector.neighbors`('Doc[dense]', :denseVec, 3),
                        (SELECT @rid, $score FROM Doc WHERE SEARCH_INDEX('Doc[content]', 'apple') = true LIMIT 3),
                        { limit: 3 }),
          :denseVec, 'dense', 2))""", Map.of("denseVec", QUERY))) {
      final List<String> contents = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("content", "dense", "@rid", "@type", "score");
        assertNoRecordKey(row);
        assertCarriesTheRecord(row);
        contents.add(row.getProperty("content"));
      }
      assertThat(contents).containsExactly("apple", "apple pie");
    }
  }

  @Test
  void theRecordIsStillReachableByPathAndByProjection() {
    try (final ResultSet rs = database.query("sql",
        "SELECT record.content AS c, record AS r, content FROM (SELECT expand(`vector.neighbors`('Doc[dense]', ?, 1)))",
        (Object) QUERY)) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("c")).isEqualTo("apple");
      assertThat(row.<String>getProperty("content")).isEqualTo("apple");
      assertThat((Object) row.getProperty("r")).isInstanceOf(Document.class);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aNeighborsListThatIsNotExpandedCarriesEachPropertyOnceToo() {
    try (final ResultSet rs = database.query("sql", "SELECT `vector.neighbors`('Doc[dense]', ?, 2) AS n", (Object) QUERY)) {
      final Result row = rs.next();
      final List<?> neighbours = row.getProperty("n");
      assertThat(neighbours).hasSize(2);
      for (final Object neighbour : neighbours) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> map = (Map<String, Object>) neighbour;
        assertThat(map.keySet()).containsExactlyInAnyOrder("content", "dense", "@rid", "@type", "distance");
        assertThat(map.get("record")).isInstanceOf(Document.class);
      }
      assertThat(new JsonSerializer(database).serializeResult(database, row).toString()).doesNotContain("\"record\"");
    }
  }

  @Test
  void theRowKeepsTheMapContractOnTheRecordKey() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").set("content", "plum", "dense", new float[] { 0.5f, 0.5f, 0.0f });
      doc.save();

      final ScoredRecordRow row = new ScoredRecordRow(doc, "score", 0.5f);
      assertThat(row.containsKey("record")).isTrue();
      assertThat(row.get("record")).isSameAs(doc);
      assertThat(row.keySet()).doesNotContain("record");
      // still answered once the row has been enumerated
      assertThat(row.get("record")).isSameAs(doc);
      assertThat(row.containsKey("record")).isTrue();

      // a write under the key is a real entry, and a removal takes it away
      final ScoredRecordRow written = new ScoredRecordRow(doc, "score", 0.5f);
      written.put("record", "mine");
      assertThat(written.keySet()).contains("record");
      assertThat(written.get("record")).isEqualTo("mine");

      final ScoredRecordRow removed = new ScoredRecordRow(doc, "score", 0.5f);
      assertThat(removed.remove("record")).isSameAs(doc);
      assertThat(removed.containsKey("record")).isFalse();
      assertThat(removed.get("record")).isNull();
      assertThat(removed.getRecord()).isSameAs(doc);

      // the score override of a row (vector.boost, vector.discover) answers the key once it has been copied as well
      final ScoreOverrideRow overridden = new ScoreOverrideRow(new ScoredRecordRow(doc, "distance", 0.1f), "score", 2.0f);
      assertThat(overridden.keySet()).doesNotContain("record");
      assertThat(overridden.get("record")).isSameAs(doc);
      assertThat(overridden.containsKey("record")).isTrue();
      assertThat(overridden.remove("record")).isSameAs(doc);
      assertThat(overridden.get("record")).isNull();

      // a real property named `record`, once removed, does not give way to the record behind it
      final MutableDocument named = database.newDocument("Doc").set("content", "fig", "record", "a property");
      named.save();
      final ScoredRecordRow withProperty = new ScoredRecordRow(named, "score", 0.5f);
      assertThat(withProperty.remove("record")).isEqualTo("a property");
      assertThat(withProperty.get("record")).isNull();
      assertThat(withProperty.containsKey("record")).isFalse();

      final ScoreOverrideRow overriddenWithProperty = new ScoreOverrideRow(new ScoredRecordRow(named, "distance", 0.1f), "score",
          2.0f);
      assertThat(overriddenWithProperty.remove("record")).isEqualTo("a property");
      assertThat(overriddenWithProperty.get("record")).isNull();
      assertThat(overriddenWithProperty.containsKey("record")).isFalse();
    });
  }

  private static void assertNoRecordKey(final Result row) {
    final JSONObject json = row.toJSON();
    assertThat(json.has("record")).isFalse();
    assertThat(json.toString()).doesNotContain("\"record\"");
  }

  private static void assertCarriesTheRecord(final Result row) {
    final Object record = row.getProperty("record");
    assertThat(record).isInstanceOf(Document.class);
    assertThat(((Document) record).getIdentity()).isEqualTo(row.<RID>getProperty("@rid"));
  }
}
