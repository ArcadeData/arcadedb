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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the vector SQL functions, one nested class per fixed issue.
 */
class VectorRecordLoadTest {
  /**
   * Issue #10129: a hybrid query {@code vector.rerank(vector.fuse(...), ...)} loaded every fused candidate twice (once in
   * {@code fuse}, again in {@code rerank}), copied every property of every record into a new map in both functions, and
   * sorted every candidate in {@code rerank} to keep only the top {@code k}.
   * <p>
   * Record loads are counted through the database's {@code readRecord} statistic, which {@code lookupByRID} increments on
   * every call.
   */
  @Nested
  class FuseRerankRecordLoadTest extends TestHelper {
    private static final String TYPE = "Passage10129";

    private List<RID> seed(final int count) {
      final List<RID> rids = new ArrayList<>(count);
      database.transaction(() -> {
        final DocumentType t = database.getSchema().createDocumentType(TYPE);
        t.createProperty("name", Type.STRING);
        t.createProperty("text", Type.STRING);
        t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql", "CREATE INDEX ON " + TYPE + " (text) FULL_TEXT");
        for (int i = 0; i < count; i++) {
          final MutableDocument d = database.newDocument(TYPE);
          d.set("name", "p" + i);
          d.set("text", (i % 2 == 0 ? "alpha beta" : "alpha gamma") + " passage " + i);
          // Only 4 distinct directions, so many candidates tie on the rerank score.
          d.set("embedding", new float[] { 1.0f, (i % 4) * 0.25f, 0.1f });
          d.set("score", -1); // a record property named like the fused score: the fused score must win
          d.save();
          rids.add(d.getIdentity());
        }
      });
      return rids;
    }

    private static List<Map<String, Object>> ridRows(final List<RID> rids, final boolean reversed) {
      final List<Map<String, Object>> rows = new ArrayList<>(rids.size());
      for (int i = 0; i < rids.size(); i++) {
        final RID rid = rids.get(reversed ? rids.size() - 1 - i : i);
        final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("@rid", rid);
        row.put("score", (float) (rids.size() - i));
        rows.add(row);
      }
      return rows;
    }

    private long readRecords() {
      return ((Number) database.getStats().get("readRecord")).longValue();
    }

    @Test
    void rerankOverFuseLoadsEachFusedCandidateOnce() {
      final List<RID> rids = seed(30);

      final long before = readRecords();
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", """
          SELECT name, score FROM (SELECT expand(`vector.rerank`(
            (SELECT expand(`vector.fuse`(?, ?))), ?, 'embedding', 5)))""",
          ridRows(rids, false), ridRows(rids, true), new float[] { 1.0f, 0.0f, 0.1f })) {
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
      }
      final long loads = readRecords() - before;

      assertThat(names).hasSize(5);
      // fuse loads each of the 30 fused candidates once; rerank reuses the record each fused row carries
      assertThat(loads).isEqualTo(rids.size());
    }

    @Test
    void rerankReusesTheRecordOfMapAndResultRows() {
      final List<RID> rids = seed(10);

      final List<Object> rows = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < rids.size(); i++) {
          final Document doc = (Document) database.lookupByRID(rids.get(i), true);
          if (i % 2 == 0) {
            final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            row.put("@rid", doc.getIdentity());
            row.put("record", doc);
            rows.add(row);
          } else
            rows.add(doc); // a bare record is its own loaded record
        }
      });
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE)) {
        while (rs.hasNext())
          rows.add(rs.next()); // a Result wrapping the loaded element
      }

      final long before = readRecords();
      @SuppressWarnings("unchecked")
      final List<Object> out = (List<Object>) new SQLFunctionVectorRerank().execute(null, null, null,
          new Object[] { rows, new float[] { 1.0f, 0.0f, 0.1f }, "embedding", 3 }, newContext());
      assertThat(readRecords() - before).isZero();
      assertThat(out).hasSize(3);
    }

    @Test
    void rerankReusesTheRecordOfAProjectionRow() {
      final List<RID> rids = seed(6);

      final List<Object> rows = new ArrayList<>();
      database.transaction(() -> {
        for (final RID rid : rids) {
          // a projection row (not an element) that carries the record under `record`
          final LinkedHashMap<String, Object> content = new LinkedHashMap<>();
          content.put("@rid", rid);
          content.put("record", database.lookupByRID(rid, true));
          final Result row = new ResultInternal(content);
          assertThat(row.isElement()).isFalse();
          rows.add(row);
        }
      });

      final long before = readRecords();
      @SuppressWarnings("unchecked")
      final List<Object> out = (List<Object>) new SQLFunctionVectorRerank().execute(null, null, null,
          new Object[] { rows, new float[] { 1.0f, 0.0f, 0.1f }, "embedding", 3 }, newContext());
      assertThat(readRecords() - before).isZero();
      assertThat(out).hasSize(3);
    }

    @Test
    void lazyRowMatchesTheEagerCopyForARecordPropertyAndANullValue() {
      seed(0);
      database.transaction(() -> {
        final MutableDocument d = database.newDocument(TYPE);
        d.set("name", "shadowed");
        d.set("record", "a property named like the record key");
        d.set("nullable", null);
        d.save();

        // the eager copy, less the `record` key that only answers when asked for (issue #10898)
        final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
        for (final String prop : d.getPropertyNames())
          eager.put(prop, d.get(prop));
        eager.put("@rid", d.getIdentity());
        eager.put("@type", d.getTypeName());
        eager.put("score", 0.5f);

        final ScoredRecordRow lazy = new ScoredRecordRow(d, "score", 0.5f);
        // a record property named `record` is a real entry and wins over the record, before and after the row is enumerated
        assertThat(lazy.get("record")).isEqualTo("a property named like the record key");
        assertThat(lazy.getRecord()).isSameAs(d);
        for (final String key : List.of("record", "name", "nullable", "missing"))
          assertThat(lazy.containsKey(key)).as(key).isEqualTo(eager.containsKey(key));
        for (final String key : eager.keySet())
          assertThat(lazy.get(key)).as(key).isEqualTo(eager.get(key));
        assertThat(lazy.keySet()).containsExactlyElementsOf(eager.keySet());
        assertThat(lazy.get("record")).isEqualTo("a property named like the record key");
      });
    }

    @Test
    void rerankSkipsACandidateWhoseRidIsNotADocument() {
      final List<RID> rids = seed(3);

      // An edge segment is a record but not a document: a candidate RID pointing at one is skipped, like a deleted record
      final RID[] segment = new RID[1];
      database.transaction(() -> {
        database.getSchema().createVertexType("V10129");
        database.getSchema().createEdgeType("E10129");
        final MutableVertex a = database.newVertex("V10129").save();
        final MutableVertex b = database.newVertex("V10129").save();
        a.newEdge("E10129", b);
        segment[0] = ((VertexInternal) a.asVertex(true)).getOutEdgesHeadChunk();
      });
      assertThat(segment[0]).isNotNull();

      final List<Object> rows = new ArrayList<>(ridRows(rids, false));
      final LinkedHashMap<String, Object> notADocument = new LinkedHashMap<>();
      notADocument.put("@rid", segment[0]);
      rows.add(notADocument);

      @SuppressWarnings("unchecked")
      final List<Object> out = (List<Object>) new SQLFunctionVectorRerank().execute(null, null, null,
          new Object[] { rows, new float[] { 1.0f, 0.0f, 0.1f }, "embedding", 10 }, newContext());
      assertThat(out).hasSize(3);
    }

    @Test
    void rerankBoundedTopKKeepsTheFullSortOrderIncludingTies() {
      final List<RID> rids = seed(40);
      final float[] query = { 1.0f, 0.3f, 0.1f };

      // Reference: the old behaviour, a stable full sort by descending similarity in candidate order.
      final List<Map<String, Object>> candidates = ridRows(rids, true);
      final List<Object[]> expected = new ArrayList<>();
      database.transaction(() -> {
        for (final Map<String, Object> row : candidates) {
          final Document doc = (Document) database.lookupByRID((RID) row.get("@rid"), true);
          expected.add(new Object[] { doc.getString("name"),
              VectorUtils.cosineSimilarity(query, (float[]) doc.get("embedding")) });
        }
      });
      expected.sort(Comparator.comparing((Object[] e) -> (Float) e[1]).reversed());

      for (final int k : new int[] { 1, 7, 10, 11, 40, 100 }) {
        final List<String> names = new ArrayList<>();
        final List<Float> scores = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT name, score FROM (SELECT expand(`vector.rerank`(?, ?, 'embedding', ?)))",
            candidates, query, k)) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            names.add(r.getProperty("name"));
            scores.add(r.getProperty("score"));
          }
        }
        final int n = Math.min(k, expected.size());
        final List<String> expectedNames = new ArrayList<>();
        final List<Float> expectedScores = new ArrayList<>();
        for (int i = 0; i < n; i++) {
          expectedNames.add((String) expected.get(i)[0]);
          expectedScores.add((Float) expected.get(i)[1]);
        }
        assertThat(names).as("k=%d", k).containsExactlyElementsOf(expectedNames);
        assertThat(scores).as("k=%d", k).containsExactlyElementsOf(expectedScores);
      }
    }

    @Test
    void fusedRowsStillExposeEveryRecordPropertyAfterExpand() {
      final List<RID> rids = seed(4);

      try (final ResultSet rs = database.query("sql", "SELECT FROM (SELECT expand(`vector.fuse`(?, ?)))", ridRows(rids, false),
          ridRows(rids, true))) {
        int count = 0;
        while (rs.hasNext()) {
          final Result r = rs.next();
          // the record is reachable under `record` but not enumerated: its properties are the row's already (issue #10898)
          assertThat(r.getPropertyNames()).containsExactly("name", "text", "embedding", "score", "@rid", "@type");
          assertThat((String) r.getProperty("name")).startsWith("p");
          assertThat((String) r.getProperty("text")).contains("passage");
          assertThat((Object) r.getProperty("embedding")).isNotNull();
          assertThat((Object) r.getProperty("record")).isInstanceOf(Document.class);
          assertThat((String) r.getProperty("@type")).isEqualTo(TYPE);
          assertThat(rids).contains((RID) r.getProperty("@rid"));
          // the fused score overrides the record's own "score" property, as it always did
          assertThat(((Number) r.getProperty("score")).floatValue()).isPositive();
          count++;
        }
        assertThat(count).isEqualTo(4);
      }

      try (final ResultSet rs = database.query("sql", "SELECT name, score FROM (SELECT expand(`vector.fuse`(?, ?)))",
          ridRows(rids, false), ridRows(rids, true))) {
        assertThat(rs.stream().map(r -> (String) r.getProperty("name")).toList()).containsExactlyInAnyOrder("p0", "p1", "p2", "p3");
      }
    }

    @Test
    void lazyRowEqualsTheEagerCopyItReplaces() {
      final List<RID> rids = seed(1);
      database.transaction(() -> {
        final Document doc = (Document) database.lookupByRID(rids.getFirst(), true);

        // the eager copy, less the `record` key that only answers when asked for (issue #10898)
        final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
        for (final String prop : doc.getPropertyNames())
          eager.put(prop, doc.get(prop));
        eager.put("@rid", doc.getIdentity());
        eager.put("@type", doc.getTypeName());
        eager.put("score", 0.5f);

        final Map<String, Object> lazy = new ScoredRecordRow(doc, "score", 0.5f);
        assertThat(lazy.isEmpty()).isFalse();
        assertThat(lazy.get("name")).isEqualTo("p0");
        assertThat(lazy.get("score")).isEqualTo(0.5f);
        assertThat(lazy.get("@rid")).isEqualTo(doc.getIdentity());
        assertThat(lazy.get("@type")).isEqualTo(TYPE);
        assertThat(lazy.get("record")).isSameAs(doc);
        assertThat(lazy.get("missing")).isNull();
        assertThat(lazy.containsKey("text")).isTrue();
        assertThat(lazy.containsKey("missing")).isFalse();
        // compared value by value: an array property decodes to a new array on every read, so Map.equals cannot be used
        for (final String key : eager.keySet())
          assertThat(lazy.get(key)).as(key).isEqualTo(eager.get(key));
        assertThat(lazy.keySet()).containsExactlyElementsOf(eager.keySet());
        assertThat(lazy.size()).isEqualTo(eager.size());

        // a write lands on the row, never on the record
        final Map<String, Object> written = new ScoredRecordRow(doc, "distance", 0.25f);
        written.put("name", "changed");
        written.remove("text");
        assertThat(written.get("name")).isEqualTo("changed");
        assertThat(written.containsKey("text")).isFalse();
        assertThat(written.get("distance")).isEqualTo(0.25f);
        assertThat(doc.getString("name")).isEqualTo("p0");
      });
    }

    @Test
    void searchIndexPassedDirectlyToFuseCarriesItsScores() {
      final List<RID> rids = seed(10);

      // LINEAR needs a score on every row: a direct SEARCH_INDEX source used to read NaN and be refused
      final List<String> direct = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", """
          SELECT name, score FROM (SELECT expand(`vector.fuse`(SEARCH_INDEX('%s[text]', 'beta'), ?, { fusion: 'LINEAR' })))"""
          .formatted(TYPE), ridRows(rids, false))) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          direct.add(r.getProperty("name") + "=" + r.getProperty("score"));
        }
      }

      final List<String> subquery = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", """
          SELECT name, score FROM (SELECT expand(`vector.fuse`((SELECT @rid, $score FROM %s WHERE SEARCH_INDEX('%s[text]', 'beta') = true),
            ?, { fusion: 'LINEAR' })))""".formatted(TYPE, TYPE), ridRows(rids, false))) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          subquery.add(r.getProperty("name") + "=" + r.getProperty("score"));
        }
      }

      assertThat(direct).isNotEmpty().containsExactlyInAnyOrderElementsOf(subquery);
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);
      return ctx;
    }
  }

  /**
   * Issue #10170: the follow-up of #10129 for the other reranker functions. {@code vector.mmr} and {@code vector.fuse} loaded the
   * record of every candidate even when the row already carried it, {@code vector.boost} and {@code vector.discover} copied every
   * property of every row only to override its score.
   * <p>
   * Record loads are counted through the database's {@code readRecord} statistic, which {@code lookupByRID} increments on every
   * call.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorRerankersCarriedRecordTest extends TestHelper {
    private static final String TYPE = "Passage10170";

    private List<RID> seed(final int count) {
      final List<RID> rids = new ArrayList<>(count);
      database.transaction(() -> {
        final DocumentType t = database.getSchema().createDocumentType(TYPE);
        t.createProperty("name", Type.STRING);
        t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        t.createProperty("popularity", Type.INTEGER);
        for (int i = 0; i < count; i++) {
          final MutableDocument d = database.newDocument(TYPE);
          d.set("name", "p" + i);
          d.set("embedding", new float[] { 1.0f, (i % 4) * 0.25f, 0.1f * i });
          d.set("popularity", i);
          d.save();
          rids.add(d.getIdentity());
        }
      });
      return rids;
    }

    /** Rows of the shape `vector.neighbors` and `vector.fuse` emit, each carrying its loaded record. */
    private List<Object> carriedRows(final List<RID> rids, final boolean reversed) {
      final List<Object> rows = new ArrayList<>(rids.size());
      database.transaction(() -> {
        for (int i = 0; i < rids.size(); i++) {
          final RID rid = rids.get(reversed ? rids.size() - 1 - i : i);
          rows.add(new ScoredRecordRow((Document) database.lookupByRID(rid, true), "score", rids.size() - i));
        }
      });
      return rows;
    }

    private static List<Object> ridRows(final List<RID> rids) {
      final List<Object> rows = new ArrayList<>(rids.size());
      for (int i = 0; i < rids.size(); i++) {
        final LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        row.put("@rid", rids.get(i));
        row.put("score", (float) (rids.size() - i));
        rows.add(row);
      }
      return rows;
    }

    private long readRecords() {
      return ((Number) database.getStats().get("readRecord")).longValue();
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);
      return ctx;
    }

    @SuppressWarnings("unchecked")
    private List<Object> mmr(final List<Object> rows) {
      return (List<Object>) new SQLFunctionVectorMmr().execute(null, null, null,
          new Object[] { rows, "embedding", Map.of("lambda", 0.5, "k", 5) }, newContext());
    }

    @Test
    void mmrReusesTheRecordEachRowCarries() {
      final List<RID> rids = seed(20);
      final List<Object> rows = carriedRows(rids, false);

      final long before = readRecords();
      final List<Object> out = mmr(rows);

      assertThat(readRecords() - before).isZero();
      assertThat(out).hasSize(5);
    }

    @Test
    void mmrStillLoadsARowThatCarriesOnlyARid() {
      final List<RID> rids = seed(20);

      final long before = readRecords();
      final List<Object> out = mmr(ridRows(rids));

      assertThat(readRecords() - before).isEqualTo(rids.size());
      assertThat(out).hasSize(5);
    }

    @Test
    void fuseReusesTheRecordTheSourceRowsCarry() {
      final List<RID> rids = seed(20);
      final List<Object> first = carriedRows(rids, false);
      final List<Object> second = carriedRows(rids, true);

      final long before = readRecords();
      @SuppressWarnings("unchecked")
      final List<Object> out = (List<Object>) new SQLFunctionVectorFuse().execute(null, null, null,
          new Object[] { first, second, Map.of("limit", 7) }, newContext());

      assertThat(readRecords() - before).isZero();
      assertThat(out).hasSize(7);
      for (final Object row : out)
        assertThat(((Map<?, ?>) row).get("record")).isInstanceOf(Document.class);
    }

    @Test
    void boostOverridesTheScoreWithoutLosingTheCarriedRecord() {
      final List<RID> rids = seed(20);
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT *, 1.0 AS score FROM " + TYPE)) {
        while (rs.hasNext())
          rows.add(rs.next()); // a Result wrapping the loaded element
      }

      @SuppressWarnings("unchecked")
      final List<Object> boosted = (List<Object>) new SQLFunctionVectorBoost().execute(null, null, null,
          new Object[] { rows, Map.of("boosts", List.of(Map.of("field", "popularity", "weight", 1.0))) }, newContext());
      assertThat(boosted).hasSize(rids.size());

      final Map<?, ?> top = (Map<?, ?>) boosted.getFirst();
      assertThat(top).isInstanceOf(ScoreOverrideRow.class);
      assertThat(top.get("score")).isEqualTo(20.0f);
      assertThat(top.get("name")).isEqualTo("p19");
      assertThat(top.get("@rid")).isEqualTo(rids.get(19));

      // A reranker downstream of the boost still finds the record the boosted row carried
      final long before = readRecords();
      assertThat(mmr(boosted)).hasSize(5);
      assertThat(readRecords() - before).isZero();
    }

    @Test
    void theScoreOverrideAnswersWhatTheEagerCopyAnswered() {
      final List<RID> rids = seed(3);
      final ScoredRecordRow upstream = (ScoredRecordRow) carriedRows(rids, false).get(1);
      final ScoreOverrideRow overridden = new ScoreOverrideRow(upstream, "score", 42.0f);

      final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
      for (final Map.Entry<String, Object> e : upstream.entrySet())
        eager.put(e.getKey(), e.getValue());
      eager.put("score", 42.0f);

      assertThat(overridden.get("name")).isEqualTo("p1");
      assertThat(overridden.containsKey("embedding")).isTrue();
      assertThat(overridden.containsKey("missing")).isFalse();
      assertThat(overridden).isEqualTo(eager);
      assertThat(new ArrayList<>(overridden.keySet())).containsExactlyElementsOf(eager.keySet());

      // A write lands on the override, never on the row it re-scores
      overridden.put("name", "changed");
      assertThat(overridden.get("name")).isEqualTo("changed");
      assertThat(upstream.get("name")).isEqualTo("p1");
    }

    @Test
    void theScoreOverrideOfAPlainMapIsNotChangedByItsHolder() {
      final LinkedHashMap<String, Object> upstream = new LinkedHashMap<>();
      upstream.put("@rid", new RID(1, 1));
      upstream.put("name", "p1");
      final ScoreOverrideRow overridden = new ScoreOverrideRow(upstream, "score", 2.0f);

      upstream.put("@rid", new RID(1, 2));
      upstream.put("name", "changed");

      assertThat(overridden.get("@rid")).isEqualTo(new RID(1, 1));
      assertThat(overridden.get("name")).isEqualTo("p1");
      assertThat(overridden.get("score")).isEqualTo(2.0f);
    }

    @Test
    void theScoreOverrideOfAResultExposesItsIdentity() {
      final List<RID> rids = seed(2);
      final Result result;
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " WHERE name = 'p1'")) {
        result = rs.next();
      }
      assertThat(result.getIdentity()).contains(rids.get(1));
      final ScoreOverrideRow overridden = new ScoreOverrideRow(result, "score", 3.0f);

      final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
      for (final String prop : result.getPropertyNames())
        eager.put(prop, result.getProperty(prop));
      result.getIdentity().ifPresent(identity -> eager.put("@rid", identity));
      eager.put("score", 3.0f);

      assertThat(overridden.get("name")).isEqualTo("p1");
      assertThat(overridden.get("score")).isEqualTo(3.0f);
      assertThat(overridden.get("@rid")).isEqualTo(rids.get(1));
      assertThat(overridden.get("popularity")).isEqualTo(1);
      // the embedding array is read anew each time, so the entries are compared by key and the scalars by value
      assertThat(new ArrayList<>(overridden.keySet())).containsExactlyElementsOf(eager.keySet());
      assertThat(overridden.get("embedding")).isEqualTo(eager.get("embedding"));
    }

    @Test
    void discoverEmitsLazyRowsCarryingTheRecord() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc10170");
        database.command("sql", "CREATE PROPERTY Doc10170.name STRING");
        database.command("sql", "CREATE PROPERTY Doc10170.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc10170 (name) UNIQUE");
        database.command("sql", """
            CREATE INDEX ON Doc10170 (embedding) LSM_VECTOR
            METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }""");
      });
      final Map<String, RID> rid = new LinkedHashMap<>();
      database.transaction(() -> {
        final float[][] vectors = { { 1.0f, 0.0f, 0.0f }, { 0.9f, 0.1f, 0.0f }, { 0.0f, 1.0f, 0.0f }, { 0.1f, 0.9f, 0.0f },
            { 0.0f, 0.0f, 1.0f }, { 0.0f, 0.1f, 0.9f } };
        for (int i = 0; i < vectors.length; i++) {
          final String name = String.valueOf((char) ('A' + i));
          rid.put(name, database.newVertex("Doc10170").set("name", name).set("embedding", vectors[i]).save().getIdentity());
        }
      });

      @SuppressWarnings("unchecked")
      final List<Object> out = (List<Object>) new SQLFunctionVectorDiscover().execute(null, null, null,
          new Object[] { "Doc10170[embedding]", List.of(List.of(rid.get("A"), rid.get("C"))), 4 }, newContext());

      assertThat(out).isNotEmpty();
      for (final Object row : out) {
        assertThat(row).isInstanceOf(ScoreOverrideRow.class);
        final Map<?, ?> map = (Map<?, ?>) row;
        assertThat(map.get("score")).isInstanceOf(Float.class);
        assertThat(map.get("record")).isInstanceOf(Document.class);
        // the upstream distance is still there, as it was in the copy
        assertThat(map.containsKey("distance")).isTrue();
      }
      assertThat(((Map<?, ?>) out.getFirst()).get("name")).isEqualTo("B");
    }
  }

  /**
   * Issue #10217: since #10181 / #10185 {@code vector.rerank}, {@code vector.mmr} and {@code vector.fuse} reuse the record a
   * candidate row carries instead of loading it. A row held in a variable before a write of the same transaction holds the record
   * as it was: a record deleted since came back in the result, and an updated one with its old values.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeldRowsAfterWriteTest extends TestHelper {
    private static final String DELETE = "DELETE FROM Doc WHERE name = 'a'";
    private static final String UPDATE = "UPDATE Doc SET name = 'a2' WHERE name = 'a'";

    private static final String MMR    = "vector.mmr($h, 'emb', {lambda: 0.5, k: 5})";
    private static final String FUSE   = "vector.fuse($h, $h, {fusion: 'RRF', limit: 5})";
    private static final String RERANK = "vector.rerank($h, [1.0, 0.0, 0.0], 'emb', 5)";
    private static final String BOOST  = "vector.mmr(vector.boost($h, {boosts: [{field: 'score', weight: 0}]}), 'emb', {lambda: 0.5, k: 5})";

    @Override
    public void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.emb ARRAY_OF_FLOATS");
      database.command("sql", "CREATE PROPERTY Doc.score FLOAT");
    }

    private void reset() {
      database.transaction(() -> {
        database.command("sql", "DELETE FROM Doc");
        database.command("sql", "INSERT INTO Doc SET name = 'a', score = 1.0, emb = [1.0, 0.0, 0.0]");
        database.command("sql", "INSERT INTO Doc SET name = 'b', score = 0.9, emb = [0.9, 0.1, 0.0]");
        database.command("sql", "INSERT INTO Doc SET name = 'c', score = 0.1, emb = [0.0, 1.0, 0.0]");
      });
    }

    /** Holds the rows of Doc in $h (as {@code held} produces them), makes one write, then passes $h to {@code call}. */
    private List<String> run(final String held, final String write, final String call) {
      reset();
      final String script = "BEGIN;\nLET $h = " + held + ";\n" + write + ";\nLET $r = SELECT expand(" + call + ");\nCOMMIT;\nRETURN $r;";
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.command("sqlscript", script)) {
        rs.stream().forEach(r -> names.add(r.<String>getProperty("name")));
      }
      names.sort(null);
      return names;
    }

    private List<String> run(final String write, final String call) {
      return run("SELECT FROM Doc", write, call);
    }

    @Test
    void aRecordDeletedAfterTheRowsWereHeldIsNotReturned() {
      assertThat(run(DELETE, MMR)).containsExactly("b", "c");
      assertThat(run(DELETE, FUSE)).containsExactly("b", "c");
      assertThat(run(DELETE, RERANK)).containsExactly("b", "c");
      assertThat(run(DELETE, BOOST)).containsExactly("b", "c");
    }

    @Test
    void aRecordUpdatedAfterTheRowsWereHeldIsReturnedWithItsNewValues() {
      assertThat(run(UPDATE, MMR)).containsExactly("a2", "b", "c");
      assertThat(run(UPDATE, FUSE)).containsExactly("a2", "b", "c");
      assertThat(run(UPDATE, RERANK)).containsExactly("a2", "b", "c");
      assertThat(run(UPDATE, BOOST)).containsExactly("a2", "b", "c");
    }

    /** The output of a vector function held in a variable carries its records too, so it is checked the same way. */
    @Test
    void theHeldOutputOfAVectorFunctionIsCheckedToo() {
      final String fused = "SELECT expand(vector.fuse((SELECT FROM Doc), (SELECT FROM Doc), {fusion: 'RRF', limit: 5}))";
      assertThat(run(fused, DELETE, MMR)).containsExactly("b", "c");
      assertThat(run(fused, UPDATE, RERANK)).containsExactly("a2", "b", "c");
    }

    /** An updated embedding is scored with its new value: {@code a} moved away from the query vector and ranks last. */
    @Test
    void theScoreIsComputedOnTheNewValues() {
      reset();
      final String script = "BEGIN;\nLET $h = SELECT FROM Doc;\nUPDATE Doc SET emb = [-1.0, 0.0, 0.0] WHERE name = 'a';\n"
          + "LET $r = SELECT expand(vector.rerank($h, [1.0, 0.0, 0.0], 'emb', 5));\nCOMMIT;\nRETURN $r;";
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.command("sqlscript", script)) {
        rs.stream().forEach(r -> names.add(r.<String>getProperty("name")));
      }
      assertThat(names.getLast()).isEqualTo("a");
    }

    /** A transaction that wrote nothing still reuses every carried record: the check is two hash probes, not a load. */
    @Test
    void aTransactionWithoutWritesStillLoadsNothing() {
      reset();
      database.transaction(() -> {
        final List<Object> rows = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT FROM Doc")) {
          rs.stream().forEach(r -> rows.add(new ScoredRecordRow((Document) r.getElement().get(), "score", 1.0f)));
        }
        final BasicCommandContext ctx = new BasicCommandContext();
        ctx.setDatabase(database);

        final long before = ((Number) database.getStats().get("readRecord")).longValue();
        final List<?> out = (List<?>) new SQLFunctionVectorMmr().execute(null, null, null,
            new Object[] { rows, "emb", Map.of("lambda", 0.5, "k", 5) }, ctx);

        assertThat(((Number) database.getStats().get("readRecord")).longValue() - before).isZero();
        assertThat(out).hasSize(3);
      });
    }
  }

  /**
   * Issue #10898: every row of {@code vector.neighbors}, {@code vector.fuse} and {@code vector.rerank} held the record's properties
   * flattened AND the whole record again under a {@code record} key, so {@code SELECT expand(...)} and the JSON of the rows carried
   * every property twice - embeddings and passages, the largest ones, included. The key is no longer enumerated: a row lists the
   * properties, {@code @rid}, {@code @type} and the score once, and whoever asks for {@code record} still gets the record.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorRowRecordKeyTest extends TestHelper {
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

  /**
   * Issue #10879: {@code vector.fuse} read the RID and the score of a source row through private copies of the shared row
   * extractors. Every row shape the extractors accept must reach the fusion exactly as {@code vector.mmr} / {@code vector.rerank}
   * read it: same RID, same score direction (a {@code distance} is flipped, so the nearest row ranks first).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorRowExtractorTest extends TestHelper {

    @Test
    void everyRowShapeIsFusedAsTheSharedExtractorReadsIt() {
      final RID[] rids = new RID[6];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc10879");
        for (int i = 0; i < rids.length; i++) {
          final MutableDocument d = database.newDocument("Doc10879");
          d.set("n", i);
          if (i == 3)
            // read through the Result that carries this record as its element
            d.set("score", 0.7f);
          d.save();
          rids[i] = d.getIdentity();
        }
      });

      final List<Object> rows = new ArrayList<>();
      // Map with @rid and a distance (the shape of `vector.neighbors`): lower distance = better
      rows.add(mapRow("@rid", rids[0], "distance", 0.9f));
      // Map with rid and $score
      rows.add(mapRow("rid", rids[1], "$score", 0.5f));
      // Map with @rid as an Identifiable (a document) and a score
      rows.add(mapRow("@rid", database.lookupByRID(rids[2], true), "score", 0.8f));
      // Result carrying the element: the identity and the score both come from the record
      rows.add(new ResultInternal(database.lookupByRID(rids[3], true)));
      // Result with an @rid property and a distance
      final ResultInternal withRidProperty = new ResultInternal(database);
      withRidProperty.setProperty("@rid", rids[4]);
      withRidProperty.setProperty("distance", 0.1f);
      rows.add(withRidProperty);
      // Result with a rid property and $score
      final ResultInternal withPlainRid = new ResultInternal(database);
      withPlainRid.setProperty("rid", rids[5]);
      withPlainRid.setProperty("$score", 0.2f);
      rows.add(withPlainRid);

      // What the shared extractors read off every row: the expected fusion of one source with itself is that ranking
      final List<Object> byScore = new ArrayList<>(rows);
      byScore.sort(Comparator.comparingDouble(r -> -SQLFunctionVectorAbstract.extractScoreFromRow(r)));
      final List<RID> expected = new ArrayList<>();
      byScore.forEach(r -> expected.add(SQLFunctionVectorAbstract.extractRidFromRow(r)));
      // The flipped distances: 0.1 is the nearest (-0.1), 0.9 the farthest (-0.9), below every positive score
      assertThat(expected).containsExactly(rids[2], rids[3], rids[1], rids[5], rids[4], rids[0]);

      final BasicCommandContext ctx = new BasicCommandContext();
      ctx.setDatabase(database);
      final Map<String, Object> options = new LinkedHashMap<>();
      options.put("fusion", "LINEAR");
      final Object fused = new SQLFunctionVectorFuse().execute(null, null, null, new Object[] { rows, rows, options }, ctx);

      final List<RID> actual = new ArrayList<>();
      for (final Object row : (List<?>) fused)
        actual.add(SQLFunctionVectorAbstract.extractRidFromRow(row));
      assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static Map<String, Object> mapRow(final String ridKey, final Object rid, final String scoreKey, final float score) {
      final Map<String, Object> row = new LinkedHashMap<>();
      row.put(ridKey, rid);
      row.put(scoreKey, score);
      return row;
    }
  }
}
