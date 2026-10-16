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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10129: a hybrid query {@code vector.rerank(vector.fuse(...), ...)} loaded every fused candidate twice (once in
 * {@code fuse}, again in {@code rerank}), copied every property of every record into a new map in both functions, and
 * sorted every candidate in {@code rerank} to keep only the top {@code k}.
 * <p>
 * Record loads are counted through the database's {@code readRecord} statistic, which {@code lookupByRID} increments on
 * every call.
 */
class Issue10129FuseRerankRecordLoadTest extends TestHelper {
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

      final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
      eager.put("record", d);
      for (final String prop : d.getPropertyNames())
        eager.put(prop, d.get(prop));
      eager.put("@rid", d.getIdentity());
      eager.put("@type", d.getTypeName());
      eager.put("score", 0.5f);

      final ScoredRecordRow lazy = new ScoredRecordRow(d, "score", 0.5f);
      // the record property overwrote the `record` key in the eager copy, and does so in the lazy row
      assertThat(lazy.get("record")).isEqualTo("a property named like the record key");
      assertThat(lazy.getRecord()).isSameAs(d);
      for (final String key : List.of("record", "name", "nullable", "missing"))
        assertThat(lazy.containsKey(key)).as(key).isEqualTo(eager.containsKey(key));
      for (final String key : eager.keySet())
        assertThat(lazy.get(key)).as(key).isEqualTo(eager.get(key));
      assertThat(lazy.keySet()).containsExactlyElementsOf(eager.keySet());
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
        assertThat(r.getPropertyNames()).containsExactly("record", "name", "text", "embedding", "score", "@rid", "@type");
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

      final LinkedHashMap<String, Object> eager = new LinkedHashMap<>();
      eager.put("record", doc);
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
