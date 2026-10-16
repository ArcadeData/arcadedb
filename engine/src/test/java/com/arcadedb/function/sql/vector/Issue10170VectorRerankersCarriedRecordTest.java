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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
class Issue10170VectorRerankersCarriedRecordTest extends TestHelper {
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
