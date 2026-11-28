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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Model-based oracle of {@code vector.neighbors} on an LSM_VECTOR index: a seeded sequence of committed and rolled-back
 * transactions (inserts, updates to a new vector, deletes, removed vectors, vectors the COSINE similarity cannot score), reopens,
 * compactions, rebuilds and graph builds, and after each step searches against a brute force model kept by the test.
 * <p>
 * What is asserted is what the documentation promises and holds for every seed: an answer holds only live records whose vector
 * the index can score, each at the distance to its CURRENT vector (never an old one), once, sorted ascending, inside the filter and
 * the {@code maxDistance} radius, and no longer than {@code k}; an allow-list of at most a tenth of the records is answered
 * exactly (the pre-filter plan scores every member); a record's own current vector finds it. The same checks run inside the
 * writing transaction, where the answer must show its own writes.
 */
class VectorIndexModelOracleTest extends TestHelper {
  private static final int    DIM   = 8;
  private static final String INDEX = "Chunk[embedding]";

  private final Map<RID, List<float[]>> history = new HashMap<>();
  private final List<RID>               deleted = new ArrayList<>();

  private Random            rnd;
  private String            quantization;
  private Map<RID, float[]> model   = new LinkedHashMap<>();
  private Map<RID, float[]> pending;
  private String            lastOp  = "init";
  private long              idSeq;

  @ParameterizedTest
  @CsvSource({ "NONE,false,11", "NONE,true,12", "INT8,true,13", "INT8,false,14" })
  void searchesAgreeWithABruteForceModelAcrossWritesRollbacksReopensAndRebuilds(final String quant, final boolean hierarchy,
      final long seed) {
    quantization = quant;
    rnd = new Random(seed);
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Chunk");
      database.command("sql", "CREATE PROPERTY Chunk.id STRING");
      database.command("sql", "CREATE PROPERTY Chunk.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Chunk (embedding) LSM_VECTOR METADATA {\"dimensions\": " + DIM
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quant + "\", \"addHierarchy\": " + hierarchy
          + ", \"maxConnections\": 16, \"beamWidth\": 100}");
    });

    for (int b = 0; b < 3; b++) {
      lastOp = "initial load";
      begin();
      for (int i = 0; i < 160; i++)
        insertOne(rnd.nextInt(100) < 3 ? special(true) : randomVector());
      commit();
    }
    lastOp = "first search";
    check(3);
    for (int step = 1; step <= 45; step++) {
      operate();
      check(2 + rnd.nextInt(3));
    }
  }

  // ---------------------------------------------------------------- operations

  private void operate() {
    final int c = rnd.nextInt(100);
    if (c < 22)
      mutate("insert", false);
    else if (c < 42)
      mutate("update", false);
    else if (c < 54)
      mutate("delete", false);
    else if (c < 64)
      mutate("mixed", true);
    else if (c < 74)
      mutate("mixed", false);
    else if (c < 78) {
      lastOp = "reopen";
      reopenDatabase();
    } else if (c < 82) {
      lastOp = "compact";
      database.command("sql", "COMPACT INDEX `" + INDEX + "`").close();
    } else if (c < 85) {
      lastOp = "rebuild";
      database.command("sql", "REBUILD INDEX `" + INDEX + "`").close();
    } else if (c < 93) {
      lastOp = "buildNow";
      for (final Index sub : ((TypeIndex) database.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets())
        ((LSMVectorIndex) sub).buildVectorGraphNow();
    }
  }

  private void mutate(final String kind, final boolean rollback) {
    lastOp = kind + (rollback ? "+rollback" : "");
    begin();
    final int n = 1 + rnd.nextInt(kind.equals("mixed") ? 25 : 30);
    for (int i = 0; i < n; i++) {
      final String k = kind.equals("mixed") ? new String[] { "insert", "update", "delete", "remove" }[rnd.nextInt(4)] : kind;
      switch (k) {
      case "insert" -> insertOne(rnd.nextInt(100) < 12 ? (rnd.nextInt(10) == 0 ? null : special(false)) : randomVector());
      case "update" -> {
        final RID r = pickAny(pending);
        if (r != null)
          updateOne(r, rnd.nextInt(100) < 15 ? (rnd.nextInt(3) == 0 ? null : special(false)) : randomVector());
      }
      case "remove" -> {
        final RID r = pickAny(pending);
        if (r != null)
          updateOne(r, null);
      }
      default -> {
        final RID r = pickAny(pending);
        if (r != null) {
          database.lookupByRID(r, true).asVertex().delete();
          pending.remove(r);
          deleted.add(r);
        }
      }
      }
    }
    // a search inside the transaction sees its own writes
    if (rnd.nextInt(3) == 0)
      checkOne(pending, true);
    if (rollback)
      rollbackTx();
    else
      commit();
  }

  private void insertOne(final float[] v) {
    final MutableDocument d = database.newVertex("Chunk").set("id", "r" + (idSeq++));
    if (v != null)
      d.set("embedding", v);
    d.save();
    final RID rid = d.getIdentity();
    pending.put(rid, v == null ? null : v.clone());
    if (v != null)
      history.computeIfAbsent(rid, k -> new ArrayList<>()).add(v.clone());
  }

  private void updateOne(final RID rid, final float[] v) {
    final MutableDocument d = database.lookupByRID(rid, true).asVertex().modify();
    if (v == null)
      d.remove("embedding");
    else
      d.set("embedding", v);
    d.save();
    pending.put(rid, v == null ? null : v.clone());
    if (v != null)
      history.computeIfAbsent(rid, k -> new ArrayList<>()).add(v.clone());
  }

  private void begin() {
    database.begin();
    pending = new LinkedHashMap<>(model);
  }

  private void commit() {
    database.commit();
    model = pending;
    pending = null;
  }

  private void rollbackTx() {
    database.rollback();
    pending = null;
  }

  // ---------------------------------------------------------------- vectors

  private float[] randomVector() {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  /**
   * An exact duplicate of a live vector, or (only before the first search, as a vector written after the graph exists takes the
   * delta path, issue #13118) the zero vector or a squared norm the float cosine cannot hold.
   */
  private float[] special(final boolean unscorable) {
    final float[] v = randomVector();
    switch (unscorable ? rnd.nextInt(4) : 3) {
    case 0 -> Arrays.fill(v, 0f);
    case 1 -> {
      for (int i = 0; i < DIM; i++)
        v[i] *= 1e-25f;
    }
    case 2 -> {
      for (int i = 0; i < DIM; i++)
        v[i] *= 1e20f;
    }
    default -> {
      final RID r = pickValid(pending != null ? pending : model);
      if (r != null)
        return (pending != null ? pending : model).get(r).clone();
    }
    }
    return v;
  }

  /** whether the index holds the vector: COSINE leaves out a squared norm outside [2^-63, 2^63] (issue #11635) */
  private static boolean valid(final float[] v) {
    if (v == null || v.length != DIM)
      return false;
    float sumsq = 0f;
    for (final float x : v)
      sumsq += x * x;
    return sumsq >= 0x1p-63f && sumsq <= 0x1p63f;
  }

  private static int validCount(final Map<RID, float[]> m) {
    int c = 0;
    for (final float[] v : m.values())
      if (valid(v))
        c++;
    return c;
  }

  private RID pickValid(final Map<RID, float[]> m) {
    final List<RID> l = new ArrayList<>();
    for (final Map.Entry<RID, float[]> e : m.entrySet())
      if (valid(e.getValue()))
        l.add(e.getKey());
    return l.isEmpty() ? null : l.get(rnd.nextInt(l.size()));
  }

  private RID pickAny(final Map<RID, float[]> m) {
    if (m.isEmpty())
      return null;
    return new ArrayList<>(m.keySet()).get(rnd.nextInt(m.size()));
  }

  private static double exactDistance(final float[] q, final float[] v) {
    double dot = 0, nq = 0, nv = 0;
    for (int i = 0; i < DIM; i++) {
      dot += (double) q[i] * v[i];
      nq += (double) q[i] * q[i];
      nv += (double) v[i] * v[i];
    }
    return 1.0 - dot / Math.sqrt(nq * nv);
  }

  private double tolerance() {
    return quantization.equals("NONE") ? 2e-4 : 0.05;
  }

  // ---------------------------------------------------------------- oracle

  private void check(final int n) {
    for (int i = 0; i < n; i++)
      checkOne(model, false);
  }

  private record Hit(RID rid, float distance) {
  }

  private List<Hit> search(final float[] q, final int k, final int ef, final List<RID> filter, final boolean ridStrings,
      final double maxDistance) {
    final StringBuilder options = new StringBuilder("{efSearch: " + ef);
    final Map<String, Object> params = new HashMap<>();
    params.put("qv", q);
    params.put("k", k);
    if (filter != null) {
      options.append(", filter: :f");
      if (ridStrings) {
        final List<String> s = new ArrayList<>();
        for (final RID r : filter)
          s.add(r.toString());
        params.put("f", s);
      } else
        params.put("f", filter);
    }
    if (!Double.isInfinite(maxDistance))
      options.append(", maxDistance: ").append(maxDistance);
    options.append("}");
    final List<Hit> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('" + INDEX + "', :qv, :k, " + options + ")))", params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(new Hit(((Identifiable) r.getProperty("rid")).getIdentity(), r.<Number>getProperty("distance").floatValue()));
      }
    }
    return out;
  }

  private void checkOne(final Map<RID, float[]> m, final boolean inTx) {
    final int valid = validCount(m);
    final String where = (inTx ? "in-tx" : "committed") + " after " + lastOp;
    float[] q = randomVector();
    RID selfRid = null;
    float[] oldVector = null;
    final int kind = rnd.nextInt(10);
    if (kind < 4 && valid > 0) {
      selfRid = pickValid(m);
      q = m.get(selfRid);
    } else if (kind < 6) {
      // the OLD vector of an updated record: the record must not answer at that distance any more
      final RID r = pickAny(m);
      if (r != null && history.containsKey(r) && valid(m.get(r)))
        for (final float[] h : history.get(r))
          if (valid(h) && !Arrays.equals(h, m.get(r)))
            oldVector = h;
      if (oldVector != null)
        q = oldVector;
    }
    final int[] ks = { 1, 3, 10, 50, Math.max(1, valid), valid + 7 };
    final int k = ks[rnd.nextInt(ks.length)];
    final int ef = rnd.nextInt(4) == 0 ? 20 : Math.max(200, 2 * valid);
    List<RID> filter = null;
    final int fm = rnd.nextInt(6);
    if (fm >= 1 && fm <= 4) {
      final Set<RID> f = new HashSet<>();
      final List<RID> all = new ArrayList<>(m.keySet());
      final int target = fm == 1 ? 1 + rnd.nextInt(10) : fm == 2 ? 1 + rnd.nextInt(Math.max(1, all.size() / 10)) : fm == 3 ? all.size() / 3 : all.size();
      for (int i = 0; i < target && !all.isEmpty(); i++)
        f.add(all.get(rnd.nextInt(all.size())));
      // a stale allow-list holds the RIDs of deleted records too
      if (!deleted.isEmpty() && rnd.nextInt(3) == 0)
        for (int i = 0; i < 1 + rnd.nextInt(5); i++)
          f.add(deleted.get(rnd.nextInt(deleted.size())));
      filter = new ArrayList<>(f);
      if (selfRid != null && !f.contains(selfRid)) {
        filter.add(selfRid);
        f.add(selfRid);
      }
    } else if (fm == 5) {
      filter = new ArrayList<>();
      for (final Map.Entry<RID, float[]> e : m.entrySet())
        if (valid(e.getValue()) && rnd.nextInt(100) < 4)
          filter.add(e.getKey());
      if (filter.isEmpty() && selfRid != null)
        filter.add(selfRid);
    }
    final double maxDistance = rnd.nextInt(8) == 0 ? 0.5 + rnd.nextDouble() : Double.POSITIVE_INFINITY;
    final boolean ridStrings = rnd.nextBoolean();

    final Set<RID> filterSet = filter == null ? null : new HashSet<>(filter);
    final Map<RID, Double> expected = new HashMap<>();
    for (final Map.Entry<RID, float[]> e : m.entrySet())
      if (valid(e.getValue()) && (filterSet == null || filterSet.contains(e.getKey())))
        expected.put(e.getKey(), exactDistance(q, e.getValue()));

    final List<Hit> hits = search(q, k, ef, filter, ridStrings, maxDistance);
    final String ctx = " (" + where + ", k=" + k + ", ef=" + ef + ", filter=" + (filter == null ? "none" : filter.size()) + ", valid="
        + valid + ", maxDistance=" + maxDistance + ")";

    final Set<RID> seen = new HashSet<>();
    float previous = -Float.MAX_VALUE;
    for (final Hit h : hits) {
      assertThat(seen.add(h.rid)).as("one row per record, %s twice%s", h.rid, ctx).isTrue();
      assertThat(m).as("only live records answer, not %s%s", h.rid, ctx).containsKey(h.rid);
      final float[] current = m.get(h.rid);
      assertThat(valid(current)).as("%s answers although its current vector is absent or unscorable%s", h.rid, ctx).isTrue();
      if (filterSet != null)
        assertThat(filterSet).as("answer outside the filter%s", ctx).contains(h.rid);
      assertThat((double) h.distance).as("distance of %s is the distance to its CURRENT vector%s", h.rid, ctx)
          .isCloseTo(exactDistance(q, current), within(tolerance()));
      assertThat(h.distance).as("ascending%s", ctx).isGreaterThanOrEqualTo(previous - 1e-6f);
      previous = h.distance;
      assertThat((double) h.distance).as("inside maxDistance%s", ctx).isLessThanOrEqualTo(maxDistance + 1e-6);
    }
    assertThat(hits.size()).as("no longer than k%s", ctx).isLessThanOrEqualTo(k);

    // an allow-list of at most a tenth of the records is answered exactly: the pre-filter plan scores every member
    if (filter != null && quantization.equals("NONE") && expected.size() <= 0.1 * valid) {
      final List<Double> sorted = new ArrayList<>(expected.values());
      Collections.sort(sorted);
      int want = 0;
      for (final double d : sorted)
        if (d <= maxDistance + 1e-6 && want < k)
          want++;
      assertThat(hits.size()).as("a small allow-list answers every eligible record up to k%s", ctx).isEqualTo(want);
      for (int i = 0; i < hits.size(); i++)
        assertThat((double) hits.get(i).distance).as("rank %d is the exact rank%s", i, ctx)
            .isCloseTo(sorted.get(i), within(tolerance()));
    }

    // a record's own current vector finds it, unless equal vectors take its slots
    if (selfRid != null && filter == null && maxDistance == Double.POSITIVE_INFINITY && ef >= 200) {
      int atZero = 0;
      for (final double d : expected.values())
        if (d <= 1e-6)
          atZero++;
      if (atZero <= k) {
        boolean found = false;
        for (final Hit h : hits)
          found |= h.rid.equals(selfRid);
        assertThat(found).as("%s is not found by its own vector%s", selfRid, ctx).isTrue();
      }
    }
  }
}
