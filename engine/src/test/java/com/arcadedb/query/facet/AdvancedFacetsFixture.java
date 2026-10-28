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
package com.arcadedb.query.facet;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Data and checks shared by the transport tests of the plan 5 facet kinds (HTTP, ndjson, {@code RemoteDatabase}, gRPC, Raft
 * followers): assets tagged from a 3-level tag tree ({@code t/1}, {@code t/1/2}, {@code t/1/2/3}, ...) plus values with no tag vertex,
 * and a {@code Measure} type with FLOAT, SHORT and INTEGER properties whose STATS sums keep the Java type {@code SELECT sum()} gives.
 */
public final class AdvancedFacetsFixture {
  public static final String       TYPE          = ColumnarTestData.TYPE;
  public static final String       MEASURE       = "Measure";
  /** Tag values no vertex of the hierarchy has: orphan roots. */
  public static final List<String> ORPHANS       = List.of("loose-a", "loose-b");
  /** HIERARCHY, DISTINCT (dictionary and numeric), STATS (LONG and DATETIME) and VALUE-ordered TERMS (STRING and DATETIME). */
  public static final String       ASSET_FACETS  = " FACETS t: tags HIERARCHY TOP 100, d: brand DISTINCT, n: fileSize DISTINCT,"
      + " s: fileSize STATS, p: publishDate STATS, b: brand TERMS TOP 3 ORDER BY VALUE, pd: publishDate TERMS TOP 2 ORDER BY VALUE";
  public static final String       ASSET_QUERY   = "SELECT FROM " + TYPE + " WHERE fileSize > 0" + ASSET_FACETS + " LIMIT 5";
  /** STATS of a FLOAT (Float sum), a SHORT and an INTEGER property, and a DISTINCT over the FLOAT. */
  public static final String       MEASURE_QUERY = "SELECT FROM " + MEASURE + " FACETS r: ratio STATS, sm: small STATS, q: qty STATS,"
      + " rd: ratio DISTINCT";
  /** The STATS facets of {@link #MEASURE_QUERY} by the property they count. */
  public static final Map<String, String> MEASURE_STATS = measureStats();
  public static final int          MEASURES      = 40;

  private AdvancedFacetsFixture() {
  }

  private static Map<String, String> measureStats() {
    final Map<String, String> map = new LinkedHashMap<>();
    map.put("r", "ratio");
    map.put("sm", "small");
    map.put("q", "qty");
    return map;
  }

  public static JSONObject hierarchyJSON() {
    return new JSONObject().put("vertexType", ColumnarTestData.TAG_TYPE).put("key", "path").put("parentEdge", ColumnarTestData.PARENT_EDGE);
  }

  /** The schema, the tag tree, {@code assets} assets tagged from it and from {@link #ORPHANS}, and the measures; every column applied. */
  public static void load(final Database db, final long seed, final int assets) {
    final ColumnarTestData data = new ColumnarTestData(seed);
    data.tagHierarchy = hierarchyJSON();
    data.maxTags = 3;
    data.createSchema(db);
    final List<String> values = new ArrayList<>(createTagTree(db));
    values.addAll(ORPHANS);
    data.tagValues = values;
    data.insertAssets(db, assets);
    createMeasures(db);
    FacetOracle.awaitCaughtUp(db);
  }

  /** The tags {@code t/i}, {@code t/i/j} and {@code t/i/j/k} for i, j, k in 1..3, each linked to its parent by {@code ParentOf}. */
  public static List<String> createTagTree(final Database db) {
    ColumnarTestData.createTagSchema(db);
    final List<String> paths = new ArrayList<>();
    db.transaction(() -> {
      final Map<String, MutableVertex> byPath = new HashMap<>();
      for (int i = 1; i <= 3; i++) {
        final String level1 = "t/" + i;
        byPath.put(level1, tag(db, level1));
        paths.add(level1);
        for (int j = 1; j <= 3; j++) {
          final String level2 = level1 + "/" + j;
          byPath.put(level2, tag(db, level2));
          byPath.get(level1).newEdge(ColumnarTestData.PARENT_EDGE, byPath.get(level2));
          paths.add(level2);
          for (int k = 1; k <= 3; k++) {
            final String level3 = level2 + "/" + k;
            byPath.put(level3, tag(db, level3));
            byPath.get(level2).newEdge(ColumnarTestData.PARENT_EDGE, byPath.get(level3));
            paths.add(level3);
          }
        }
      }
    });
    return paths;
  }

  private static MutableVertex tag(final Database db, final String path) {
    return db.newVertex(ColumnarTestData.TAG_TYPE).set("path", path).save();
  }

  /**
   * {@code Measure(ratio FLOAT, small SHORT, qty INTEGER)} with COLUMNAR indexes and {@link #MEASURES} records; every 10th record has no
   * ratio. The ratios are multiples of 1/8, so a float sum is exact whatever order adds them.
   */
  public static void createMeasures(final Database db) {
    db.command("sql", "CREATE DOCUMENT TYPE " + MEASURE + " BUCKETS 1");
    db.command("sql", "CREATE PROPERTY " + MEASURE + ".ratio FLOAT");
    db.command("sql", "CREATE PROPERTY " + MEASURE + ".small SHORT");
    db.command("sql", "CREATE PROPERTY " + MEASURE + ".qty INTEGER");
    for (final String property : MEASURE_STATS.values())
      db.command("sql", "CREATE INDEX ON " + MEASURE + " (" + property + ") COLUMNAR");
    db.transaction(() -> {
      for (int i = 0; i < MEASURES; i++) {
        final MutableDocument record = db.newDocument(MEASURE).set("small", (short) (i % 7)).set("qty", i * 3);
        if (i % 10 != 0)
          record.set("ratio", i / 8f);
        record.save();
      }
    });
    FacetOracle.awaitCaughtUp(db);
  }

  /**
   * {@code SELECT min(p), max(p), sum(p), avg(p)} of every property of {@link #MEASURE_STATS} through {@code db} (embedded or remote),
   * by facet alias: what a STATS facet of the same transport must equal, value and Java type.
   */
  public static Map<String, Object[]> measureAggregates(final BasicDatabase db) {
    final Map<String, Object[]> rows = new LinkedHashMap<>();
    for (final Map.Entry<String, String> entry : MEASURE_STATS.entrySet()) {
      final String p = entry.getValue();
      try (final ResultSet rs = db.query("sql",
          "SELECT min(" + p + ") AS mn, max(" + p + ") AS mx, sum(" + p + ") AS sm, avg(" + p + ") AS av FROM " + MEASURE)) {
        final Result row = rs.next();
        rows.put(entry.getKey(), new Object[] { row.getProperty("mn"), row.getProperty("mx"), row.getProperty("sm"), row.getProperty("av") });
      }
    }
    return rows;
  }

  /** The STATS facets of {@code facets} against {@link #measureAggregates}: min, max, sum and average, value and class. */
  public static void assertStatsEqualRows(final String label, final FacetResults facets, final Map<String, Object[]> rows) {
    for (final Map.Entry<String, Object[]> entry : rows.entrySet()) {
      final FacetStats stats = facets.get(entry.getKey()).stats();
      final Object[] row = entry.getValue();
      final Object[] facet = { stats.min(), stats.max(), stats.sum(), stats.avg() };
      final String[] names = { "min", "max", "sum", "avg" };
      for (int i = 0; i < names.length; i++) {
        assertThat(facet[i]).as("%s %s.%s", label, entry.getKey(), names[i]).isEqualTo(row[i]);
        assertThat(facet[i].getClass()).as("%s %s.%s class", label, entry.getKey(), names[i]).isEqualTo(row[i].getClass());
      }
    }
  }

  /**
   * Every member of every facet of {@code actual} equals the one of {@code expected}, each value of the data (bucket values, bounds, the
   * cursor, the DISTINCT value, the STATS members) with its Java class, and the bucket attributes as JSON; {@code meta} is not compared
   * (a transport may run the query again).
   */
  public static void assertSameValues(final String label, final FacetResults actual, final FacetResults expected) {
    assertThat(actual).as(label).isNotNull();
    assertThat(actual.aliases()).as(label).containsExactlyElementsOf(expected.aliases());
    for (final String alias : expected.aliases()) {
      final String at = label + " " + alias;
      final FacetResult a = actual.get(alias);
      final FacetResult e = expected.get(alias);
      assertThat(a.kind()).as(at).isEqualTo(e.kind());
      assertThat(a.source()).as(at).isEqualTo(e.source());
      assertThat(a.reason()).as(at).isEqualTo(e.reason());
      assertThat(a.otherCount()).as(at).isEqualTo(e.otherCount());
      assertThat(a.missing()).as(at).isEqualTo(e.missing());
      assertThat(a.distinct()).as(at).isEqualTo(e.distinct());
      assertThat(a.hasAfter()).as(at).isEqualTo(e.hasAfter());
      assertSameValue(at + " after", a.after(), e.after());
      if (e.stats() == null)
        assertThat(a.stats()).as(at).isNull();
      else {
        assertThat(a.stats().count()).as(at).isEqualTo(e.stats().count());
        assertThat(a.stats().missing()).as(at).isEqualTo(e.stats().missing());
        assertSameValue(at + " min", a.stats().min(), e.stats().min());
        assertSameValue(at + " max", a.stats().max(), e.stats().max());
        assertSameValue(at + " sum", a.stats().sum(), e.stats().sum());
        assertSameValue(at + " avg", a.stats().avg(), e.stats().avg());
      }
      assertThat(a.buckets()).as(at).hasSameSizeAs(e.buckets());
      for (int i = 0; i < e.buckets().size(); i++) {
        final String bt = at + " bucket " + i;
        final FacetBucket ab = a.buckets().get(i);
        final FacetBucket eb = e.buckets().get(i);
        assertSameValue(bt + " value", ab.value(), eb.value());
        assertThat(ab.count()).as(bt).isEqualTo(eb.count());
        assertThat(ab.key()).as(bt).isEqualTo(eb.key());
        assertThat(ab.isMissing()).as(bt).isEqualTo(eb.isMissing());
        assertThat(ab.isDefault()).as(bt).isEqualTo(eb.isDefault());
        assertSameValue(bt + " from", ab.from(), eb.from());
        assertSameValue(bt + " to", ab.to(), eb.to());
        assertThat(ab.attributes().toString()).as(bt + " attributes").isEqualTo(eb.attributes().toString());
      }
    }
  }

  private static void assertSameValue(final String label, final Object actual, final Object expected) {
    assertThat(actual).as(label).isEqualTo(expected);
    if (expected != null)
      assertThat(actual.getClass()).as(label + " class").isEqualTo(expected.getClass());
  }

  /** The facets of {@code sql} on {@code db}. */
  public static FacetResults facets(final BasicDatabase db, final String sql) {
    try (final ResultSet rs = db.query("sql", sql)) {
      return rs.getFacets();
    }
  }
}
