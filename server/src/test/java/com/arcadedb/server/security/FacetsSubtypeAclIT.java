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
package com.arcadedb.server.security;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Facets never count the records of a bucket the user may not read. {@code Image extends Asset}; the COLUMNAR index on
 * {@code Asset.brand} (and on {@code Asset.score}) projects Image's buckets too, the FULL_TEXT index on {@code Asset.title} and the
 * LSM index on {@code Asset.brand} span both types. A user whose group reads every type but {@code Image} runs FACETS queries over
 * {@link RemoteDatabase}; every path that builds or counts matches without reading the rows (the projection with no WHERE, the
 * full-text index, the FILTER index evaluator, the fallback scan of an unindexed property, the patcher, the scan of a bucket the
 * caller's transaction changed) must answer as if Image's buckets did not exist: terms, MISSING, otherCount, INCLUDE, ranges,
 * histograms, and the PROFILE match count. The oracle is root's answer to the same facets over Asset's own buckets only.
 */
class FacetsSubtypeAclIT extends BaseGraphServerTest {
  private static final String SUPER      = "Asset";
  private static final String SUB        = "Image";
  private static final String USER       = "facets-no-images";
  private static final String GROUP      = "noImages";
  private static final String PASSWORD   = "facetsuser17";
  private static final int    ASSETS     = 400;
  private static final int    IMAGES     = 300;
  /** Every facet shape that counts: terms with MISSING, a short TOP (otherCount), INCLUDE probes, ranges, a histogram, a scanned property. */
  private static final String FACETS     = "brand TERMS TOP 1000 MISSING, top2: brand TERMS TOP 2, "
      + "inc: brand TERMS INCLUDE ['brand-5', 'brand-0'], sc: score RANGES ([0, 50), [50, *)), h: score HISTOGRAM FIXED 25, "
      + "w: weight TERMS TOP 100 MISSING";
  private static final String RED        = "search_fields(['title'], 'red')";
  private static final Pattern MATCHES   = Pattern.compile("matches=(\\d+)");
  private static final Pattern COUNTER   = Pattern.compile("counting=\\S+ budget=\\d+ cardinality=\\d+");

  @Override
  protected int getServerCount() {
    return 1;
  }

  /**
   * Asset brands are brand-0 to brand-4, Image brands brand-3 to brand-6 (brand-5 and brand-6 exist only in Image); Image weights
   * 10 to 14 exist only in Image; Image scores reach 149, beyond every Asset score. Both types have records without a brand or a weight.
   */
  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    // two buckets each: the own-transaction test changes both of Asset's, so no bucket the user may read is left to the projection
    db.command("sql", "CREATE DOCUMENT TYPE " + SUPER + " BUCKETS 2");
    db.command("sql", "CREATE DOCUMENT TYPE " + SUB + " EXTENDS " + SUPER + " BUCKETS 2");
    db.command("sql", "CREATE PROPERTY " + SUPER + ".brand STRING");
    db.command("sql", "CREATE PROPERTY " + SUPER + ".title STRING");
    db.command("sql", "CREATE PROPERTY " + SUPER + ".score INTEGER");
    db.command("sql", "CREATE PROPERTY " + SUPER + ".weight INTEGER");
    db.command("sql", "CREATE INDEX ON " + SUPER + " (brand) COLUMNAR");
    db.command("sql", "CREATE INDEX ON " + SUPER + " (score) COLUMNAR");
    db.command("sql", "CREATE INDEX ON " + SUPER + " (title) FULL_TEXT");
    db.command("sql", "CREATE INDEX ON " + SUPER + " (brand) NOTUNIQUE");
    db.transaction(() -> {
      for (int i = 0; i < ASSETS; i++)
        db.newDocument(SUPER).set("brand", i % 20 == 0 ? null : "brand-" + (i % 5)).set("title", (i % 3 == 0 ? "red" : "blue") + " asset")
            .set("score", i % 100).set("weight", i % 11 == 0 ? null : i % 7).save();
      for (int i = 0; i < IMAGES; i++)
        db.newDocument(SUB).set("brand", i % 15 == 0 ? null : "brand-" + (3 + i % 4)).set("title", (i % 2 == 0 ? "red" : "green") + " image")
            .set("score", 50 + i % 100).set("weight", i % 9 == 0 ? null : 10 + i % 5).save();
    });
    FacetOracle.awaitCaughtUp(db);
  }

  @BeforeEach
  void createRestrictedUser() {
    FacetOracle.awaitCaughtUp(getServerDatabase(0, getDatabaseName()));
    final ServerSecurity security = getServer(0).getSecurity();
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put(GROUP,
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")))
                .put(SUB, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();
    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(new JSONObject()
        .put("name", USER)
        .put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));
  }

  @AfterEach
  void dropRestrictedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(USER))
      security.dropUser(USER);
  }

  /** No WHERE: the match set is taken from the projection, which holds Image's buckets too. */
  @Test
  void subtypeBucketIsNotCounted() {
    final String sql = "SELECT FROM " + SUPER + " FACETS " + FACETS + " LIMIT 0";
    final FacetResults restricted = restricted(sql);
    assertVisibleOnly(restricted, null, root(sql));
    assertThat(counts(restricted.get("brand"))).isEqualTo(groupBy("brand", null));
    assertThat(counts(restricted.get("w"))).isEqualTo(groupBy("weight", null));
    assertThat(restricted.get("brand").missing()).isEqualTo(ASSETS / 20);
    // INCLUDE of a value only Image holds: present, count 0
    assertThat(includeCount(restricted.get("inc"), "brand-5")).isEqualTo(0);
    assertThat(includeCount(root(sql).get("inc"), "brand-5")).isGreaterThan(0);
  }

  /**
   * The full-text index hands the match set every RID it holds, Image's included, without loading a record. Root searches first: the
   * index's type-wide BM25 counters are computed by the first search after open from every bucket of the type, which the restricted
   * user may not count (with or without FACETS).
   */
  @Test
  void fullTextFastPathDoesNotLeakSubtype() {
    final String sql = "SELECT FROM " + SUPER + " WHERE " + RED + " FACETS " + FACETS + " LIMIT 0";
    assertThat(rootProfile(sql)).contains("matchSource=index");
    final FacetResults restricted = restricted(sql);
    assertVisibleOnly(restricted, "title LIKE 'red%'", root(sql));
    assertThat(counts(restricted.get("brand"))).isEqualTo(groupBy("brand", "title LIKE 'red%'"));
  }

  /**
   * A FILTER over brand with an LSM index on it is evaluated from the RIDs the index holds, Image's included. The rows are read, so the
   * query targets the buckets of Asset the user may read: the index's Image RIDs must not reach a count.
   */
  @Test
  void disjunctiveIndexFilterDoesNotLeak() {
    final String facets = "brand TERMS TOP 1000 FILTER brand IN ['brand-3', 'brand-4', 'brand-5'] MISSING, "
        + "w: weight TERMS TOP 100 FILTER brand IN ['brand-3', 'brand-4', 'brand-5']";
    final String sql = "SELECT FROM " + ownBuckets() + " FACETS " + facets + " LIMIT 0";
    assertThat(rootProfile(sql)).contains("[INDEX]");
    final FacetResults restricted = restricted(sql);
    assertThat(restricted.toJSON().toString()).isEqualTo(root(sql).toJSON().toString());
    assertThat(counts(restricted.get("w"))).isEqualTo(groupBy("weight", "brand IN ['brand-3', 'brand-4', 'brand-5']"));
    // the same facets over every type count Image: the oracle can tell the two apart
    assertThat(root("SELECT FROM " + SUPER + " FACETS " + facets + " LIMIT 0").toJSON().toString())
        .isNotEqualTo(restricted.toJSON().toString());
  }

  /** PROFILE reports the size of the match set: Image's records are not part of it. */
  @Test
  void profileMatchCountDoesNotLeak() {
    final String sql = "SELECT FROM " + SUPER + " FACETS brand TERMS LIMIT 0";
    assertThat(matchCount(restrictedProfile(sql))).isEqualTo(ASSETS);
    assertThat(matchCount(rootProfile(sql))).isEqualTo(ASSETS + IMAGES);

    // root first: it computes the full-text index's BM25 counters (see fullTextFastPathDoesNotLeakSubtype)
    final String red = "SELECT FROM " + SUPER + " WHERE " + RED + " FACETS brand TERMS LIMIT 0";
    final String rootRed = rootProfile(red);
    assertThat(rootRed).contains("matchSource=index");
    final long visibleRed = groupBy("title", "title LIKE 'red%'").values().stream().mapToLong(Long::longValue).sum();
    assertThat(matchCount(rootRed)).isGreaterThan(visibleRed);
    final String restrictedRed = restrictedProfile(red);
    assertThat(restrictedRed).contains("matchSource=index");
    assertThat(matchCount(restrictedRed)).isEqualTo(visibleRed);
  }

  /**
   * The merged dictionary of Asset's buckets is cached per (type, property) for the database. Once root counted brand over every bucket,
   * the cached merge also holds brand-5 and brand-6, which only Image records have: the restricted user's counter must still be sized,
   * and PROFILE must still report {@code counting}, {@code budget} and {@code cardinality}, from Asset's dictionaries alone, exactly as
   * before root ran.
   */
  @Test
  void profileCounterFiguresDoNotDependOnHiddenDictionaries() {
    final String sql = "SELECT FROM " + SUPER + " FACETS brand TERMS LIMIT 0";
    final String fresh = counterFigures(restrictedProfile(sql));
    assertThat(fresh).contains("cardinality=5");

    final String root = counterFigures(rootProfile(sql));
    assertThat(root).contains("cardinality=7");

    assertThat(counterFigures(restrictedProfile(sql))).isEqualTo(fresh);
    // and the restricted merge did not shrink root's
    assertThat(counterFigures(rootProfile(sql))).isEqualTo(root);
  }

  /**
   * Lag: with the apply passes held, root changes records of both types. The restricted user's facets patch Asset's changed records from
   * the records and never read Image's pending pages: the counts are Asset's committed ones and {@code patched} is what the same facets
   * over Asset's own buckets patch.
   */
  @Test
  void patchingUnderLagDoesNotLeakSubtype() throws Exception {
    final Database db = getServerDatabase(0, getDatabaseName());
    final String sql = "SELECT FROM " + SUPER + " FACETS " + FACETS + " LIMIT 0";
    final FacetResults restricted;
    final FacetResults ownBuckets;
    final FacetResults polymorphic;
    try (final AutoCloseable hold = ((LocalDatabase) ((DatabaseInternal) db).getEmbedded()).getColumnarIndexManager().suspendApply()) {
      db.transaction(() -> {
        db.command("sql", "UPDATE " + SUPER + " SET brand = 'brand-8', weight = 99 WHERE score = 7");
        db.command("sql", "UPDATE " + SUB + " SET brand = 'brand-8', weight = 99 WHERE score = 60");
      });
      restricted = restricted(sql);
      ownBuckets = root("SELECT FROM " + ownBuckets() + " FACETS " + FACETS + " LIMIT 0");
      polymorphic = root(sql);
    }
    FacetOracle.awaitCaughtUp(db);

    assertThat(counts(restricted.get("brand"))).isEqualTo(groupBy("brand", null)).containsKey("brand-8");
    assertThat(counts(restricted.get("w"))).isEqualTo(groupBy("weight", null));
    assertThat(restricted.get("sc").toJSON().toString()).isEqualTo(ownBuckets.get("sc").toJSON().toString());
    assertThat(restricted.patched()).isGreaterThan(0).isEqualTo(ownBuckets.patched()).isLessThan(polymorphic.patched());
  }

  /**
   * The caller's open transaction changed both of Asset's buckets: they are read from their records (the own-transaction scan), and no
   * bucket the user may read is left to the projection. Image's buckets are still left out, and nothing falls back to the rows, whose
   * drain would read them.
   */
  @Test
  void ownTransactionScanDoesNotLeakSubtype() {
    final String sql = "SELECT FROM " + SUPER + " FACETS " + FACETS + " LIMIT 0";
    final Map<Object, Long> expected = groupBy("brand", null);
    final List<RID> changed = new ArrayList<>();
    for (final Bucket bucket : getServerDatabase(0, getDatabaseName()).getSchema().getType(SUPER).getBuckets(false))
      for (final Result row : rootRows("SELECT @rid AS v, brand FROM bucket:[`" + bucket.getName() + "`] WHERE brand IS NOT NULL LIMIT 3")) {
        changed.add(row.getProperty("v"));
        expected.merge(row.getProperty("brand"), -1L, Long::sum);
        expected.merge("brand-9", 1L, Long::sum);
      }
    expected.values().removeIf(count -> count == 0);
    assertThat(changed).hasSize(6);
    try (final RemoteDatabase remote = remote()) {
      remote.begin();
      try {
        for (final RID rid : changed)
          remote.command("sql", "UPDATE " + rid + " SET brand = 'brand-9'");
        final FacetResults restricted;
        try (final ResultSet rs = remote.query("sql", sql)) {
          restricted = rs.getFacets();
        }
        assertThat(counts(restricted.get("brand"))).isEqualTo(expected);
        assertThat(counts(restricted.get("w"))).isEqualTo(groupBy("weight", null));
      } finally {
        remote.rollback();
      }
    }
  }

  /**
   * A FACETS query whose rows reach Image's buckets fails exactly as the same query without FACETS and LIMIT does: the facets add no
   * error of their own and name nothing the plain query would not. That holds for a WHERE (the matches are the rows) and for a FILTER
   * over a polymorphic target (a FILTER masks rows one by one, so the rows are drained, Image's included), though the plain query with
   * the LIMIT would return: facets cover every match (D6), and the user may not read some of them.
   */
  @Test
  void refusedQueryFailsLikeThePlainQuery() {
    assertFailsLikeThePlainQuery("SELECT FROM " + SUPER + " WHERE score >= 0", " FACETS " + FACETS + " LIMIT 1");
    assertFailsLikeThePlainQuery("SELECT FROM " + SUPER, " FACETS b: brand TERMS FILTER brand = 'brand-1' LIMIT 1");
  }

  private void assertFailsLikeThePlainQuery(final String plainSql, final String facetsTail) {
    final Throwable plain = catchThrowable(() -> {
      try (final RemoteDatabase remote = remote(); final ResultSet rs = remote.query("sql", plainSql)) {
        rs.stream().count();
      }
    });
    final Throwable faceted = catchThrowable(() -> restricted(plainSql + facetsTail));
    assertThat(plain).isNotNull();
    assertThat(faceted).as(plainSql + facetsTail).isNotNull();
    assertThat(faceted.getClass()).isEqualTo(plain.getClass());
    assertThat(faceted.getMessage()).isEqualTo(plain.getMessage());
  }

  @Test
  void rootSeesEverything() {
    final String sql = "SELECT FROM " + SUPER + " FACETS " + FACETS + " LIMIT 0";
    final FacetResults root;
    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS); final ResultSet rs = remote.query("sql", sql)) {
      root = rs.getFacets();
    }
    assertThat(counts(root.get("brand"))).isEqualTo(polymorphicGroupBy("brand"));
    assertThat(counts(root.get("w"))).isEqualTo(polymorphicGroupBy("weight"));
    assertThat(root.toJSON().toString()).isEqualTo(root(sql).toJSON().toString());
  }

  /**
   * The restricted answer is root's answer over Asset's own buckets (same WHERE, given as a row condition), and differs from root's
   * answer over both types.
   */
  private void assertVisibleOnly(final FacetResults restricted, final String rowCondition, final FacetResults rootPolymorphic) {
    final String where = rowCondition == null ? "" : " WHERE " + rowCondition;
    final FacetResults expected = root("SELECT FROM " + ownBuckets() + where + " FACETS " + FACETS + " LIMIT 0");
    assertThat(restricted.toJSON().toString()).isEqualTo(expected.toJSON().toString());
    assertThat(rootPolymorphic.toJSON().toString()).isNotEqualTo(expected.toJSON().toString());
  }

  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), USER, PASSWORD);
  }

  private FacetResults restricted(final String sql) {
    try (final RemoteDatabase remote = remote(); final ResultSet rs = remote.query("sql", sql)) {
      rs.stream().count();
      final FacetResults facets = rs.getFacets();
      assertThat(facets).as(sql).isNotNull();
      return facets;
    }
  }

  /** PROFILE as the restricted user, bound to the server's database as the HTTP handler binds it (the remote client drops the plan). */
  private String restrictedProfile(final String sql) {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    return DatabaseUserContext.runAs(database, getServer(0).getSecurity().getUser(USER), () -> {
      try (final ResultSet rs = database.query("sql", "PROFILE " + sql)) {
        return planText(rs);
      }
    });
  }

  private FacetResults root(final String sql) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", sql)) {
      return rs.getFacets();
    }
  }

  private String rootProfile(final String sql) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "PROFILE " + sql)) {
      return planText(rs);
    }
  }

  private List<Result> rootRows(final String sql) {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", sql)) {
      return rs.stream().toList();
    }
  }

  private static String planText(final ResultSet rs) {
    final Result row = rs.next();
    final Object plan = row.getProperty("executionPlanAsString");
    return plan != null ? plan.toString() : row.toJSON().toString();
  }

  /** The {@code counting=... budget=... cardinality=...} figures PROFILE prints for the first facet. */
  private static String counterFigures(final String plan) {
    final Matcher matcher = COUNTER.matcher(plan);
    assertThat(matcher.find()).as(plan).isTrue();
    return matcher.group();
  }

  private static long matchCount(final String plan) {
    final Matcher matcher = MATCHES.matcher(plan);
    assertThat(matcher.find()).as(plan).isTrue();
    return Long.parseLong(matcher.group(1));
  }

  /** {@code bucket:[...]} of the buckets Asset owns (not those of its subtype). */
  private String ownBuckets() {
    final List<Bucket> buckets = getServerDatabase(0, getDatabaseName()).getSchema().getType(SUPER).getBuckets(false);
    final StringBuilder target = new StringBuilder("bucket:[");
    for (int i = 0; i < buckets.size(); i++)
      target.append(i > 0 ? ", " : "").append('`').append(buckets.get(i).getName()).append('`');
    return target.append(']').toString();
  }

  /** Root's GROUP BY over Asset's own records, the records without a value under {@link FacetOracle#MISSING}. */
  private Map<Object, Long> groupBy(final String property, final String where) {
    return groupBy("SELECT " + property + " AS v, count(*) AS c FROM " + SUPER + " WHERE @type = '" + SUPER + "'" + (where == null ? ""
        : " AND (" + where + ")") + " GROUP BY " + property);
  }

  private Map<Object, Long> polymorphicGroupBy(final String property) {
    return groupBy("SELECT " + property + " AS v, count(*) AS c FROM " + SUPER + " GROUP BY " + property);
  }

  private Map<Object, Long> groupBy(final String sql) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Object value = row.getProperty("v");
        counts.put(value == null ? FacetOracle.MISSING : value, row.<Number>getProperty("c").longValue());
      }
    }
    return counts;
  }

  /** The counts of a facet by value (numbers as their long value), the records without a value under {@link FacetOracle#MISSING}. */
  private static Map<Object, Long> counts(final FacetResult result) {
    final Map<Object, Long> counts = new HashMap<>();
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value() instanceof Number n ? (Object) n.intValue() : bucket.value(), bucket.count());
    if (result.missing() > 0)
      counts.put(FacetOracle.MISSING, result.missing());
    return counts;
  }

  private static long includeCount(final FacetResult result, final Object value) {
    for (final FacetBucket bucket : result.buckets())
      if (value.equals(bucket.value()))
        return bucket.count();
    throw new AssertionError("INCLUDE value " + value + " not returned: " + result.toJSON());
  }
}
