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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.parser.StatementCache;
import com.arcadedb.query.sql.parser.UpdateStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL parameter binding (positional, named, Postgres-style {@code $N}, a sole Map value) and the statement / execution plan
 * caches that must key, reuse and invalidate plans without mixing up parameters or projection aliases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlParameterPlanCacheTest extends TestHelper {

  // ---- #8260: the default alias of a projection item is cached on the parsed node ----
  // ProjectionItem.getProjectionAlias() rebuilt and escaped a fresh Identifier on every call for a projection with no explicit
  // AS, which on a full-scan GROUP BY runs once per record for nothing. The default alias is now cached on the ProjectionItem
  // node after the first computation.
  // The parsed ProjectionItem nodes are reused across repeated executions of the same statement (the SQL statement cache), so
  // this pins that the cache does not go stale, and does not leak between distinct queries that happen to share the query
  // engine's cache, or between the pre-aggregate/aggregate/final split each GROUP BY copies a projection item into.

  private void loadSaleFixture() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Sale");
      database.command("SQL", "INSERT INTO Sale SET mode = 'AIR', amount = 10");
      database.command("SQL", "INSERT INTO Sale SET mode = 'AIR', amount = 20");
      database.command("SQL", "INSERT INTO Sale SET mode = 'SHIP', amount = 5");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 1");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 2");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 3");
    });
  }

  private Map<String, Long> groupCounts(final String query, final String keyProperty, final String countProperty) {
    final Map<String, Long> result = new LinkedHashMap<>();
    try (ResultSet rs = database.query("SQL", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.put(r.getProperty(keyProperty), ((Number) r.getProperty(countProperty)).longValue());
      }
    }
    return result;
  }

  /**
   * Regression test for issue #8260.
   */
  @Test
  void groupByWithoutExplicitAliasUsesThePropertyNameOnEveryRow() {
    loadSaleFixture();
    for (int run = 0; run < 3; run++) {
      // RUN THE SAME STATEMENT TEXT REPEATEDLY: the parsed ProjectionItem nodes come from the SQL statement cache,
      // so a stale or cross-execution-leaked cached alias would surface on a later run, not necessarily the first.
      final Map<String, Long> counts = groupCounts("SELECT mode, count(*) AS n FROM Sale GROUP BY mode", "mode", "n");
      assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    }
  }

  /**
   * Regression test for issue #8260.
   */
  @Test
  void groupByOnAComputedExpressionWithNoAliasDefaultsToItsOwnText() {
    loadSaleFixture();
    // `mode` alone is a base identifier and takes the other branch of Expression.getDefaultAlias(); `mode || ''`
    // is not, so this exercises the this.toString() branch of the same cached call.
    final ResultSet rs = database.query("SQL", "SELECT mode || '' FROM Sale GROUP BY mode || '' ORDER BY mode || ''");

    final List<String> defaultAliasColumns = new ArrayList<>();
    while (rs.hasNext())
      defaultAliasColumns.addAll(rs.next().getPropertyNames());

    // every row exposes the same default-alias column name - a stale/misrendered cache would show up as a
    // different (or missing) column name on a later row
    assertThat(defaultAliasColumns).isNotEmpty();
    assertThat(defaultAliasColumns).allSatisfy(name -> assertThat(name).isEqualTo("mode || ''"));
  }

  /**
   * Regression test for issue #8260.
   */
  @Test
  void distinctQueriesWithTheSameUnaliasedExpressionDoNotShareAStaleAlias() {
    loadSaleFixture();
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE OtherSale");
      database.command("SQL", "INSERT INTO OtherSale SET mode = 'TRUCK', amount = 99");
    });

    final Map<String, Long> first = groupCounts("SELECT mode, count(*) AS n FROM Sale GROUP BY mode", "mode", "n");
    final Map<String, Long> second = groupCounts("SELECT mode, count(*) AS n FROM OtherSale GROUP BY mode", "mode", "n");

    assertThat(first).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    assertThat(second).containsExactlyInAnyOrderEntriesOf(Map.of("TRUCK", 1L));
  }

  /**
   * Regression test for issue #8260.
   */
  @Test
  void unaliasedAggregateProjectionSharesTheCachedIdentifierWithItsSplitCopySafely() {
    loadSaleFixture();
    // count(*) WITHOUT "AS": splitForAggregation() copies this ProjectionItem into a pre-aggregate/aggregate/final
    // split, and the final item's explicit alias is set from the ORIGINAL item's (now cached) default alias
    // (ProjectionItem.splitForAggregation: `result.alias = getProjectionAlias();`), so the two items end up sharing
    // the same Identifier instance. Run it more than once to also exercise that shared instance across repeated
    // executions of the same cached statement.
    for (int run = 0; run < 3; run++) {
      final Map<String, Long> counts = groupCounts("SELECT mode, count(*) FROM Sale GROUP BY mode", "mode", "count(*)");
      assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    }
  }

  // ---- #9046: a sole positional Map parameter ----
  // A positional parameter that is a Map was lost when it was the only parameter of a SELECT, because the statement took it
  // for the named-parameter map.

  private void loadMapParameterFixture() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.id INTEGER");
      database.command("sql", "INSERT INTO T SET id = 1, m = {'a': 1}");
    });
  }

  /**
   * Regression test for issue #9046.
   */
  @Test
  void soleMapIsBoundToThePlaceholder() {
    loadMapParameterFixture();
    final Map<String, Object> m = Map.of("a", 1);
    try (final ResultSet rs = database.query("sql", "SELECT ? AS v", (Object) m)) {
      assertThat(rs.next().<Map<String, Object>>getProperty("v")).isEqualTo(m);
    }
  }

  /**
   * Regression test for issue #9046.
   */
  @Test
  void soleMapInWhere() {
    loadMapParameterFixture();
    final Map<String, Object> m = Map.of("a", 1);
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE m = ?", (Object) m)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
  }

  /**
   * Regression test for issue #9046.
   */
  @Test
  void mapWithNumericKeyIsNotReadAsParameterZero() {
    loadMapParameterFixture();
    try (final ResultSet rs = database.query("sql", "SELECT ? AS v", (Object) Map.of("0", "x"))) {
      assertThat(rs.next().<Object>getProperty("v")).isEqualTo(Map.of("0", "x"));
    }
  }

  /**
   * Regression test for issue #9046.
   */
  @Test
  void namedMapOverloadStillWorks() {
    loadMapParameterFixture();
    try (final ResultSet rs = database.query("sql", "SELECT :p AS v", Map.of("p", 5))) {
      assertThat(rs.next().<Integer>getProperty("v")).isEqualTo(5);
    }
  }

  // ---- #9207: UPDATE and DELETE take their source plan from the execution plan cache ----
  // UPDATE and DELETE planned their WHERE as a SELECT with the plan cache off, so every execution paid the planning. The read
  // side now comes from the execution plan cache, keyed on the text of the synthetic SELECT.

  private static final String PART_SOURCE = "SELECT FROM Part WHERE p_partkey = :k";

  private void loadPartFixture() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Part");
      database.command("sql", "CREATE PROPERTY Part.p_partkey LONG");
      database.command("sql", "CREATE INDEX ON Part (p_partkey) UNIQUE");
      for (long k = 0; k < 20; k++)
        database.command("sql", "INSERT INTO Part SET p_partkey = ?, stock = 100", k);
    });
  }

  private int stock(final long key) {
    try (final ResultSet rs = database.query("sql", "SELECT stock FROM Part WHERE p_partkey = ?", key)) {
      return rs.next().<Integer>getProperty("stock");
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void updateReusesTheCachedSourcePlanWithTheRightParameters() {
    loadPartFixture();
    for (int run = 0; run < 5; run++)
      database.transaction(() -> database.command("sql", "UPDATE Part SET stock = stock - 1 WHERE p_partkey = :k", Map.of("k", 0L)));
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(PART_SOURCE)).isTrue();

    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = stock - 1 WHERE p_partkey = :k", Map.of("k", 7L)));
    assertThat(stock(0L)).isEqualTo(95);
    assertThat(stock(7L)).isEqualTo(99);
    assertThat(stock(8L)).isEqualTo(100);
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void deleteReusesTheCachedSourcePlanWithTheRightParameters() {
    loadPartFixture();
    for (long k = 0; k < 3; k++) {
      final long key = k;
      database.transaction(() -> database.command("sql", "DELETE FROM Part WHERE p_partkey = :k", Map.of("k", key)));
    }
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(PART_SOURCE)).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(17L);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void cachedSourcePlanIsDroppedOnSchemaChange() {
    loadPartFixture();
    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 1 WHERE p_partkey = :k", Map.of("k", 1L)));
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(PART_SOURCE)).isTrue();
    database.command("sql", "DROP INDEX `Part[p_partkey]`");
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(PART_SOURCE)).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 2 WHERE p_partkey = :k", Map.of("k", 1L)));
    assertThat(stock(1L)).isEqualTo(2);
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void cachedFullScanSourcePlanBecomesAnIndexPlanAfterCreateIndex() {
    loadPartFixture();
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Tmp");
      database.command("sql", "CREATE PROPERTY Tmp.k LONG");
      for (long k = 0; k < 10; k++)
        database.command("sql", "INSERT INTO Tmp SET k = ?, v = 0", k);
    });
    for (int run = 0; run < 2; run++)
      database.transaction(() -> database.command("sql", "UPDATE Tmp SET v = v + 1 WHERE k = :k", Map.of("k", 3L)));
    final String source = "SELECT FROM Tmp WHERE k = :k";
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(source)).isTrue();
    database.command("sql", "CREATE INDEX ON Tmp (k) NOTUNIQUE");
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains(source)).isFalse();
    database.transaction(() -> database.command("sql", "UPDATE Tmp SET v = v + 1 WHERE k = :k", Map.of("k", 3L)));
    try (final ResultSet rs = database.query("sql", "SELECT v FROM Tmp WHERE k = 3")) {
      assertThat(rs.next().<Integer>getProperty("v")).isEqualTo(3);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void typeDropAndRecreateDoesNotServeAStaleSourcePlan() {
    loadPartFixture();
    database.transaction(() -> database.command("sql", "DELETE FROM Part WHERE p_partkey = :k", Map.of("k", 1L)));
    database.transaction(() -> database.command("sql", "DELETE FROM Part WHERE p_partkey = :k", Map.of("k", 2L)));
    database.command("sql", "DROP TYPE Part UNSAFE");
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Part");
      database.command("sql", "INSERT INTO Part SET p_partkey = 2, stock = 5");
    });
    database.transaction(() -> database.command("sql", "DELETE FROM Part WHERE p_partkey = :k", Map.of("k", 2L)));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void halloweenGuardStillMaterializesOnACacheHit() {
    loadPartFixture();
    // the second execution takes the source plan from the cache: rows moved ahead of the index walk must not be revisited
    for (int run = 0; run < 2; run++) {
      database.transaction(() -> database.command("sql", "UPDATE Part SET p_partkey = p_partkey + 100 WHERE p_partkey >= 10"));
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(20L);
      }
    }
    try (final ResultSet rs = database.query("sql", "SELECT min(p_partkey) AS lo, max(p_partkey) AS hi FROM Part WHERE p_partkey >= 10")) {
      final Result r = rs.next();
      assertThat(r.<Long>getProperty("lo")).isEqualTo(210L);
      assertThat(r.<Long>getProperty("hi")).isEqualTo(219L);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void limitAndTimeoutAreNotLeakedThroughTheCache() {
    loadPartFixture();
    for (int run = 0; run < 2; run++) {
      database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 0 WHERE p_partkey >= 10 LIMIT 3 TIMEOUT 60000"));
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part WHERE stock = 0")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
      }
    }
    database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 7 WHERE p_partkey >= 10"));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part WHERE stock = 7")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(10L);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  void parameterDependentWhereStaysCorrectAcrossRuns() {
    loadPartFixture();
    for (long k = 0; k < 4; k++) {
      final long key = k;
      database.transaction(() -> database.command("sql", "UPDATE Part SET stock = 1 WHERE p_partkey IN [:a, :b]", Map.of("a", key, "b", key + 10)));
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Part WHERE stock = 1")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(8L);
    }
  }

  /**
   * Regression test for issue #9207.
   */
  @Test
  @Timeout(120)
  void sameCachedUpdateRunConcurrently() throws Exception {
    loadPartFixture();
    final int threads = 4;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        final long key = t;
        futures.add(pool.submit(() -> {
          for (int i = 0; i < 25; i++)
            database.transaction(() -> database.command("sql", "UPDATE Part SET stock = stock - 1 WHERE p_partkey = :k", Map.of("k", key)));
        }));
      }
      for (final Future<?> f : futures)
        f.get();
    } finally {
      pool.shutdown();
    }
    for (long k = 0; k < threads; k++)
      assertThat(stock(k)).isEqualTo(75);
  }

  // ---- #9245: the source plan of an UPDATE or DELETE is keyed on its parameter positions ----
  // The source plan of an UPDATE or DELETE was cached under a text that prints every positional parameter as ?, so statements
  // reading the same WHERE from different parameter positions shared one plan.

  private void createIndexedSkuType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".sku STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".brand STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (sku) UNIQUE");
    database.transaction(() -> {
      database.newDocument(type).set("sku", "S1").set("brand", "b1").save();
      database.newDocument(type).set("sku", "S2").set("brand", "b2").save();
      database.newDocument(type).set("sku", "NEW").set("brand", "b3").save();
    });
  }

  private long command(final String sql, final Object... params) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql, params)) {
        count[0] = ((Number) rs.next().getProperty("count")).longValue();
      }
    });
    return count[0];
  }

  private String select(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> out = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("sku") + ":" + r.getProperty("brand"));
      }
      return String.join(" ", out);
    }
  }

  private String rows(final String type) {
    return select("SELECT sku, brand FROM " + type + " ORDER BY sku");
  }

  private static String keyOf(final UpdateStatement statement) {
    return statement.getSourcePlanKey().resolve(statement.getTarget(), statement.getWhereClause(), null, 0L);
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void updateAfterSelectWithSameWhere() {
    createIndexedSkuType("A");
    select("SELECT FROM A WHERE sku = ?", "S1");
    assertThat(command("UPDATE A SET brand = ? WHERE sku = ?", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("A")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void updateAfterUpdateWithDifferentSetParameterCount() {
    createIndexedSkuType("B");
    command("UPDATE B SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(command("UPDATE B SET category = ?, brand = ? WHERE sku = ?", "c-new", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("B")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void selectAfterUpdateWithSameWhere() {
    createIndexedSkuType("C");
    command("UPDATE C SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(select("SELECT FROM C WHERE sku = ?", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void deleteAfterUpdateWithSameWhere() {
    createIndexedSkuType("D");
    command("UPDATE D SET brand = ? WHERE sku = ?", "b1", "S1");
    assertThat(command("DELETE FROM D WHERE sku = ?", "S2")).isEqualTo(1L);
    assertThat(rows("D")).isEqualTo("NEW:b3 S1:b1");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void updateAfterDeleteWithSameWhere() {
    createIndexedSkuType("E");
    command("DELETE FROM E WHERE sku = ?", "S1");
    assertThat(command("UPDATE E SET brand = ? WHERE sku = ?", "NEW", "S2")).isEqualTo(1L);
    assertThat(rows("E")).isEqualTo("NEW:b3 S2:NEW");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void twoPropertiesWithoutIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE I");
    database.transaction(() -> {
      database.newDocument("I").set("sku", "S1").set("brand", "b1").save();
      database.newDocument("I").set("sku", "S2").set("brand", "b2").save();
      database.newDocument("I").set("sku", "NEW").set("brand", "b3").save();
    });
    select("SELECT FROM I WHERE sku = ? AND brand = ?", "S1", "b1");
    assertThat(command("UPDATE I SET brand = ? WHERE sku = ? AND brand = ?", "NEW", "S2", "b2")).isEqualTo(1L);
    assertThat(rows("I")).isEqualTo("NEW:b3 S1:b1 S2:NEW");
  }

  /**
   * Regression test for issue #9245.
   */
  @Test
  void reparsedStatementsOfTheSameTextShareOneKeyAndDifferentPositionsDoNot() {
    final String sql = "UPDATE K SET brand = ? WHERE sku = ?";
    // two parses of the same text are two statement objects, as after an eviction from the statement cache
    final UpdateStatement first = (UpdateStatement) new StatementCache(database, 0).get(sql);
    final UpdateStatement second = (UpdateStatement) new StatementCache(database, 0).get(sql);
    final UpdateStatement other = (UpdateStatement) new StatementCache(database, 0).get("UPDATE K SET brand = ?, category = ? WHERE sku = ?");
    assertThat(first).isNotSameAs(second);

    assertThat(keyOf(first)).isNotNull().isEqualTo(keyOf(second)).isNotEqualTo(keyOf(other));
  }

  // ---- #9247: a sqlscript statement is cached under its parameter positions ----
  // A statement of a sqlscript was cached under its printed text, which prints every positional parameter as ? whatever its
  // number, so statements that read the same text from different parameter positions shared one plan.

  private void createScriptSkuType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".sku STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".n INTEGER");
    database.transaction(() -> {
      database.newDocument(type).set("sku", "S1").set("brand", "b1").set("n", 1).save();
      database.newDocument(type).set("sku", "S2").set("brand", "b2").set("n", 2).save();
      database.newDocument(type).set("sku", "S3").set("brand", "b3").set("n", 3).save();
    });
  }

  private void createScriptRefType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument(type).set("sku", "S" + i).set("ref", "r" + i).save();
    });
  }

  private static String render(final ResultSet rs) {
    final List<String> out = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      out.add(r.getProperty("sku") + ":" + r.getProperty("brand"));
    }
    return out.isEmpty() ? "(none)" : String.join(" ", out);
  }

  private String script(final String script, final Object... params) {
    try (final ResultSet rs = database.command("sqlscript", script, params)) {
      return render(rs);
    }
  }

  private String scriptNamed(final String script, final Map<String, Object> params) {
    try (final ResultSet rs = database.command("sqlscript", script, params)) {
      return render(rs);
    }
  }

  private String renderQuery(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return render(rs);
    }
  }

  private String renderQueryNamed(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return render(rs);
    }
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void twoSelectsInOneScript() {
    createScriptSkuType("A");
    assertThat(script("SELECT FROM A WHERE sku = ?; SELECT FROM A WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void selectAloneThenScript() {
    createScriptSkuType("B");
    renderQuery("SELECT FROM B WHERE sku = ?", "S1");
    assertThat(script("SELECT FROM B WHERE brand = ?; SELECT FROM B WHERE sku = ?;", "b3", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void scriptThenSelectAlone() {
    createScriptSkuType("C");
    script("SELECT FROM C WHERE brand = ?; SELECT FROM C WHERE sku = ?;", "b3", "S1");
    assertThat(renderQuery("SELECT FROM C WHERE sku = ?", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void withUniqueIndex() {
    createScriptSkuType("D");
    database.command("sql", "CREATE INDEX ON D (sku) UNIQUE");
    assertThat(script("SELECT FROM D WHERE sku = ?; SELECT FROM D WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void lowerCaseSecondSelect() {
    createScriptSkuType("E");
    assertThat(script("SELECT FROM E WHERE sku = ?; select from E where sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void namedParametersAndLiterals() {
    createScriptSkuType("F");
    assertThat(scriptNamed("SELECT FROM F WHERE sku = :a; SELECT FROM F WHERE sku = :b;", Map.of("a", "S1", "b", "S2"))).isEqualTo("S2:b2");
    createScriptSkuType("G");
    assertThat(script("SELECT FROM G WHERE sku = 'S1'; SELECT FROM G WHERE sku = 'S2';")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void sameScriptRepeatedStaysRight() {
    createScriptSkuType("K");
    for (int i = 0; i < 3; i++)
      assertThat(script("SELECT FROM K WHERE sku = ?; SELECT FROM K WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
    // the keys are stable: both statements are in the plan cache, each under its own parameter numbers
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains("SELECT FROM K WHERE sku = ?")).isTrue();
    assertThat(((DatabaseInternal) database).getExecutionPlanCache().contains("SELECT FROM K WHERE sku = ? /*params:1*/")).isTrue();
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void inSubqueryNamedParameterBoundByPositionAlone() {
    createScriptSkuType("H");
    createScriptRefType("HR");
    assertThat(renderQuery("SELECT FROM H WHERE sku IN (SELECT sku FROM HR WHERE ref = :r)", "r2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void inSubqueryNamedParameterBoundByPosition() {
    createScriptSkuType("I");
    createScriptRefType("IR");
    renderQuery("SELECT FROM I WHERE n > :lo AND sku IN (SELECT sku FROM IR WHERE ref = :r)", 0, "r1");
    assertThat(renderQuery("SELECT FROM I WHERE sku IN (SELECT sku FROM IR WHERE ref = :r)", "r2")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void inSubqueryNamedParameterBoundByName() {
    createScriptSkuType("J");
    createScriptRefType("JR");
    renderQueryNamed("SELECT FROM J WHERE n > :lo AND sku IN (SELECT sku FROM JR WHERE ref = :r)", Map.of("lo", 0, "r", "r1"));
    assertThat(renderQueryNamed("SELECT FROM J WHERE sku IN (SELECT sku FROM JR WHERE ref = :r)", Map.of("r", "r2"))).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void dmlInScriptWithOffsetParameter() {
    createScriptSkuType("L");
    for (int i = 0; i < 2; i++) {
      database.transaction(() -> database.command("sqlscript",
          "UPDATE L SET brand = 'x' WHERE sku = ?; UPDATE L SET brand = 'y' WHERE sku = ?;", "S1", "S2").close());
      assertThat(renderQuery("SELECT FROM L ORDER BY sku")).isEqualTo("S1:x S2:y S3:b3");
      database.transaction(() -> database.command("sqlscript",
          "UPDATE L SET brand = 'b1' WHERE sku = 'S1'; UPDATE L SET brand = 'b2' WHERE sku = 'S2';").close());
    }
    database.transaction(() -> database.command("sqlscript", "DELETE FROM L WHERE sku = ?; DELETE FROM L WHERE sku = ?;", "S1", "S2").close());
    assertThat(renderQuery("SELECT FROM L")).isEqualTo("S3:b3");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void uncorrelatedAndParentInSubqueryUnchanged() {
    createScriptSkuType("M");
    createScriptRefType("MR");
    assertThat(renderQuery("SELECT FROM M WHERE sku IN (SELECT sku FROM MR WHERE ref = 'r2')")).isEqualTo("S2:b2");
    assertThat(renderQuery("SELECT FROM M WHERE sku IN (SELECT sku FROM MR WHERE ref = 'r' + $parent.$current.n)")).isEqualTo("S1:b1 S2:b2 S3:b3");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void letVariableReadInsideInSubquery() {
    createScriptSkuType("N");
    createScriptRefType("NR");
    assertThat(renderQuery("SELECT FROM N LET $x = 'r2' WHERE sku IN (SELECT sku FROM NR WHERE ref = $x)")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void literalWithColonOrQuestionMarkHasNoSuffix() {
    createScriptSkuType("O");
    assertThat(script("SELECT FROM O WHERE brand = 'a:b?'; SELECT FROM O WHERE sku = 'S2';")).isEqualTo("S2:b2");
  }

  /**
   * Regression test for issue #9247.
   */
  @Test
  void firstScriptStatementSharesTheEntryOfTheSameStatementRunAlone() {
    createScriptSkuType("P");
    script("SELECT FROM P WHERE sku = ?; SELECT FROM P WHERE brand = ?;", "S1", "b2");
    final DatabaseInternal db = (DatabaseInternal) database;
    // numbered from 0 like a statement parsed alone: no suffix, so it is the key a standalone run uses too
    assertThat(db.getExecutionPlanCache().contains("SELECT FROM P WHERE sku = ?")).isTrue();
    assertThat(db.getExecutionPlanCache().contains("SELECT FROM P WHERE brand = ? /*params:1*/")).isTrue();
    assertThat(db.getExecutionPlanCache().contains("SELECT FROM P WHERE brand = ?")).isFalse();
  }

  // ---- #9248: Postgres-style $N parameters on every evaluation path ----
  // A Postgres-style $N parameter was read only by the Result overload of BaseExpression.execute, so every path evaluating
  // through the Identifiable overload got null.

  private void loadDollarParameterFixture(final boolean index) {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.sku STRING");
    if (index)
      database.command("sql", "CREATE INDEX ON T (sku) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.name STRING");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("T").set("sku", "S" + i).save();
      final MutableVertex a = database.newVertex("V").set("name", "A").save();
      final MutableVertex b = database.newVertex("V").set("name", "B").save();
      database.newVertex("V").set("name", "C").save();
      a.newEdge("E", b);
    });
  }

  /**
   * Regression test for issue #9248.
   */
  @Test
  void createEdgeSet() {
    loadDollarParameterFixture(false);
    final Object[] params = new Object[3];
    database.transaction(() -> {
      params[0] = database.query("sql", "SELECT FROM V WHERE name = 'B'").next().getIdentity().get();
      params[1] = database.query("sql", "SELECT FROM V WHERE name = 'C'").next().getIdentity().get();
      params[2] = 9;
      try (final ResultSet rs = database.command("sql", "CREATE EDGE E FROM $1 TO $2 SET w = $3", params)) {
        final Edge e = rs.next().getEdge().get();
        assertThat(e.get("w")).isEqualTo(9);
      }
    });
  }

  /**
   * Regression test for issue #9248.
   */
  @Test
  void functionArgumentOnRecord() {
    loadDollarParameterFixture(false);
    try (final ResultSet rs = database.query("sql", "SELECT sku FROM T WHERE sku = coalesce($1, 'S1')", "S2")) {
      assertThat(rs.next().<String>getProperty("sku")).isEqualTo("S2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #9248.
   */
  @Test
  void functionArgumentOnRecordWithIndex() {
    loadDollarParameterFixture(true);
    try (final ResultSet rs = database.query("sql", "SELECT sku FROM T WHERE sku = coalesce($1, 'S1')", "S2")) {
      assertThat(rs.next().<String>getProperty("sku")).isEqualTo("S2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * Regression test for issue #9248.
   */
  @Test
  void functionInProjection() {
    loadDollarParameterFixture(false);
    try (final ResultSet rs = database.query("sql", "SELECT sku, coalesce($1, 'none') AS v FROM T WHERE sku = 'S1'", "x")) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("x");
    }
  }

  /**
   * Regression test for issue #9248.
   */
  @Test
  void matchFilterAfterHop() {
    loadDollarParameterFixture(false);
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: V, as: a, where: (name = $1)}.out('E'){as: b, where: (name = $2)} RETURN b.name AS name", "A", "B")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("B");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- #10721: positional values are counted against the placeholders of the statement ----
  // Positional values were never counted against the placeholders of the statement. A surplus value was dropped without a
  // word, so a list spread into two values for one IN ? answered one row where the caller meant two, and a missing value was
  // bound to null. Both are refused before the statement runs, with the two counts in the message.

  private final RID[] rids = new RID[3];

  private void loadPositionalCountFixture() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.transaction(() -> {
      for (int k = 0; k < 3; k++)
        rids[k] = database.newVertex("V").set("k", k).save().getIdentity();
    });
  }

  private long countRows(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.stream().count();
    }
  }

  private static void assertRefused(final Runnable call, final int placeholders, final int values) {
    assertThatThrownBy(call::run).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining(placeholders + " positional parameter")
        .hasMessageContaining(values + " value");
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void aSurplusValueIsRefused() {
    loadPositionalCountFixture();
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ?", 0, 1), 1, 2);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ?", 1, 0), 1, 2);
    assertRefused(() -> countRows("SELECT FROM V WHERE @rid IN ?", rids[0], rids[1]), 1, 2);
    assertRefused(() -> countRows("SELECT FROM V", 1), 0, 1);
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void aMissingValueIsRefused() {
    loadPositionalCountFixture();
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ? AND k < ?", 1), 2, 1);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ? OR k = ?", 1), 2, 1);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ?"), 1, 0);
    // a named placeholder takes a positional value by its position, so it counts as one too
    assertRefused(() -> countRows("SELECT FROM V WHERE k = :a OR k = :b", 1), 2, 1);
    // a $N does not excuse a missing value of a ? before it, nor a surplus value past the highest position
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ? OR k = ? OR k = $2"), 2, 0);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = $2", 0, 2, 1), 0, 3);
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void aWriteIsRefusedBeforeItRuns() {
    loadPositionalCountFixture();
    database.transaction(() -> assertRefused(() -> database.command("sql", "UPDATE V SET hit = true WHERE k = ?", 0, 1).close(), 1, 2));
    assertThat(countRows("SELECT FROM V WHERE hit = true")).isZero();

    database.transaction(() -> assertRefused(() -> database.command("sql", "UPDATE V SET hit = ? WHERE k = ?", true).close(), 2, 1));
    assertThat(countRows("SELECT FROM V WHERE hit = true")).isZero();
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void aStatementWithLiteralsExtractedByTheCacheCountsOnlyTheCallersPlaceholders() {
    loadPositionalCountFixture();
    // the literals 'x' and 1 become generated parameters of the cached statement: they take no positional value
    assertThat(countRows("SELECT FROM V WHERE k = ? AND 'x' = 'x' AND 1 = 1", 1)).isEqualTo(1);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ? AND 'x' = 'x' AND 1 = 1", 1, 2), 1, 2);
    // the same shape with other literals is the same cached statement: still one placeholder, whatever ran before it
    assertThat(countRows("SELECT FROM V WHERE k = ? AND 'y' = 'y' AND 2 = 2", 2)).isEqualTo(1);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = ? AND 'z' = 'z' AND 3 = 3"), 1, 0);
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void anExactBindingStillAnswers() {
    loadPositionalCountFixture();
    assertThat(countRows("SELECT FROM V WHERE @rid IN ?", List.of(rids[0], rids[1]))).isEqualTo(2);
    // a Java array is one value too, the list of its elements, on the RID fetch and on a scan
    assertThat(countRows("SELECT FROM V WHERE @rid IN ?", (Object) new RID[] { rids[0], rids[1] })).isEqualTo(2);
    assertThat(countRows("SELECT FROM V WHERE k IN ?", (Object) new int[] { 0, 2 })).isEqualTo(2);
    assertThat(countRows("SELECT FROM V WHERE k IN ?", (Object) new Integer[] { 0, 2 })).isEqualTo(2);
    assertThat(countRows("SELECT FROM V WHERE k = ? OR k = ?", 1, 2)).isEqualTo(2);
    assertThat(countRows("SELECT FROM V WHERE k = :a OR k = :b", 1, 2)).isEqualTo(2);
    assertThat(countRows("SELECT FROM V WHERE k IN (SELECT k FROM V WHERE k > ?) AND k < ?", 0, 2)).isEqualTo(1);
    assertThat(countRows("SELECT FROM V")).isEqualTo(3);
    // every place a placeholder can stand is counted: a function argument, a LET, a projection, a nested sub-query
    assertThat(countRows("SELECT FROM V WHERE k = abs(?)", -1)).isEqualTo(1);
    assertRefused(() -> countRows("SELECT FROM V WHERE k = abs(?)"), 1, 0);
    assertThat(countRows("SELECT $a FROM V LET $a = ? WHERE k = ?", "x", 1)).isEqualTo(1);
    assertRefused(() -> countRows("SELECT $a FROM V LET $a = ? WHERE k = ?", "x"), 2, 1);
    assertThat(countRows("SELECT ? AS p FROM V WHERE k IN (SELECT k FROM V WHERE k IN (SELECT k FROM V WHERE k = ?))", "x", 2)).isEqualTo(1);
    assertRefused(() -> countRows("SELECT ? AS p FROM V WHERE k IN (SELECT k FROM V WHERE k IN (SELECT k FROM V WHERE k = ?))", "x"), 2, 1);
    // an explicit null is a value
    assertThat(countRows("SELECT FROM V WHERE k = ? OR k = ?", 1, null)).isEqualTo(1);
    // the Postgres-style $N reads the value at its position, and a lower position may be left unread, as in Postgres
    assertThat(countRows("SELECT FROM V WHERE k = $2", 0, 2)).isEqualTo(1);
    assertThat(countRows("SELECT FROM V WHERE k = ? OR k = $3", 1, null, 2)).isEqualTo(2);
    // named values are not counted: a map binds by name
    try (final ResultSet rs = database.query("sql", "SELECT FROM V WHERE k = :a", Map.of("a", 1, "unused", 2))) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  /**
   * Regression test for issue #10721.
   */
  @Test
  void aScriptCountsThePlaceholdersOfAllItsStatements() {
    loadPositionalCountFixture();
    try (final ResultSet rs = database.command("sqlscript", "LET a = SELECT FROM V WHERE k = ?; SELECT FROM V WHERE k = ?;", 1, 2)) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
    assertRefused(() -> database.command("sqlscript", "LET a = SELECT FROM V WHERE k = ?; SELECT FROM V WHERE k = ?;", 1).close(), 2,
        1);
    assertRefused(() -> database.command("sqlscript", "SELECT FROM V WHERE k = ?;", 1, 2).close(), 1, 2);
  }
}
