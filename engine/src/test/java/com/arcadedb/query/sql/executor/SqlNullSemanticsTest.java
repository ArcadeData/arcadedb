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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.parser.ContainsKeyOperator;
import com.arcadedb.query.sql.parser.ContainsValueOperator;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.ILikeOperator;
import com.arcadedb.query.sql.parser.InOperator;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LikeOperator;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.NeOperator;
import com.arcadedb.query.sql.parser.NeqOperator;
import com.arcadedb.query.sql.parser.NullSafeEqualsCompareOperator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL NULL semantics: the three-valued logic of comparisons, IN, LIKE and arithmetic on a NULL operand, and the NULL keys an
 * index holds (NULL_STRATEGY INDEX and SKIP, unique, hash and composite keys) or a cached plan re-run with a NULL parameter must
 * answer exactly as a scan does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlNullSemanticsTest extends TestHelper {
  private static final String[][] UNIQUE_IS_NULL_TYPES = { { "Scan", null }, { "NotUniqueIdx", "NOTUNIQUE" }, { "UniqueIdx", "UNIQUE" },
      { "UniqueHashIdx", "UNIQUE_HASH" }, { "NotUniqueHashIdx", "NOTUNIQUE_HASH" } };
  private static final String[] EQUALS_NULL_TYPES = { "Scan", "Lsm", "Composite", "CompositeP", "Hash", "Err" };

  // ---- #8833: upper-bound-only range on a NULL_STRATEGY INDEX index ----
  // Regression guard for #8833: a range with only an upper bound on a {@code NULL_STRATEGY INDEX} index started at the first key,
  // where the null keys sort, and returned the records with no value.

  private void createNullStrategyIndexFixture() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.n LONG");
    database.command("sql", "CREATE INDEX ON T (n) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newVertex("T").set("n", i).save();
      for (int i = 0; i < 3; i++)
        database.newVertex("T").save();
    });
  }

  private long countWhereOnNullIndex(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE " + where)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private List<Object> nullIndexValues(final String sql) {
    final List<Object> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("n"));
    }
    return got;
  }

  /** Regression test for issue #8833. */
  @Test
  void upperBoundOnlyExcludesNullKeys() {
    createNullStrategyIndexFixture();
    assertThat(countWhereOnNullIndex("n < 2")).isEqualTo(2);
    assertThat(countWhereOnNullIndex("n <= 0")).isEqualTo(1);
    assertThat(countWhereOnNullIndex("n < 0")).isZero();
    assertThat(countWhereOnNullIndex("n >= 0 AND n < 2")).isEqualTo(2);
    assertThat(countWhereOnNullIndex("n BETWEEN -1 AND 1")).isEqualTo(2);
    assertThat(countWhereOnNullIndex("n IS NULL")).isEqualTo(3);
  }

  /** Regression test for issue #8833. */
  @Test
  void upperBoundOnlyRowsAndOrder() {
    createNullStrategyIndexFixture();
    assertThat(nullIndexValues("SELECT n FROM T WHERE n < 2")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(nullIndexValues("SELECT n FROM T WHERE n <= 0 ORDER BY n LIMIT 1")).containsExactly(0L);
    assertThat(nullIndexValues("SELECT n FROM T WHERE n < 3 ORDER BY n DESC")).containsExactly(2L, 1L, 0L);
    assertThat(nullIndexValues("SELECT min(n) AS n FROM T WHERE n < 5")).containsExactly(0L);
    assertThat(nullIndexValues("SELECT n FROM T WHERE n < 0 ORDER BY n DESC")).isEmpty();
  }

  /** Regression test for issue #8833. */
  @Test
  void compositeIndexUpperBoundOnLeadingColumnExcludesNullKeys() {
    createNullStrategyIndexFixture();
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE PROPERTY C.a LONG");
    database.command("sql", "CREATE PROPERTY C.b LONG");
    database.command("sql", "CREATE INDEX ON C (a, b) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 10; i++)
        database.newVertex("C").set("a", i, "b", i).save();
      for (long i = 0; i < 3; i++)
        database.newVertex("C").set("b", i).save(); // a is null, b is not
    });
    assertThat(nullIndexValues("SELECT a AS n FROM C WHERE a < 2")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(nullIndexValues("SELECT a AS n FROM C WHERE a < 3 ORDER BY a ASC")).containsExactly(0L, 1L, 2L);
    assertThat(nullIndexValues("SELECT a AS n FROM C WHERE a < 3 ORDER BY a DESC")).containsExactly(2L, 1L, 0L);
  }

  /** Regression test for issue #8833. */
  @Test
  void compositeIndexUpperBoundOnLaterColumnExcludesNullKeys() {
    createNullStrategyIndexFixture();
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.a LONG");
    database.command("sql", "CREATE PROPERTY D.b LONG");
    database.command("sql", "CREATE INDEX ON D (a, b) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 5; i++)
        database.newVertex("D").set("a", 5L, "b", i).save();
      for (int i = 0; i < 3; i++)
        database.newVertex("D").set("a", 5L).save(); // b is null
    });
    assertThat(nullIndexValues("SELECT b AS n FROM D WHERE a = 5 AND b < 3")).containsExactlyInAnyOrder(0L, 1L, 2L);
    assertThat(nullIndexValues("SELECT b AS n FROM D WHERE a = 5 AND b < 3 ORDER BY b DESC")).containsExactly(2L, 1L, 0L);
  }

  /** Regression test for issue #8833. */
  @Test
  void boundedBothSidesOnLaterColumnStillWorks() {
    createNullStrategyIndexFixture();
    database.command("sql", "CREATE VERTEX TYPE E");
    database.command("sql", "CREATE PROPERTY E.a LONG");
    database.command("sql", "CREATE PROPERTY E.b LONG");
    database.command("sql", "CREATE INDEX ON E (a, b) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (long i = 0; i < 5; i++)
        database.newVertex("E").set("a", 5L, "b", i).save();
      database.newVertex("E").set("a", 5L).save();
    });
    assertThat(nullIndexValues("SELECT b AS n FROM E WHERE a = 5 AND b > 1 AND b < 4")).containsExactlyInAnyOrder(2L, 3L);
  }

  /** Regression test for issue #8833. */
  @Test
  void stringKeyAndUniqueIndexExcludeNullKeys() {
    createNullStrategyIndexFixture();
    database.command("sql", "CREATE VERTEX TYPE S");
    database.command("sql", "CREATE PROPERTY S.s STRING");
    database.command("sql", "CREATE INDEX ON S (s) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE VERTEX TYPE U");
    database.command("sql", "CREATE PROPERTY U.n LONG");
    database.command("sql", "CREATE INDEX ON U (n) UNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (final String v : List.of("a", "b", "c"))
        database.newVertex("S").set("s", v).save();
      database.newVertex("S").save();
      for (long i = 0; i < 5; i++)
        database.newVertex("U").set("n", i).save();
      database.newVertex("U").save();
    });
    assertThat(nullIndexValues("SELECT s AS n FROM S WHERE s < 'c' ORDER BY s")).containsExactly("a", "b");
    assertThat(nullIndexValues("SELECT n FROM U WHERE n < 3 ORDER BY n")).containsExactly(0L, 1L, 2L);
  }

  // ---- #8978: composite NULL_STRATEGY SKIP index with a null leading key ----
  // Regression guard for #8978: a composite index with the default NULL_STRATEGY SKIP holds a key whose first property is null
  // when a later one is not, so ORDER BY on the leading property returned those records twice and an upper-bound-only range
  // returned them as matches.

  private void createCompositeSkipFixture() {
    for (final String type : new String[] { "Plain", "Composite" }) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".b INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    }
    database.command("sql", "CREATE INDEX ON Composite (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "Plain", "Composite" }) {
        database.newVertex(type).set("name", "r1", "a", 1, "b", 1).save();
        database.newVertex(type).set("name", "r2", "a", 2, "b", null).save();
        database.newVertex(type).set("name", "r3", "a", null, "b", 3).save();
        database.newVertex(type).set("name", "r4", "a", null, "b", null).save();
        database.newVertex(type).set("name", "r5", "b", 5).save();
      }
    });
  }

  private List<String> compositeNames(final String sql) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  private long compositeCount(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  /** Regression test for issue #8978. */
  @Test
  void orderByLeadingPropertyReturnsEachRecordOnce() {
    createCompositeSkipFixture();
    final List<String> got = compositeNames("SELECT name FROM Composite ORDER BY a");
    assertThat(got).hasSize(5).containsExactlyInAnyOrder("r1", "r2", "r3", "r4", "r5");
    assertThat(got.subList(3, 5)).containsExactly("r1", "r2");

    final List<String> desc = compositeNames("SELECT name FROM Composite ORDER BY a DESC");
    assertThat(desc).hasSize(5).containsExactlyInAnyOrder("r1", "r2", "r3", "r4", "r5");
    assertThat(desc.subList(0, 2)).containsExactly("r2", "r1");
  }

  /** Regression test for issue #8978. */
  @Test
  void orderByAllIndexedPropertiesSortsTheNullRows() {
    createCompositeSkipFixture();
    assertThat(compositeNames("SELECT name FROM Composite ORDER BY a, b")).isEqualTo(compositeNames("SELECT name FROM Plain ORDER BY a, b"));
    assertThat(compositeNames("SELECT name FROM Composite ORDER BY a, b")).containsExactly("r4", "r3", "r5", "r1", "r2");
    assertThat(compositeNames("SELECT name FROM Composite ORDER BY a DESC, b DESC")).containsExactly("r2", "r1", "r5", "r3", "r4");
  }

  /** Regression test for issue #8978. */
  @Test
  void groupByLeadingPropertyCountsTheNullGroupOnce() {
    createCompositeSkipFixture();
    try (final ResultSet rs = database.query("sql", "SELECT a, count(*) AS n FROM Composite GROUP BY a ORDER BY a")) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext())
        counts.add(rs.next().<Number>getProperty("n").longValue());
      assertThat(counts).containsExactly(3L, 1L, 1L);
    }
  }

  /** Regression test for issue #8978. */
  @Test
  void upperBoundOnlyRangeSkipsLeadingNullKeys() {
    createCompositeSkipFixture();
    assertThat(compositeNames("SELECT name FROM Composite WHERE a < 5")).containsExactlyInAnyOrder("r1", "r2");
    assertThat(compositeCount("SELECT count(*) AS n FROM Composite WHERE a <= 2")).isEqualTo(2);
    assertThat(compositeCount("SELECT count(*) AS n FROM Composite WHERE a < 0")).isZero();
    assertThat(compositeNames("SELECT name FROM Composite WHERE a < 5 ORDER BY a DESC")).containsExactly("r2", "r1");
    assertThat(compositeNames("SELECT name FROM Composite WHERE a IS NULL")).containsExactlyInAnyOrder("r3", "r4", "r5");
  }

  /** Regression test for issue #8978. */
  @Test
  void everyComparisonOperatorAgreesWithTheScan() {
    createCompositeSkipFixture();
    for (final String where : new String[] { "a < 2", "a <= 2", "a > 0", "a >= 1", "a <> 1", "a != 2", "a = 1", "a BETWEEN 1 AND 2",
        "NOT (a > 1)", "a <= 2 AND b IS NULL" }) {
      assertThat(compositeNames("SELECT name FROM Composite WHERE " + where + " ORDER BY name")).as(where)
          .isEqualTo(compositeNames("SELECT name FROM Plain WHERE " + where + " ORDER BY name"));
      assertThat(compositeNames("SELECT name FROM Composite WHERE " + where + " ORDER BY a DESC")).as(where + " DESC")
          .containsExactlyInAnyOrderElementsOf(compositeNames("SELECT name FROM Plain WHERE " + where + " ORDER BY name"));
    }
  }

  // ---- #8979: three-valued comparisons on a null operand ----
  // Regression guard for #8979: the SQL comparison operators answered false or true for a null operand instead of unknown, so a
  // NOT around them (or {@code <>} itself) turned a missing value into a match, disagreeing with {@code NOT IN}, openCypher and
  // the documented CASE behavior.

  private void createThreeValuedFixture() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.transaction(() -> {
      database.newVertex("T").set("name", "one", "k", 1).save();
      database.newVertex("T").set("name", "five", "k", 5).save();
      database.newVertex("T").set("name", "null", "k", null).save();
      database.newVertex("T").set("name", "missing").save();
    });
  }

  private List<String> namesWhere(final String where) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where + " ORDER BY name")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  /** Regression test for issue #8979. */
  @Test
  void negatedComparisonsLeaveOutTheRecordsWithoutAValue() {
    createThreeValuedFixture();
    for (final String where : new String[] { "k <> 5", "k != 5", "NOT (k = 5)", "k NOT IN [5]", "NOT (k IN [5])", "NOT (k > 2)",
        "NOT (k BETWEEN 4 AND 6)", "NOT (k >= 2)", "NOT (k > 1 AND k > 2)" })
      assertThat(namesWhere(where)).as(where).containsExactly("one");
  }

  /** Regression test for issue #8979. */
  @Test
  void comparisonWithNullMatchesNothing() {
    createThreeValuedFixture();
    for (final String where : new String[] { "k = null", "k <> null", "k != null", "k < null", "k >= null", "k >= nothing",
        "k <= nothing", "k > nothing", "k < nothing", "k = nothing", "k <> nothing" })
      assertThat(namesWhere(where)).as(where).isEmpty();
  }

  /** Regression test for issue #8979. */
  @Test
  void explicitNullTestsStillWork() {
    createThreeValuedFixture();
    assertThat(namesWhere("k IS NULL")).containsExactly("missing", "null");
    assertThat(namesWhere("k IS NOT NULL")).containsExactly("five", "one");
    assertThat(namesWhere("k <=> null")).containsExactly("missing", "null");
    assertThat(namesWhere("NOT (k IS NULL)")).containsExactly("five", "one");
    assertThat(namesWhere("NOT (k < 0)")).containsExactly("five", "one");
  }

  /** Regression test for issue #8979. */
  @Test
  void booleanLogicCarriesTheUnknown() {
    createThreeValuedFixture();
    assertThat(namesWhere("k <> 5 OR k = 5")).containsExactly("five", "one");
    assertThat(namesWhere("k = 5 OR k IS NULL")).containsExactly("five", "missing", "null");
    assertThat(namesWhere("NOT (k = 5 AND name = 'x')")).containsExactly("five", "missing", "null", "one");
  }

  /** Regression test for issue #8979. */
  @Test
  void caseAnswersNoForAMissingValue() {
    createThreeValuedFixture();
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, CASE WHEN k <> 5 THEN 'yes' ELSE 'no' END AS ne FROM T ORDER BY name")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        got.add(r.getProperty("name") + ":" + r.getProperty("ne"));
      }
    }
    assertThat(got).containsExactly("five:no", "missing:no", "null:no", "one:yes");
  }

  /** Regression test for issue #8979. */
  @Test
  void indexedAndOrderedPathsAgreeWithTheScan() {
    createThreeValuedFixture();
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
    assertThat(namesWhere("k <> 5")).containsExactly("one");
    assertThat(namesWhere("NOT (k > 2)")).containsExactly("one");
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE k <> 5 ORDER BY k")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("one");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Regression test for issue #8979. */
  @Test
  void unknownIsNotMetInModifierFilterAndScriptControlFlow() {
    createThreeValuedFixture();
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.transaction(() -> database.command("sql", "INSERT INTO D SET items = [{x: 1}, {x: 9}, {y: 2}, {x: null}]"));
    try (final ResultSet rs = database.query("sql", "SELECT items[x > 5] AS hit FROM D")) {
      final List<?> hit = rs.next().getProperty("hit");
      assertThat(hit).hasSize(1);
    }

    try (final ResultSet rs = database.command("sqlscript", "LET $v = null;\nIF ($v > 5) {\n  RETURN 'yes';\n}\nRETURN 'no';")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("no");
    }

    try (final ResultSet rs = database.command("sqlscript",
        "LET $v = null;\nLET $n = 0;\nWHILE ($v < 10) {\n  LET $n = $n + 1;\n}\nRETURN $n;")) {
      assertThat(rs.next().<Number>getProperty("value").intValue()).isZero();
    }
  }

  /** Regression test for issue #8979. */
  @Test
  void onlyTheValueComparisonsAreUnknownOnNull() {
    createThreeValuedFixture();
    assertThat(new EqualsCompareOperator().isUnknownOnNull()).isTrue();
    assertThat(new NeOperator().isUnknownOnNull()).isTrue();
    assertThat(new NeqOperator().isUnknownOnNull()).isTrue();
    assertThat(new LtOperator().isUnknownOnNull()).isTrue();
    assertThat(new LeOperator().isUnknownOnNull()).isTrue();
    assertThat(new GtOperator().isUnknownOnNull()).isTrue();
    assertThat(new GeOperator().isUnknownOnNull()).isTrue();
    assertThat(new NullSafeEqualsCompareOperator().isUnknownOnNull()).isFalse();
    assertThat(new LikeOperator().isUnknownOnNull()).isTrue(); // #10186
    assertThat(new ILikeOperator().isUnknownOnNull()).isTrue(); // #10186
    assertThat(new InOperator().isUnknownOnNull()).isFalse();
    assertThat(new ContainsKeyOperator().isUnknownOnNull()).isFalse();
    assertThat(new ContainsValueOperator().isUnknownOnNull()).isFalse();
  }

  /** Regression test for issue #8979. */
  @Test
  void multiValueFilterEvaluatesEachItem() {
    createThreeValuedFixture();
    try (final ResultSet rs = database.query("sql", "SELECT [{x: 1}, {x: 9}, {x: 12}][x > 5].size() AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(2);
    }
  }

  /** Regression test for issue #8979. */
  @Test
  void betweenFollowsKleeneLogicWithANullBound() {
    createThreeValuedFixture();
    // 1 >= 5 is false, so the whole BETWEEN is false even though the upper bound is unknown; 7 >= 5 is true and 7 <= null is unknown
    assertThat(namesWhere("name = 'one' AND NOT (k BETWEEN 5 AND null)")).containsExactly("one");
    assertThat(namesWhere("name = 'five' AND NOT (k BETWEEN 5 AND null)")).isEmpty();
    assertThat(namesWhere("k BETWEEN null AND 6")).isEmpty();
    assertThat(namesWhere("k BETWEEN 0 AND 6")).containsExactly("five", "one");
  }

  /** Regression test for issue #8979. */
  @Test
  void rightBinaryFilterDropsNullElements() {
    createThreeValuedFixture();
    try (final ResultSet rs = database.query("sql", "SELECT [1, null, 5, 7][<> 5].size() AS n")) {
      assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(2);
    }
  }

  /** Regression test for issue #8979. */
  @Test
  void nullOperandsAgreeWithTheScanOnEveryIndexStrategy() {
    createThreeValuedFixture();
    final String[] wheres = { "k = null", "k <> null", "k >= nothing", "k <= nothing", "k > nothing", "k < nothing", "k <> 5",
        "NOT (k = 5)", "k < 5", "k <= 5", "k >= 1" };
    database.command("sql", "CREATE VERTEX TYPE Scan");
    database.command("sql", "CREATE PROPERTY Scan.k INTEGER");
    database.command("sql", "CREATE VERTEX TYPE IdxSkip");
    database.command("sql", "CREATE PROPERTY IdxSkip.k INTEGER");
    database.command("sql", "CREATE INDEX ON IdxSkip (k) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE IdxNull");
    database.command("sql", "CREATE PROPERTY IdxNull.k INTEGER");
    database.command("sql", "CREATE INDEX ON IdxNull (k) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (final String type : new String[] { "Scan", "IdxSkip", "IdxNull" }) {
        database.newVertex(type).set("name", "one", "k", 1).save();
        database.newVertex(type).set("name", "five", "k", 5).save();
        database.newVertex(type).set("name", "null", "k", null).save();
        database.newVertex(type).set("name", "missing").save();
      }
    });
    for (final String where : wheres)
      for (final String type : new String[] { "IdxSkip", "IdxNull" })
        assertThat(namesOf(type, where)).as(type + " WHERE " + where).isEqualTo(namesOf("Scan", where));
  }

  private List<String> namesOf(final String type, final String where) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM " + type + " WHERE " + where + " ORDER BY name")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  // ---- #9032: IN with a null element through an index ----
  // Regression test for #9032: {@code x IN (..., null)} must not return the records whose {@code x} is null when an index that holds null
  // keys answers it, as a scan does not.

  private void createInWithNullFixture() {
    for (final String t : new String[] { "I", "C", "S", "Src" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".x INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".z INTEGER");
    }
    database.command("sql", "CREATE INDEX ON I (x) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON C (x, z) NOTUNIQUE");
    database.transaction(() -> {
      for (final String t : new String[] { "I", "C", "S" }) {
        database.newDocument(t).set("id", 1, "x", 1, "z", 5).save();
        database.newDocument(t).set("id", 2, "x", 2, "z", 5).save();
        database.newDocument(t).set("id", 3, "x", null, "z", 5).save();
      }
      database.newDocument("Src").set("id", 1, "x", 1).save();
      database.newDocument("Src").set("id", 2, "x", null).save();
    });
  }

  private List<Integer> idsOf(final String type, final String where, final Object... params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where + " ORDER BY id", params)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    return ids;
  }

  /** Regression test for issue #9032. */
  @Test
  void inParameterWithNullElement() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(idsOf(t, "x IN ?", Arrays.asList(1, null))).as(t).containsExactly(1);
  }

  /** Regression test for issue #9032. */
  @Test
  void inSubqueryReturningNull() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(idsOf(t, "x IN (SELECT x FROM Src)")).as(t).containsExactly(1);
  }

  /** Regression test for issue #9032. */
  @Test
  void isNullStillFindsTheNullRecord() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" }) {
      assertThat(idsOf(t, "(x IN ?) IS NULL", Arrays.asList(1, null))).as(t).containsExactly(2, 3);
      assertThat(idsOf(t, "x IS NULL")).as(t).containsExactly(3);
    }
  }

  /** Regression test for issue #9032. */
  @Test
  void inWithoutNullUnchanged() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" }) {
      assertThat(idsOf(t, "x IN ?", Arrays.asList(1, 2))).as(t).containsExactly(1, 2);
      assertThat(idsOf(t, "x IN [1, null]")).as(t).containsExactly(1);
    }
  }

  /** Regression test for issue #9032. */
  @Test
  void compositeInWithNullInSecondSlot() {
    createInWithNullFixture();
    assertThat(idsOf("C", "x = 1 AND z IN ?", Arrays.asList(5, null))).containsExactly(1);
  }

  /** Regression test for issue #9032. */
  @Test
  void notInWithNullIsNeverTrue() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(idsOf(t, "x NOT IN ?", Arrays.asList(1, null))).as(t).isEmpty();
  }

  /** Regression test for issue #9032. */
  @Test
  void uniqueIndexInWithNull() {
    createInWithNullFixture();
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.x INTEGER");
    database.command("sql", "CREATE INDEX ON U (x) UNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newDocument("U").set("id", 1, "x", 1).save();
      database.newDocument("U").set("id", 2, "x", null).save();
    });
    assertThat(idsOf("U", "x IN ?", Arrays.asList(1, null))).containsExactly(1);
  }

  /** Regression test for issue #9032. */
  @Test
  void compositeInWithNullAndRange() {
    createInWithNullFixture();
    assertThat(idsOf("C", "x IN ? AND z > 3", Arrays.asList(1, null))).containsExactly(1);
    assertThat(idsOf("S", "x IN ? AND z > 3", Arrays.asList(1, null))).containsExactly(1);
  }

  /** Regression test for issue #9032. */
  @Test
  void inWithOnlyNullMatchesNothing() {
    createInWithNullFixture();
    for (final String t : new String[] { "I", "C", "S" })
      assertThat(idsOf(t, "x IN ?", Arrays.asList((Object) null))).as(t).isEmpty();
  }

  // ---- #9237: IS NULL through a unique NULL_STRATEGY INDEX index ----
  // Issue #9237: {@code p IS NULL} answered through a UNIQUE or UNIQUE_HASH index with NULL_STRATEGY INDEX returned one of the
  // records whose p is null, because the point lookup of a unique index stops at the first entry of a key.

  private void createUniqueIsNullFixture() {
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      database.command("sql", "CREATE VERTEX TYPE " + t[0]);
      database.command("sql", "CREATE PROPERTY " + t[0] + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t[0] + ".p INTEGER");
      if (t[1] != null)
        database.command("sql", "CREATE INDEX ON " + t[0] + " (p) " + t[1] + " NULL_STRATEGY INDEX");
      database.transaction(() -> database.newVertex(t[0]).set("id", 1, "p", 1).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 2, "p", null).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 3).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 4, "p", null).save());
    }
  }

  private List<Long> sortedIds(final String query) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    Collections.sort(ids);
    return ids;
  }

  /** Regression test for issue #9237. */
  @Test
  void isNullReturnsEveryRecordWhoseKeyIsNull() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L);
      assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL AND id > 0")).as(t[0]).containsExactly(2L, 3L, 4L);
      assertThat(sortedIds("SELECT count(*) AS id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3L);
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void isNullAfterDeletingOneOfTheNullRecords() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      database.transaction(() -> database.command("sql", "DELETE FROM " + t[0] + " WHERE id = 2"));
      assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3L, 4L);
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void isNullAfterAPendingRemovalOfOneNullRecord() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES)
      database.transaction(() -> {
        database.command("sql", "DELETE FROM " + t[0] + " WHERE id = 2");
        assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3L, 4L);
      });
  }

  /** Regression test for issue #9237. */
  @Test
  void manyNullRecordsSpanningOverflowPages() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      database.transaction(() -> {
        for (int i = 100; i < 3100; i++)
          database.newVertex(t[0]).set("id", i).save();
      });
      assertThat(sortedIds("SELECT count(*) AS id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3003L);
      database.transaction(() -> database.command("sql", "DELETE FROM " + t[0] + " WHERE id >= 100 AND id < 105"));
      assertThat(sortedIds("SELECT count(*) AS id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2998L);
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void aBoundedLookupOfTheNullKeyReturnsUpToTheLimit() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      if (t[1] == null)
        continue;
      final TypeIndex index = database.getSchema().getType(t[0]).getIndexByProperties("p");
      for (int limit = 1; limit <= 3; limit++) {
        int entries = 0;
        for (final IndexCursor c = index.get(new Object[] { null }, limit); c.hasNext(); c.next())
          entries++;
        assertThat(entries).as(t[0] + " limit " + limit).isEqualTo(limit);
      }
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void aDuplicateNonNullKeyIsStillRefusedNextToNullKeys() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      if (t[1] == null || t[1].startsWith("NOTUNIQUE"))
        continue;
      assertThatThrownBy(() -> database.transaction(() -> {
        database.newVertex(t[0]).set("id", 10).save();
        database.newVertex(t[0]).set("id", 11, "p", 7).save();
        database.newVertex(t[0]).set("id", 12, "p", 7).save();
        database.newVertex(t[0]).set("id", 13).save();
      })).as(t[0]).isInstanceOf(DuplicatedKeyException.class);
      assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L);
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void compositeUniqueKeysAllNullAreExemptAndPartiallyNullAreStillEnforced() {
    createUniqueIsNullFixture();
    database.command("sql", "CREATE VERTEX TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newVertex("Pair").save();
      database.newVertex("Pair").save();
      database.newVertex("Pair").set("a", 1).save();
    });
    assertThat(sortedIds("SELECT count(*) AS id FROM Pair WHERE a IS NULL AND b IS NULL")).containsExactly(2L);
    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Pair").set("a", 1).save()))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /** Regression test for issue #9237. */
  @Test
  void aBoundedLookupInsideATransactionCountsOnlyTheRowsThatSurviveItsRemovals() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      if (t[1] == null)
        continue;
      final TypeIndex index = database.getSchema().getType(t[0]).getIndexByProperties("p");
      database.transaction(() -> {
        database.command("sql", "DELETE FROM " + t[0] + " WHERE id = 2");
        for (int limit = 1; limit <= 2; limit++) {
          int entries = 0;
          for (final IndexCursor c = index.get(new Object[] { null }, limit); c.hasNext(); c.next())
            entries++;
          assertThat(entries).as(t[0] + " limit " + limit).isEqualTo(limit);
        }
      });
    }
  }

  /** Regression test for issue #9237. */
  @Test
  void isNullAfterAReopen() {
    createUniqueIsNullFixture();
    reopenDatabase();
    for (final String[] t : UNIQUE_IS_NULL_TYPES)
      assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L);
  }

  /** Regression test for issue #9237. */
  @Test
  void isNullInsideTheTransactionThatWritesTheNulls() {
    createUniqueIsNullFixture();
    for (final String[] t : UNIQUE_IS_NULL_TYPES) {
      database.transaction(() -> {
        database.newVertex(t[0]).set("id", 5, "p", null).save();
        assertThat(sortedIds("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L, 5L);
      });
    }
  }

  // ---- #9238: equality with a null parameter through an index ----
  // Regression test for #9238: an equality whose value is a parameter bound to null matches no record, through an index as in a
  // scan. The index must not answer it with the records whose key is null, and a {@code NULL_STRATEGY ERROR} index must not throw.

  private void createEqualsNullParameterFixture() {
    for (final String t : EQUALS_NULL_TYPES) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".p INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".q INTEGER");
    }
    database.command("sql", "CREATE INDEX ON Lsm (p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON Composite (q, p) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON CompositeP (p, q) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Hash (p) NOTUNIQUE_HASH NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON Err (p) NOTUNIQUE NULL_STRATEGY ERROR");
    database.transaction(() -> {
      for (final String t : EQUALS_NULL_TYPES) {
        database.newDocument(t).set("id", 1, "p", 1, "q", 7).save();
        if (!t.equals("Err")) {
          database.newDocument(t).set("id", 2, "p", null, "q", 7).save();
          database.newDocument(t).set("id", 3, "q", 7).save();
        }
      }
    });
  }

  /** Regression test for issue #9238. */
  @Test
  void equalsNullPositionalParameter() {
    createEqualsNullParameterFixture();
    for (final String t : EQUALS_NULL_TYPES)
      assertThat(idsOf(t, "p = ?", (Object) null)).as(t).isEmpty();
  }

  /** Regression test for issue #9238. */
  @Test
  void equalsNullNamedParameter() {
    createEqualsNullParameterFixture();
    final Map<String, Object> params = new HashMap<>();
    params.put("x", null);
    for (final String t : EQUALS_NULL_TYPES) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT id FROM " + t + " WHERE p = :x", params)) {
        rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
      }
      assertThat(ids).as(t).isEmpty();
    }
  }

  /** Regression test for issue #9238. */
  @Test
  void compositeEqualsNullParameter() {
    createEqualsNullParameterFixture();
    for (final String t : EQUALS_NULL_TYPES)
      assertThat(idsOf(t, "q = 7 AND p = ?", (Object) null)).as(t).isEmpty();
  }

  /** Regression test for issue #9238. */
  @Test
  void deleteWithNullParameterDeletesNothing() {
    createEqualsNullParameterFixture();
    for (final String t : EQUALS_NULL_TYPES) {
      database.transaction(() -> database.command("sql", "DELETE FROM " + t + " WHERE p = ?", (Object) null));
      assertThat(database.countType(t, false)).as(t).isEqualTo(t.equals("Err") ? 1 : 3);
    }
  }

  /** Regression test for issue #9238. */
  @Test
  void equalsNonNullStillUsesTheIndex() {
    createEqualsNullParameterFixture();
    for (final String t : EQUALS_NULL_TYPES)
      assertThat(idsOf(t, "p = ?", 1)).as(t).containsExactly(1);
    assertThat(idsOf("Composite", "q = 7 AND p = ?", 1)).containsExactly(1);
  }

  /** Regression test for issue #9238. */
  @Test
  void nullSafeEqualsAndIsNullStillFindNullRecords() {
    createEqualsNullParameterFixture();
    for (final String t : new String[] { "Scan", "Lsm", "Composite", "CompositeP", "Hash" }) {
      assertThat(idsOf(t, "p <=> ?", (Object) null)).as(t).containsExactly(2, 3);
      assertThat(idsOf(t, "p IS NULL")).as(t).containsExactly(2, 3);
    }
  }

  /** Regression test for issue #9238. */
  @Test
  void caseInsensitiveIndexEqualsNullParameter() {
    createEqualsNullParameterFixture();
    database.command("sql", "CREATE DOCUMENT TYPE Ci");
    database.command("sql", "CREATE PROPERTY Ci.id INTEGER");
    database.command("sql", "CREATE PROPERTY Ci.p STRING");
    database.command("sql", "CREATE INDEX ON Ci (p COLLATE ci) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newDocument("Ci").set("id", 1, "p", "A").save();
      database.newDocument("Ci").set("id", 2, "p", null).save();
    });
    assertThat(idsOf("Ci", "p.toLowerCase() = ?", (Object) null)).isEmpty();
    assertThat(idsOf("Ci", "p.toLowerCase() = ?", "a")).containsExactly(1);
  }

  // ---- #9274: cached index plan re-run with a null parameter ----
  // Regression test for #9274: a statement first run with a value keeps its index plan, and a later run of the same text with a
  // null parameter must still match no record (#9238), for SELECT and for DELETE.

  private void createCachedPlanFixture() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.p INTEGER");
    database.command("sql", "CREATE PROPERTY T.q INTEGER");
    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON T (q, p) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE INDEX ON T (s COLLATE ci) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.id INTEGER");
    database.command("sql", "CREATE PROPERTY U.p INTEGER");
    database.command("sql", "CREATE INDEX ON U (p) UNIQUE NULL_STRATEGY INDEX");
    reloadCachedPlanRows();
  }

  /** Loads the base data set; TestHelper recreates the database per test, so a test can add its own rows. */
  private void reloadCachedPlanRows() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM T");
      database.newDocument("T").set("id", 1, "p", 1, "q", 7, "s", "Abc").save();
      database.newDocument("T").set("id", 2, "p", null, "q", 7).save();
      database.newDocument("T").set("id", 3, "q", 7).save();
    });
  }

  private List<Integer> cachedPlanIds(final String where, final Object... params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM T WHERE " + where, params)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    Collections.sort(ids);
    return ids;
  }

  /** Regression test for issue #9274. */
  @Test
  void selectValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("p = ?", 1)).containsExactly(1);
    assertThat(cachedPlanIds("p = ?", nul)).isEmpty();
    assertThat(cachedPlanIds("p = ?", 1)).containsExactly(1);
    assertThat(cachedPlanIds("p = ?", nul)).isEmpty();
  }

  /** Regression test for issue #9274. */
  @Test
  void selectCompositeValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    database.transaction(() -> database.newDocument("T").set("id", 4, "p", 1, "q", null).save());
    assertThat(cachedPlanIds("q = ? AND p = ?", 7, 1)).containsExactly(1);
    assertThat(cachedPlanIds("q = ? AND p = ?", 7, nul)).isEmpty();
    assertThat(cachedPlanIds("q = ? AND p = ?", nul, 1)).isEmpty();
    assertThat(cachedPlanIds("q = ? AND p = ?", 7, 1)).containsExactly(1);
  }

  /** Regression test for issue #9274. */
  @Test
  void selectReversedOperandsValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("? = p", 1)).containsExactly(1);
    assertThat(cachedPlanIds("? = p", nul)).isEmpty();
  }

  /** Regression test for issue #9274. */
  @Test
  void deleteValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", 99));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", nul));
    assertThat(database.countType("T", false)).isEqualTo(3);

    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ? AND id > 100", 99));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ? AND id > 100", nul));
    assertThat(database.countType("T", false)).isEqualTo(3);
  }

  /** Regression test for issue #9274. */
  @Test
  void selectNullLiteralAfterValue() {
    createCachedPlanFixture();
    assertThat(cachedPlanIds("p = ?", 1)).containsExactly(1);
    assertThat(cachedPlanIds("p = null")).isEmpty();
  }

  /** Regression test for issue #9274. */
  @Test
  void selectNullOnRangeSideOfCompositeKey() {
    createCachedPlanFixture();
    final Object nul = null;
    database.transaction(() -> database.newDocument("T").set("id", 4, "p", 1, "q", null).save());
    assertThat(cachedPlanIds("q = ? AND p > ?", 7, 0)).containsExactly(1);
    assertThat(cachedPlanIds("q = ? AND p > ?", 7, nul)).isEmpty();
    assertThat(cachedPlanIds("q = ? AND p > ?", 7, 0)).containsExactly(1);
  }

  /** Regression test for issue #9274. */
  @Test
  void selectCaseInsensitiveIndexValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("s.toLowerCase() = ?", "abc")).containsExactly(1);
    assertThat(cachedPlanIds("s.toLowerCase() = ?", nul)).isEmpty();
    assertThat(cachedPlanIds("s.toLowerCase() = ?", "abc")).containsExactly(1);
  }

  /** Regression test for issue #9274. */
  @Test
  void selectUniqueIndexValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    database.transaction(() -> database.newDocument("U").set("id", 1, "p", 5).save());
    database.transaction(() -> database.newDocument("U").set("id", 2, "p", null).save());
    final String sql = "SELECT id FROM U WHERE p = ?";
    for (final Object value : new Object[] { 5, nul, 5, nul }) {
      final List<Integer> found = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql, value)) {
        rs.forEachRemaining(r -> found.add(r.getProperty("id")));
      }
      assertThat(found).isEqualTo(value == null ? List.<Integer>of() : List.of(1));
    }
  }

  /** Regression test for issue #9274. */
  @Test
  void deleteWithValueStillDeletes() {
    createCachedPlanFixture();
    final Object nul = null;
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", nul));
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE p = ?", 1));
    assertThat(database.countType("T", false)).isEqualTo(2);
    assertThat(cachedPlanIds("id > ?", 0)).containsExactly(2, 3);
  }

  /** Regression test for issue #9274. */
  @Test
  void selectNullThenValue() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("p = ?", nul)).isEmpty();
    assertThat(cachedPlanIds("p = ?", 1)).containsExactly(1);
    assertThat(cachedPlanIds("p = ?", nul)).isEmpty();
  }

  /** Regression test for issue #9274. */
  @Test
  void selectInListValueThenNullElement() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("p IN [?]", 1)).containsExactly(1);
    assertThat(cachedPlanIds("p IN [?]", nul)).isEmpty();
    assertThat(cachedPlanIds("p IN [?]", 1)).containsExactly(1);
  }

  /** Regression test for issue #9274. */
  @Test
  void countValueThenNull() {
    createCachedPlanFixture();
    final Object nul = null;
    for (final Object value : new Object[] { 1, nul, 1, nul }) {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM T WHERE p = ?", value)) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(value == null ? 0L : 1L);
      }
    }
  }

  /** Regression test for issue #9274. */
  @Test
  void selectNullFirstSlotWithRangeOnSecond() {
    createCachedPlanFixture();
    final Object nul = null;
    assertThat(cachedPlanIds("q = ? AND p > ?", 7, 0)).containsExactly(1);
    assertThat(cachedPlanIds("q = ? AND p > ?", nul, 0)).isEmpty();
  }

  // ---- #10186: NOT IN over a sub-query, NOT LIKE and arithmetic on a NULL ----
  // Regression guard for #10186: three places where SQL still answered a NULL as a value after the three-valued logic of #4591,
  // #8979 and #9032 - {@code NOT IN} over a sub-query returning a NULL, {@code NOT LIKE} on a NULL, and {@code +}/{@code -} with
  // a NULL operand.

  private void createNullGapsFixture() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.name STRING");
    database.command("sql", "CREATE PROPERTY T.i INTEGER");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.command("sql", "CREATE PROPERTY V.k INTEGER");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET name = 'three', i = 3, s = 'abc'");
      database.command("sql", "INSERT INTO T SET name = 'null', i = null, s = null");
      database.command("sql", "INSERT INTO T SET name = 'missing'");
      database.command("sql", "INSERT INTO U SET k = 4");
      database.command("sql", "INSERT INTO U SET k = null");
      database.command("sql", "INSERT INTO V SET k = 4");
    });
  }

  /** Regression test for issue #10186. */
  @Test
  void notInOverASubQueryReturningANullIsUnknown() {
    createNullGapsFixture();
    // 3 NOT IN (4, NULL) is unknown, as the literal list already answers
    assertThat(namesWhere("i NOT IN [4, null]")).isEmpty();
    assertThat(namesWhere("i NOT IN (SELECT k FROM U)")).isEmpty();
    assertThat(namesWhere("NOT (i IN (SELECT k FROM U))")).isEmpty();
    assertThat(namesWhere("i IN (SELECT k FROM U)")).isEmpty();
    // a NULL row read through an expression, not a bare property, is the same NULL
    assertThat(namesWhere("i NOT IN (SELECT k + 0 AS k FROM U)")).isEmpty();
    // without the NULL the sub-query answers a definite FALSE for 3, and unknown for the rows without i
    assertThat(namesWhere("i NOT IN (SELECT k FROM V)")).containsExactly("three");
    assertThat(namesWhere("i NOT IN (SELECT k FROM U WHERE k IS NOT NULL)")).containsExactly("three");
    // an empty sub-query: NOT IN is TRUE for every row, even the ones without i
    assertThat(namesWhere("i NOT IN (SELECT k FROM U WHERE k = 99)")).containsExactly("missing", "null", "three");
    // a row of several columns is a row, not a NULL, even when one of its columns is null
    assertThat(namesWhere("i NOT IN (SELECT k, 1 AS one FROM U)")).containsExactly("three");
    // a record of the sub-query is not a NULL either
    assertThat(namesWhere("i NOT IN (SELECT FROM U)")).containsExactly("three");
  }

  /** Regression test for issue #10186. */
  @Test
  void notInOverALargeSubQueryWithOneNull() {
    createNullGapsFixture();
    database.transaction(() -> {
      for (int k = 100; k < 5_100; k++)
        database.newDocument("V").set("k", k).save();
    });
    assertThat(namesWhere("i NOT IN (SELECT k FROM V)")).containsExactly("three");
    database.transaction(() -> database.newDocument("V").set("k", null).save());
    assertThat(namesWhere("i NOT IN (SELECT k FROM V)")).isEmpty();
    assertThat(namesWhere("i IN (SELECT k FROM V)")).isEmpty();
  }

  /** Regression test for issue #10186. */
  @Test
  void notInOverAParameterSetOrArrayHoldingANullIsUnknown() {
    createNullGapsFixture();
    final Set<Object> set = new HashSet<>(Arrays.asList(4, null));
    for (final Object right : new Object[] { set, new Object[] { 4, null }, Arrays.asList(4, null) }) {
      final Map<String, Object> params = new HashMap<>();
      params.put("p", right);
      try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE i NOT IN :p", params)) {
        assertThat(rs.hasNext()).as(right.getClass().getSimpleName()).isFalse();
      }
    }
  }

  /** Regression test for issue #10186. */
  @Test
  void anUnknownLikeDoesNotHideATrueBranchOfAnOr() {
    createNullGapsFixture();
    assertThat(namesWhere("s LIKE 'x%' OR name = 'null'")).containsExactly("null");
    assertThat(namesWhere("s NOT LIKE 'x%' OR name = 'null'")).containsExactly("null", "three");
    assertThat(namesWhere("NOT (s LIKE 'x%' AND name = 'null')")).containsExactly("missing", "three");
  }

  /** Regression test for issue #10186. */
  @Test
  void aNullPatternParameterIsUnknown() {
    createNullGapsFixture();
    final Map<String, Object> params = new HashMap<>();
    params.put("p", null);
    for (final String where : new String[] { "NOT (s LIKE :p)", "s NOT ILIKE :p", "NOT (s MATCHES :p)" })
      try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where, params)) {
        assertThat(rs.hasNext()).as(where).isFalse();
      }
  }

  /** Regression test for issue #10186. */
  @Test
  void notInOverASubQueryReturningANullIsUnknownThroughAnIndex() {
    createNullGapsFixture();
    database.command("sql", "CREATE INDEX ON T (i) NOTUNIQUE");
    assertThat(namesWhere("i NOT IN (SELECT k FROM U)")).isEmpty();
    assertThat(namesWhere("i IN (SELECT k FROM U)")).isEmpty();
    assertThat(namesWhere("i NOT IN (SELECT k FROM V)")).containsExactly("three");
  }

  /** Regression test for issue #10186. */
  @Test
  void likeOnANullIsUnknownThroughAnIndex() {
    createNullGapsFixture();
    database.command("sql", "CREATE INDEX ON T (s) NOTUNIQUE");
    assertThat(namesWhere("s NOT LIKE 'x%'")).containsExactly("three");
    assertThat(namesWhere("s LIKE 'a%'")).containsExactly("three");
    assertThat(namesWhere("NOT (s LIKE null)")).isEmpty();
  }

  /** Regression test for issue #10186. */
  @Test
  void likeOnANullIsUnknown() {
    createNullGapsFixture();
    for (final String where : new String[] { "s NOT LIKE 'x%'", "NOT (s LIKE 'x%')", "s NOT ILIKE 'x%'", "NOT (s ILIKE 'x%')",
        "NOT (s MATCHES 'x.*')", "NOT (s = 'x')", "s LIKE '%'", "s ILIKE '%'", "s MATCHES '.*'" })
      assertThat(namesWhere(where)).as(where).containsExactly("three");

    // a NULL pattern is unknown too
    assertThat(namesWhere("NOT (s LIKE null)")).isEmpty();
    assertThat(namesWhere("NOT (s ILIKE null)")).isEmpty();
    assertThat(namesWhere("NOT (s MATCHES null)")).isEmpty();
    assertThat(namesWhere("s IS NULL")).containsExactly("missing", "null");
  }

  /** Regression test for issue #10186. */
  @Test
  void additionAndSubtractionWithANullNumberAreNull() {
    createNullGapsFixture();
    try (final ResultSet rs = database.query("sql",
        "SELECT i + 1 AS plus, 1 + i AS plus2, 1 - i AS minus, i - 1 AS minus2, -i AS neg, i * 2 AS times, i / 2 AS div FROM T WHERE name = 'null'")) {
      final Result r = rs.next();
      for (final String p : new String[] { "plus", "plus2", "minus", "minus2", "neg", "times", "div" })
        assertThat(r.<Object>getProperty(p)).as(p).isNull();
    }

    assertThat(namesWhere("i - 4 < 6")).containsExactly("three");
    assertThat(namesWhere("i + 4 < 60")).containsExactly("three");
    assertThat(namesWhere("-i < 6")).containsExactly("three");
    assertThat(namesWhere("NOT (i - 4 < 6)")).isEmpty();
    assertThat(namesWhere("i * 1 < 6")).containsExactly("three");
    assertThat(namesWhere("i < 10")).containsExactly("three");
  }

  /** Regression test for issue #10186. */
  @Test
  void nullOperandIsNullForEveryScalarArithmeticOperator() {
    createNullGapsFixture();
    for (final String expr : new String[] { "i + 1", "1 + i", "i - 1", "1 - i", "-i", "i + 1 + 2", "1 + 2 * i", "i * 2 + 1", "i ^ 1",
        "1 ^ i", "i | 1", "1 | i", "i & 1", "null + null", "null - null", "date('2026-10-17', 'yyyy-MM-dd') - i",
        "date('2026-10-17', 'yyyy-MM-dd') + i", "i - date('2026-10-17', 'yyyy-MM-dd')", "duration(1, 'hour') + i", "i + duration(1, 'hour')" })
      assertThat(this.<Object>value(expr)).as(expr).isNull();

    // the non-null forms are unchanged
    assertThat(this.<Number>valueOf("three", "i + 1 + 2").intValue()).isEqualTo(6);
    assertThat(this.<Number>valueOf("three", "-i").intValue()).isEqualTo(-3);
    assertThat(this.<Number>valueOf("three", "i ^ 1").intValue()).isEqualTo(2);
    assertThat(this.<Number>valueOf("three", "i | 4").intValue()).isEqualTo(7);
  }

  /** Regression test for issue #10186. */
  @Test
  void concatenationKeepsTreatingANullAsAbsent() {
    createNullGapsFixture();
    assertThat(this.<String>value("'a' + s")).isEqualTo("a");
    assertThat(this.<String>value("s + 'a'")).isEqualTo("a");
    assertThat(this.<String>value("'a' + i")).isEqualTo("a");
    // a value that is not a number, a date or a duration is concatenated, not added, so it keeps the concatenation rule
    database.transaction(() -> database.command("sql", "UPDATE T SET flag = true WHERE name = 'null'"));
    assertThat(this.<Boolean>value("flag + i")).isTrue();
    assertThat(this.<List<Object>>value("[1, 2] + i")).containsExactly(1, 2);
    assertThat(this.<List<Object>>value("[1, 2] - i")).containsExactly(1, 2);
    assertThat(this.<Map<String, Object>>value("{'a': 1} - i")).containsOnlyKeys("a");
  }

  /** Regression test for issue #10186. */
  @Test
  void compoundAssignmentStartsAMissingCounterFromNothing() {
    createNullGapsFixture();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET name = 'counter'");
      database.command("sql", "UPDATE T SET up += 2, down -= 3, text += 'x', times *= 2, div /= 2 WHERE name = 'counter'");
    });
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text, times, div FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(2);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-3);
      assertThat(r.<String>getProperty("text")).isEqualTo("x");
      assertThat(r.<Object>getProperty("times")).isNull();
      assertThat(r.<Object>getProperty("div")).isNull();
    }

    database.transaction(() -> database.command("sql", "UPDATE T SET up += 2, down -= 3, text += 'y' WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(4);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-6);
      assertThat(r.<String>getProperty("text")).isEqualTo("xy");
    }

    // a NULL increment leaves the field as it is: SET n += :unset does not wipe n
    final Map<String, Object> params = new HashMap<>();
    params.put("x", null);
    database.transaction(() -> database.command("sql", "UPDATE T SET up += :x, down -= :x, text += :x WHERE name = 'counter'", params));
    try (final ResultSet rs = database.query("sql", "SELECT up, down, text FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("up").intValue()).isEqualTo(4);
      assertThat(r.<Number>getProperty("down").intValue()).isEqualTo(-6);
      assertThat(r.<String>getProperty("text")).isEqualTo("xy");
    }

    // a NULL increment of an absent field leaves it absent, and rewrites nothing
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET never += :x, never2 -= :x WHERE name = 'counter'", params)) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.hasProperty("never")).isFalse();
      assertThat(r.hasProperty("never2")).isFalse();
      assertThat(r.getRecord().get().asDocument().has("never")).isFalse();
    }

    // a list starts from nothing too, and subtracting from a missing field has nothing to subtract from
    database.transaction(() -> database.command("sql", "UPDATE T SET tags += ['a'], gone -= 'a', gone2 -= 5 WHERE name = 'counter'"));
    database.transaction(() -> database.command("sql", "UPDATE T SET tags += ['b'], tags -= 'a' WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT tags, gone, gone2 FROM T WHERE name = 'counter'")) {
      final Result r = rs.next();
      assertThat(r.<List<Object>>getProperty("tags")).containsExactly("b");
      assertThat(r.<Object>getProperty("gone")).isNull();
      assertThat(r.<Number>getProperty("gone2").intValue()).isEqualTo(-5);
    }

    // the right-hand side of a compound assignment is evaluated once per record
    database.transaction(() -> database.command("sql", "UPDATE T SET up += (SELECT count(*) AS c FROM U) WHERE name = 'counter'"));
    try (final ResultSet rs = database.query("sql", "SELECT up FROM T WHERE name = 'counter'")) {
      assertThat(rs.next().<Number>getProperty("up").intValue()).isEqualTo(6);
    }
  }

  private <T> T value(final String expression) {
    return valueOf("null", expression);
  }

  private <T> T valueOf(final String name, final String expression) {
    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS v FROM T WHERE name = ?", name)) {
      return rs.next().getProperty("v");
    }
  }
}
