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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SQL equality across value classes: FLOAT against DOUBLE and DECIMAL, nested numbers in DISTINCT/GROUP BY keys, IN against a
 * scalar or a sub-query, a list operand of {@code =}, and lists, maps, sets and arrays whose elements are numbers or temporals
 * of different classes. The index, the scan, openCypher and the comparator must all give the same answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlEqualityCoercionTest extends TestHelper {
  private static final float[]  FLOATS  = { 0.1f, 1.0f / 3, 3.14159f, 0.001f, 2.5f };
  private static final double[] DOUBLES = { 0.1, 1.0 / 3, Math.PI, 0.001, 2.5 };
  private static final long EPOCH_2000_MILLIS = 946684800000L;
  private static final String[][] EQUALS_LIST_TYPES = { { "Plain", "" }, { "Hash", "UNIQUE_HASH" }, { "Sorted", "UNIQUE" }, { "Multi", "NOTUNIQUE" } };
  private static final List<String> ALL_KEYS = List.of("integer", "long", "sql_content");
  private static final LocalDateTime STORED  = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
  private static final Instant       INSTANT = STORED.toInstant(ZoneOffset.UTC);

  // ---- #8882: FLOAT and DOUBLE equality through index, scan and Cypher ----
  // Issue #8882: an equality lookup on a FLOAT or DOUBLE property must answer the same through an index, through a scan
  // and in Cypher, for every operand that {@code Type.convert} turns into exactly the stored value.

  /** Regression test for issue #8882. */
  @Test
  void floatLookedUpByWidenedDouble() {
    createFloatDoubleFixture();
    for (int i = 0; i < FLOATS.length; i++) {
      final float f = FLOATS[i];
      assertFoundEverywhere(i, "f", "g", (double) f);
      assertFoundEverywhere(i, "f", "g", Float.valueOf(f));
      assertFoundEverywhere(i, "f", "g", Double.valueOf(Float.toString(f)));
      assertFoundEverywhere(i, "f", "g", new BigDecimal(f).round(new MathContext(9)));
    }
  }

  /** Regression test for issue #8882. */
  @Test
  void doubleLookedUpByBigDecimal() {
    createFloatDoubleFixture();
    for (int i = 0; i < DOUBLES.length; i++) {
      final double d = DOUBLES[i];
      assertFoundEverywhere(i, "d", "e", new BigDecimal(d).round(new MathContext(17)));
      assertFoundEverywhere(i, "d", "e", new BigDecimal(d));
      assertFoundEverywhere(i, "d", "e", Double.valueOf(d));
    }
  }

  /** Regression test for issue #8882. */
  @Test
  void literalOperands() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE g = 0.100000001 AND k = 0", null)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE f = 0.100000001 AND k = 0", null)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g = 0.100000001 AND n.k = 0 RETURN n", null)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE g = 0.1 AND k = 0", null)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g = 0.1 AND n.k = 0 RETURN n", null)).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void differentValuesStillDiffer() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE g = ? AND k = 0", 0.2)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE g = ? AND k = 0", 0.10000001)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE e = ? AND k = 0", new BigDecimal("0.1000000000000001"))).isEqualTo(0);
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g = $p AND n.k = 0 RETURN n", 0.10000001)).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void inListFindsTheWidenedDouble() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [?] AND k = 0", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE f IN [?] AND k = 0", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [?, ?] AND k = 0", 5.5d, (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [?] AND k = 0", 0.2d)).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void cypherNotEqualsNaNAndSignedZero() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.f FLOAT");
    database.transaction(() -> {
      database.newVertex("N").set("f", Float.NaN).save();
      database.newVertex("N").set("f", 0.1f).save();
      database.newVertex("N").set("f", 0.0f).save();
    });
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f = n.f RETURN n", null)).isEqualTo(2);
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f <> n.f RETURN n", null)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f = $p RETURN n", Double.NaN)).isEqualTo(0);
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f = $p RETURN n", -0.0d)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f <> $p RETURN n", (double) 0.1f)).isEqualTo(2);
    assertThat(countRows("opencypher", "MATCH (n:N) WHERE n.f = $p RETURN n", (double) 0.1f)).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void inListMissesStayThreeValued() {
    createFloatDoubleFixture();
    // a miss on a mixed Float/Double list stays a plain FALSE, and UNKNOWN once the list holds a null
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [0.2, 0.3] AND k = 0")).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE NOT (g IN [0.2, 0.3]) AND k = 0")).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE NOT (g IN [0.2, null]) AND k = 0")).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [?, null] AND k = 0", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g IN [$p, 7.5] AND n.k = 0 RETURN n", (double) 0.1f)).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void negationAndNearMissesThroughTheIndex() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE g <> ? AND k = 0", (double) 0.1f)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE NOT (g = ?) AND k = 0", (double) 0.1f)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE f = ? AND k = 0", 0.10000001d)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE d = ? AND k = 0", 0.1000000000000001d)).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void inlinePropertyMatchesTheWidenedDouble() {
    createFloatDoubleFixture();
    assertThat(countRows("opencypher", "MATCH (n:T {g: $p, k: 0}) RETURN n", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T {f: $p, k: 0}) RETURN n", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T {g: $p, k: 0}) RETURN n", 0.2d)).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void inListWithBigDecimalItem() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE e IN [?] AND k = 0", new BigDecimal(0.1d).round(new MathContext(17)))).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE d IN [?] AND k = 0", new BigDecimal(0.1d))).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE e IN [?] AND k = 0", new BigDecimal("0.2"))).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void orderingKeepsTheDecimalReading() {
    createFloatDoubleFixture();
    // a FLOAT equal to the bound (the index key) is neither below nor above it (#8919); between two stored values the
    // ordering still keeps the decimal reading of the FLOAT (#8252)
    assertThat(countRows("sql", "SELECT FROM T WHERE g = ? AND k = 0", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE g >= ? AND k = 0", (double) 0.1f)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM T WHERE g < ? AND k = 0", (double) 0.1f)).isEqualTo(0);
  }

  /** Regression test for issue #8882. */
  @Test
  void underflowReadsAsTheStoredValueLikeTheIndexKey() {
    database.command("sql", "CREATE DOCUMENT TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.f FLOAT");
    database.command("sql", "CREATE PROPERTY Z.g FLOAT");
    database.command("sql", "CREATE INDEX ON Z (f) NOTUNIQUE");
    database.transaction(() -> database.newDocument("Z").set("f", 0.0f, "g", 0.0f).save());
    // Type.convert narrows 1e-50 to 0.0f for the index key, so the scan answers the same
    assertThat(countRows("sql", "SELECT FROM Z WHERE f = ?", 1e-50d)).isEqualTo(countRows("sql", "SELECT FROM Z WHERE g = ?", 1e-50d));
  }

  /** Regression test for issue #8882. */
  @Test
  void storedNegativeZeroAgainstPositiveZeroScan() {
    database.command("sql", "CREATE DOCUMENT TYPE Z");
    database.command("sql", "CREATE PROPERTY Z.g FLOAT");
    database.transaction(() -> database.newDocument("Z").set("g", -0.0f).save());
    // a negative zero is a zero (IEEE 754, #8920)
    assertThat(countRows("sql", "SELECT FROM Z WHERE g = ?", -0.0d)).isEqualTo(1);
    assertThat(countRows("sql", "SELECT FROM Z WHERE g = ?", 0.0d)).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void floatPropertyAgainstBigDecimalInItem() {
    createFloatDoubleFixture();
    assertThat(countRows("sql", "SELECT FROM T WHERE g IN [?] AND k = 0", new BigDecimal(0.1f))).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void notEqualsIsTheNegationAcrossFloatAndDouble() {
    createFloatDoubleFixture();
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g <> $p AND n.k = 0 RETURN n", 0.2d)).isEqualTo(1);
    assertThat(countRows("opencypher", "MATCH (n:T) WHERE n.g <> $p AND n.k = 0 RETURN n", (double) 0.1f)).isEqualTo(0);
    assertThat(countRows("sql", "SELECT FROM T WHERE g <> ? AND k = 0", 0.2d)).isEqualTo(1);
  }

  /** Regression test for issue #8882. */
  @Test
  void binaryComparatorFollowsTheSameRule() {
    assertThat(BinaryComparator.equals(0.1f, (double) 0.1f)).isTrue();
    assertThat(BinaryComparator.equals(0.1f, 0.2d)).isFalse();
  }

  /** Regression test for issue #8882. */
  @Test
  void numbersEqualEdgeCases() {
    assertThat(Type.numbersEqual(0.1f, (double) 0.1f)).isTrue();
    assertThat(Type.numbersEqual((double) 0.1f, 0.1f)).isTrue();
    assertThat(Type.numbersEqual(0.1f, 0.1d)).isTrue();
    assertThat(Type.numbersEqual(0.1f, 0.10000001d)).isFalse();
    assertThat(Type.numbersEqual(0.0f, -0.0d)).isTrue();
    assertThat(Type.numbersEqual(-0.0f, 0.0d)).isTrue();
    assertThat(Type.numbersEqual(Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)).isTrue();
    assertThat(Type.numbersEqual(Float.POSITIVE_INFINITY, 1e300d)).isFalse();
    assertThat(Type.numbersEqual(1e10d, new BigDecimal("1e400"))).isFalse();
    assertThat(Type.numbersEqual(Double.POSITIVE_INFINITY, new BigDecimal("1e400"))).isFalse();
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.10000000000000001"))).isTrue();
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.1000000000000001"))).isFalse();
    assertThat(Type.numbersEqual(0.1d, new BigDecimal("0.1"))).isTrue();
    assertThat(Type.numbersEqual(Float.NaN, Double.NaN)).isTrue();
    assertThat(Type.numbersEqual(1.5f, 1)).isFalse();
    assertThat(Type.numbersEqual(2.0f, 2)).isTrue();
  }

  private void createFloatDoubleFixture() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.command("sql", "CREATE PROPERTY T.f FLOAT");
    database.command("sql", "CREATE PROPERTY T.g FLOAT");
    database.command("sql", "CREATE PROPERTY T.d DOUBLE");
    database.command("sql", "CREATE PROPERTY T.e DOUBLE");
    database.command("sql", "CREATE INDEX ON T (f) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (d) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < FLOATS.length; i++)
        database.newVertex("T").set("k", i, "f", FLOATS[i], "g", FLOATS[i], "d", DOUBLES[i], "e", DOUBLES[i]).save();
    });
  }

  private void assertFoundEverywhere(final int k, final String indexed, final String plain, final Object param) {
    final String label = param.getClass().getSimpleName() + " " + param + " k=" + k;
    for (final String p : new String[] { indexed, plain }) {
      assertThat(countRows("sql", "SELECT FROM T WHERE " + p + " = ? AND k = " + k, param)).as("sql " + p + " " + label).isEqualTo(1);
      assertThat(countRows("opencypher", "MATCH (n:T) WHERE n." + p + " = $p AND n.k = " + k + " RETURN n", param)).as("cypher " + p + " " + label)
          .isEqualTo(1);
    }
  }

  private int countRows(final String language, final String query, final Object... params) {
    final boolean none = params == null || params.length == 0;
    int n = 0;
    try (final ResultSet rs = none ?
        database.query(language, query) :
        language.equals("sql") ? database.query(language, query, params) : database.query(language, query, Map.of("p", params[0]))) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }

  // ---- #8977: nested numbers in DISTINCT and GROUP BY keys ----
  // Regression guard for #8977: SQL DISTINCT and GROUP BY kept [1], [1L], [1.0] and {a: 1}, {a: 1L} apart while count(DISTINCT)
  // and openCypher merged them, giving three different answers for the number of distinct values.

  private void createNestedNumberKeysFixture() {
    database.command("sql", "CREATE VERTEX TYPE T");
    final List<Object> values = List.of(List.of(1), List.of(1L), List.of(1.0d), List.of(new BigDecimal("1.00")), List.of(1, 2),
        List.of(1L, 2L), Map.of("a", 1), Map.of("a", 1L), 1, 1L, 1.0d);
    database.transaction(() -> {
      for (final Object v : values)
        database.newVertex("T").set("x", v).save();
    });
  }

  private long rowCount(final String language, final String query) {
    long n = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }

  private long scalarCount(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** Regression test for issue #8977. */
  @Test
  void selectDistinctMergesNestedNumbers() {
    createNestedNumberKeysFixture();
    assertThat(rowCount("sql", "SELECT DISTINCT x FROM T")).isEqualTo(4);
  }

  /** Regression test for issue #8977. */
  @Test
  void groupByMergesNestedNumbers() {
    createNestedNumberKeysFixture();
    assertThat(rowCount("sql", "SELECT x, count(*) AS n FROM T GROUP BY x")).isEqualTo(4);
  }

  /** Regression test for issue #8977. */
  @Test
  void countDistinctMergesNestedNumbers() {
    createNestedNumberKeysFixture();
    assertThat(scalarCount("sql", "SELECT count(DISTINCT x) AS c FROM T")).isEqualTo(4);
  }

  /** Regression test for issue #8977. */
  @Test
  void allFormsAgreeWithOpenCypher() {
    createNestedNumberKeysFixture();
    assertThat(scalarCount("opencypher", "MATCH (n:T) RETURN count(DISTINCT n.x) AS c")).isEqualTo(4);
    assertThat(scalarCount("sql", "SELECT count(*) AS c FROM (SELECT DISTINCT x FROM T)")).isEqualTo(4);
  }

  /** Regression test for issue #8977. */
  @Test
  void setsKeyByContentNotIterationOrder() {
    createNestedNumberKeysFixture();
    assertThat(Type.normalizeForKey(new LinkedHashSet<>(List.of(1, 2, 3)))).isEqualTo(Type.normalizeForKey(new LinkedHashSet<>(List.of(3L, 2L, 1L))));
  }

  /** Regression test for issue #8977. */
  @Test
  void normalizeForKeyShapes() {
    createNestedNumberKeysFixture();
    // list vs set stay different, sets ignore order, nested containers and null items are handled
    assertThat(Type.normalizeForKey(List.of(1, 2))).isEqualTo(Type.normalizeForKey(new Object[] { 1L, 2.0d }));
    assertThat(Type.normalizeForKey(List.of(1, 2))).isNotEqualTo(Type.normalizeForKey(Set.of(1, 2)));
    assertThat(Type.normalizeForKey(Map.of("a", List.of(1, Map.of("b", 2))))).isEqualTo(Type.normalizeForKey(Map.of("a", List.of(1L, Map.of("b", 2.0d)))));
    assertThat(Type.normalizeForKey(Arrays.asList(1, null))).isEqualTo(Type.normalizeForKey(Arrays.asList(1L, null)));
    assertThat(Type.normalizeForKey(null)).isNull();
    // a collection that holds nothing to canonicalize is returned as it is
    final List<String> plain = List.of("a", "b");
    assertThat(Type.normalizeForKey(plain)).isSameAs(plain);
  }

  /** Regression test for issue #8977. */
  @Test
  void primitiveArraysKeyByContentWithoutBoxing() {
    createNestedNumberKeysFixture();
    assertThat(Type.normalizeForKey(new float[] { 1f, 2f })).isEqualTo(Type.normalizeForKey(new float[] { 1f, 2f }));
    assertThat(Type.normalizeForKey(new float[] { 1f, 2f })).isNotEqualTo(Type.normalizeForKey(new float[] { 1f, 3f }));
    assertThat(Type.normalizeForKey(new byte[] { 1 })).isNotEqualTo(Type.normalizeForKey(new int[] { 1 }));
    assertThat(Type.normalizeForKey(new byte[] { 1, 2 }).hashCode()).isEqualTo(Type.normalizeForKey(new byte[] { 1, 2 }).hashCode());
  }

  // ---- #9113: IN (?) and IN (SELECT) compare by value on a scan ----
  // Regression guard for #9113 (#9030, #9031): without an index, {@code IN (?)} (a scalar right-hand side) and {@code IN (SELECT ...)}
  // compared with {@code Object.equals()}, so a Long and an Integer, two BigDecimals of different scale, a Date and a stored DATETIME,
  // or 0.0 and -0.0 were different. {@code =}, the list forms and the index all compare by value.

  private void createInScalarFixture() {
    for (final String t : new String[] { "I", "S" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".l LONG");
      database.command("sql", "CREATE PROPERTY " + t + ".m DECIMAL");
      database.command("sql", "CREATE PROPERTY " + t + ".t DATETIME");
      database.command("sql", "CREATE PROPERTY " + t + ".d DOUBLE");
    }
    for (final String p : new String[] { "k", "l", "m", "t", "d" })
      database.command("sql", "CREATE INDEX ON I (" + p + ") NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Src");
    database.transaction(() -> {
      for (final String t : new String[] { "I", "S" })
        database.newDocument(t).set("id", 1, "k", 7, "l", 5L, "m", new BigDecimal("1.5"), "t", new Date(EPOCH_2000_MILLIS), "d", -0.0).save();
      database.newDocument("Src").set("k", 7, "kd", 7.0, "kl", 7L, "l", 5, "m", new BigDecimal("1.50"), "d", 0.0).save();
    });
  }

  private List<Object> inIds(final String type, final String where, final Object... params) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where, params)) {
      rs.forEachRemaining(r -> out.add(r.getProperty("id")));
    }
    return out;
  }

  private void assertIndexAndScanFind(final String where, final Object... params) {
    assertThat(inIds("I", where, params)).as("index: " + where).containsExactly(1);
    assertThat(inIds("S", where, params)).as("scan: " + where).containsExactly(1);
  }

  /** Regression test for issue #9113. */
  @Test
  void singleParameterInParentheses() {
    createInScalarFixture();
    assertIndexAndScanFind("k IN (?)", 7L);
    assertIndexAndScanFind("l IN (?)", 5);
    assertIndexAndScanFind("m IN (?)", new BigDecimal("1.50"));
    assertIndexAndScanFind("t IN (?)", new Date(EPOCH_2000_MILLIS));
    assertIndexAndScanFind("d IN (?)", 0.0);
    assertThat(inIds("S", "k IN (?)", 8L)).isEmpty();
    assertThat(inIds("S", "m IN (?)", new BigDecimal("1.51"))).isEmpty();
    assertThat(inIds("S", "t IN (?)", new Date(EPOCH_2000_MILLIS + 1))).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void subqueryCompareByValue() {
    createInScalarFixture();
    assertIndexAndScanFind("k IN (SELECT k FROM Src)");
    assertIndexAndScanFind("k IN (SELECT kd FROM Src)");
    assertIndexAndScanFind("k IN (SELECT kl FROM Src)");
    assertIndexAndScanFind("l IN (SELECT l FROM Src)");
    assertIndexAndScanFind("m IN (SELECT m FROM Src)");
    assertIndexAndScanFind("d IN (SELECT d FROM Src)");
    assertThat(inIds("S", "k IN (SELECT l FROM Src)")).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void subqueryRowHoldingAListMatchesElementWiseByValue() {
    createInScalarFixture();
    database.transaction(() -> database.newDocument("Src").set("nums", new ArrayList<>(List.of(3.0, 7.0))).save());
    assertThat(inIds("S", "k IN (SELECT nums FROM Src WHERE nums IS NOT NULL)")).containsExactly(1);
    assertThat(inIds("S", "k IN (SELECT nums FROM Src WHERE nums IS NOT NULL) AND l = 6")).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void subqueryRowHoldingALinkDoesNotRecurse() {
    createInScalarFixture();
    database.command("sql", "CREATE DOCUMENT TYPE Ln");
    database.command("sql", "CREATE PROPERTY Ln.ref LINK");
    database.transaction(() -> {
      final MutableDocument a = database.newDocument("Ln").save();
      final MutableDocument b = database.newDocument("Ln").set("ref", a.getIdentity()).save();
      a.set("ref", b.getIdentity()).save();
    });
    assertThat(inIds("S", "k IN (SELECT ref FROM Ln)")).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void subqueryStringAgainstNumberAgreesWithTheListForm() {
    createInScalarFixture();
    database.transaction(() -> database.newDocument("Src").set("s", "7").save());
    // whatever "=" and the list form decide for a STRING against a number, the sub-query form decides the same
    assertThat(inIds("S", "k IN (SELECT s FROM Src WHERE s IS NOT NULL)")).isEqualTo(inIds("S", "k IN ['7']"));
    assertThat(inIds("S", "k IN (SELECT s FROM Src WHERE s IS NOT NULL)")).isEqualTo(inIds("S", "k = '7'"));
  }

  /** Regression test for issue #9113. */
  @Test
  void nullParameterIsUnknown() {
    createInScalarFixture();
    assertThat(inIds("S", "k IN (?)", (Object) null)).isEmpty();
    assertThat(inIds("S", "k NOT IN (?)", (Object) null)).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void subqueryDateAndDecimalAgainstStringAgreeWithTheListForm() {
    createInScalarFixture();
    database.transaction(() -> database.newDocument("Src").set("ts", "2000-01-01 00:00:00").set("ms", "1.5").save());
    for (final String col : new String[] { "t", "m" }) {
      final String src = col.equals("t") ? "ts" : "ms";
      final String literal = col.equals("t") ? "'2000-01-01 00:00:00'" : "'1.5'";
      assertThat(inIds("S", col + " IN (SELECT " + src + " FROM Src WHERE " + src + " IS NOT NULL)")).as(col)
          .isEqualTo(inIds("S", col + " IN [" + literal + "]"));
    }
    // the shared answer is a match, not a shared miss
    assertThat(inIds("S", "t IN (SELECT ts FROM Src WHERE ts IS NOT NULL)")).containsExactly(1);
    assertThat(inIds("S", "m IN (SELECT ms FROM Src WHERE ms IS NOT NULL)")).containsExactly(1);
  }

  /** Regression test for issue #9113. */
  @Test
  void scalarInOnAComputedLeftSide() {
    createInScalarFixture();
    assertThat(inIds("S", "(k + 0) IN (?)", 7L)).containsExactly(1);
    assertThat(inIds("S", "(k + 0) IN (?)", 7.0d)).containsExactly(1);
    assertThat(inIds("S", "(k + 0) IN (?)", 8L)).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void mixedListAndPrimitiveArrayParameters() {
    createInScalarFixture();
    assertIndexAndScanFind("k IN ?", List.of(3L, 7, 9));
    assertIndexAndScanFind("k IN ?", new int[] { 3, 7 });
  }

  /** Regression test for issue #9113. */
  @Test
  void equalsOnTheScanTypeMatchesByValue() {
    createInScalarFixture();
    assertIndexAndScanFind("l = ?", 5);
    assertIndexAndScanFind("m = ?", new BigDecimal("1.50"));
    assertIndexAndScanFind("k = ?", 7L);
    assertThat(inIds("S", "m = ?", new BigDecimal("1.51"))).isEmpty();
  }

  /** Regression test for issue #9113. */
  @Test
  void incompatibleTypesAreAMissNotAnError() {
    createInScalarFixture();
    database.transaction(() -> database.newDocument("Src").set("word", "not a number").save());
    assertThat(inIds("S", "k IN (SELECT word FROM Src WHERE word IS NOT NULL)")).isEmpty();
    assertThat(inIds("S", "t IN (SELECT word FROM Src WHERE word IS NOT NULL)")).isEmpty();
    assertThat(inIds("S", "k IN (?)", "not a number")).isEmpty();
  }

  // ---- #9370: = with a list operand, with and without an index ----
  // Issue #9370: {@code WHERE k = <list>} matched nothing on a scan but the rows of every element through an index, so adding an
  // index changed the answer and {@code =} and {@code <>} were not complements.

  private void createEqualsListFixture() {
    for (final String[] t : EQUALS_LIST_TYPES) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t[0]);
      database.command("sql", "CREATE PROPERTY " + t[0] + ".k INTEGER");
      if (!t[1].isEmpty())
        database.command("sql", "CREATE INDEX ON " + t[0] + " (k) " + t[1]);
      database.transaction(() -> {
        for (int i = 1; i <= 5; i++)
          database.command("sql", "INSERT INTO " + t[0] + " SET k = " + i).close();
      });
    }
  }

  private List<Integer> kValues(final String sql, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = params == null ? database.query("sql", sql) : database.query("sql", sql, params)) {
      while (rs.hasNext())
        out.add(rs.next().<Integer>getProperty("k"));
    }
    return out;
  }

  /** Regression test for issue #9370. */
  @Test
  void equalsListMatchesNothingWithOrWithoutIndex() {
    createEqualsListFixture();
    for (final String[] t : EQUALS_LIST_TYPES) {
      assertThat(kValues("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", List.of(1, 2)))).as(t[0] + " bound list").isEmpty();
      assertThat(kValues("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", List.of(1)))).as(t[0] + " one-element list").isEmpty();
      assertThat(kValues("SELECT k FROM " + t[0] + " WHERE k = [1, 2]", null)).as(t[0] + " literal list").isEmpty();
    }
  }

  /** Regression test for issue #9370. */
  @Test
  void compositeKeyExpandsOnlyTheInSlot() {
    createEqualsListFixture();
    database.command("sql", "CREATE DOCUMENT TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a INTEGER");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int a = 1; a <= 3; a++)
        for (int b = 1; b <= 3; b++)
          database.command("sql", "INSERT INTO Pair SET a = " + a + ", b = " + b).close();
    });
    assertThat(pairCount("SELECT FROM Pair WHERE a = 1 AND b IN :v", Map.of("v", List.of(1, 2)))).as("IN in the last slot").isEqualTo(2);
    assertThat(pairCount("SELECT FROM Pair WHERE a IN :v AND b = 1", Map.of("v", List.of(1, 2)))).as("IN in the first slot").isEqualTo(2);
    assertThat(pairCount("SELECT FROM Pair WHERE a = 1 AND b = :v", Map.of("v", List.of(1, 2)))).as("= with a list in the last slot").isZero();
    assertThat(pairCount("SELECT FROM Pair WHERE a = :v AND b = 1", Map.of("v", List.of(1, 2)))).as("= with a list in the first slot").isZero();
    assertThat(pairCount("SELECT FROM Pair WHERE a = 2 AND b = 3", Map.of())).as("scalars").isEqualTo(1);
  }

  private int pairCount(final String sql, final Map<String, Object> params) {
    int count = 0;
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    return count;
  }

  /** Regression test for issue #9370. */
  @Test
  void inStillExpandsAndScalarEqualsStillWorks() {
    createEqualsListFixture();
    for (final String[] t : EQUALS_LIST_TYPES) {
      assertThat(kValues("SELECT k FROM " + t[0] + " WHERE k IN :v ORDER BY k", Map.of("v", List.of(1, 2)))).as(t[0] + " IN")
          .containsExactly(1, 2);
      assertThat(kValues("SELECT k FROM " + t[0] + " WHERE k = :v", Map.of("v", 3))).as(t[0] + " scalar").containsExactly(3);
    }
  }

  // ---- #10448: lists and maps of numbers of different classes ----
  // #10448: SQL {@code =} on two lists or two maps compared the elements with Java {@code equals()}, so {@code [1L, 2L]} was not
  // equal to {@code [1, 2]} (a list written by the Java API or a 64-bit client against the literal's {@code Integer} elements),
  // where {@code CONTAINSALL}, {@code GROUP BY} and openCypher treat them as one value, as SQL {@code =} already does for two
  // scalars of different numeric classes. Every operator that compares a whole list or map inherits the same rule, and a
  // {@code BY ITEM} index must answer {@code li = [1, 2]} as the scan does.

  private List<String> keys(final String sql, final Object... params) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        keys.add(String.valueOf(rs.next().<Object>getProperty("k")));
    }
    return keys;
  }

  private void createThreeVertices() {
    database.command("sql", "CREATE VERTEX TYPE D").close();
    database.transaction(() -> {
      database.newVertex("D").set("k", "long").set("li", List.of(1L, 2L)).set("mp", Map.of("a", 1L)).save();
      database.newVertex("D").set("k", "integer").set("li", List.of(1, 2)).set("mp", Map.of("a", 1)).save();
      database.command("sql", "CREATE VERTEX D CONTENT {\"k\": \"sql_content\", \"li\": [1, 2], \"mp\": {\"a\": 1}}").close();
    });
  }

  /** Regression test for issue #10448. */
  @Test
  void listEqualsLiteralMatchesEveryElementClass() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 2] ORDER BY k")).isEqualTo(ALL_KEYS);
    assertThat(keys("SELECT k FROM D WHERE [1, 2] = li ORDER BY k")).isEqualTo(ALL_KEYS);
  }

  /** Regression test for issue #10448. */
  @Test
  void listEqualsParameterMatchesEveryElementClass() {
    createThreeVertices();
    final Map<String, Object> longs = new HashMap<>();
    longs.put("p", List.of(1L, 2L));
    final Map<String, Object> ints = new HashMap<>();
    ints.put("p", List.of(1, 2));
    for (final Map<String, Object> params : List.of(longs, ints))
      try (final ResultSet rs = database.query("sql", "SELECT k FROM D WHERE li = :p ORDER BY k", params)) {
        final List<String> found = new ArrayList<>();
        while (rs.hasNext())
          found.add(rs.next().getProperty("k"));
        assertThat(found).as("parameter " + params.get("p")).isEqualTo(ALL_KEYS);
      }
  }

  /** Regression test for issue #10448. */
  @Test
  void mapEqualsLiteralMatchesEveryValueClass() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 1} ORDER BY k")).isEqualTo(ALL_KEYS);
  }

  /** Regression test for issue #10448. */
  @Test
  void notEqualsAgreesWithEquals() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li <> [1, 2] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp <> {\"a\": 1} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li <> [1, 3] ORDER BY k")).isEqualTo(ALL_KEYS);
  }

  /** Regression test for issue #10448. */
  @Test
  void differentListsAndMapsStayDifferent() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 3] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [2, 1] ORDER BY k")).as("order is part of a list").isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [1, 2, 3] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE li = [1] ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 2} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"b\": 1} ORDER BY k")).isEmpty();
    assertThat(keys("SELECT k FROM D WHERE mp = {\"a\": 1, \"b\": 1} ORDER BY k")).isEmpty();
  }

  /** Regression test for issue #10448. */
  @Test
  void mapsWithNullValuesAndNestedCollections() {
    database.command("sql", "CREATE DOCUMENT TYPE M").close();
    database.transaction(() -> {
      database.newDocument("M").set("k", "long").set("mp", Map.of("a", List.of(1L, 2L), "b", Map.of("c", 3L))).save();
      database.newDocument("M").set("k", "integer").set("mp", Map.of("a", List.of(1, 2), "b", Map.of("c", 3))).save();
      final Map<String, Object> withNull = new HashMap<>();
      withNull.put("a", null);
      database.newDocument("M").set("k", "null").set("mp", withNull).save();
    });
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": [1, 2], \"b\": {\"c\": 3}} ORDER BY k")).containsExactly("integer", "long");
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": null} ORDER BY k")).containsExactly("null");
    assertThat(keys("SELECT k FROM M WHERE mp = {\"a\": [1, 2], \"b\": {\"c\": 4}} ORDER BY k")).isEmpty();
  }

  /** Regression test for issue #10448. */
  @Test
  void everyOperatorThatComparesAWholeList() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.transaction(() -> {
      database.newVertex("V").set("k", "long").set("li", List.of(1L, 2L)).set("nl", List.of(List.of(1L, 2L), List.of(3L))).save();
      database.newVertex("V").set("k", "integer").set("li", List.of(1, 2)).set("nl", List.of(List.of(1, 2), List.of(3))).save();
    });
    for (final String condition : List.of("li = [1, 2]", "[1, 2] = li", "li IN [[1, 2]]", "[1, 2] IN nl", "nl CONTAINS [1, 2]",
        "nl CONTAINSALL [[3]]", "nl CONTAINSANY [[3]]", "nl[0] = [1, 2]", "li[0] = 1", "li CONTAINSALL [1, 2]", "li CONTAINS 2"))
      assertThat(keys("SELECT k FROM V WHERE " + condition + " ORDER BY k")).as(condition).containsExactly("integer", "long");
    for (final String condition : List.of("li <> [1, 2]", "NOT (li = [1, 2])", "nl CONTAINS [1, 3]", "nl CONTAINSALL [[4]]",
        "nl CONTAINSANY [[4]]", "li IN [[1, 3]]"))
      assertThat(keys("SELECT k FROM V WHERE " + condition + " ORDER BY k")).as(condition).isEmpty();
  }

  /** Regression test for issue #10448. */
  @Test
  void groupByAndContainsAllStillTreatTheListsAsOneValue() {
    createThreeVertices();
    assertThat(keys("SELECT k FROM D WHERE li CONTAINSALL [1, 2] ORDER BY k")).isEqualTo(ALL_KEYS);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM D GROUP BY li")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  /** Regression test for issue #10448. */
  @Test
  void theComparatorHandlesNullsNestingAndSizes() {
    assertThat(BinaryComparator.equals(List.of(1L, 2L), List.of(1, 2))).isTrue();
    assertThat(BinaryComparator.equals(new LinkedList<>(List.of(1L, 2L)), new ArrayList<>(List.of(1, 2)))).as("two implementations").isTrue();
    assertThat(BinaryComparator.equals(List.of(), List.of())).isTrue();
    assertThat(BinaryComparator.equals(List.of(1), List.of(1, 1))).as("size").isFalse();
    assertThat(BinaryComparator.equals(List.of(1, 2), List.of(2, 1))).as("order").isFalse();
    assertThat(BinaryComparator.equals(Arrays.asList(1L, null), Arrays.asList(1, null))).isTrue();
    assertThat(BinaryComparator.equals(Arrays.asList(1L, null), Arrays.asList(1, 2))).isFalse();
    assertThat(BinaryComparator.equals(Arrays.asList(null, 1L), Arrays.asList(1, null))).isFalse();

    assertThat(BinaryComparator.equals(List.of(List.of(1L), Map.of("a", List.of(2L))), List.of(List.of(1), Map.of("a", List.of(2)))))
        .as("nested").isTrue();
    assertThat(BinaryComparator.equals(List.of(List.of(1L), Map.of("a", List.of(2L))), List.of(List.of(1), Map.of("a", List.of(3)))))
        .as("nested, one value apart").isFalse();

    final Map<String, Object> nullValue = new HashMap<>();
    nullValue.put("a", null);
    final Map<String, Object> otherKey = new HashMap<>();
    otherKey.put("b", null);
    assertThat(BinaryComparator.equals(Map.of("a", 1L), Map.of("a", 1))).isTrue();
    assertThat(BinaryComparator.equals(Map.of("a", 1L), Map.of("a", 1, "b", 2))).as("size").isFalse();
    assertThat(BinaryComparator.equals(nullValue, new HashMap<>(nullValue))).isTrue();
    assertThat(BinaryComparator.equals(nullValue, otherKey)).as("a null value is still a key").isFalse();
    assertThat(BinaryComparator.equals(nullValue, Map.of("a", 1))).isFalse();
    assertThat(BinaryComparator.equals(Map.of(1L, "x"), Map.of(1, "x"))).as("keys are matched by the map, as Map.equals does").isFalse();
    assertThat(BinaryComparator.equals(new TreeMap<>(Map.of("a", 1L)), Map.of("a", 1))).as("a sorted map and a hash map").isTrue();
    // a sorted map asked for a key of a class it cannot hold throws: that is a key it does not have, not a failure of the comparison
    assertThat(BinaryComparator.equals(Map.of(1, "x"), new TreeMap<>(Map.of("a", "x")))).as("key class the sorted map cannot hold").isFalse();
    assertThat(BinaryComparator.equals(new TreeMap<>(Map.of("a", "x")), Map.of(1, "x"))).isFalse();
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * Lists of links compare as they always did: by the records they point to, a loaded record or its RID alike.
   */
  @Test
  void listsOfLinksAreComparedByTheirRecords() {
    database.command("sql", "CREATE VERTEX TYPE T").close();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("T").set("name", "first").save().getIdentity();
      rids[1] = database.newVertex("T").set("name", "second").save().getIdentity();
      database.newVertex("T").set("name", "holder").set("refs", List.of(rids[0], rids[1])).save();
      database.newVertex("T").set("name", "reversed").set("refs", List.of(rids[1], rids[0])).save();
    });

    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[0] + ", " + rids[1] + "]")).containsExactly("holder");
    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[1] + ", " + rids[0] + "]")).containsExactly("reversed");
    assertThat(names("SELECT name FROM T WHERE refs = [" + rids[0] + "]")).isEmpty();
    assertThat(names("SELECT name FROM T WHERE refs CONTAINS " + rids[1] + " ORDER BY name")).containsExactly("holder", "reversed");
    // the loaded records on one side, their RIDs on the other
    assertThat(names("SELECT name FROM T WHERE refs = (SELECT FROM T WHERE name IN ['first', 'second'] ORDER BY name DESC)"))
        .as("a sub-query result is not a list of links in this order, and must not blow up").hasSizeLessThanOrEqualTo(2);
    assertThat(BinaryComparator.equals(List.of(rids[0], rids[1]), List.of(rids[0].asVertex(), rids[1].asVertex()))).isTrue();
    assertThat(BinaryComparator.equals(List.of(rids[0], rids[1]), List.of(rids[1].asVertex(), rids[0].asVertex()))).isFalse();
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * A list searched for as one element is looked for among the lists and maps of the collection only.
   */
  @Test
  void containsFindsAListOrMapAmongMixedElements() {
    database.command("sql", "CREATE DOCUMENT TYPE Mixed").close();
    database.transaction(() -> database.newDocument("Mixed").set("name", "mixed")
        .set("items", Arrays.asList("x", 5L, null, List.of(1L, 2L), Map.of("a", 1L))).save());

    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS [1, 2]")).containsExactly("mixed");
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS {\"a\": 1}")).containsExactly("mixed");
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS [1, 3]")).isEmpty();
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS {\"a\": 2}")).isEmpty();
    assertThat(names("SELECT name FROM Mixed WHERE items CONTAINS 5")).containsExactly("mixed");
  }

  private List<String> names(final String sql) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * The hash keys keep the strict comparison: the hash of {@code [1L]} is not the hash of {@code [1]}.
   */
  @Test
  void theExactComparisonForHashKeysStaysStrict() {
    assertThat(BinaryComparator.equalsExact(List.of(1L, 2L), List.of(1, 2))).isFalse();
    assertThat(BinaryComparator.equalsExact(List.of(1, 2), List.of(1, 2))).isTrue();
    assertThat(BinaryComparator.equalsExact(Map.of("a", 1L), Map.of("a", 1))).isFalse();
    assertThat(BinaryComparator.equalsExact(Map.of("a", 1), Map.of("a", 1))).isTrue();
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * The values that group together do not depend on the element class either, and are not changed by the comparison of {@code =}.
   */
  @Test
  void groupByAndDistinctAgreeOnTheClassOfTheElements() {
    createThreeVertices();
    long groups;
    try (final ResultSet rs = database.query("sql", "SELECT li FROM D GROUP BY li")) {
      groups = rs.stream().count();
    }
    long distinct;
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT li FROM D")) {
      distinct = rs.stream().count();
    }
    assertThat(groups).isEqualTo(1L);
    assertThat(distinct).isEqualTo(groups);
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * A regular index on a list holds the whole list as its key, element classes included, so it must answer {@code li = [1, 2]}
   * with the rows the scan answers, whichever class the elements were written with.
   */
  @Test
  void aRegularIndexOnAListAnswersWholeListEqualityAsTheScanDoes() {
    for (final String type : List.of("P", "R")) {
      database.command("sql", "CREATE VERTEX TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".li LIST").close();
      if (type.equals("R"))
        database.command("sql", "CREATE INDEX ON R (li) NOTUNIQUE").close();
      database.transaction(() -> {
        database.newVertex(type).set("k", "long").set("li", List.of(1L, 2L)).save();
        database.newVertex(type).set("k", "integer").set("li", List.of(1, 2)).save();
        database.newVertex(type).set("k", "other").set("li", List.of(2, 3)).save();
      });
    }
    for (final String condition : List.of("li = [1, 2]", "li = [2, 3]", "li <> [1, 2]", "li IN [[1, 2]]", "li IN [[1, 2], [2, 3]]")) {
      final List<String> scan = keys("SELECT k FROM P WHERE " + condition + " ORDER BY k");
      final List<String> indexed = keys("SELECT k FROM R WHERE " + condition + " ORDER BY k");
      assertThat(indexed).as(condition).isEqualTo(scan);
    }
    assertThat(keys("SELECT k FROM R WHERE li = [1, 2] ORDER BY k")).containsExactly("integer", "long");
    try (final ResultSet rs = database.query("sql", "EXPLAIN SELECT k FROM R WHERE li = [1, 2]")) {
      assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("the index is the one asked").contains("FETCH FROM INDEX");
    }
  }

  /**
   * Regression test for issue #10448.
   * <p>
   * The scan answers {@code li = [1, 2]} with every vertex; a BY ITEM index (one entry per item) must give the same rows.
   */
  @Test
  void byItemIndexAnswersWholeListEqualityAsTheScanDoes() {
    for (final String type : List.of("P", "X")) {
      database.command("sql", "CREATE VERTEX TYPE " + type).close();
      database.command("sql", "CREATE PROPERTY " + type + ".li LIST").close();
      if (type.equals("X"))
        database.command("sql", "CREATE INDEX ON X (li BY ITEM) NOTUNIQUE").close();
      database.transaction(() -> {
        database.newVertex(type).set("k", "long").set("li", List.of(1L, 2L)).save();
        database.newVertex(type).set("k", "integer").set("li", List.of(1, 2)).save();
        database.newVertex(type).set("k", "other").set("li", List.of(2, 3)).save();
        database.newVertex(type).set("k", "empty").set("li", List.of()).save();
      });
    }
    for (final String condition : List.of("li = [1, 2]", "li = [2, 3]", "li = []", "li <> [1, 2]", "li CONTAINS 2",
        "li CONTAINSANY [2]", "li CONTAINSALL [1, 2]")) {
      final List<String> scan = keys("SELECT k FROM P WHERE " + condition + " ORDER BY k");
      final List<String> indexed = keys("SELECT k FROM X WHERE " + condition + " ORDER BY k");
      assertThat(indexed).as(condition).isEqualTo(scan);
    }
    assertThat(keys("SELECT k FROM X WHERE li = [1, 2] ORDER BY k")).containsExactly("integer", "long");
    assertThat(keys("SELECT k FROM X WHERE li = [] ORDER BY k")).containsExactly("empty");
  }

  // ---- #10595: lists and maps of temporals of different classes ----
  // #10595: SQL {@code =} on a list or a map compared a temporal element with Java {@code equals()}, so {@code [<Date>]} was not
  // equal to the stored {@code [<LocalDateTime>]} of the same instant, where the scalar {@code =}, {@code CONTAINS},
  // {@code CONTAINSALL} and openCypher all say it is - and {@code <>} answered the same row, asserting the two differ. The numeric
  // half (#10448) routed the list and map arms into the comparator that knows numbers; the scalar rule (a {@code Calendar} is its
  // instant, a value is converted to the class of the other side) was never applied to an element.
  // <p>
  // The rule the engine states is: two values that {@code =} reports equal are equal as elements of a list or a map too. Both halves
  // are pinned: the statements of the report, and the invariant over a pool of values of every temporal representation.

  /** One instant, as every representation a client can bind it in. */
  private static Map<String, Object> representations() {
    final Map<String, Object> reps = new LinkedHashMap<>();
    reps.put("LocalDateTime", STORED);
    reps.put("java.util.Date", Date.from(INSTANT));
    reps.put("Instant", INSTANT);
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
    calendar.setTimeInMillis(INSTANT.toEpochMilli());
    reps.put("Calendar", calendar);
    reps.put("java.sql.Timestamp", Timestamp.from(INSTANT));
    reps.put("epoch Long", INSTANT.toEpochMilli());
    reps.put("ISO String", "2020-01-02T03:04:05");
    return reps;
  }

  private void createTemporalStoredValues() {
    database.command("sql", "CREATE VERTEX TYPE L").close();
    database.transaction(() -> {
      database.newVertex("L").set("k", "l").set("v", new ArrayList<>(List.of(STORED))).save();
      database.newVertex("L").set("k", "s").set("v", STORED).save();
      database.newVertex("L").set("k", "m").set("v", new LinkedHashMap<>(Map.of("x", STORED))).save();
      database.newVertex("L").set("k", "n").set("v", new ArrayList<>(List.of(new ArrayList<>(List.of(STORED))))).save();
    });
  }

  private long countMatches(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return rs.stream().count();
    }
  }

  /** Regression test for issue #10595. */
  @Test
  void aListEqualsTheStoredListForEveryRepresentationTheScalarEqualityAccepts() {
    createTemporalStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      final long scalar = countMatches("SELECT FROM L WHERE k = 's' AND v = ?", rep.getValue());
      assertThat(scalar).as("the scalar = of %s is the oracle", rep.getKey()).isEqualTo(1L);
      assertThat(countMatches("SELECT FROM L WHERE k = 's' AND ? = v", rep.getValue())).as("the scalar = of %s, reversed, is the oracle too",
          rep.getKey()).isEqualTo(1L);

      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(rep.getValue()))).as("[%s] =", rep.getKey()).isEqualTo(scalar);
      // the same value in the other implementation of List: no conversion step is skipped for a pair of one class
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", new ArrayList<>(List.of(rep.getValue())))).as("ArrayList [%s] =",
          rep.getKey()).isEqualTo(scalar);
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND ? = v", List.of(rep.getValue()))).as("[%s] = (reversed)", rep.getKey())
          .isEqualTo(scalar);
    }
  }

  /** Regression test for issue #10595. */
  @Test
  void aMapEqualsTheStoredMapForEveryRepresentation() {
    createTemporalStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      assertThat(countMatches("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", rep.getValue()))).as("{x: %s} =", rep.getKey()).isEqualTo(1L);
      assertThat(countMatches("SELECT FROM L WHERE k = 'm' AND ? = v", Map.of("x", rep.getValue()))).as("{x: %s} = (reversed)", rep.getKey())
          .isEqualTo(1L);
    }
  }

  /** Regression test for issue #10595. */
  @Test
  void nestedCollectionsFollowTheSameRule() {
    createTemporalStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet())
      assertThat(countMatches("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(List.of(rep.getValue())))).as("[[%s]] =", rep.getKey())
          .isEqualTo(1L);
  }

  /** Regression test for issue #10595. */
  @Test
  void inContainsAndNotEqualsAgreeWithEquals() {
    createTemporalStoredValues();
    for (final Map.Entry<String, Object> rep : representations().entrySet()) {
      final String name = rep.getKey();
      final List<Object> asList = List.of(rep.getValue());
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v IN ?", List.of(asList))).as("IN [[%s]]", name).isEqualTo(1L);
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND ? CONTAINS v", List.of(asList))).as("[[%s]] CONTAINS v", name).isEqualTo(1L);
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v CONTAINSALL ?", asList)).as("CONTAINSALL [%s]", name).isEqualTo(1L);
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v <> ?", asList)).as("<> [%s]", name).isZero();
      assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND NOT (v = ?)", asList)).as("NOT (= [%s])", name).isZero();
      assertThat(countMatches("SELECT FROM L WHERE k = 'm' AND v <> ?", Map.of("x", rep.getValue()))).as("<> {x: %s}", name).isZero();
    }
  }

  /** Regression test for issue #10595. */
  @Test
  void anotherInstantStaysDifferent() {
    createTemporalStoredValues();
    final Date other = Date.from(INSTANT.plusSeconds(1));
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(other))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v <> ?", List.of(other))).isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(Date.from(INSTANT), other))).as("size").isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", other))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(List.of(other)))).isZero();
  }

  /** Regression test for issue #10595. */
  @Test
  void aListWithAnElementOfAnotherKindStaysDifferent() {
    createTemporalStoredValues();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of("not a date"))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(true))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(List.of(INSTANT)))).as("a list is not its element").isZero();
  }

  /**
   * Regression test for issue #10595.
   * <p>
   * The invariant, over a pool of values rather than the cases of the report: whatever the scalar {@code =} says of a pair, the
   * same pair as the elements of two lists, of two maps, or nested says too - in both directions and in both implementations
   * of the collection.
   */
  @Test
  void theScalarRuleHoldsForEveryPairOfAPoolAsAnElement() {
    final List<Object> pool = new ArrayList<>(representations().values());
    pool.addAll(List.of(Date.from(INSTANT.plusSeconds(1)), STORED.plusSeconds(1), INSTANT.plusMillis(1), LocalDate.of(2020, 1, 2),
        Date.from(LocalDate.of(2020, 1, 2).atStartOfDay().toInstant(ZoneOffset.UTC)), ZonedDateTime.of(STORED, ZoneId.of("UTC")),
        OffsetDateTime.of(STORED, ZoneOffset.UTC), 1, 1L, (short) 1, 1.0f, 1.0d, new BigDecimal("1.00"), "1", "a", true, false,
        new byte[] { 1 }, new byte[] { 1 }, new RID(1, 2), new RID(1, 3), List.of(1), Map.of("a", 1)));

    int agreed = 0;
    for (final Object a : pool)
      for (final Object b : pool) {
        final boolean scalar = QueryOperatorEquals.equals(a, b);
        final String pair = a.getClass().getSimpleName() + " " + a + " vs " + b.getClass().getSimpleName() + " " + b;

        assertThat(QueryOperatorEquals.equals(List.of(a), List.of(b))).as("[a] = [b] for " + pair).isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(a)), new ArrayList<>(List.of(b)))).as("ArrayLists for " + pair)
            .isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(Map.of("x", a), Map.of("x", b))).as("{x: a} = {x: b} for " + pair).isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(List.of(List.of(a)), List.of(List.of(b)))).as("[[a]] = [[b]] for " + pair)
            .isEqualTo(scalar);
        assertThat(QueryOperatorEquals.equals(List.of(a, a), List.of(b, b))).as("[a, a] = [b, b] for " + pair).isEqualTo(scalar);
        if (scalar)
          agreed++;
      }
    // not vacuous: every value is equal to itself and the representations of the instant are equal to each other
    assertThat(agreed).isGreaterThan(pool.size() + 20);
  }

  /**
   * Regression test for issue #10595.
   * <p>
   * {@code Timestamp.equals(Date)} is false for a {@code Date} that is not a {@code Timestamp}, where {@code Date.equals(Timestamp)} is
   * true: a Timestamp on the left of {@code =} was never equal to the same instant converted to a plain Date, so the answer depended
   * on the side the parameter stood on.
   */
  @Test
  void aTimestampAndADateOfTheSameInstantAreEqualWhicheverSideTheyStandOn() {
    final Timestamp timestamp = Timestamp.from(INSTANT);
    final Date date = Date.from(INSTANT);
    assertThat(BinaryComparator.equals(timestamp, date)).isTrue();
    assertThat(BinaryComparator.equals(date, timestamp)).isTrue();
    assertThat(BinaryComparator.equals(timestamp, Date.from(INSTANT.plusMillis(1)))).isFalse();
    // two Timestamps keep their own comparison, nanoseconds included
    assertThat(BinaryComparator.equals(Timestamp.from(INSTANT.plusNanos(1_000)), Timestamp.from(INSTANT))).isFalse();
    assertThat(BinaryComparator.equals(Timestamp.from(INSTANT), Timestamp.from(INSTANT))).isTrue();

    assertThat(QueryOperatorEquals.equals(timestamp, STORED)).isTrue();
    assertThat(QueryOperatorEquals.equals(STORED, timestamp)).isTrue();

    // against another class the Timestamp is its instant at the millisecond a Date holds: the nanoseconds below it are not compared
    final Timestamp withNanos = Timestamp.from(INSTANT.plusNanos(500_000));
    assertThat(QueryOperatorEquals.equals(withNanos, STORED)).isTrue();
    assertThat(QueryOperatorEquals.equals(STORED, withNanos)).isTrue();
    assertThat(QueryOperatorEquals.equals(withNanos, Date.from(INSTANT))).isTrue();
    assertThat(QueryOperatorEquals.equals(List.of(withNanos), List.of(STORED))).as("as an element too").isTrue();
    // ...and against another Timestamp they are
    assertThat(QueryOperatorEquals.equals(withNanos, timestamp)).isFalse();
  }

  /** Regression test for issue #10595. */
  @Test
  void nullElementsKeepTheirMeaning() {
    final List<Object> withNull = new ArrayList<>();
    withNull.add(null);
    final List<Object> otherNull = new ArrayList<>();
    otherNull.add(null);
    assertThat(QueryOperatorEquals.equals(withNull, otherNull)).as("[null] = [null], as a list is").isTrue();
    assertThat(QueryOperatorEquals.equals(withNull, List.of(1))).isFalse();
    assertThat(QueryOperatorEquals.equals(List.of(1), withNull)).isFalse();
  }

  // ---- #10736: the element rule for every collection and array container ----
  // #10736: the element rule #10595 gave SQL {@code =} (two values {@code =} reports equal are equal as elements of a list or a map
  // too) was guarded on {@code java.util.List}, so a {@code Set} fell past it to the conversion fallback, became an {@code ArrayList}
  // and was compared with the comparator's own element rule: a {@code LinkedHashSet} holding a {@code Date} was not equal to the
  // stored {@code [LocalDateTime]} of the same instant, and {@code <>} answered the row.
  // <p>
  // The rule is now reached however the collection arrives: every {@code Collection} and every array but a {@code byte[]} (BINARY) on
  // either side, and the collection a conversion produces. The census of #10595 is widened by the dimension it missed: the container.

  /** Every container the engine can be handed on either side of {@code =}, as a function of its one element. */
  private static Map<String, Function<Object, Object>> containers() {
    final Map<String, Function<Object, Object>> containers = new LinkedHashMap<>();
    containers.put("List.of", List::of);
    containers.put("ArrayList", e -> new ArrayList<>(List.of(e)));
    containers.put("LinkedList", e -> new LinkedList<>(List.of(e)));
    containers.put("LinkedHashSet", e -> new LinkedHashSet<>(List.of(e)));
    containers.put("HashSet", e -> new HashSet<>(List.of(e)));
    containers.put("Set.of", Set::of);
    containers.put("TreeSet", e -> e instanceof Comparable<?> ? new TreeSet<>(List.of(e)) : null);
    containers.put("Object[]", e -> new Object[] { e });
    return containers;
  }

  private void createCollectionStoredValues() {
    database.command("sql", "CREATE VERTEX TYPE L").close();
    database.transaction(() -> {
      database.newVertex("L").set("k", "l").set("v", new ArrayList<>(List.of(STORED))).save();
      database.newVertex("L").set("k", "m").set("v", new LinkedHashMap<>(Map.of("x", new ArrayList<>(List.of(STORED))))).save();
      database.newVertex("L").set("k", "n").set("v", new ArrayList<>(List.of(new ArrayList<>(List.of(STORED))))).save();
    });
  }

  /** Regression test for issue #10736. */
  @Test
  void aSetOfTheSameInstantEqualsTheStoredList() {
    createCollectionStoredValues();
    final Date date = Date.from(INSTANT);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", date)).as("the scalar oracle").isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", List.of(date))).isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", new LinkedHashSet<>(List.of(date)))).isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v <> ?", new LinkedHashSet<>(List.of(date)))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", (Object) new Date[] { date })).as("Date[]").isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", (Object) new Object[] { date })).as("Object[]").isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'n' AND v = ?", List.of(new LinkedHashSet<>(List.of(date))))).as("nested").isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'm' AND v = ?", Map.of("x", new LinkedHashSet<>(List.of(date))))).as("map").isEqualTo(1L);
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v IN ?", List.of(new LinkedHashSet<>(List.of(date))))).as("IN").isEqualTo(1L);

    // another instant stays different, in a Set too
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v = ?", new LinkedHashSet<>(List.of(Date.from(INSTANT.plusSeconds(1)))))).isZero();
    assertThat(countMatches("SELECT FROM L WHERE k = 'l' AND v <> ?", new LinkedHashSet<>(List.of(Date.from(INSTANT.plusSeconds(1))))))
        .isEqualTo(1L);
  }

  /** Regression test for issue #10736. */
  @Test
  void theDirectCallsOfTheReport() {
    final Date date = Date.from(INSTANT);
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), new LinkedHashSet<>(List.of(date)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(date)), new ArrayList<>(List.of(STORED)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), List.of(date))).isTrue();
    // a list against the scalar it holds converts the scalar into a list: that list is compared under the same rule
    assertThat(QueryOperatorEquals.equals(new ArrayList<>(List.of(STORED)), date)).isTrue();
  }

  /** Regression test for issue #10736. */
  @Test
  void twoSetsAreEqualWhateverTheirOrderAndUnderTheElementRule() {
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(1, 2)), new LinkedHashSet<>(List.of(2L, 1L)))).isTrue();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(STORED, "a")), new LinkedHashSet<>(List.of("a", Date.from(INSTANT)))))
        .isTrue();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(1, 2)), new HashSet<>(List.of(1, 3)))).isFalse();
    assertThat(QueryOperatorEquals.equals(new HashSet<>(List.of(1, 2)), new HashSet<>(List.of(1)))).isFalse();
    // a list keeps its order against a list, and against a set it is compared in the set's iteration order, as before
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), List.of(2, 1))).isFalse();
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), new LinkedHashSet<>(List.of(1L, 2L)))).isTrue();
    assertThat(QueryOperatorEquals.equals(List.of(1, 2), new LinkedHashSet<>(List.of(2L, 1L)))).as("the set's order").isFalse();
    assertThat(QueryOperatorEquals.equals(new LinkedHashSet<>(List.of(2L, 1L)), List.of(1, 2))).as("either side").isFalse();
    // the comparator holds one rule for both kinds of collection too: a Long and an Integer are one value
    assertThat(BinaryComparator.equals(new HashSet<>(List.of(1L, 2L)), new LinkedHashSet<>(List.of(2, 1)))).isTrue();
  }

  /**
   * Regression test for issue #10736.
   * <p>
   * The census of #10595 ({@code theScalarRuleHoldsForEveryPairOfAPoolAsAnElement}) widened by the container: whatever the scalar
   * {@code =} says of a pair, the same pair held in any two containers, nested in a list, or as the value of a map, says too.
   */
  @Test
  void theScalarRuleHoldsForEveryPairOfAPoolInEveryPairOfContainers() {
    final List<Object> pool = new ArrayList<>();
    pool.add(STORED);
    pool.add(Date.from(INSTANT));
    pool.add(INSTANT);
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Asia/Tokyo"));
    calendar.setTimeInMillis(INSTANT.toEpochMilli());
    pool.add(calendar);
    pool.add(Timestamp.from(INSTANT));
    pool.add(INSTANT.toEpochMilli());
    pool.add("2020-01-02T03:04:05");
    pool.addAll(List.of(Date.from(INSTANT.plusSeconds(1)), STORED.plusSeconds(1), LocalDate.of(2020, 1, 2),
        ZonedDateTime.of(STORED, ZoneId.of("UTC")), OffsetDateTime.of(STORED, ZoneOffset.UTC), 1, 1L, (short) 1, 1.0f, 1.0d,
        new BigDecimal("1.00"), "1", "a", true, false, new byte[] { 1 }, new RID(1, 2), new RID(1, 3)));

    final Map<String, Function<Object, Object>> containers = containers();
    int agreed = 0;
    for (final Object a : pool)
      for (final Object b : pool) {
        final boolean scalar = QueryOperatorEquals.equals(a, b);
        final String pair = a.getClass().getSimpleName() + " " + a + " vs " + b.getClass().getSimpleName() + " " + b;
        for (final Map.Entry<String, Function<Object, Object>> left : containers.entrySet())
          for (final Map.Entry<String, Function<Object, Object>> right : containers.entrySet()) {
            final Object ca = left.getValue().apply(a);
            final Object cb = right.getValue().apply(b);
            if (ca == null || cb == null)
              continue;
            final String as = left.getKey() + " = " + right.getKey() + " for " + pair;
            assertThat(QueryOperatorEquals.equals(ca, cb)).as(as).isEqualTo(scalar);
            assertThat(QueryOperatorEquals.equals(List.of(ca), List.of(cb))).as("nested " + as).isEqualTo(scalar);
            assertThat(QueryOperatorEquals.equals(Map.of("x", ca), Map.of("x", cb))).as("map of " + as).isEqualTo(scalar);
          }
        if (scalar)
          agreed++;
      }
    // not vacuous: every value is equal to itself and the representations of the instant are equal to each other
    assertThat(agreed).isGreaterThan(pool.size() + 20);
  }
}
