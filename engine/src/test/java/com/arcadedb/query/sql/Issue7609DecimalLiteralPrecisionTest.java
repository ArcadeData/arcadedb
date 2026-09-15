/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7609: a floating point literal written without a type suffix ({@code 0.05}) was
 * parsed into a {@link Float} and then widened back to {@code double} with {@code .doubleValue()}, which
 * reproduces the single precision rounding error as a double instead of removing it. Against a {@code DOUBLE}
 * or {@code DECIMAL} property the literal was therefore not the number the user typed, and every row sitting
 * exactly on the boundary was decided the wrong way: {@code >= 0.05} answered what {@code > 0.05} answers,
 * {@code = 0.05} answered nothing, and {@code < 0.05} answered the rows that are EQUAL to 0.05.
 * <p>
 * Which side loses depends only on the sign of the rounding error of that particular literal: at 0.05, 0.1,
 * 0.2 and 0.3 the float form rounds UP, so the lower bound loses its boundary rows; at 0.7 it rounds DOWN, so
 * the upper bound does.
 * <p>
 * The same query was already correct through {@code BETWEEN}, through a bound parameter, with an explicit
 * {@code D} suffix, and through an index - so the presence of an index changed the answer, which is the part
 * that makes this more than a rounding curiosity.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7609DecimalLiteralPrecisionTest extends TestHelper {

  /** ten rows at each of these five discounts, the TPC-H Q6 shape the defect was found on */
  private static final String[] DISCOUNTS = { "0.04", "0.05", "0.06", "0.07", "0.08" };
  /** ten rows at each of these, to exercise literals whose float form rounds the other way */
  private static final String[] SWEEP     = { "0.1", "0.2", "0.3", "0.7", "0.8" };
  private static final int      PER_VALUE = 10;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Li");
    database.command("sql", "CREATE PROPERTY Li.dDouble DOUBLE");
    database.command("sql", "CREATE PROPERTY Li.dFloat FLOAT");
    database.command("sql", "CREATE PROPERTY Li.dDecimal DECIMAL");

    database.command("sql", "CREATE DOCUMENT TYPE Sw");
    database.command("sql", "CREATE PROPERTY Sw.v DOUBLE");

    database.command("sql", "CREATE DOCUMENT TYPE Ix");
    database.command("sql", "CREATE PROPERTY Ix.v DOUBLE");
    database.command("sql", "CREATE INDEX ON Ix (v) NOTUNIQUE");

    database.transaction(() -> {
      for (final String d : DISCOUNTS)
        for (int i = 0; i < PER_VALUE; i++)
          database.newDocument("Li")//
              .set("dDouble", Double.parseDouble(d))//
              .set("dFloat", Float.parseFloat(d))//
              .set("dDecimal", new BigDecimal(d))//
              .save();

      for (final String d : SWEEP)
        for (int i = 0; i < PER_VALUE; i++)
          database.newDocument("Sw").set("v", Double.parseDouble(d)).save();

      for (final String d : DISCOUNTS)
        for (int i = 0; i < PER_VALUE; i++)
          database.newDocument("Ix").set("v", Double.parseDouble(d)).save();
    });
  }

  /**
   * The reported shape. Every operator has to agree with the number the user typed, on a DOUBLE property with
   * no index, and the three ways of writing the same range have to answer the same rows.
   */
  @Test
  void everyComparisonOperatorSeesTheBoundaryRowOnADoubleProperty() {
    assertThat(count("SELECT FROM Li WHERE dDouble = 0.05")).isEqualTo(10);
    assertThat(count("SELECT FROM Li WHERE dDouble > 0.05")).isEqualTo(30);
    assertThat(count("SELECT FROM Li WHERE dDouble >= 0.05")).isEqualTo(40);
    assertThat(count("SELECT FROM Li WHERE dDouble < 0.05")).isEqualTo(10);
    assertThat(count("SELECT FROM Li WHERE dDouble <= 0.05")).isEqualTo(20);
    assertThat(count("SELECT FROM Li WHERE dDouble <> 0.05")).isEqualTo(40);
    assertThat(count("SELECT FROM Li WHERE dDouble IN [0.05]")).isEqualTo(10);

    assertThat(count("SELECT FROM Li WHERE dDouble >= 0.05 AND dDouble <= 0.07")).isEqualTo(30);
    assertThat(count("SELECT FROM Li WHERE dDouble BETWEEN 0.05 AND 0.07")).isEqualTo(30);
    assertThat(count("SELECT FROM Li WHERE dDouble >= 0.05D AND dDouble <= 0.07D")).isEqualTo(30);

    // the TPC-H Q6 symptom: the window summed two of the three buckets and reported revenue ~30% low
    assertThat(sum("SELECT sum(dDouble) AS s FROM Li WHERE dDouble >= 0.05 AND dDouble <= 0.07"))
        .isCloseTo(1.8, Offset.offset(1e-9));
  }

  /** DOUBLE and DECIMAL were both wrong; FLOAT was accidentally right because it carried the same error. */
  @Test
  void theSameComparisonsAreCorrectOnEveryDeclaredPropertyType() {
    for (final String column : new String[] { "dDouble", "dFloat", "dDecimal" }) {
      assertThat(count("SELECT FROM Li WHERE " + column + " = 0.05")).as(column + " = 0.05").isEqualTo(10);
      assertThat(count("SELECT FROM Li WHERE " + column + " >= 0.05")).as(column + " >= 0.05").isEqualTo(40);
      assertThat(count("SELECT FROM Li WHERE " + column + " <= 0.05")).as(column + " <= 0.05").isEqualTo(20);
      assertThat(count("SELECT FROM Li WHERE " + column + " < 0.05")).as(column + " < 0.05").isEqualTo(10);
      assertThat(count("SELECT FROM Li WHERE " + column + " > 0.05")).as(column + " > 0.05").isEqualTo(30);
      assertThat(count("SELECT FROM Li WHERE " + column + " BETWEEN 0.05 AND 0.07")).as(column + " between")
          .isEqualTo(30);
    }
  }

  /**
   * At 0.05/0.1/0.2/0.3 the float form of the literal sits ABOVE the stored double, so {@code >=} lost its
   * boundary rows; at 0.7 it sits BELOW, so {@code <=} did. A range written as two comparisons could
   * therefore lose either end, both, or neither, depending only on the two constants.
   */
  @Test
  void aLiteralWhoseFloatFormRoundsDownLosesTheOtherBound() {
    // rows strictly below the literal, out of the fifty in Sw
    assertSweep("0.1", 0);
    assertSweep("0.2", 10);
    assertSweep("0.3", 20);
    assertSweep("0.7", 30);
  }

  private void assertSweep(final String literal, final int below) {
    assertThat(count("SELECT FROM Sw WHERE v = " + literal)).as("v = " + literal).isEqualTo(10);
    assertThat(count("SELECT FROM Sw WHERE v >= " + literal)).as("v >= " + literal).isEqualTo(50 - below);
    assertThat(count("SELECT FROM Sw WHERE v <= " + literal)).as("v <= " + literal).isEqualTo(below + 10);
    assertThat(count("SELECT FROM Sw WHERE v > " + literal)).as("v > " + literal).isEqualTo(40 - below);
    assertThat(count("SELECT FROM Sw WHERE v < " + literal)).as("v < " + literal).isEqualTo(below);
  }

  /**
   * The index path converts the bound to the property's own class before comparing, so it was right while the
   * filter path was wrong: adding or dropping an index changed the answer of the very same SQL. The two plans
   * must now agree row for row.
   */
  @Test
  void theIndexedAndUnindexedPlansAnswerTheSameRows() {
    for (final String predicate : new String[] { "= 0.05", "> 0.05", ">= 0.05", "< 0.05", "<= 0.05",
        "BETWEEN 0.05 AND 0.07" }) {
      final long indexed = count("SELECT FROM Ix WHERE v " + predicate);
      final long filtered = count("SELECT FROM Li WHERE dDouble " + predicate);
      assertThat(indexed).as("index vs filter on " + predicate).isEqualTo(filtered);
    }

    assertThat(explain("SELECT FROM Ix WHERE v >= 0.05")).contains("FETCH FROM INDEX");
    assertThat(count("SELECT FROM Ix WHERE v >= 0.05")).isEqualTo(40);
  }

  /** The three spellings that were already correct must stay correct. */
  @Test
  void boundParametersAndExplicitSuffixesKeepWorking() {
    final Map<String, Object> params = Map.of("d", 0.05d);
    assertThat(count("SELECT FROM Li WHERE dDouble = :d", params)).isEqualTo(10);
    assertThat(count("SELECT FROM Li WHERE dDouble >= :d", params)).isEqualTo(40);

    assertThat(count("SELECT FROM Li WHERE dDouble = 0.05D")).isEqualTo(10);
    assertThat(count("SELECT FROM Li WHERE dDouble >= 0.05D")).isEqualTo(40);

    // a Float parameter still binds as a Float, and still matches the FLOAT property
    assertThat(count("SELECT FROM Li WHERE dFloat = :d", Map.of("d", 0.05f))).isEqualTo(10);
  }

  /**
   * The literal itself: a suffix-less floating point literal is a {@code double}, an {@code F} suffix still
   * asks for single precision, and a {@code D} suffix is unchanged.
   */
  @Test
  void aSuffixLessLiteralIsADoubleAndTheSuffixesStillSelectTheirType() {
    assertThat((Object) single("SELECT 0.05 AS v").getProperty("v")).isInstanceOf(Double.class).isEqualTo(0.05d);
    assertThat((Object) single("SELECT -0.05 AS v").getProperty("v")).isInstanceOf(Double.class).isEqualTo(-0.05d);
    assertThat((Object) single("SELECT 0.05D AS v").getProperty("v")).isInstanceOf(Double.class).isEqualTo(0.05d);
    assertThat((Object) single("SELECT 0.05F AS v").getProperty("v")).isInstanceOf(Float.class).isEqualTo(0.05f);
    // a magnitude beyond the float range was already a double, and stays one
    assertThat((Object) single("SELECT 1.0E39 AS v").getProperty("v")).isInstanceOf(Double.class).isEqualTo(1.0E39d);
  }

  /**
   * The comparator half of the same defect, reachable without any literal at all: widening a genuine
   * {@link Float} to {@code double} or {@link BigDecimal} must re-read its decimal form, exactly as
   * {@link Type#convert} already does for the index path, rather than reproduce its rounding error.
   */
  @Test
  void castComparableNumberWidensAFloatWithoutReproducingItsRoundingError() {
    assertThat(compare(Type.castComparableNumber(0.05f, 0.05d))).as("0.05f vs 0.05d").isZero();
    assertThat(Type.castComparableNumber(0.05f, 0.05d)[0]).isEqualTo(0.05d);

    assertThat(compare(Type.castComparableNumber(0.05d, 0.05f))).as("0.05d vs 0.05f").isZero();
    assertThat(compare(Type.castComparableNumber(0.05f, new BigDecimal("0.05")))).as("0.05f vs decimal").isZero();
    assertThat(compare(Type.castComparableNumber(new BigDecimal("0.05"), 0.05f))).as("decimal vs 0.05f").isZero();

    // ordering is still respected, the widening only removes the error it used to introduce
    assertThat(compare(Type.castComparableNumber(0.05f, 0.06d))).as("0.05f vs 0.06d").isNegative();
    assertThat(compare(Type.castComparableNumber(0.06f, 0.05d))).as("0.06f vs 0.05d").isPositive();

    // the allocation-free paths: an integral float widens exactly, and the non-finite values have no decimal form
    assertThat(Type.castComparableNumber(42.0f, 0.0d)[0]).isEqualTo(42.0d);
    assertThat(compare(Type.castComparableNumber(42.0f, 42.0d))).as("42.0f vs 42.0d").isZero();
    assertThat(Type.castComparableNumber(Float.NaN, 0.0d)[0]).isEqualTo(Double.NaN);
    assertThat(Type.castComparableNumber(Float.POSITIVE_INFINITY, 0.0d)[0]).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(compare(Type.castComparableNumber(Float.MAX_VALUE, 0.0d))).as("MAX_VALUE vs 0").isPositive();
  }

  // ---------------------------------------------------------------------------------------------

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static int compare(final Number[] pair) {
    return ((Comparable) pair[0]).compareTo(pair[1]);
  }

  private long count(final String sql) {
    return count(sql, Map.of());
  }

  private long count(final String sql, final Map<String, Object> params) {
    long rows = 0;
    try (final ResultSet rs = params.isEmpty() ? database.query("sql", sql) : database.query("sql", sql, params)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private double sum(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final Object value = rs.next().getProperty("s");
      return value == null ? 0 : ((Number) value).doubleValue();
    }
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next();
    }
  }

  private String explain(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString").toString();
    }
  }
}
