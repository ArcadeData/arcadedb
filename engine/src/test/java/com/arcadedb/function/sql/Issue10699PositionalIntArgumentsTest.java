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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, see the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.misc.SQLFunctionIf;
import com.arcadedb.function.sql.vector.SparseVector;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10699: three SQL functions read a positional numeric argument through {@code Number.intValue()}, which
 * truncates a fraction and wraps a value past the int range, so the function went on with a number the caller never
 * wrote: {@code vector.sparseCreate} built a vector of the wrong dimension, {@code randomInt} reported every bad bound as
 * "must be positive" (and a non-numeric one as a raw {@link NumberFormatException}), and {@code if} took the false branch
 * for {@code 0.5} and {@code 3000000000}.
 */
class Issue10699PositionalIntArgumentsTest extends TestHelper {

  // ---------------------------------------------------------------- vector.sparseCreate <dimensions>

  @Test
  void sparseCreateStillBuildsTheDimensionAskedFor() {
    assertThat(sparseCreate("[0,1]", "[1.0,2.0]", "6").getDimensions()).isEqualTo(6);
    assertThat(sparseCreate("[0,1]", "[1.0,2.0]", "6.0").getDimensions()).isEqualTo(6);
  }

  @Test
  void sparseCreateRefusesAFractionalDimensionInsteadOfTruncatingIt() {
    assertThatThrownBy(() -> sparseCreate("[0,1]", "[1.0,2.0]", "2.9"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.sparseCreate")
        .hasMessageContaining("dimensions")
        .hasMessageContaining("2.9");
  }

  @Test
  void sparseCreateRefusesADimensionPastTheIntRangeInsteadOfWrappingIt() {
    assertThatThrownBy(() -> sparseCreate("[0,1]", "[1.0,2.0]", "3000000000"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.sparseCreate")
        .hasMessageContaining("dimensions")
        .hasMessageNotContaining("-1294967296");
  }

  @Test
  void sparseCreateRefusesANonPositiveDimension() {
    for (final String dims : new String[] { "0", "-3" })
      assertThatThrownBy(() -> sparseCreate("[0]", "[1.0]", dims))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("vector.sparseCreate")
          .hasMessageContaining("dimensions must be positive");
  }

  @Test
  void sparseCreateNamesItselfWhenAnIndexDoesNotFitTheDimension() {
    assertThatThrownBy(() -> sparseCreate("[0,5]", "[1.0,2.0]", "3"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("vector.sparseCreate")
        .hasMessageContaining("5");
  }

  @Test
  void sparseCreateRefusesAFractionalOrWrappingIndex() {
    assertThatThrownBy(() -> sparseCreate("[0,1.5]", "[1.0,2.0]", "4"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("1.5");
    assertThatThrownBy(() -> sparseCreate("[0,3000000000]", "[1.0,2.0]", null))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("3000000000");
  }

  /**
   * The indices conversion is shared with {@code vector.sparseNeighbors}' query vector, so it is checked at the helper
   * too, for every container shape it reads.
   */
  @Test
  void sparseIndicesConversionRefusesWhatItUsedToTruncate() {
    assertThat(VectorUtils.toIntArray(List.of(0, 2L, 4.0))).containsExactly(0, 2, 4);
    assertThat(VectorUtils.toIntArray(new long[] { 1, 7 })).containsExactly(1, 7);

    assertThatThrownBy(() -> VectorUtils.toIntArray(List.of(0, 2.5))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2.5");
    assertThatThrownBy(() -> VectorUtils.toIntArray(new Object[] { 3_000_000_000L }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("3000000000");
    assertThatThrownBy(() -> VectorUtils.toIntArray(new long[] { 3_000_000_000L }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("3000000000");
  }

  // ---------------------------------------------------------------- randomInt <bound>

  @Test
  void randomIntStillDrawsBelowAWholeBound() {
    for (final String bound : new String[] { "10", "10.0", "'10'" })
      try (final ResultSet rs = database.query("sql", "SELECT randomInt(" + bound + ") AS v")) {
        assertThat(rs.next().<Integer>getProperty("v")).isBetween(0, 9);
      }
  }

  @Test
  void randomIntRefusesAFractionalBoundInsteadOfTruncatingIt() {
    for (final String bound : new String[] { "2.5", "0.9" })
      assertThatThrownBy(() -> randomInt(bound))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("randomInt()")
          .hasMessageContaining("whole number")
          .hasMessageContaining(bound);
  }

  @Test
  void randomIntRefusesABoundPastTheIntRangeWithoutCallingItNonPositive() {
    assertThatThrownBy(() -> randomInt("3000000000"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("randomInt()")
        .hasMessageContaining("3000000000")
        .hasMessageNotContaining("must be positive");
  }

  @Test
  void randomIntRefusesANonNumericBoundWithItsOwnName() {
    assertThatThrownBy(() -> randomInt("'abc'"))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(NumberFormatException.class)
        .hasMessageContaining("randomInt()")
        .hasMessageContaining("abc");
  }

  @Test
  void randomIntRefusesANonPositiveBoundWithItsOwnName() {
    for (final String bound : new String[] { "0", "-4" })
      assertThatThrownBy(() -> randomInt(bound))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("randomInt()")
          .hasMessageContaining("must be positive");
  }

  // ---------------------------------------------------------------- if <condition>

  @Test
  void ifTakesTheTrueBranchForAPositiveFractionOrAValuePastTheIntRange() {
    for (final String condition : new String[] { "0.5", "0.9999", "3000000000", "1", "2.5" })
      assertThat(ifResult(condition)).as("if(%s, 'T', 'F')", condition).isEqualTo("T");
  }

  @Test
  void ifTakesTheFalseBranchForZeroAndNegatives() {
    for (final String condition : new String[] { "0", "0.0", "-0.5", "-1", "-3000000000" })
      assertThat(ifResult(condition)).as("if(%s, 'T', 'F')", condition).isEqualTo("F");
  }

  @Test
  void ifReadsTheSignOfEveryNumberShapeWithoutNarrowingIt() {
    final SQLFunctionIf function = new SQLFunctionIf();
    final Object[][] cases = { //
        { new BigDecimal("1E-400"), "T" }, //
        { new BigDecimal("-1E-400"), "F" }, //
        { new BigInteger("100000000000000000000000"), "T" }, //
        { new BigInteger("-100000000000000000000000"), "F" }, //
        { 4294967296L, "T" }, // intValue() is 0
        { Double.NaN, "F" }, //
        { 0.1f, "T" } };
    for (final Object[] c : cases)
      assertThat(function.execute(null, null, null, new Object[] { c[0], "T", "F" }, null)).as("if(%s)", c[0])
          .isEqualTo(c[1]);
  }

  // ---------------------------------------------------------------- helpers

  private SparseVector sparseCreate(final String indices, final String values, final String dimensions) {
    final String sql = "SELECT `vector.sparseCreate`(" + indices + ", " + values + (dimensions != null ? ", " + dimensions : "")
        + ") AS v";
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }

  private Object randomInt(final String bound) {
    try (final ResultSet rs = database.query("sql", "SELECT randomInt(" + bound + ") AS v")) {
      return rs.next().getProperty("v");
    }
  }

  private Object ifResult(final String condition) {
    try (final ResultSet rs = database.query("sql", "SELECT if(" + condition + ", 'T', 'F') AS v")) {
      return rs.next().getProperty("v");
    }
  }
}
