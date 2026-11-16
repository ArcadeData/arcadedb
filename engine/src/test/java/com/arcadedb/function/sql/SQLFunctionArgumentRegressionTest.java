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
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.sql.misc.SQLFunctionIf;
import com.arcadedb.function.sql.misc.SQLStaticReflectiveFunction;
import com.arcadedb.function.sql.vector.SparseVector;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression tests for the argument handling of the SQL functions, one nested class per reported issue.
 */
class SQLFunctionArgumentRegressionTest {

  /**
   * Regression for #10699: positional numeric arguments of vector.sparseCreate, randomInt and if were narrowed through intValue().
   *
   * Issue #10699: three SQL functions read a positional numeric argument through {@code Number.intValue()}, which
   * truncates a fraction and wraps a value past the int range, so the function went on with a number the caller never
   * wrote: {@code vector.sparseCreate} built a vector of the wrong dimension, {@code randomInt} reported every bad bound as
   * "must be positive" (and a non-numeric one as a raw {@link NumberFormatException}), and {@code if} took the false branch
   * for {@code 0.5} and {@code 3000000000}.
   */
  @Nested
  class PositionalIntArguments extends TestHelper {

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
    void sparseCreateRefusesANullDimensionNamingItself() {
      assertThatThrownBy(() -> sparseCreate("[0,1]", "[1.0,2.0]", "null"))
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("vector.sparseCreate")
          .hasMessageContaining("dimensions");
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

  /**
   * Regression for #11051: the SQL function factory looked names up with the raw spelling in hasFunction, isBuiltIn and unregister.
   *
   * Issue #11051: the SQL function factory stores every name lower-cased, and {@code register} and {@code getFunctionInstance}
   * lower-cased on their way in, but {@code hasFunction}, {@code isSharedInstance}, {@code isBuiltIn} and {@code unregister} used
   * the raw name. A Cypher {@code CALL} gates on {@code hasFunction}, so {@code CALL vector.l2Distance(...)} was refused as an
   * unknown procedure under the very name the function registers itself with, and {@code unregister("Acme.Twice")} removed
   * nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FunctionNameCase extends TestHelper {

    /** A scalar function registered under a mixed-case name, the way an application spells its own namespace. */
    public static class Twice extends SQLFunctionAbstract {
      public static final String NAME = "Acme11051.Twice";

      public Twice() {
        super(NAME);
      }

      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        return ((Number) params[0]).intValue() * 2;
      }

      @Override
      public String getAlias() {
        return "acme11051.doubleIt";
      }

      @Override
      public String getSyntax() {
        return NAME + "(<n>)";
      }
    }

    @Test
    void everyBuiltInAnswersTheSameUnderItsOwnSpellingAndAnyOther() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      final List<String> disagreements = new ArrayList<>();
      for (final String key : new TreeSet<>(factory.getBuiltInFunctionNames())) {
        final SQLFunction function = factory.getFunctionInstance(key);
        assertThat(function).as(key).isNotNull();
        // The canonical spelling is the one the function gives itself; an alias key keeps its own spelling
        final String canonical = function.getName().toLowerCase(Locale.ENGLISH).equals(key) ? function.getName() : key;
        for (final String spelling : new String[] { canonical, key.toUpperCase(Locale.ENGLISH) }) {
          if (!factory.hasFunction(spelling))
            disagreements.add("hasFunction(" + spelling + ")");
          if (!factory.isBuiltIn(spelling))
            disagreements.add("isBuiltIn(" + spelling + ")");
          if (factory.isSharedInstance(spelling) != factory.isSharedInstance(key))
            disagreements.add("isSharedInstance(" + spelling + ")");
          if (factory.getFunctionInstance(spelling) == null)
            disagreements.add("getFunctionInstance(" + spelling + ")");
        }
      }
      assertThat(disagreements).isEmpty();
    }

    @Test
    void cypherCallResolvesCamelCaseBuiltInsUnderTheirCanonicalNames() {
      assertThat(callValue("CALL vector.l2Distance([1,0],[0,1]) YIELD value RETURN value")).isCloseTo(Math.sqrt(2.0), within(1e-6));
      assertThat(callValue("CALL vector.l2distance([1,0],[0,1]) YIELD value RETURN value")).isCloseTo(Math.sqrt(2.0), within(1e-6));
      assertThat(callValue("CALL vector.Magnitude([3,4]) YIELD value RETURN value")).isCloseTo(5.0, within(1e-6));
      assertThat(callValue("CALL vector.Dimension([1,2,3]) YIELD value RETURN value")).isCloseTo(3.0, within(1e-6));
      assertThat(callValue("CALL vector.cosineSimilarity([1,0],[1,0]) YIELD value RETURN value")).isCloseTo(1.0, within(1e-6));

      try (final ResultSet rs = database.query("opencypher", "CALL geo.asText(geo.point(1,2)) YIELD value RETURN value")) {
        assertThat(rs.next().<String>getProperty("value")).isEqualTo("POINT (1 2)");
      }
    }

    @Test
    void unregisterUnderTheRegisteredSpellingRemovesTheFunctionAndItsAlias() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      factory.register(new Twice());
      try {
        assertThat(factory.hasFunction(Twice.NAME)).isTrue();
        assertThat(factory.hasFunction("acme11051.twice")).isTrue();
        assertThat(factory.hasFunction("ACME11051.DOUBLEIT")).isTrue();
        assertThat(factory.isSharedInstance(Twice.NAME)).isTrue();
        assertThat(factory.isBuiltIn(Twice.NAME)).as("registered after construction").isFalse();
        try (final ResultSet rs = database.query("opencypher", "CALL Acme11051.Twice(21) YIELD value RETURN value")) {
          assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(42);
        }
      } finally {
        factory.unregister(Twice.NAME);
        // An instance registration also lands in the process-wide unified registry, which unregister() leaves alone on purpose
        FunctionRegistry.unregister(Twice.NAME);
      }
      assertThat(factory.hasFunction(Twice.NAME)).isFalse();
      assertThat(factory.getFunctionInstance("acme11051.twice")).isNull();
      assertThat(factory.hasFunction("acme11051.doubleit")).as("the alias goes with the function").isFalse();
    }

    @Test
    void unregisterOfAClassRegistrationUnderAnotherCaseRemovesIt() {
      final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
      factory.register(Twice.NAME, Twice.class);
      try {
        assertThat(factory.hasFunction(Twice.NAME)).isTrue();
        assertThat(factory.isSharedInstance(Twice.NAME)).as("a class registration makes one instance per call").isFalse();
        assertThat(factory.getFunctionInstance(Twice.NAME)).isInstanceOf(Twice.class);
      } finally {
        factory.unregister("ACME11051.TWICE");
      }
      assertThat(factory.hasFunction(Twice.NAME)).isFalse();
      assertThat(factory.hasFunction("acme11051.doubleit")).isFalse();
    }

    private double callValue(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        final Result row = rs.next();
        return ((Number) row.getProperty("value")).doubleValue();
      }
    }
  }

  /**
   * Regression for #6389: collection, string and misc functions threw raw runtime exceptions for ordinary input.
   *
   * A batch of SQL collection/string/misc methods and functions threw a raw runtime exception - an HTTP 500 - for
   * ordinary valid input, or returned a confident answer for input they never looked at (issue #6389). Each is now
   * either the natural answer or a typed argument error, which the HTTP layer maps to 400.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FunctionArgumentErrors extends TestHelper {

    @Test
    void lastIndexOfGuardsNullsLikeItsIndexOfSibling() {
      try (final ResultSet rs = database.query("sql",
          "SELECT nothing.lastindexof('a') AS a, nothing.indexof('a') AS b, 'abcabc'.lastindexof('a') AS c")) {
        final var row = rs.next();
        assertThat(row.<Object>getProperty("a")).isNull();
        assertThat(row.<Object>getProperty("b")).isNull();
        assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(3);
      }
    }

    @Test
    void joinRendersANullElementInsteadOfThrowing() {
      try (final ResultSet rs = database.query("sql", "SELECT [1,null,3].join('-') AS j")) {
        assertThat(rs.next().<String>getProperty("j")).isEqualTo("1-null-3");
      }
    }

    @Test
    void mapRejectsANonStringKey() {
      assertThatThrownBy(() -> database.query("sql", "SELECT map(1,'a') AS m").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("STRING key");
    }

    /**
     * A null key is rejected rather than turned into a null entry. The raw cast used to produce {@code (String) null}
     * and store it silently, so a mistyped call built a map with a key nobody can name.
     */
    @Test
    void mapRejectsANullKey() {
      assertThatThrownBy(() -> database.query("sql", "SELECT map(null,'a') AS m").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("STRING key");
    }

    @Test
    void asMapConvertsANonStringKey() {
      try (final ResultSet rs = database.query("sql", "SELECT [1,2,3,4].asMap() AS m")) {
        assertThat(rs.next().<Map<String, Object>>getProperty("m")).containsEntry("1", 2).containsEntry("3", 4);
      }
    }

    @Test
    void convertRejectsAnUnknownTypeName() {
      assertThatThrownBy(() -> database.query("sql", "SELECT 'x'.convert('nope') AS c").next())
          .isInstanceOf(CommandSQLParsingException.class)
          .hasMessageContaining("nope");
    }

    @Test
    void convertStillWorksForAKnownType() {
      try (final ResultSet rs = database.query("sql", "SELECT '42'.convert('integer') AS c")) {
        assertThat(rs.next().<Object>getProperty("c")).isEqualTo(42);
      }
    }

    @Test
    void decodeReportsMalformedInputAndPassesNullThrough() {
      assertThatThrownBy(() -> database.query("sql", "SELECT decode('!!!','base64') AS d").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("base64");

      try (final ResultSet rs = database.query("sql", "SELECT decode(null,'base64') AS d")) {
        assertThat(rs.next().<Object>getProperty("d")).isNull();
      }
    }

    @Test
    void decodeStillRoundTripsWithEncode() {
      try (final ResultSet rs = database.query("sql", "SELECT decode(encode('hello','base64'),'base64').asString() AS d")) {
        assertThat(rs.next().<String>getProperty("d")).contains("hello");
      }
    }

    @Test
    void formatReportsAMismatchedConversion() {
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%d','x') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("%d");
    }

    @Test
    void formatReportsAnUnknownConversion() {
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%y') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("%y");
    }

    @Test
    void formatRefusesAnExcessiveFieldWidth() {
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%99999999s','x') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
      assertThatThrownBy(() -> database.query("sql", "SELECT 'x'.format('%99999999s') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
    }

    /**
     * Both halves of a `%<width>.<precision>s` allocate, and `<` is a Formatter flag - reuse the previous argument -
     * not a conversion character. Measuring only the last number read, and stopping the scan at `<`, each let a field
     * far over the ceiling through the check the ceiling exists for.
     */
    @Test
    void formatWidthCeilingHasNoBypass() {
      // Width behind a precision: the argument is truncated to one character, then padded back out to two million.
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%2000000.1s','x') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
      // The same shape on a numeric conversion, where the width is large enough to be an OutOfMemoryError.
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%2000000000.1f', 3.14) AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
      // '<' reuses the previous argument; the width follows it.
      assertThatThrownBy(() -> database.query("sql", "SELECT format('%s%<2000000s','x') AS f").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
      // And an argument index still is not a field size, so an ordinary reference is not mistaken for one.
      try (final ResultSet rs = database.query("sql", "SELECT format('%2000000$s','x','y') AS f")) {
        assertThatThrownBy(rs::next).isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("limit");
      }
    }

    @Test
    void formatStillFormats() {
      try (final ResultSet rs = database.query("sql",
          "SELECT format('%s-%05d','a',7) AS f, format('%1$s%1$s','z') AS g, format('100%%') AS h, "
              + "format('%10.3s|','abcdef') AS i, format('%s%<s','q') AS j")) {
        final var row = rs.next();
        assertThat(row.<String>getProperty("f")).isEqualTo("a-00007");
        assertThat(row.<String>getProperty("g")).isEqualTo("zz");
        assertThat(row.<String>getProperty("h")).isEqualTo("100%");
        assertThat(row.<String>getProperty("i")).isEqualTo("       abc|");
        assertThat(row.<String>getProperty("j")).isEqualTo("qq");
      }
    }

    @Test
    void maxAndMinRejectMutuallyIncomparableValues() {
      assertThatThrownBy(() -> database.query("sql", "SELECT max([1,'a']) AS m").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot compare");
      assertThatThrownBy(() -> database.query("sql", "SELECT min([1,'a']) AS m").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot compare");
    }

    @Test
    void maxAndMinStillWorkOnHomogeneousInput() {
      try (final ResultSet rs = database.query("sql", "SELECT max([3,1,2]) AS a, min([3,1,2]) AS b, max(['a','c','b']) AS c")) {
        final var row = rs.next();
        assertThat(row.<Number>getProperty("a").intValue()).isEqualTo(3);
        assertThat(row.<Number>getProperty("b").intValue()).isEqualTo(1);
        assertThat(row.<String>getProperty("c")).isEqualTo("c");
      }
    }

    @Test
    void boolAndOrRejectNonBooleanInput() {
      for (final String query : new String[] { "SELECT bool_and([1,2,3]) AS b", "SELECT bool_and(1,2) AS b",
          "SELECT bool_and(5) AS b", "SELECT bool_or([1,2,3]) AS b", "SELECT bool_or(5) AS b" })
        assertThatThrownBy(() -> database.query("sql", query).next())
            .as(query)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("boolean input");
    }

    @Test
    void boolAndOrStillWorkOnBooleans() {
      try (final ResultSet rs = database.query("sql",
          "SELECT bool_and([true,true]) AS a, bool_and([true,false]) AS b, bool_or([false,true]) AS c")) {
        final var row = rs.next();
        assertThat(row.<Boolean>getProperty("a")).isTrue();
        assertThat(row.<Boolean>getProperty("b")).isFalse();
        assertThat(row.<Boolean>getProperty("c")).isTrue();
      }
    }

    @Test
    void stringIndexMethodsAcceptADecimalIndex() {
      try (final ResultSet rs = database.query("sql",
          "SELECT 'abcdef'.substring(2.5) AS a, 'abcdef'.left(2.9) AS b, 'abcdef'.right(2.1) AS c, 'abcdef'.charAt(1.7) AS d")) {
        final var row = rs.next();
        // Truncated toward zero, then the #5885 clamps apply as they always did.
        assertThat(row.<String>getProperty("a")).isEqualTo("cdef");
        assertThat(row.<String>getProperty("b")).isEqualTo("ab");
        assertThat(row.<String>getProperty("c")).isEqualTo("ef");
        assertThat(row.<String>getProperty("d")).isEqualTo("b");
      }
    }

    @Test
    void stringIndexMethodsRejectNonNumericIndexes() {
      assertThatThrownBy(() -> database.query("sql", "SELECT 'abcdef'.substring('x') AS a").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("numeric");
    }

    /**
     * The decimal fallback must not hand an unbounded literal to BigDecimal: a character index is ten digits and a
     * sign, so a literal orders of magnitude longer is refused by length rather than parsed to find out.
     */
    @Test
    void stringIndexMethodsRefuseAnAbsurdlyLongLiteral() {
      final String huge = "9".repeat(100_000);
      assertThatThrownBy(() -> database.query("sql", "SELECT 'abcdef'.substring('" + huge + "') AS a").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit")
          // The message summarises the literal by length rather than echoing 100,000 characters into a log.
          .hasMessageNotContaining(huge);
      // A long-but-plausible literal still saturates rather than being refused.
      try (final ResultSet rs = database.query("sql", "SELECT 'abcdef'.substring('00000000000000000002.5') AS a")) {
        assertThat(rs.next().<String>getProperty("a")).isEqualTo("cdef");
      }
    }
  }

  /**
   * Regression for #9986: a null argument to a reflective function raised a NullPointerException instead of a parsing error.
   *
   * Issue #9986: {@code SELECT math_max(null)} raised a raw {@link NullPointerException} out of the very message that
   * was meant to report that no overload matches, because the diagnostic called {@code getClass()} on each argument.
   * <p>
   * The neighbouring case went wrong too, more quietly: a null where every overload wants a primitive
   * ({@code math_abs(null)}) was matched anyway, failed inside {@code Method.invoke}, and came back as {@code null} after
   * logging a SEVERE line - once per row. A null now matches only a reference parameter, and a null that leaves no
   * overload of the right arity is SQL's NULL in, NULL out, with nothing logged.
   * <p>
   * The guarantee is a sweep over every registered reflective function, at every arity from one to three, with all-null
   * arguments: the answer is a value or a {@link CommandParsingException}, never anything else.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReflectiveFunctionNullArgument extends TestHelper {

    @Test
    void aNullArgumentToAnArityNoOverloadHasIsReportedNotAnNpe() {
      assertThatThrownBy(() -> single("select math_max(null) as v"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Unable to find a function for math_max(null [ null ])");
    }

    @Test
    void aNullForAPrimitiveParameterIsNullInNullOutWithNothingLogged() {
      final List<String> severe = WarningCapture.captureSevere(() -> {
        assertThat(single("select math_abs(null) as v")).isNull();
        assertThat(single("select math_max(null, 1) as v")).isNull();
        assertThat(single("select math_max(1, null) as v")).isNull();
      });
      assertThat(severe).as("a NULL argument is not an engine error").isEmpty();

      // AND A NON-NULL CALL STILL PICKS ITS OVERLOAD
      assertThat(single("select math_max(3, 7) as v")).isEqualTo(7);
      assertThat(single("select math_abs(-2.5) as v")).isEqualTo(2.5);
    }

    @Test
    void aWrongTypedArgumentBesideANullIsStillReported() {
      // NO math_max OVERLOAD TAKES A STRING: THE NULL BESIDE IT DOES NOT TURN THAT INTO A SILENT NULL
      assertThatThrownBy(() -> single("select math_max('text', null) as v"))
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Unable to find a function for math_max(text [ java.lang.String ], null [ null ])");
    }

    @Test
    void everyReflectiveFunctionAnswersAllNullArgumentsWithAValueOrAParsingError() {
      final List<String> failures = new ArrayList<>();
      for (final Map.Entry<String, Object> entry : DefaultSQLFunctionFactory.getInstance().getFunctions().entrySet()) {
        if (!(entry.getValue() instanceof SQLStaticReflectiveFunction))
          continue;

        for (int arity = 1; arity <= 3; arity++) {
          final String sql = "select " + entry.getKey() + "(" + String.join(", ", Collections.nCopies(arity, "null"))
              + ") as v";
          try {
            single(sql);
          } catch (final CommandParsingException expected) {
            // NO OVERLOAD OF THIS ARITY: REPORTED, WHICH IS FINE
          } catch (final RuntimeException e) {
            failures.add(sql + " -> " + e);
          }
        }
      }

      assertThat(failures).isEmpty();
    }

    private Object single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().getProperty("v");
      }
    }
  }
}
