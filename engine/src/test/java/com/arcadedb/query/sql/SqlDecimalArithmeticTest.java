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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests, one nested class per reported case.
 */
class SqlDecimalArithmeticTest {
  /**
   * Regression for #7609.
   * <p>
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
  @Nested
  class DecimalLiteralPrecision extends TestHelper {

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

      database.command("sql", "CREATE DOCUMENT TYPE Neg");
      database.command("sql", "CREATE PROPERTY Neg.v DOUBLE");

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

        for (final String d : new String[] { "-0.06", "-0.05", "-0.04" })
          for (int i = 0; i < PER_VALUE; i++)
            database.newDocument("Neg").set("v", Double.parseDouble(d)).save();

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
        // IN and NOT IN take the hashed fast path of InListMembership, which keys its operands separately
        assertThat(count("SELECT FROM Li WHERE " + column + " IN [0.05]")).as(column + " IN [0.05]").isEqualTo(10);
        assertThat(count("SELECT FROM Li WHERE " + column + " IN [0.05, 0.07]")).as(column + " IN [0.05,0.07]")
            .isEqualTo(20);
        assertThat(count("SELECT FROM Li WHERE " + column + " NOT IN [0.05]")).as(column + " NOT IN [0.05]")
            .isEqualTo(40);
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

    /**
     * Asserts every operator at {@code literal} against the fifty records of type {@code Sw}.
     *
     * @param literal the suffix-less literal under test
     * @param below   how many of the fifty records sit strictly below it
     */
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
     * The sign travels a path of its own - the JavaCC node carries it in a field while the ANTLR tree makes it a
     * unary minus over the literal - so the boundary is asserted on the negative side too. Thirty records, ten
     * each at -0.06, -0.05 and -0.04.
     */
    @Test
    void aNegativeLiteralSeesItsBoundaryRecordsToo() {
      assertThat(count("SELECT FROM Neg WHERE v = -0.05")).isEqualTo(10);
      assertThat(count("SELECT FROM Neg WHERE v > -0.05")).isEqualTo(10);
      assertThat(count("SELECT FROM Neg WHERE v >= -0.05")).isEqualTo(20);
      assertThat(count("SELECT FROM Neg WHERE v < -0.05")).isEqualTo(10);
      assertThat(count("SELECT FROM Neg WHERE v <= -0.05")).isEqualTo(20);
      assertThat(count("SELECT FROM Neg WHERE v BETWEEN -0.05 AND -0.04")).isEqualTo(20);
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
     * The arithmetic half of the same defect. {@code MathExpression} and {@link Type#increment} carried the very
     * same {@code .doubleValue()} / {@code BigDecimal.valueOf(float)} widening, and making a suffix-less literal a
     * Double made those lines MORE reachable, not less: {@code dFloat + 0.05} used to be Float plus Float and is
     * now Float plus Double. Adding 0.05 to a FLOAT column holding 0.05 has to answer 0.1, not 0.10000000149011612.
     */
    @Test
    void arithmeticMixingAFloatWithADoubleDoesNotCarryTheRoundingError() {
      assertThat(sum("SELECT dFloat + 0.05 AS s FROM Li WHERE dFloat = 0.05F LIMIT 1")).isEqualTo(0.1d);
      assertThat(sum("SELECT 0.05 + dFloat AS s FROM Li WHERE dFloat = 0.05F LIMIT 1")).isEqualTo(0.1d);
      assertThat(sum("SELECT dFloat - 0.05 AS s FROM Li WHERE dFloat = 0.05F LIMIT 1")).isEqualTo(0.0d);
      assertThat(sum("SELECT 0.05 - dFloat AS s FROM Li WHERE dFloat = 0.05F LIMIT 1")).isEqualTo(0.0d);

      // sum() and avg() reach the same widening through Type.increment, once a Double joins the accumulation
      assertThat(sum("SELECT sum(dFloat + 0.05) AS s FROM Li WHERE dFloat = 0.05F")).isCloseTo(1.0d, Offset.offset(1e-12));

      // the decimal side, where BigDecimal.valueOf(float) had no float overload to protect it
      assertThat(compare(Type.castComparableNumber(Type.increment(0.05f, new BigDecimal("0.05")), new BigDecimal("0.10"))))
          .as("0.05f + decimal 0.05").isZero();
      assertThat(compare(Type.castComparableNumber(Type.decrement(new BigDecimal("0.05"), 0.05f), BigDecimal.ZERO)))
          .as("decimal 0.05 - 0.05f").isZero();
      assertThat(Type.increment(0.05f, 0.05d)).isEqualTo(0.1d);
      assertThat(Type.increment(0.05d, 0.05f)).isEqualTo(0.1d);
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

      // the BigDecimal branch had no Long case at all, so the couple came back uncast and the caller threw
      assertThat(compare(Type.castComparableNumber(new BigDecimal("5"), 5L))).as("decimal 5 vs 5L").isZero();
      assertThat(compare(Type.castComparableNumber(new BigDecimal("3000000000"), 3000000000L)))
          .as("decimal vs a Long past the int range").isZero();
      assertThat(compare(Type.castComparableNumber(new BigDecimal("4"), 5L))).as("decimal 4 vs 5L").isNegative();

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

    /**
     * The write path had the same widening: coercing a value into a {@code DOUBLE} property with
     * {@code .doubleValue()} persists the single precision error, where it outlives the statement that wrote it.
     */
    @Test
    void writingAFloatIntoADoublePropertyStoresTheNumberItReads() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Written");
        database.command("sql", "CREATE PROPERTY Written.d DOUBLE");
        database.command("sql", "INSERT INTO Written SET d = :v", Map.of("v", 0.05f));
        database.command("sql", "INSERT INTO Written SET d = 0.05");
      });

      assertThat(count("SELECT FROM Written WHERE d = 0.05")).as("both records read back as 0.05").isEqualTo(2);
      try (final ResultSet rs = database.query("sql", "SELECT d FROM Written")) {
        while (rs.hasNext())
          assertThat((Object) rs.next().getProperty("d")).isEqualTo(0.05d);
      }
    }

    /**
     * The 2^24 bound on {@link Type#widenFloat}'s allocation-free path is the crux of when the decimal round-trip
     * is paid and when it is skipped, so it is pinned here rather than left to a code comment. Below the bound an
     * integral float IS its own shortest decimal; above it the ulp exceeds 1, a shorter decimal fits the same
     * rounding interval, and the two answers diverge - which is why the bound must not be raised.
     */
    @Test
    void widenFloatIsExactAtTheIntegralBoundAndReadsTheDecimalAboveIt() {
      final float bound = 1 << 24;
      assertThat(Type.widenFloat(bound)).as("2^24 itself takes the exact path").isEqualTo(16777216.0d);
      assertThat(Type.widenFloat(bound - 1)).isEqualTo(16777215.0d);
      assertThat(Type.widenFloat(-bound)).isEqualTo(-16777216.0d);

      // the counterexample the bound exists for: its exact value is 33554448, but its shortest decimal is
      // 3.355445E7, so raising the bound would start answering the wrong number on the fast path
      final float above = 33554448f;
      assertThat((double) above).as("the primitive widening").isEqualTo(33554448.0d);
      assertThat(Type.widenFloat(above)).as("the decimal form").isEqualTo(33554450.0d);

      // the fractional case the whole issue is about still reads its decimal
      assertThat(Type.widenFloat(0.05f)).isEqualTo(0.05d);

      // signed zero survives the fast path, and the non-finite values skip the parse
      assertThat(Type.widenFloat(-0.0f)).isEqualTo(-0.0d);
      assertThat(Type.widenFloat(Float.NaN)).isNaN();
      assertThat(Type.widenFloat(Float.NEGATIVE_INFINITY)).isNegative().isInfinite();
    }

    /**
     * The openCypher engine evaluates its own predicates, and its comparator carried the same widening: a FLOAT
     * property holding 0.05 did not equal the literal 0.05, so the same records answered differently depending on
     * which query language asked. Cypher has no 32-bit float of its own - a literal there is a 64-bit float, and
     * Neo4j, which has no FLOAT storage type at all, matches these records - so the SQL answer is the right one.
     */
    @Test
    void theOpenCypherComparatorAgreesWithTheSqlOneOnAFloatProperty() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Cy");
        database.command("sql", "CREATE PROPERTY Cy.fp FLOAT");
        for (int i = 0; i < PER_VALUE; i++)
          database.newVertex("Cy").set("fp", 0.05f).save();
        for (int i = 0; i < PER_VALUE; i++)
          database.newVertex("Cy").set("fp", 0.07f).save();
      });

      assertThat(cypher("MATCH (n:Cy) WHERE n.fp = 0.05 RETURN n")).as("cypher =").isEqualTo(10);
      assertThat(cypher("MATCH (n:Cy) WHERE n.fp >= 0.05 RETURN n")).as("cypher >=").isEqualTo(20);
      assertThat(cypher("MATCH (n:Cy) WHERE n.fp < 0.05 RETURN n")).as("cypher <").isZero();
      assertThat(cypher("MATCH (n:Cy) WHERE n.fp <= 0.05 RETURN n")).as("cypher <=").isEqualTo(10);
      assertThat(cypher("MATCH (n:Cy) WHERE n.fp <> 0.05 RETURN n")).as("cypher <>").isEqualTo(10);

      // and the two languages agree record for record over the same data
      assertThat(cypher("MATCH (n:Cy) WHERE n.fp = 0.05 RETURN n"))
          .isEqualTo(count("SELECT FROM Cy WHERE fp = 0.05"));
    }

    /**
     * @param cypher the openCypher statement to run
     *
     * @return how many records it answers
     */
    private long cypher(final String cypher) {
      long rows = 0;
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
      }
      return rows;
    }

    /**
     * {@link Type#normalizeNumberForKey} is the GROUP BY / DISTINCT key, and its whole purpose is to let the same
     * logical value reaching a grouping step as two different numeric types land in ONE group. It widened a Float
     * with {@code .doubleValue()} like everything else did, so 0.05f keyed as 0.05000000074505806 against the
     * Double 0.05 and the group split in two.
     */
    @Test
    void aFloatAndADoubleOfTheSameValueShareOneGroupingKey() {
      assertThat(Type.normalizeNumberForKey(0.05f)).isEqualTo(Type.normalizeNumberForKey(0.05d));
      assertThat(Type.normalizeNumberForKey(0.1f)).isEqualTo(Type.normalizeNumberForKey(0.1d));
      assertThat(Type.normalizeNumberForKey(42.0f)).isEqualTo(Type.normalizeNumberForKey(42.0d));
      assertThat(Type.normalizeNumberForKey(42.0f)).isEqualTo(Type.normalizeNumberForKey(42));

      // values that are genuinely different still key apart
      assertThat(Type.normalizeNumberForKey(0.05f)).isNotEqualTo(Type.normalizeNumberForKey(0.06d));

      // the non-finite values have no decimal form and are returned unchanged, as before
      assertThat(Type.normalizeNumberForKey(Float.NaN)).isEqualTo(Float.NaN);
      assertThat(Type.normalizeNumberForKey(Float.POSITIVE_INFINITY)).isEqualTo(Float.POSITIVE_INFINITY);

      // and end to end, through a GROUP BY that sees the same number as a FLOAT and as a DOUBLE property
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Grp");
        database.newDocument("Grp").set("v", 0.05f).save();
        database.newDocument("Grp").set("v", 0.05d).save();
      });
      assertThat(count("SELECT v, count(*) AS n FROM Grp GROUP BY v")).as("one group, not two").isEqualTo(1);
    }

    /**
     * {@link BinaryComparator} has two entry points and they have to answer the same: the typed
     * {@code compare(value, type, value, type)} the index cursor walks a range with, and {@code equals()}, which
     * routes through {@link Type#castComparableNumber}. Moving the equality side onto the decimal form would have
     * split them - the same class of split #6997 closed for strings - so the ordering side reads the decimal too.
     */
    @Test
    void bothBinaryComparatorEntryPointsAgreeOnAFloatAgainstADouble() {
      final BinaryComparator comparator = new BinaryComparator();

      assertThat(comparator.compare(0.05f, BinaryTypes.TYPE_FLOAT, 0.05d, BinaryTypes.TYPE_DOUBLE))
          .as("compare(0.05f, 0.05d)").isZero();
      assertThat(BinaryComparator.equals(0.05f, 0.05d)).as("equals(0.05f, 0.05d)").isTrue();

      assertThat(comparator.compare(0.05d, BinaryTypes.TYPE_DOUBLE, 0.05f, BinaryTypes.TYPE_FLOAT))
          .as("compare(0.05d, 0.05f)").isZero();
      assertThat(BinaryComparator.equals(0.05d, 0.05f)).as("equals(0.05d, 0.05f)").isTrue();

      assertThat(comparator.compare(new BigDecimal("0.05"), BinaryTypes.TYPE_DECIMAL, 0.05f, BinaryTypes.TYPE_FLOAT))
          .as("compare(decimal 0.05, 0.05f)").isZero();
      assertThat(BinaryComparator.equals(new BigDecimal("0.05"), 0.05f)).as("equals(decimal 0.05, 0.05f)").isTrue();

      // an integral operand against a float still orders the same through both narrow-integral helpers
      assertThat(comparator.compare(1, BinaryTypes.TYPE_INT, 0.05f, BinaryTypes.TYPE_FLOAT)).isPositive();
      assertThat(comparator.compare(1L, BinaryTypes.TYPE_LONG, 0.05f, BinaryTypes.TYPE_FLOAT)).isPositive();

      // and the ordering is untouched where the two genuinely differ
      assertThat(comparator.compare(0.05f, BinaryTypes.TYPE_FLOAT, 0.06d, BinaryTypes.TYPE_DOUBLE)).isNegative();
    }

    /**
     * {@code -0.0} is {@code 0.0} (IEEE 754, issue #8920), so a bound {@code Float} carrying either zero matches both
     * records, whatever its sign: the sign of a bound zero must not be read as a different value.
     */
    @Test
    void aBoundNegativeZeroMatchesBothZeros() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Zero");
        database.newDocument("Zero").set("v", -0.0d).set("sign", "negative").save();
        database.newDocument("Zero").set("v", 0.0d).set("sign", "positive").save();
      });

      assertThat(count("SELECT FROM Zero WHERE v = :z", Map.of("z", -0.0f))).as("bound -0.0f matches both").isEqualTo(2);
      assertThat(count("SELECT FROM Zero WHERE v = :z", Map.of("z", 0.0f))).as("bound 0.0f matches both").isEqualTo(2);

      // a non-finite Float parameter binds without the suffix: "NaNF" would be a spelling the grammar cannot lex,
      // and NaN carries the same value as a Double. It must still bind rather than throw
      assertThat(count("SELECT FROM Zero WHERE v = :z", Map.of("z", Float.NaN))).as("bound NaN").isZero();
      assertThat(count("SELECT FROM Zero WHERE v < :z", Map.of("z", Float.POSITIVE_INFINITY)))
          .as("bound +Infinity").isEqualTo(2);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Compares the two numbers {@link Type#castComparableNumber} brought to a common type, the way every operator
     * that calls it does.
     *
     * @param pair the couple returned by {@code castComparableNumber}
     *
     * @return negative, zero or positive as the first is below, equal to or above the second
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int compare(final Number[] pair) {
      return ((Comparable) pair[0]).compareTo(pair[1]);
    }

    /**
     * @param sql the statement to run
     *
     * @return how many records the statement answers
     */
    private long count(final String sql) {
      return count(sql, Map.of());
    }

    /**
     * @param sql    the statement to run
     * @param params the bound parameters, empty to run the statement without any
     *
     * @return how many records the statement answers
     */
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

    /**
     * @param sql a statement whose first record carries the aggregate under the alias {@code s}
     *
     * @return that aggregate as a double, zero when there is nothing to sum
     */
    private double sum(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        final Object value = rs.next().getProperty("s");
        return value == null ? 0 : ((Number) value).doubleValue();
      }
    }

    /**
     * @param sql a statement expected to answer at least one record
     *
     * @return its first record
     */
    private Result single(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next();
      }
    }

    /**
     * @param sql the statement to explain
     *
     * @return the execution plan as text, so a test can tell an index fetch from a full scan
     */
    private String explain(final String sql) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
        return rs.next().getProperty("executionPlanAsString").toString();
      }
    }
  }

  /**
   * Regression for #8041.
   * <p>
   * Regression test for issue #8041.
   * <p>
   * The #7917 fix gave {@code MathExpression.Operator.apply(Number, Operator, Number)} a {@code BigInteger} arm in
   * every position, and every one of them meets the two operands in {@code BigDecimal} via
   * {@code new BigDecimal(BigInteger)} - which has scale 0, always. {@code SLASH.apply(BigDecimal, BigDecimal)}
   * divided with {@code left.divide(right, RoundingMode.HALF_UP)}, an overload that answers AT THE SCALE OF THE LEFT
   * OPERAND, so every inexact quotient involving a {@code BigInteger} came back rounded to a whole number:
   * {@code 1 / 2} was {@code 1} and {@code 7 / 2} was {@code 4}. Loud failure turned into a silently wrong number.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BigIntegerDivision extends TestHelper {

    @Test
    void bigIntegerDivisionIsNotRoundedToAWholeNumber() {
      assertThat(divide(BigInteger.ONE, BigInteger.TWO)).isEqualByComparingTo("0.5");
      assertThat(divide(BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("3.5");
      assertThat(divide(BigInteger.TEN, BigInteger.valueOf(4))).isEqualByComparingTo("2.5");
    }

    /**
     * A BigInteger on EITHER side drags the other operand into a scale-0 BigDecimal, so a plain SQL literal was
     * affected too.
     */
    @Test
    void aBigIntegerOnEitherSideAloneIsEnough() {
      assertThat(divide(BigInteger.valueOf(7), 2)).isEqualByComparingTo("3.5");
      assertThat(divide(7, BigInteger.TWO)).isEqualByComparingTo("3.5");
      assertThat(divide(BigInteger.valueOf(7), 2L)).isEqualByComparingTo("3.5");
      assertThat(divide(BigInteger.valueOf(7), (short) 2)).isEqualByComparingTo("3.5");
      assertThat(divide(BigInteger.valueOf(7), (byte) 2)).isEqualByComparingTo("3.5");
    }

    /** An inline literal division, the reference the issue measures the BigInteger arms against. */
    @Test
    void aBigIntegerQuotientAgreesWithTheSameDivisionOnPlainIntegers() {
      try (final ResultSet rs = database.query("sql", "SELECT 10 / 4 AS r")) {
        assertThat(rs.next().<Number>getProperty("r").doubleValue()).isEqualTo(2.5d);
      }
      assertThat(divide(BigInteger.TEN, BigInteger.valueOf(4)).doubleValue()).isEqualTo(2.5d);
    }

    /** An exact quotient must stay exact, and must not gain the 34 digits of the fallback precision. */
    @Test
    void anExactQuotientStaysExact() {
      assertThat(divide(BigInteger.TEN, BigInteger.valueOf(5))).isEqualByComparingTo("2");
      assertThat(divide(new BigDecimal("6.0"), new BigDecimal("3"))).isEqualByComparingTo("2");
      assertThat(divide(new BigDecimal("1.0"), new BigDecimal("2"))).isEqualByComparingTo("0.5");
    }

    /**
     * The SCALE of every division that already answered correctly is unchanged, so this stays a bug fix rather
     * than a formatting change (PR #8093 review).
     * <p>
     * The old {@code divide(right, HALF_UP)} padded as well as rounded: {@code 10.00 / 2.00} came back
     * {@code 5.00}, not {@code 5}, and an application that renders money off {@code toString()} reads that
     * difference even though the numbers compare equal. {@code isEqualByComparingTo} cannot see it, so these
     * assert {@code toPlainString()}.
     */
    @Test
    void theScaleOfAnAlreadyCorrectDivisionIsPreserved() {
      assertThat(divide(new BigDecimal("10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("5.00");
      assertThat(divide(new BigDecimal("6.0"), new BigDecimal("3")).toPlainString()).isEqualTo("2.0");
      assertThat(divide(new BigDecimal("1.000"), new BigDecimal("4")).toPlainString()).isEqualTo("0.250");
      // ...and the scale only ever WIDENS: where the old code had to round to reach the left scale, it no longer
      // does, which is the fix itself.
      assertThat(divide(new BigDecimal("1"), new BigDecimal("4")).toPlainString()).isEqualTo("0.25");
      assertThat(divide(BigInteger.ONE, BigInteger.TWO).toPlainString()).isEqualTo("0.5");
    }

    /**
     * The only case that HAS to round. It must round at a real precision rather than at the left operand's scale,
     * which for a BigInteger is zero - the whole of this issue.
     */
    @Test
    void aNonTerminatingQuotientRoundsAtFullPrecisionRatherThanAtTheLeftScale() {
      final BigDecimal third = divide(BigInteger.ONE, BigInteger.valueOf(3));
      assertThat(third.doubleValue()).isCloseTo(1d / 3d, org.assertj.core.data.Offset.offset(1e-15));
      assertThat(third.precision()).isGreaterThan(30);
    }

    /**
     * A negative operand on either side, or both. The exact divide and the scale floor are both sign-agnostic, but
     * the rounding mode is not - HALF_UP rounds away from zero - so the sign belongs in the test rather than in an
     * assumption (PR #8093 review).
     */
    @Test
    void signIsCarriedThroughTheQuotient() {
      assertThat(divide(BigInteger.valueOf(-7), BigInteger.TWO)).isEqualByComparingTo("-3.5");
      assertThat(divide(BigInteger.valueOf(7), BigInteger.valueOf(-2))).isEqualByComparingTo("-3.5");
      assertThat(divide(BigInteger.valueOf(-7), BigInteger.valueOf(-2))).isEqualByComparingTo("3.5");
      assertThat(divide(BigInteger.valueOf(-1), BigInteger.TWO)).isEqualByComparingTo("-0.5");
      assertThat(divide(new BigDecimal("-10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("-5.00");
      // The non-terminating fallback, mirrored about zero: -1/3 must be the negation of 1/3 to the last digit.
      assertThat(divide(BigInteger.valueOf(-1), BigInteger.valueOf(3)))
          .isEqualByComparingTo(divide(BigInteger.ONE, BigInteger.valueOf(3)).negate());
    }

    /** The other operators were never affected, and must not become so. */
    @Test
    void theOtherOperatorsAreUnchanged() {
      assertThat(apply("+", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("9");
      assertThat(apply("-", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("5");
      assertThat(apply("*", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("14");
      assertThat(apply("%", BigInteger.valueOf(7), BigInteger.TWO)).isEqualByComparingTo("1");
    }

    private BigDecimal divide(final Object a, final Object b) {
      return apply("/", a, b);
    }

    private BigDecimal apply(final String operator, final Object a, final Object b) {
      try (final ResultSet rs = database.query("sql", "SELECT :a " + operator + " :b AS r", Map.of("a", a, "b", b))) {
        final Number value = rs.next().getProperty("r");
        return value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
      }
    }
  }

  /**
   * Regression for #8165.
   * <p>
   * Regression test for issue #8165, a regression introduced by the #8041 fix (PR #8093).
   * <p>
   * That fix bounded a non-terminating quotient at {@code MathContext.DECIMAL128} - 34 significant digits - and then
   * reinstated the left operand's scale as a floor with {@code setScale()}. For a left operand carrying MORE than 34
   * fractional digits, {@code setScale()} pads the rounded value with zeros, so the answer asserts precision that was
   * never computed and is LESS accurate than the single line the fix replaced
   * ({@code left.divide(right, RoundingMode.HALF_UP)}, which answers at the left scale, correctly rounded at every
   * digit). Nothing in the result distinguishes the padding from computed precision.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class WideScaleDivision extends TestHelper {

    /** 34 significant digits: what {@code MathContext.DECIMAL128} gives, and the point past which padding began. */
    private static final int DECIMAL128_DIGITS = MathContext.DECIMAL128.getPrecision();

    /**
     * The defect. A left operand of scale 40 divided by a divisor whose quotient does not terminate must answer 40
     * COMPUTED fractional digits, not 34 computed ones followed by 6 zeros.
     */
    @Test
    void aQuotientWiderThanDecimal128IsComputedRatherThanPaddedWithZeros() {
      final BigDecimal left = new BigDecimal("1.0000000000000000000000000000000000000000"); // scale 40
      final BigDecimal right = new BigDecimal("3");

      final BigDecimal quotient = divide(left, right);

      assertThat(quotient.scale()).as("the left operand's scale is still the floor").isEqualTo(left.scale());
      assertThat(quotient).as("and every digit of it was computed, as the pre-#8093 code computed them")
          .isEqualByComparingTo(left.divide(right, left.scale(), RoundingMode.HALF_UP));
      assertThat(quotient.toPlainString()).isEqualTo("0.3333333333333333333333333333333333333333");
      assertThat(quotient.toPlainString()).as("no fabricated trailing zeros").doesNotEndWith("000000");
    }

    /**
     * The same shape one digit either side of the DECIMAL128 boundary, so the fix is pinned at the edge rather than
     * only well past it.
     */
    @Test
    void theBoundaryBetweenTheBoundedAndTheWidenedArmIsTheLeftScale() {
      for (int scale = DECIMAL128_DIGITS - 1; scale <= DECIMAL128_DIGITS + 2; scale++) {
        final BigDecimal left = BigDecimal.ONE.setScale(scale, RoundingMode.UNNECESSARY);
        final BigDecimal quotient = divide(left, new BigDecimal("7"));

        assertThat(quotient.scale()).as("scale %d: never narrower than the left operand", scale)
            .isGreaterThanOrEqualTo(scale);
        assertThat(quotient).as("scale %d: correctly rounded at the last digit it claims", scale)
            .isEqualByComparingTo(left.divide(new BigDecimal("7"), quotient.scale(), RoundingMode.HALF_UP));
      }
    }

    /**
     * The behaviour #8093 was protecting must be untouched: an EXACT quotient keeps the left operand's scale, zeros
     * and all, because there the padding is the exact value.
     */
    @Test
    void anExactQuotientStillKeepsTheLeftOperandScale() {
      assertThat(divide(new BigDecimal("10.00"), new BigDecimal("2.00")).toPlainString()).isEqualTo("5.00");
      assertThat(divide(new BigDecimal("1.000"), new BigDecimal("4")).toPlainString()).isEqualTo("0.250");
    }

    /**
     * And a left operand that asks for LESS than DECIMAL128 gives still answers at DECIMAL128: the fallback is a
     * floor on the precision, not a cap.
     */
    @Test
    void aNarrowLeftOperandStillAnswersAtFullFallbackPrecision() {
      final BigDecimal third = divide(BigDecimal.ONE, new BigDecimal("3"));
      assertThat(third.precision()).isEqualTo(DECIMAL128_DIGITS);
      assertThat(third.toPlainString()).isEqualTo("0.3333333333333333333333333333333333");
    }

    /** The widened arm is sign-agnostic: HALF_UP rounds away from zero on both sides of it. */
    @Test
    void theWidenedQuotientIsMirroredAboutZero() {
      final BigDecimal left = new BigDecimal("1.0000000000000000000000000000000000000000");
      assertThat(divide(left.negate(), new BigDecimal("3"))).isEqualByComparingTo(divide(left, new BigDecimal("3")).negate());
    }

    private BigDecimal divide(final Object a, final Object b) {
      try (final ResultSet rs = database.query("sql", "SELECT :a / :b AS r", Map.of("a", a, "b", b))) {
        final Number value = rs.next().getProperty("r");
        return value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
      }
    }
  }

  /**
   * Regression for #8872.
   * <p>
   * Regression test for issue #8872: a suffix-less decimal literal was always parsed as a double, so a DECIMAL
   * property set or compared with a literal of more than ~16 significant digits lost the rest.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DecimalLiteralDigits extends TestHelper {
    private static final String EXACT = "12345678901234567890.123456789012345678";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.k STRING");
      database.command("sql", "CREATE PROPERTY T.dec DECIMAL");
    }

    @Test
    void literalKeepsAllDigitsOnInsertAndLookup() {
      final BigDecimal v = new BigDecimal(EXACT);
      database.transaction(() -> {
        database.command("sql", "INSERT INTO T SET k = 'parameter', dec = ?", v);
        database.command("sql", "INSERT INTO T SET k = 'literal', dec = " + EXACT);
        database.command("sql", "INSERT INTO T SET k = 'string', dec = '" + EXACT + "'");
        database.newDocument("T").set("k", "api", "dec", v).save();
      });

      try (final ResultSet rs = database.query("sql", "SELECT k, dec FROM T")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat((BigDecimal) r.getProperty("dec")).as(r.<String>getProperty("k")).isEqualByComparingTo(v);
        }
      }
      assertThat(keys("SELECT k FROM T WHERE dec = " + EXACT)).containsExactlyInAnyOrder("parameter", "literal", "string", "api");
      assertThat(keys("SELECT k FROM T WHERE dec = -" + EXACT)).isEmpty();
    }

    @Test
    void shortLiteralsStayDoubles() {
      try (final ResultSet rs = database.query("sql", "SELECT 0.1 AS a, 1.5e3 AS b, -2.25 AS c")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isEqualTo(0.1d);
        assertThat(r.<Object>getProperty("b")).isEqualTo(1500d);
        assertThat(r.<Object>getProperty("c")).isEqualTo(-2.25d);
      }
    }

    @Test
    void negativeLongLiteral() {
      try (final ResultSet rs = database.query("sql", "SELECT -" + EXACT + " AS a")) {
        assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal("-" + EXACT));
      }
    }

    @Test
    void indexedDecimalAndDoubleLookups() {
      database.command("sql", "CREATE DOCUMENT TYPE I");
      database.command("sql", "CREATE PROPERTY I.k STRING");
      database.command("sql", "CREATE PROPERTY I.dec DECIMAL");
      database.command("sql", "CREATE PROPERTY I.dbl DOUBLE");
      database.command("sql", "CREATE INDEX ON I (dec) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON I (dbl) NOTUNIQUE");
      final BigDecimal v = new BigDecimal(EXACT);
      database.transaction(() -> {
        database.newDocument("I").set("k", "exact", "dec", v, "dbl", 1.5d).save();
        database.newDocument("I").set("k", "above", "dec", v.add(BigDecimal.ONE), "dbl", 2.5d).save();
      });
      assertThat(keys("SELECT k FROM I WHERE dec = " + EXACT)).containsExactly("exact");
      assertThat(keys("SELECT k FROM I WHERE dec > " + EXACT)).containsExactly("above");
      assertThat(keys("SELECT k FROM I WHERE dec >= " + EXACT)).containsExactlyInAnyOrder("exact", "above");
      assertThat(keys("SELECT k FROM I WHERE dbl = 1.5000000000000000")).containsExactly("exact");
      assertThat(keys("SELECT k FROM I WHERE dbl > 1.5000000000000000000001")).containsExactly("above");
    }

    @Test
    void arithmeticWithLongLiteral() {
      try (final ResultSet rs = database.query("sql", "SELECT " + EXACT + " + 1 AS a")) {
        assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal(EXACT).add(BigDecimal.ONE));
      }
    }

    @Test
    void longAndShortLiteralsPickTheirType() {
      try (final ResultSet rs = database.query("sql", "SELECT 0.1000000000000000 AS a, 1.2345678901234567890e5 AS b, 123456789012345.5 AS c")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isEqualTo(0.1d);
        assertThat(r.<Object>getProperty("b")).isInstanceOf(BigDecimal.class);
        assertThat(r.<Object>getProperty("c")).isEqualTo(123456789012345.5d);
      }
    }

    @Test
    void oversizedScaleStaysDouble() {
      final String literal = "0." + "0".repeat(449) + "1234567890123456789";
      try (final ResultSet rs = database.query("sql", "SELECT " + literal + " AS a")) {
        assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(Double.class);
      }
    }

    @Test
    void commonFunctionsAcceptLongLiterals() {
      try (final ResultSet rs = database.query("sql",
          "SELECT pow(2.12345678901234567890, 2) AS r, abs(-1.2345678901234567890) AS a, sqrt(2.2345678901234567890) AS s,"
              + " geo.point(12.12345678901234567890, 41.1234567890123456789) AS p")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("r")).isNotNull();
        assertThat(r.<Object>getProperty("a")).isNotNull();
        assertThat(r.<Object>getProperty("s")).isNotNull();
        assertThat(r.<Object>getProperty("p")).isNotNull();
      }
    }

    @Test
    void hexAndExtremeExponentsStayDoubles() {
      try (final ResultSet rs = database.query("sql", "SELECT 0x1.0000000000000p0 AS a, 1e999999999 AS b, 1e-999999999 AS c")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isEqualTo(1d);
        assertThat(r.<Object>getProperty("b")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(r.<Object>getProperty("c")).isEqualTo(0d);
      }
    }

    @Test
    void underflowingExponentAndDSuffixStayDoubles() {
      try (final ResultSet rs = database.query("sql", "SELECT 1.0000000000000000e-9999999999 AS a, " + EXACT + "D AS b")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isEqualTo(0d);
        assertThat(r.<Object>getProperty("b")).isInstanceOf(Double.class);
      }
    }

    @Test
    void renderedLiteralReparsesToTheSameValue() {
      for (final String literal : new String[] { "12345678901234567890.", "12345678901234567890e0", "-12345678901234567890.", EXACT, "1.2345678901234567890e30" }) {
        final StringBuilder rendered = new StringBuilder();
        new SQLAntlrParser(null).parse("SELECT " + literal + " AS a").toString(null, rendered);
        try (final ResultSet rs = database.query("sql", rendered.toString())) {
          assertThat((BigDecimal) rs.next().getProperty("a")).as(literal).isEqualByComparingTo(new BigDecimal(literal.endsWith(".") ? literal + "0" : literal));
        }
      }
    }

    @Test
    void longLiteralAssignedToDoubleProperty() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.v DOUBLE");
      database.transaction(() -> database.command("sql", "INSERT INTO D SET v = 1.2345678901234567890"));
      try (final ResultSet rs = database.query("sql", "SELECT v FROM D")) {
        assertThat(rs.next().<Double>getProperty("v")).isEqualTo(1.2345678901234567890d);
      }
    }

    @Test
    void doublePropertyTimesLongLiteralIsExactDecimal() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      database.command("sql", "CREATE PROPERTY M.v DOUBLE");
      database.transaction(() -> database.command("sql", "INSERT INTO M SET v = 2.0"));
      try (final ResultSet rs = database.query("sql", "SELECT v * 3.14159265358979323846 AS a FROM M")) {
        assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(BigDecimal.class);
      }
    }

    @Test
    void negativeLongLiteralAsDecimalIndexKey() {
      database.command("sql", "CREATE DOCUMENT TYPE N");
      database.command("sql", "CREATE PROPERTY N.k STRING");
      database.command("sql", "CREATE PROPERTY N.dec DECIMAL");
      database.command("sql", "CREATE INDEX ON N (dec) NOTUNIQUE");
      database.transaction(() -> database.newDocument("N").set("k", "neg", "dec", new BigDecimal("-" + EXACT)).save());
      try (final ResultSet rs = database.query("sql", "SELECT k FROM N WHERE dec = -" + EXACT)) {
        assertThat(rs.next().<String>getProperty("k")).isEqualTo("neg");
      }
    }

    @Test
    void seventeenDigitRoundTripLiteralMatchesStoredDoubleWithoutIndex() {
      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.command("sql", "CREATE PROPERTY U.k STRING");
      database.command("sql", "CREATE PROPERTY U.d2 DOUBLE");
      database.transaction(() -> {
        database.newDocument("U").set("k", "tenth", "d2", 0.1d).save();
        database.newDocument("U").set("k", "third", "d2", 1d / 3).save();
      });
      assertThat(keys("SELECT k FROM U WHERE d2 = 0.10000000000000001")).containsExactly("tenth");
      assertThat(keys("SELECT k FROM U WHERE d2 = 0.33333333333333331")).containsExactly("third");
      try (final ResultSet rs = database.query("sql", "SELECT 0.10000000000000001 AS a")) {
        assertThat(rs.next().<Object>getProperty("a")).isEqualTo(0.1d);
      }
    }

    @Test
    void indexedAndUnindexedDoubleAgreeOnALongLiteral() {
      database.command("sql", "CREATE DOCUMENT TYPE X");
      database.command("sql", "CREATE PROPERTY X.k STRING");
      database.command("sql", "CREATE PROPERTY X.plain DOUBLE");
      database.command("sql", "CREATE PROPERTY X.idx DOUBLE");
      database.command("sql", "CREATE INDEX ON X (idx) NOTUNIQUE");
      database.transaction(() -> database.newDocument("X").set("k", "row", "plain", 1.5d, "idx", 1.5d).save());
      for (final String op : new String[] { "=", ">=", "<=", ">", "<" }) {
        final String literal = "1.5000000000000000000001";
        assertThat(keys("SELECT k FROM X WHERE idx " + op + " " + literal)).as("idx " + op)
            .isEqualTo(keys("SELECT k FROM X WHERE plain " + op + " " + literal));
      }
      assertThat(keys("SELECT k FROM X WHERE idx IN [1.5000000000000000000001]")).isEqualTo(keys("SELECT k FROM X WHERE plain IN [1.5000000000000000000001]"));
      assertThat(keys("SELECT k FROM X WHERE idx IN [1.5000000000000000000001]")).isEmpty();
      assertThat(keys("SELECT k FROM X WHERE idx >= 1.5000000000000000000001")).isEmpty();
      assertThat(keys("SELECT k FROM X WHERE idx <= 1.5000000000000000000001")).containsExactly("row");
      assertThat(keys("SELECT k FROM X WHERE idx BETWEEN 1.4000000000000000000001 AND 1.5000000000000000000001")).containsExactly("row");
    }

    @Test
    void updateDeleteAndInOnIndexedDecimalAndDouble() {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.command("sql", "CREATE PROPERTY W.k STRING");
      database.command("sql", "CREATE PROPERTY W.dec DECIMAL");
      database.command("sql", "CREATE PROPERTY W.dbl DOUBLE");
      database.command("sql", "CREATE INDEX ON W (dec) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON W (dbl) NOTUNIQUE");
      database.transaction(() -> database.newDocument("W").set("k", "row", "dec", new BigDecimal(EXACT), "dbl", 1.5d).save());
      assertThat(keys("SELECT k FROM W WHERE dec IN [" + EXACT + "]")).containsExactly("row");
      // the exact comparison finds nothing on the DOUBLE row, indexed or not, so UPDATE and DELETE touch nothing
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", "UPDATE W SET k = 'changed' WHERE dbl = 1.5000000000000000000001")) {
          assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
        }
        try (final ResultSet rs = database.command("sql", "DELETE FROM W WHERE dbl = 1.5000000000000000000001")) {
          assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
        }
      });
      assertThat(keys("SELECT k FROM W")).containsExactly("row");
    }

    @Test
    void mixedShortAndLongLiteralArithmeticIsDecimal() {
      try (final ResultSet rs = database.query("sql", "SELECT 0.1 + 1.2345678901234567890 AS a")) {
        assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal("1.3345678901234567890"));
      }
    }

    @Test
    void nonFiniteDoubleTimesLongLiteralDoesNotThrow() {
      database.command("sql", "CREATE DOCUMENT TYPE F");
      database.command("sql", "CREATE PROPERTY F.v DOUBLE");
      database.transaction(() -> database.newDocument("F").set("v", Double.POSITIVE_INFINITY).save());
      try (final ResultSet rs = database.query("sql",
          "SELECT v * 3.14159265358979323846 AS a, 3.14159265358979323846 * v AS b, 1e999999999 * 1.2345678901234567890 AS c FROM F")) {
        final Result r = rs.next();
        assertThat(r.<Object>getProperty("a")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(r.<Object>getProperty("b")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(r.<Object>getProperty("c")).isEqualTo(Double.POSITIVE_INFINITY);
      }
    }

    @Test
    void reversedComparisonOrderByAndLimitWithLongLiteral() {
      database.command("sql", "CREATE DOCUMENT TYPE O");
      database.command("sql", "CREATE PROPERTY O.k STRING");
      database.command("sql", "CREATE PROPERTY O.idx DOUBLE");
      database.command("sql", "CREATE INDEX ON O (idx) NOTUNIQUE");
      database.transaction(() -> database.newDocument("O").set("k", "row", "idx", 1.5d).save());
      assertThat(keys("SELECT k FROM O WHERE 1.5000000000000000000001 > idx")).containsExactly("row");
      assertThat(keys("SELECT k FROM O WHERE 1.5000000000000000000001 <= idx")).isEmpty();
      assertThat(keys("SELECT k FROM O WHERE idx < 1.5000000000000000000001 ORDER BY idx")).containsExactly("row");
      assertThat(keys("SELECT k FROM O LIMIT 1.5000000000000000000001")).hasSize(1);
    }

    @Test
    void notEqualsBetweenAndDoubleNegationOnDecimal() {
      database.command("sql", "CREATE DOCUMENT TYPE Q");
      database.command("sql", "CREATE PROPERTY Q.k STRING");
      database.command("sql", "CREATE PROPERTY Q.dec DECIMAL");
      database.command("sql", "CREATE INDEX ON Q (dec) NOTUNIQUE");
      database.transaction(() -> database.newDocument("Q").set("k", "row", "dec", new BigDecimal(EXACT)).save());
      assertThat(keys("SELECT k FROM Q WHERE dec <> " + EXACT)).isEmpty();
      assertThat(keys("SELECT k FROM Q WHERE dec != " + EXACT.replace("12345", "12346"))).containsExactly("row");
      assertThat(keys("SELECT k FROM Q WHERE dec BETWEEN " + EXACT + " AND " + EXACT)).containsExactly("row");
      try (final ResultSet rs = database.query("sql", "SELECT - -" + EXACT + " AS a")) {
        assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal(EXACT));
      }
    }

    /** pins the documented limit: a 17 digit literal is a double, so it does not reach a DECIMAL beyond double precision */
    @Test
    void seventeenDigitLiteralOnDecimalGoesThroughTheDouble() {
      try (final ResultSet rs = database.query("sql", "SELECT 0.12345678901234567 AS a")) {
        assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(Double.class);
      }
    }

    private List<String> keys(final String sql) {
      final List<String> ks = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          ks.add(rs.next().getProperty("k"));
      }
      return ks;
    }
  }
}
