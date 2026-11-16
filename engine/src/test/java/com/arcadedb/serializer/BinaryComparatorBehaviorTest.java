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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.query.sql.executor.QueryOperatorEquals;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferUnderflowException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the BinaryComparator and the comparison helpers built on it, one nested class per reported issue.
 */
class BinaryComparatorBehaviorTest {
  /**
   * Regression for #10541: {@link BinaryComparator#compareTo(Object, Object)} on a date and an operand that has no timestamp (a list, a map) threw a
   * {@code NullPointerException} unboxing the answer of the timestamp helper, which is {@code null} for them. A Boolean already got the
   * "not supported" refusal (issue #7754), which the SQL operators catch to report "not comparable"; every other operand without a
   * timestamp gets the same one, in both directions.
   */
  @Nested
  class DateAgainstNonTemporalOperand {

    private static final LocalDate     DATE      = LocalDate.of(2020, 1, 1);
    private static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0);

    @Test
    void aDateAgainstAListOrAMapIsRefusedInBothDirections() {
      for (final Object date : new Object[] { DATE, DATE_TIME })
        for (final Object other : new Object[] { List.of(7), Map.of("a", 7) }) {
          assertThatThrownBy(() -> BinaryComparator.compareTo(date, other)).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("not supported");
          assertThatThrownBy(() -> BinaryComparator.compareTo(other, date)).isInstanceOf(IllegalArgumentException.class)
              .hasMessageContaining("not supported");
        }
    }

    @Test
    void aBooleanKeepsItsRefusal() {
      assertThatThrownBy(() -> BinaryComparator.compareTo(DATE, true)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void twoDatesStillCompare() {
      assertThat(BinaryComparator.compareTo(DATE, DATE.plusDays(1))).isNegative();
      assertThat(BinaryComparator.compareTo(DATE_TIME.plusHours(1), DATE_TIME)).isPositive();
      assertThat(BinaryComparator.compareTo(DATE, DATE_TIME)).isZero();
    }
  }

  /**
   * Regression for #5945: {@link BinaryComparator}'s {@code compareAgainstNumericString} helper (added by
   * PR #5922 fixing #5900) widens a numeric {@code String} to {@code long} or {@code double} based on its shape, but
   * an integral string outside {@code long}'s range (e.g. 20+ digits) threw a raw, uncaught
   * {@code NumberFormatException} instead of comparing correctly. It also mis-routed {@code "NaN"} /
   * {@code "Infinity"} / {@code "-Infinity"} into the {@code Long.parseLong} branch, since none of those strings
   * contain {@code '.'}, {@code 'e'} or {@code 'E'}.
   */
  @Nested
  class NumericStringOverflow {
    private final BinaryComparator comparator = new BinaryComparator();

    @Test
    void intVersusOutOfLongRangeNumericStringDoesNotThrow() {
      // 20-digit integral string, far beyond Long.MAX_VALUE (19 digits): must compare, not throw.
      final String huge = "99999999999999999999";

      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, huge, BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(huge, BinaryTypes.TYPE_STRING, 5, BinaryTypes.TYPE_INT)).isGreaterThan(0);
    }

    @Test
    void longVersusOutOfLongRangeNumericStringIsAntisymmetricAndExact() {
      final String huge = "99999999999999999999";
      final long five = 5L;

      final int forward = comparator.compare(five, BinaryTypes.TYPE_LONG, huge, BinaryTypes.TYPE_STRING);
      final int backward = comparator.compare(huge, BinaryTypes.TYPE_STRING, five, BinaryTypes.TYPE_LONG);

      assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(backward));
      assertThat(forward).isLessThan(0);
      assertThat(backward).isGreaterThan(0);
    }

    @Test
    void negativeOutOfLongRangeNumericStringComparesCorrectly() {
      // 20-digit negative integral string, beyond Long.MIN_VALUE's range.
      final String hugeNegative = "-99999999999999999999";

      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, hugeNegative, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare(hugeNegative, BinaryTypes.TYPE_STRING, 5, BinaryTypes.TYPE_INT)).isLessThan(0);
    }

    @Test
    void outOfLongRangeNumericStringComparisonIsExactNotDoubleApproximated() {
      // Long.MAX_VALUE (19 digits, 9223372036854775807) versus a 20-digit value one order of magnitude larger.
      // A double round-trip of the huge string would lose precision but must still land unambiguously above
      // Long.MAX_VALUE, so this alone would not catch a double fallback; the real exactness check is that a
      // string differing from Long.MAX_VALUE by a small amount round-trips through BigDecimal, not through a
      // double that cannot represent 19-digit integers exactly.
      final long maxLong = Long.MAX_VALUE;
      // One more than Long.MAX_VALUE, expressed as its own out-of-range string.
      final String maxLongPlusOne = "9223372036854775808";

      assertThat(comparator.compare(maxLong, BinaryTypes.TYPE_LONG, maxLongPlusOne, BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(maxLongPlusOne, BinaryTypes.TYPE_STRING, maxLong, BinaryTypes.TYPE_LONG)).isGreaterThan(0);
    }

    @Test
    void nanStringComparesAsDoubleNaNRatherThanThrowing() {
      // "NaN" contains none of '.', 'e', 'E', so it fell into the Long.parseLong branch and threw instead of
      // being routed to Double.parseDouble, which correctly parses it as Double.NaN.
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "NaN", BinaryTypes.TYPE_STRING))
          .isEqualTo(Double.compare(5, Double.NaN));

      // Double.parseDouble's grammar also accepts an explicit sign on NaN (Signopt NaN), even though it has no
      // effect on the parsed value: both must be routed the same way as the unsigned form, not fall through to
      // Long.parseLong.
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "+NaN", BinaryTypes.TYPE_STRING))
          .isEqualTo(Double.compare(5, Double.NaN));
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "-NaN", BinaryTypes.TYPE_STRING))
          .isEqualTo(Double.compare(5, Double.NaN));
    }

    @Test
    void infinityStringComparesAsDoubleInfinityRatherThanThrowing() {
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "+Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(5, BinaryTypes.TYPE_INT, "-Infinity", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "Infinity", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "-Infinity", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
    }

    @Test
    void minLongMinusOneNumericStringComparesCorrectly() {
      // Long.MIN_VALUE (9223372036854775808, 19 digits after the sign) minus one, expressed as its own
      // out-of-range string: covers the low-end boundary, complementing the MAX_VALUE + 1 check above.
      final long minLong = Long.MIN_VALUE;
      final String minLongMinusOne = "-9223372036854775809";

      assertThat(comparator.compare(minLong, BinaryTypes.TYPE_LONG, minLongMinusOne, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare(minLongMinusOne, BinaryTypes.TYPE_STRING, minLong, BinaryTypes.TYPE_LONG)).isLessThan(0);
    }

    @Test
    void nonNumericStringStillThrowsRatherThanSilentlyComparing() {
      // Regression guard: the overflow catch must not swallow a genuinely non-numeric string; BigDecimal's own
      // NumberFormatException should still propagate, same as before this fix.
      assertThatThrownBy(() -> comparator.compare(5, BinaryTypes.TYPE_INT, "abc", BinaryTypes.TYPE_STRING))
          .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void ordinaryInRangeNumericStringsAreUnaffected() {
      assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "15", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare(10, BinaryTypes.TYPE_INT, "10", BinaryTypes.TYPE_STRING)).isZero();
      assertThat(comparator.compare(5L, BinaryTypes.TYPE_LONG, "5.5", BinaryTypes.TYPE_STRING)).isLessThan(0);
    }
  }

  /**
   * Regression for #6997: {@link BinaryComparator#compare(Object, byte, Object, byte)} ordered two STRINGs by
   * UTF-16 code units ({@code String.compareTo}) while the LSM pages, {@code lookupInPage} and the sibling
   * {@link BinaryComparator#compareTo(Object, Object)} order them by unsigned UTF-8 bytes. The two orders disagree for any
   * pair of a non-BMP character (a surrogate pair, {@code 0xD800-0xDBFF} lead unit, {@code 0xF0-0xF4} lead byte) and a
   * BMP character above {@code U+E000}, so an indexed range scan whose stop bound sits on the wrong side of the disagreement
   * stopped before emitting anything.
   */
  @Nested
  class Utf8Order extends TestHelper {

    /** U+FF21 FULLWIDTH LATIN CAPITAL LETTER A: one UTF-16 unit above every surrogate, three UTF-8 bytes. */
    private static final String FULLWIDTH_A = "Ａ";
    /** U+1F600 GRINNING FACE: a surrogate pair in UTF-16, four UTF-8 bytes. */
    private static final String EMOJI       = new String(Character.toChars(0x1F600));

    @Test
    void bothEntryPointsAgreeWithTheUtf8ByteOrder() {
      final BinaryComparator comparator = new BinaryComparator();

      // THE PREMISE: THE TWO ORDERS REALLY DISAGREE ON THIS PAIR
      assertThat(FULLWIDTH_A.compareTo(EMOJI)).isGreaterThan(0);
      assertThat(BinaryComparator.compareTo(FULLWIDTH_A, EMOJI)).isLessThan(0);

      assertThat(comparator.compare(FULLWIDTH_A, BinaryTypes.TYPE_STRING, EMOJI, BinaryTypes.TYPE_STRING))
          .as("compare() must order two STRINGs the way the pages do: unsigned UTF-8 bytes").isLessThan(0);
      assertThat(comparator.compare(EMOJI, BinaryTypes.TYPE_STRING, FULLWIDTH_A, BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare(EMOJI, BinaryTypes.TYPE_STRING, EMOJI, BinaryTypes.TYPE_STRING)).isZero();

      // THE ORDERS AGREE ON EVERYTHING ELSE, AND SO MUST THE FIX
      assertThat(comparator.compare("2", BinaryTypes.TYPE_STRING, "10", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare("abc", BinaryTypes.TYPE_STRING, "abd", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare("ab", BinaryTypes.TYPE_STRING, "abc", BinaryTypes.TYPE_STRING)).isLessThan(0);
      assertThat(comparator.compare("abc", BinaryTypes.TYPE_STRING, "ab", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare("", BinaryTypes.TYPE_STRING, "", BinaryTypes.TYPE_STRING)).isZero();
      assertThat(comparator.compare("é", BinaryTypes.TYPE_STRING, "z", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      assertThat(comparator.compare("", BinaryTypes.TYPE_STRING, "퟿", BinaryTypes.TYPE_STRING)).isGreaterThan(0);
      // A NON-STRING type2 STILL FALLS BACK TO ITS toString() FORM
      assertThat(comparator.compare("#1:0", BinaryTypes.TYPE_STRING, "#1:0", BinaryTypes.TYPE_COMPRESSED_RID)).isZero();
    }

    /**
     * An isolated surrogate has no code point, so the encoder writes a replacement byte for it and the UTF-16-as-UTF-8
     * remap no longer describes what the pages hold. Every shape of malformed input must still agree with the encoded
     * order, which is the order the pages use.
     */
    @Test
    void isolatedSurrogatesFollowTheEncodedOrder() {
      final String loneHigh = "\uD800";
      final String loneLow = "\uDC00";
      final String nonCharacter = "\uFFFE";
      final String[] samples = { loneHigh, loneLow, nonCharacter, EMOJI, FULLWIDTH_A, "a", "", "a" + loneHigh, loneHigh + "a",
          loneLow + "a", "a" + loneLow, EMOJI + "a", "a" + EMOJI, loneHigh + EMOJI, EMOJI + loneHigh, loneHigh + loneHigh,
          loneHigh + nonCharacter, "\uD800\uD800\uDC00", "\uD800\uDC00\uDC00" };

      for (final String a : samples)
        for (final String b : samples) {
          final int expected = Integer.signum(BinaryComparator.compareBytes(a.getBytes(StandardCharsets.UTF_8),
              b.getBytes(StandardCharsets.UTF_8)));
          assertThat(Integer.signum(BinaryComparator.compareStrings(a, b)))
              .as("'%s' vs '%s' must follow the unsigned UTF-8 order of the encodings", escape(a), escape(b)).isEqualTo(expected);
        }
    }

    private static String escape(final String s) {
      final StringBuilder out = new StringBuilder();
      for (final char c : s.toCharArray())
        out.append(c < 0x80 ? String.valueOf(c) : String.format("\\u%04X", (int) c));
      return out.toString();
    }

    @Test
    void indexedRangeScanOverABmpAndANonBmpKeyReturnsBothRows() {
      final DocumentType type = database.getSchema().createDocumentType("S");
      type.createProperty("k", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");

      database.transaction(() -> {
        database.newDocument("S").set("k", FULLWIDTH_A).save();
        database.newDocument("S").set("k", EMOJI).save();
      });

      assertThat(keys("SELECT k FROM S", Map.of())).hasSize(2);

      assertThat(keys("SELECT k FROM S WHERE k <= :bound", Map.of("bound", EMOJI)))
          .as("both keys sort at or below the emoji in UTF-8 order").containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
      assertThat(keys("SELECT k FROM S WHERE k >= :bound", Map.of("bound", FULLWIDTH_A)))
          .containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
      assertThat(keys("SELECT k FROM S WHERE k < :bound", Map.of("bound", EMOJI))).containsExactly(FULLWIDTH_A);
      assertThat(keys("SELECT k FROM S WHERE k > :bound", Map.of("bound", FULLWIDTH_A))).containsExactly(EMOJI);
      assertThat(keys("SELECT k FROM S WHERE k BETWEEN :from AND :to", Map.of("from", FULLWIDTH_A, "to", EMOJI)))
          .containsExactlyInAnyOrder(FULLWIDTH_A, EMOJI);
      assertThat(keys("SELECT k FROM S WHERE k > :bound", Map.of("bound", EMOJI))).isEmpty();
    }

    private List<String> keys(final String query, final Map<String, Object> params) {
      final List<String> keys = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query, params)) {
        while (rs.hasNext())
          keys.add(rs.next().getProperty("k"));
      }
      return keys;
    }
  }

  /**
   * Regression for #6998: two independent defects in {@link BinaryComparator}.
   * <p>
   * 1. {@code equalsBytes} applied a "compare the last byte first" fast path with no guard for an empty array, so two
   * empty {@code byte[]} instances read index -1 and threw {@code ArrayIndexOutOfBoundsException} instead of comparing
   * equal. It is reachable from SQL: an empty BINARY property compared against a different empty {@code byte[]}
   * instance failed the query rather than matching it.
   * <p>
   * 2. {@code compareTo} encoded its left String operand with the JVM default charset and its right one with the
   * configured charset. On a non-UTF-8 default the two disagree, so an equal pair of non-ASCII strings compared as
   * less-than and ORDER BY / range predicates answered wrongly. That precondition cannot be created inside a running
   * JVM - {@code Charset.defaultCharset()} is fixed at startup - so the case is exercised in a forked JVM started with
   * {@code -Dfile.encoding=ISO-8859-1}, which reproduces the wrong answer against the unfixed comparator.
   */
  @Nested
  class EmptyArrayAndNullGuards {

    /** A pair of identical non-ASCII strings: one byte under ISO-8859-1, two bytes under UTF-8. */
    private static final String NON_ASCII = "é";

    @Test
    void twoEmptyByteArraysAreEqual() {
      assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[0]))
          .as("two empty byte[] are equal; the last-byte fast path used to read index -1 and throw")
          .isTrue();

      // The length check still catches a mismatched pair before the fast path.
      assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[] { 1 })).isFalse();
      assertThat(BinaryComparator.equalsBytes(new byte[] { 1 }, new byte[0])).isFalse();

      // Same route through the generic entry points, which is what SQL equality reaches.
      assertThat(BinaryComparator.equals(new byte[0], new byte[0])).isTrue();
      assertThat(QueryOperatorEquals.equals(new byte[0], new byte[0]))
          .as("SQL equality over two distinct empty BINARY values must match, not fail")
          .isTrue();

      // Non-empty arrays keep the behavior the fast path was written for.
      assertThat(BinaryComparator.equalsBytes(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 })).isTrue();
      assertThat(BinaryComparator.equalsBytes(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 4 })).isFalse();
    }

    @Test
    void emptyBinaryPropertyIsMatchedBySql() throws Exception {
      TestHelper.executeInNewDatabase("Issue6998", db -> {
        db.getSchema().createDocumentType("Doc6998").createProperty("bin", Type.BINARY);

        db.transaction(() -> db.newDocument("Doc6998").set("bin", new byte[0]).save());

        // A distinct byte[0] instance, so QueryOperatorEquals' identity shortcut cannot fire and the comparison goes
        // through BinaryComparator.equalsBytes - which used to throw.
        try (final ResultSet rs = db.query("sql", "SELECT FROM Doc6998 WHERE bin = :p", Map.of("p", new byte[0]))) {
          assertThat(rs.hasNext()).as("an empty BINARY value must match another empty BINARY value").isTrue();
          rs.next();
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void compareToEncodesBothStringOperandsWithTheSameCharset() throws Exception {
      // Holds on any JVM, but only proves the fix where the default charset is not UTF-8, hence the fork below.
      assertThat(BinaryComparator.compareTo(NON_ASCII, NON_ASCII)).isZero();
      assertThat(BinaryComparator.compareTo("a" + NON_ASCII, "a" + NON_ASCII)).isZero();
      assertThat(BinaryComparator.compareTo(NON_ASCII, "f")).isNotZero();

      final String forked = runInForkedJvm(StandardCharsets.ISO_8859_1);

      assertThat(forked)
          .as("the forked JVM must actually run with a non-UTF-8 default charset, otherwise this proves nothing")
          .startsWith("charset=ISO-8859-1 ");
      assertThat(forked)
          .as("with a non-UTF-8 default charset an equal pair of non-ASCII strings must still compare equal: "
              + "encoding one operand with the platform charset and the other with the configured one made it less-than")
          .isEqualTo("charset=ISO-8859-1 compareTo=0");
    }

    /**
     * Runs {@link CharsetProbe} in a child JVM whose default charset is the given one, and returns the single line it
     * prints. {@code Charset.defaultCharset()} is decided at JVM startup, so a fork is the only way to exercise a
     * platform charset other than the one this suite runs under.
     */
    private static String runInForkedJvm(final Charset charset) throws Exception {
      final String classpath = System.getProperty("java.class.path");
      assertThat(classpath).as("the forked JVM needs this JVM's classpath").isNotBlank();

      final List<String> command = new ArrayList<>();
      command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
      command.add("-Dfile.encoding=" + charset.name());
      command.add("-cp");
      command.add(classpath);
      command.add(CharsetProbe.class.getName());

      // stderr is inherited rather than merged: a JVM warning on the child's stderr must not end up mixed into the
      // single line this method returns, but it still has to be visible in the build log when something goes wrong.
      final Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.INHERIT).start();
      try {
        final String output;
        try (final var in = process.getInputStream()) {
          output = new String(in.readAllBytes(), StandardCharsets.US_ASCII).trim();
        }

        // A hang detector, not a latency bound: the child prints one line and exits in well under a second, so no
        // amount of load brings it near this. StallAwareStopwatch does not apply - it discounts a stall observed in
        // THIS JVM, and the thing being waited on here is a separate process.
        final boolean exited = process.waitFor(60, TimeUnit.SECONDS);
        assertThat(exited).as("the forked JVM must terminate; it printed: %s", output).isTrue();
        assertThat(process.exitValue()).as("the forked JVM failed; it printed: %s", output).isZero();
        return output;
      } finally {
        // Leave nothing behind on any exit path, a timed-out or half-read child included.
        process.destroyForcibly();
      }
    }

    /**
     * Child-JVM entry point: prints its default charset and the comparison of two identical non-ASCII strings, in
     * ASCII only so the child's own stdout encoding cannot alter the answer.
     */
    public static class CharsetProbe {
      public static void main(final String[] args) {
        System.out.println("charset=" + Charset.defaultCharset().name() //
            + " compareTo=" + BinaryComparator.compareTo(NON_ASCII, NON_ASCII));
      }
    }
  }

  /**
   * Regression for #7754: a BOOLEAN compared against a timestamp type answered one way forwards and threw
   * {@code NullPointerException} backwards. The comparison has no meaning (true is not "one millisecond after the
   * epoch"), so both directions now refuse it with the same {@code IllegalArgumentException} naming both types, which
   * the SQL comparison operators already turn into "no match".
   */
  @Nested
  class BooleanVersusTemporalComparison {
    private final BinaryComparator comparator = new BinaryComparator();

    @ParameterizedTest
    @ValueSource(bytes = { BinaryTypes.TYPE_DATE, BinaryTypes.TYPE_DATETIME, BinaryTypes.TYPE_DATETIME_SECOND, BinaryTypes.TYPE_DATETIME_MICROS,
        BinaryTypes.TYPE_DATETIME_NANOS })
    void booleanAgainstTemporalIsRefusedInBothDirections(final byte temporalType) {
      assertThatThrownBy(() -> comparator.compare(true, BinaryTypes.TYPE_BOOLEAN, 5L, temporalType))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> comparator.compare(5L, temporalType, true, BinaryTypes.TYPE_BOOLEAN))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(bytes = { BinaryTypes.TYPE_INT, BinaryTypes.TYPE_LONG })
    void booleanAgainstNumericStillAgreesInBothDirections(final byte numericType) {
      final Object one = numericType == BinaryTypes.TYPE_INT ? (Object) 1 : (Object) 1L;
      assertThat(comparator.compare(true, BinaryTypes.TYPE_BOOLEAN, one, numericType)).isZero();
      assertThat(comparator.compare(one, numericType, true, BinaryTypes.TYPE_BOOLEAN)).isZero();
    }

    @Test
    void untypedComparisonOfBooleanAndTemporalObjectIsRefusedInBothDirections() {
      final LocalDateTime when = LocalDateTime.of(2026, 1, 1, 0, 0);
      assertThatThrownBy(() -> BinaryComparator.compareTo(true, when)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> BinaryComparator.compareTo(when, false)).isInstanceOf(IllegalArgumentException.class);
    }
  }

  /**
   * Regression for #7754: a schemaless column holding both a boolean and a timestamp must still be filterable, through SQL and
   * through the native Select API, because refusing the BOOLEAN-versus-timestamp pair must read as "no match" and not
   * fail the query.
   */
  @Nested
  class MixedBooleanTemporalQuery extends TestHelper {
    @Test
    void orderByAndComparisonOnAMixedColumnDoNotFail() {
      database.getSchema().createDocumentType("Mixed");
      database.transaction(() -> {
        database.newDocument("Mixed").set("v", true).save();
        database.newDocument("Mixed").set("v", LocalDateTime.of(2026, 1, 1, 0, 0)).save();
        database.newDocument("Mixed").set("v", false).save();
      });

      try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed ORDER BY v")) {
        assertThat(rs.stream().count()).isEqualTo(3L);
      }

      final LocalDateTime bound = LocalDateTime.of(2025, 1, 1, 0, 0);

      // The timestamp row is the only one that answers; the booleans have no ordering against it and do not match
      try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed WHERE v > ?", bound)) {
        assertThat(rs.stream().map(r -> r.getProperty("v")).toList()).containsExactly(LocalDateTime.of(2026, 1, 1, 0, 0));
      }
      // And a boolean bound matches no timestamp row: only the boolean rows can be below true
      try (final ResultSet rs = database.query("sql", "SELECT FROM Mixed WHERE v < ?", true)) {
        assertThat(rs.stream().map(r -> r.getProperty("v")).toList()).containsExactly(false);
      }

      // A sort has no "no match" to answer, and a string-order fallback mixed with the typed order would not be
      // transitive, so the native orderBy refuses the pair loudly rather than failing data-dependently in TimSort
      assertThatThrownBy(() -> database.select().fromType("Mixed").orderBy("v", true).documents().toList())
          .isInstanceOf(IllegalArgumentException.class);

      // Native Select API, same column
      assertThat(database.select().fromType("Mixed").where().property("v").gt().value(bound).documents().toList()).hasSize(1);
      assertThat(database.select().fromType("Mixed").where().property("v").lt().value(true).documents().toList()).hasSize(1);
    }
  }

  /**
   * Regression for #7840: an LSM composite key whose components are long strings (the reported shape is a
   * 5-column UNIQUE index of four short scoping keys plus a project-relative file path) compared one code unit - and one
   * page byte - at a time on every probe, which made a seek cost proportional to the key width with no vectorisation.
   * <p>
   * {@link BinaryComparator#compareBytes(byte[], Binary)}, the probe's raw-bytes comparison of a key component against
   * the page, walked the run with one bounds-checked {@code getByte()} per byte; it now skips the common prefix in bulk.
   * {@link Binary#getString()} decoded through a throwaway {@code byte[]} that the {@code String} constructor copied
   * again; it now decodes straight out of the backing array.
   * <p>
   * What the tests below lock in is EQUIVALENCE: the buffer comparison must answer what the unsigned UTF-8 order
   * answers and must leave the read position exactly where the byte walk left it, because {@code lookupInPage} reads
   * the next component of a composite key from there; and both string readers must still return what was written, and
   * still refuse a corrupt length prefix. {@link BinaryComparator#compareStrings(String, String)} is unchanged by this
   * work - the cross-product below simply pins the ordering it has to keep answering.
   */
  @Nested
  class WideKeyComparison extends TestHelper {

    /** The reported index: four short scoping keys plus a long, high-cardinality trailing path. */
    private static final String[] KEY_COLUMNS = { "organizationId", "projectId", "branch", "indexAttemptId", "path" };

    @Test
    void compareStringsFollowsTheEncodedOrderOnEveryShape() {
      // EVERY REGION THE FAST PATH HAS TO GET RIGHT: ASCII, LATIN-1, THE BMP BELOW AND ABOVE THE SURROGATE BLOCK, A
      // WELL-FORMED PAIR, AND BOTH HALVES OF A PAIR ON THEIR OWN - THE ONE CASE THE REMAP ITSELF CANNOT HANDLE
      final String[] alphabet = { "a", "b", "Z", "0", "/", "é", "\u07FF", "\u0800", "\uD7FF", "\uE000", "\uFFFD", "\uFFFE",
          "\uFF21", new String(Character.toChars(0x1F600)), new String(Character.toChars(0x10000)), "\uD800", "\uDBFF", "\uDC00",
          "\uDFFF" };

      final List<String> samples = new ArrayList<>();
      samples.add("");
      for (final String first : alphabet) {
        samples.add(first);
        for (final String second : alphabet)
          samples.add(first + second);
      }
      // AND THE SHAPE THE ISSUE IS ABOUT: A LONG SHARED PREFIX WITH THE DIFFERENCE AT THE VERY END
      final String prefix = "src/main/java/com/acme/platform/service/internal/";
      for (final String tail : alphabet)
        samples.add(prefix + tail);

      for (final String a : samples)
        for (final String b : samples)
          assertThat(Integer.signum(BinaryComparator.compareStrings(a, b)))
              .as("'%s' vs '%s' must follow the unsigned UTF-8 order of the encodings", escape(a), escape(b))
              .isEqualTo(Integer.signum(BinaryComparator.compareBytes(a.getBytes(StandardCharsets.UTF_8),
                  b.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    void compareBytesAgainstABufferAnswersAndAdvancesLikeTheByteWalk() {
      final byte[][] runs = { new byte[0], bytes(0x00), bytes(0x41), bytes(0x41, 0x42), bytes(0x41, 0x42, 0x43),
          bytes(0x41, 0x42, 0x44), bytes(0x41, 0x42, 0x43, 0x44), bytes(0x7F), bytes(0x80), bytes(0xFF),
          // THE #5321 SHAPE: A CONTINUATION BYTE IS NEGATIVE AS A JAVA byte AND MUST STILL SORT ABOVE ASCII
          "é".getBytes(StandardCharsets.UTF_8), "src/main/java/com/acme/platform/Handler1.java".getBytes(StandardCharsets.UTF_8),
          "src/main/java/com/acme/platform/Handler2.java".getBytes(StandardCharsets.UTF_8),
          "src/main/java/com/acme/platform/Handler1.javax".getBytes(StandardCharsets.UTF_8) };

      final BinaryComparator comparator = new BinaryComparator();
      for (final byte[] key : runs)
        for (final byte[] stored : runs) {
          final Binary page = pageWith(stored);
          final int start = page.position();

          final int result = comparator.compareBytes(key, page);

          assertThat(Integer.signum(result)).as("%s vs %s", Arrays.toString(key), Arrays.toString(stored))
              .isEqualTo(Integer.signum(BinaryComparator.compareBytes(key, stored)));
          assertThat(page.position() - start - varIntSize(stored.length))
              .as("the position must land where one getByte() per compared byte would have left it")
              .isEqualTo(expectedConsumedBytes(key, stored));
        }
    }

    @Test
    void compareBytesReadsThroughASliceWithANonZeroArrayOffset() {
      // A PAGE IS ALWAYS HANDED OVER AS A SLICE, SO THE BACKING ARRAY OFFSET IS NEVER 0: A BULK READ THAT IGNORED IT
      // WOULD COMPARE THE WRONG BYTES ENTIRELY
      final byte[] stored = "src/main/java/com/acme/platform/Handler1.java".getBytes(StandardCharsets.UTF_8);

      final Binary whole = new Binary();
      whole.putByteArray(new byte[] { 9, 9, 9, 9, 9, 9, 9 }); // A HEADER THE SLICE STARTS AFTER
      final int contentAt = whole.position();
      whole.putBytes(stored);
      whole.flip();

      final Binary slice = whole.slice(contentAt);
      final BinaryComparator comparator = new BinaryComparator();

      assertThat(comparator.compareBytes(stored, slice)).isZero();
      assertThat(slice.position()).isEqualTo(varIntSize(stored.length) + stored.length);
    }

    @Test
    void mismatchAdvancesLikeTheByteWalkAndRefusesARunLongerThanTheBufferHolds() {
      final Binary page = new Binary();
      page.putByteArray(bytes(0x41, 0x42, 0x43, 0x44));
      page.flip();

      assertThat(page.mismatch(bytes(0x41, 0x42), 2)).as("an identical run has no mismatch").isEqualTo(-1);
      assertThat(page.position()).as("and leaves the position past the whole run, like two getByte() calls").isEqualTo(2);

      page.position(0);
      assertThat(page.mismatch(bytes(0x41, 0x99), 2)).as("the offset of the first difference").isEqualTo(1);
      assertThat(page.position()).as("and the position past the differing byte, like two getByte() calls").isEqualTo(2);

      page.position(0);
      assertThatThrownBy(() -> page.mismatch(bytes(0x41, 0x42, 0x43, 0x44, 0x45), 5))
          .as("a run longer than the buffer holds fails like the getByte() walk it replaces")
          .isInstanceOf(BufferUnderflowException.class);
      assertThat(page.position()).as("and leaves the position untouched").isZero();

      assertThatThrownBy(() -> page.mismatch(bytes(0x41, 0x42), 4))
          .as("and so does a run longer than the OTHER operand holds, rather than an array index error")
          .isInstanceOf(BufferUnderflowException.class);
      assertThat(page.position()).isZero();
    }

    @Test
    void aStringReadsBackByteForByteWhateverWasWritten() {
      // getString() no longer routes through getBytes(), so the two readers have to be shown to still agree - on an
      // empty string, on multi-byte UTF-8, on a surrogate pair, and through a slice whose backing array offset is not 0
      final String[] samples = { "", "a", "src/main/java/com/acme/platform/Handler1.java", "\u00e9\u00e0\u00fc",
          "\u4e2d\u6587", new String(Character.toChars(0x1F600)), "mixed \u00e9 " + new String(Character.toChars(0x1F600)) };

      for (final String sample : samples) {
        final Binary whole = new Binary();
        whole.putByteArray(new byte[] { 7, 7, 7 }); // A HEADER, SO THE SLICE BELOW CARRIES A NON-ZERO ARRAY OFFSET
        final int contentAt = whole.position();
        whole.putString(sample);
        final int afterContent = whole.position();
        whole.flip();

        whole.position(contentAt);
        assertThat(whole.getString()).as("read back in place").isEqualTo(sample);
        assertThat(whole.position()).as("the position must land past the content").isEqualTo(afterContent);

        assertThat(whole.getString(contentAt)).as("read back by absolute index").isEqualTo(sample);

        whole.position(contentAt);
        assertThat(new String(whole.getBytes(), StandardCharsets.UTF_8)).as("getBytes() must still agree").isEqualTo(sample);

        final Binary slice = whole.slice(contentAt);
        assertThat(slice.getString()).as("read back through a slice").isEqualTo(sample);
      }
    }

    @Test
    void aCorruptLengthPrefixIsStillRefusedByBothReaders() {
      // readLengthPrefix() is now shared, so the #4420 guard has to hold on the string path too and not decode whatever
      // happens to sit past the end of the buffer
      final Binary page = new Binary();
      page.putUnsignedNumber(64); // CLAIMS 64 BYTES
      page.putByteArray(bytes(0x41, 0x42));
      page.flip();

      assertThatThrownBy(page::getString).isInstanceOf(SerializationException.class);
      page.position(0);
      assertThatThrownBy(page::getBytes).isInstanceOf(SerializationException.class);
    }

    @Test
    void aSeekOnAWideCompositeKeyFindsExactlyItsOwnRow() {
      // THE REPORTED WORKLOAD: ONE UNIQUE 5-COLUMN INDEX WHOSE FOUR LEADING COMPONENTS REPEAT ACROSS EVERY ROW, SO THE
      // WHOLE DECISION FALLS ON THE LONG TRAILING PATH - EXACTLY THE COMPARISON BOTH OPTIMISATIONS TOUCH
      final int rows = 2_000;
      final DocumentType file = database.getSchema().createVertexType("File");
      for (final String column : KEY_COLUMNS)
        file.createProperty(column, Type.STRING);
      file.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, KEY_COLUMNS);
      database.getSchema().createEdgeType("IMPORTS");

      database.transaction(() -> {
        for (int i = 0; i < rows; i++)
          database.newVertex("File").set("organizationId", "org-0001").set("projectId", "proj-0001").set("branch", "main")
              .set("indexAttemptId", "attempt-0001").set("path", pathOf(i)).save();
      });

      database.transaction(() -> {
        for (int i = 0; i < rows; i++) {
          final List<String> found = pathsOf(i);
          assertThat(found).as("the seek for path %d must answer its own row and nothing else", i).containsExactly(pathOf(i));
        }
      });

      // AND THE EDGE HALF OF THE REPORTED BLOCK, WHICH SEEKS BOTH ENDPOINTS THROUGH THE SAME INDEX
      database.transaction(() -> database.command("sqlscript", """
          LET a = SELECT FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
                  AND indexAttemptId = 'attempt-0001' AND path = :src;
          LET b = SELECT FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
                  AND indexAttemptId = 'attempt-0001' AND path = :tgt;
          IF ($a.size() > 0 AND $b.size() > 0) {
            CREATE EDGE IMPORTS FROM $a TO $b SET line = 12;
          }
          """, Map.of("src", pathOf(7), "tgt", pathOf(1_234))).close());

      try (final ResultSet rs = database.query("sql", "SELECT out().path AS target FROM File WHERE path = :p",
          Map.of("p", pathOf(7)))) {
        assertThat(rs.next().<List<String>>getProperty("target")).containsExactly(pathOf(1_234));
      }
    }

    private List<String> pathsOf(final int i) {
      final List<String> found = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", """
          SELECT path FROM File WHERE organizationId = 'org-0001' AND projectId = 'proj-0001' AND branch = 'main'
            AND indexAttemptId = 'attempt-0001' AND path = :p""", Map.of("p", pathOf(i)))) {
        while (rs.hasNext())
          found.add(rs.next().getProperty("path"));
      }
      return found;
    }

    /** A project-relative path: a long prefix shared with thousands of siblings and a difference only near the end. */
    private static String pathOf(final int i) {
      return "src/main/java/com/acme/platform/service/internal/generated/Handler" + i + "ServiceImplementation.java";
    }

    /** The bytes a {@code lookupInPage} probe walks: a var-int length prefix followed by the stored key component. */
    private static Binary pageWith(final byte[] stored) {
      final Binary page = new Binary();
      page.putBytes(stored);
      page.flip();
      return page;
    }

    /** How far one {@code getByte()} per compared byte leaves the position past the length prefix. */
    private static int expectedConsumedBytes(final byte[] key, final byte[] stored) {
      final int min = Math.min(key.length, stored.length);
      for (int i = 0; i < min; i++)
        if (key[i] != stored[i])
          return i + 1;
      return min;
    }

    private static int varIntSize(final int value) {
      final Binary sizer = new Binary();
      return sizer.putUnsignedNumber(value);
    }

    private static byte[] bytes(final int... values) {
      final byte[] result = new byte[values.length];
      for (int i = 0; i < values.length; i++)
        result[i] = (byte) values[i];
      return result;
    }

    private static String escape(final String s) {
      final StringBuilder out = new StringBuilder();
      for (final char c : s.toCharArray())
        out.append(c >= 0x20 && c < 0x7F ? String.valueOf(c) : String.format("\\u%04X", (int) c));
      return out.toString();
    }
  }

  /**
   * Regression for #7879: {@code CollectionUtils.compare(Map, Map)} was hardened against a heterogeneous key set (#7111) but
   * not against a heterogeneous value set - the values of paired entries, and the elements of the {@code List}
   * overload, went straight to {@code BinaryComparator.compareTo(Object, Object)}, which threw
   * {@code ClassCastException} for any two values of different classes. A schemaless document mixes {@code Integer}
   * and {@code Long} in the same property routinely (any value read back from JSON, or written by a client that does
   * not pin the width), so ordering one failed the whole query.
   */
  @Nested
  class MixedTypeValueComparison {

    @Test
    void mixedBoxedWidthsOfEqualValueCompareEqual() {
      assertThat(BinaryComparator.compareTo(1, 1L)).isZero();
      assertThat(BinaryComparator.compareTo(1L, 1)).isZero();
      assertThat(BinaryComparator.compareTo((byte) 1, (short) 1)).isZero();
      assertThat(BinaryComparator.compareTo(1.5f, 1.5d)).isZero();
      assertThat(BinaryComparator.compareTo(new BigDecimal("1.0"), 1)).isZero();
      assertThat(BinaryComparator.compareTo(1, new BigDecimal("1.0"))).isZero();
      assertThat(BinaryComparator.compareTo(BigInteger.valueOf(5), 5L)).isZero();
      assertThat(BinaryComparator.compareTo(BigInteger.valueOf(5), new BigDecimal("5.0"))).isZero();
    }

    @Test
    void mixedBoxedWidthsOrderByMagnitudeAndStayAntisymmetric() {
      assertThat(BinaryComparator.compareTo(1, 2L)).isLessThan(0);
      assertThat(BinaryComparator.compareTo(2L, 1)).isGreaterThan(0);
      assertThat(BinaryComparator.compareTo(1.5f, 2.5d)).isLessThan(0);
      assertThat(BinaryComparator.compareTo(2.5d, 1.5f)).isGreaterThan(0);
      assertThat(BinaryComparator.compareTo(new BigDecimal("1.5"), 2)).isLessThan(0);
      assertThat(BinaryComparator.compareTo(2, new BigDecimal("1.5"))).isGreaterThan(0);
    }

    /** A `long` past 2^53 must not lose precision through a `double` round-trip when it meets a floating operand. */
    @Test
    void largeLongAgainstFloatingStaysExact() {
      final long exactlyAboveDoublePrecision = (1L << 53) + 1;
      assertThat(BinaryComparator.compareTo(exactlyAboveDoublePrecision, (double) exactlyAboveDoublePrecision - 1))
          .isGreaterThan(0);
    }

    @Test
    void nonFiniteFloatingValuesStillOrder() {
      assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(Double.NaN, 1));
      assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(1, Double.POSITIVE_INFINITY));
      assertThat(BinaryComparator.compareTo(1, Double.POSITIVE_INFINITY)).isLessThan(0);
      assertThat(BinaryComparator.compareTo(Double.NEGATIVE_INFINITY, 1)).isLessThan(0);
    }

    /** Genuinely unrelated classes must order (by class name, the same tiebreak Map keys already use) rather than throw. */
    @Test
    void unrelatedClassesOrderByClassNameInsteadOfThrowing() {
      assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo("x", 1));
      final int cmp = BinaryComparator.compareTo("x", 1);
      assertThat(Integer.signum(cmp)).isEqualTo(-Integer.signum(BinaryComparator.compareTo(1, "x")));
    }

    /** {@code ArrayList} is not {@code Comparable}: the List arm must delegate to {@link CollectionUtils#compare} instead
     *  of falling through to the raw cast, exactly as {@code TransactionIndexContext.ComparableKey} already relies on. */
    @Test
    void listsCompareThroughCollectionUtilsInsteadOfThrowing() {
      assertThat(BinaryComparator.compareTo(List.of(1), List.of(1))).isZero();
      assertThat(BinaryComparator.compareTo(List.of(1), List.of(2))).isLessThan(0);
      assertThat(BinaryComparator.compareTo(List.of(2), List.of(1))).isGreaterThan(0);
      // The element classes themselves may differ too, the same gap as the map value case.
      assertThatNoException().isThrownBy(() -> BinaryComparator.compareTo(List.of("x"), List.of(1)));
    }

    @Test
    void collectionUtilsCompareListDoesNotThrowOnMixedElementClasses() {
      assertThatNoException().isThrownBy(() -> CollectionUtils.compare(List.of("x"), List.of(1)));
    }

    /**
     * CodeRabbit suggested guarding the same-class fast path with {@code a instanceof Comparable}, so two instances
     * of a non-Comparable class would fall to the class-name tiebreak (0, since the name is identical) instead of
     * throwing. Tried and reverted: {@code LtOperatorTest}/{@code GeOperatorTest}/{@code LeOperatorTest} explicitly
     * assert that {@code op.execute(null, new Object(), new Object())} throws {@code ClassCastException} - two
     * genuinely unrelated same-class objects are meant to be "not comparable", not silently "equal". Pinned here so
     * a future change cannot reintroduce the CodeRabbit suggestion without noticing the conflict.
     */
    @Test
    void sameClassNonComparablePairStillThrows() {
      final Object a = new Object();
      final Object b = new Object();
      assertThat(a.getClass()).isEqualTo(b.getClass());
      assertThatThrownBy(() -> BinaryComparator.compareTo(a, b)).isInstanceOf(ClassCastException.class);
    }

    /**
     * CodeRabbit review follow-up: {@link RID#compareTo(Object)} deliberately compares against a {@code String}
     * operand by parsing it (issue #6188), so {@code RID vs its own string spelling} must answer 0 in BOTH
     * directions. The forward direction (RID first) always worked because RID's own compareTo runs directly, but
     * the reverse (String first) used to fall straight to the class-name tiebreak the instant
     * {@code String#compareTo(Object)} blind-cast the RID and threw - which does not equal 0 for two different
     * classes, breaking antisymmetry. The fallback now retries the reverse direction (negated) before giving up.
     */
    @Test
    void ridAndItsStringSpellingCompareEqualInEitherOrder() {
      final RID rid = new RID(3, 7);
      final String spelling = rid.toString();

      assertThat(BinaryComparator.compareTo(rid, spelling)).isZero();
      assertThat(BinaryComparator.compareTo(spelling, rid)).isZero();
    }

    @Test
    void ridAndAMismatchedStringSpellingStayAntisymmetric() {
      final RID rid = new RID(3, 7);
      final String other = new RID(3, 9).toString();

      final int forward = BinaryComparator.compareTo(rid, other);
      final int reverse = BinaryComparator.compareTo(other, rid);
      assertThat(forward).isNotZero();
      assertThat(Integer.signum(forward)).isEqualTo(-Integer.signum(reverse));
    }

    /**
     * The reverse-direction retry must still only absorb ClassCastException: a malformed, non-RID-shaped String
     * reaches RID#compareTo(String) exactly as it would have in the forward direction, and that still throws
     * (issue #6188) rather than being swallowed into a false "not equal" class-name answer.
     */
    @Test
    void malformedStringReverseAgainstRidStillThrows() {
      final RID rid = new RID(3, 7);
      assertThatThrownBy(() -> BinaryComparator.compareTo("not-a-rid", rid))
          .isInstanceOfAny(IllegalArgumentException.class, IndexOutOfBoundsException.class);
    }

    /**
     * CodeRabbit review follow-up: negating a raw {@code compareTo()} result to reverse it overflows when that
     * result is {@code Integer.MIN_VALUE} ({@code -Integer.MIN_VALUE == Integer.MIN_VALUE} in two's complement),
     * silently breaking antisymmetry for that one pair - a real bug even though {@code RID}'s own bounded
     * {@code {-1, 0, 1}} compareTo() never triggers it. A minimal {@code Comparable} that returns
     * {@code MIN_VALUE} proves the fix ({@code Integer.compare(0, reversed)}) gets the sign right where a raw
     * negation would not.
     */
    @Test
    void reverseDirectionSurvivesAMinValueCompareToWithoutOverflow() {
      final class ExtremeComparable implements Comparable<Object> {
        @Override
        public int compareTo(final Object o) {
          if (o instanceof String)
            return Integer.MIN_VALUE;
          throw new ClassCastException();
        }
      }

      final Object extreme = new ExtremeComparable();
      // Forward: extreme.compareTo("x") answers MIN_VALUE directly - no negation involved, so this is just a sanity
      // check that a legitimately extreme comparator value passes through unchanged.
      assertThat(BinaryComparator.compareTo(extreme, "x")).isEqualTo(Integer.MIN_VALUE);
      // Reverse: String#compareTo(Object) blind-casts `extreme` and throws, forcing the retry path this test is
      // actually about. A raw `-Integer.MIN_VALUE` would overflow back to MIN_VALUE (still "less than"), which
      // contradicts the forward direction above; the fix must answer "greater than" (positive) instead.
      assertThat(BinaryComparator.compareTo("x", extreme)).isGreaterThan(0);
    }

    /** The three repro lines from the issue report, direct on the utility. */
    @Test
    void issueReportedRepros() {
      final Map<String, Comparable> m1 = new LinkedHashMap<>(Map.of("a", 1));
      final Map<String, Comparable> m2 = new LinkedHashMap<>(Map.of("a", 1L));
      assertThat(CollectionUtils.compare(m1, m2)).isZero();

      final Map<String, Comparable> m3 = new LinkedHashMap<>(Map.of("a", "x"));
      final Map<String, Comparable> m4 = new LinkedHashMap<>(Map.of("a", 1));
      assertThatNoException().isThrownBy(() -> CollectionUtils.compare(m3, m4));

      assertThatNoException().isThrownBy(() -> CollectionUtils.compare(List.of("x"), List.of(1)));
    }

    /**
     * Antisymmetry and transitivity of the induced order over a handful of mixed-class values - what a sorted index
     * relies on to place the same entries in the same order on every build. Mirrors
     * {@code CollectionUtilsTest.compareMapsSatisfiesComparatorContract}.
     */
    @Test
    void mixedValueComparisonSatisfiesComparatorContract() {
      final List<Object> values = List.of(1, 2L, 1.5f, 2.5d, new BigDecimal("1.5"), "a", "b", BigInteger.valueOf(10));

      for (final Object x : values) {
        assertThat(BinaryComparator.compareTo(x, x)).as("%s vs itself", x).isZero();
        for (final Object y : values) {
          final int xy = Integer.signum(BinaryComparator.compareTo(x, y));
          assertThat(xy).as("%s vs %s", x, y).isEqualTo(-Integer.signum(BinaryComparator.compareTo(y, x)));
        }
      }
    }
  }

  /**
   * Regression for #7890: {@code PureJavaComparator.equals(left, right, length)} tested the last byte first
   * with no guard for {@code length == 0}, so it read index -1 and threw {@code ArrayIndexOutOfBoundsException}. That is
   * the #6998 defect one frame below the guard #6998 added to the two-argument {@code BinaryComparator.equalsBytes}: the
   * three-argument {@code equalsBytes} and {@code equalsBinary} forward a zero length straight to the selected
   * comparator, which is the pure-Java one whenever the VarHandle implementation is unavailable.
   */
  @Nested
  class PureJavaComparatorZeroLength {

    @Test
    void pureJavaComparatorTreatsZeroLengthAsEqual() {
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[0], new byte[0], 0))
          .as("two empty arrays compared over length 0 are equal; the last-byte fast path used to read index -1")
          .isTrue();

      // Length 0 over non-empty arrays compares nothing, so it is equal too - the VarHandle implementation agrees.
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1 }, new byte[] { 2 }, 0)).isTrue();
      assertThat(UnsignedBytesComparator.BEST_COMPARATOR.equals(new byte[] { 1 }, new byte[] { 2 }, 0)).isTrue();

      // Non-zero lengths keep their behavior.
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 2 }, 2)).isTrue();
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 }, 2)).isFalse();
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 2, 2 }, 2)).isFalse();
      assertThat(UnsignedBytesComparator.PURE_JAVA_COMPARATOR.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 }, 1)).isTrue();
    }

    @Test
    void threeArgumentEqualsBytesTreatsZeroLengthAsEqual() {
      assertThat(BinaryComparator.equalsBytes(new byte[0], new byte[0], 0)).isTrue();
    }

    @Test
    void twoEmptyBinaryValuesAreEqual() {
      assertThat(BinaryComparator.equalsBinary(new Binary(new byte[0]), new Binary(new byte[0]))).isTrue();
      assertThat(new Binary(new byte[0]).equals(new Binary(new byte[0]))).isTrue();
      assertThat(BinaryComparator.equals(new Binary(new byte[0]), new Binary(new byte[0]))).isTrue();

      // A size mismatch is still caught before the comparator.
      assertThat(BinaryComparator.equalsBinary(new Binary(new byte[0]), new Binary(new byte[] { 1 }))).isFalse();
    }
  }

  /**
   * Regression for #8252: {@code BinaryComparator.compareNumbers} read a {@code Float} by its exact binary
   * value against an integral operand ({@code Number.doubleValue()}) and by its shortest decimal everywhere else, so
   * {@code Long 33554449 == Double 33554449.0 < Float 3.355445E7 < Long 33554449}: a cycle, which made the native Select API
   * disagree with SQL and left {@code ORDER BY} over mixed boxed widths unsorted.
   */
  @Nested
  class FloatAgainstIntegralOrder {
    // exact binary value 33554448, shortest decimal 33554450
    private static final float  FLOAT_VALUE = 3.355445E7f;
    private static final long   LONG_VALUE  = 33554449L;
    private static final double DOUBLE_VALUE = 33554449.0;

    @Test
    void comparatorReadsTheFloatAsItsShortestDecimalAgainstEveryOperand() {
      assertThat(BinaryComparator.compareTo(LONG_VALUE, FLOAT_VALUE)).isNegative();
      assertThat(BinaryComparator.compareTo(FLOAT_VALUE, LONG_VALUE)).isPositive();
      assertThat(BinaryComparator.compareTo((int) LONG_VALUE, FLOAT_VALUE)).isNegative();
      assertThat(BinaryComparator.compareTo(BigDecimal.valueOf(LONG_VALUE), FLOAT_VALUE)).isNegative();
      assertThat(BinaryComparator.compareTo(BigInteger.valueOf(LONG_VALUE), FLOAT_VALUE)).isNegative();
      assertThat(BinaryComparator.compareTo(DOUBLE_VALUE, FLOAT_VALUE)).isNegative();
      assertThat(BinaryComparator.compareTo(LONG_VALUE, DOUBLE_VALUE)).isZero();

      // the typed path (the one the index pages use) already answered this way: both entry points must agree
      assertThat(new BinaryComparator().compare(LONG_VALUE, BinaryTypes.TYPE_LONG, FLOAT_VALUE, BinaryTypes.TYPE_FLOAT)).isNegative();
      // and so does the SQL operators' cast
      final Number[] cast = Type.castComparableNumber(LONG_VALUE, FLOAT_VALUE);
      assertThat(((Comparable<Object>) cast[0]).compareTo(cast[1])).isNegative();
    }

    @Test
    void orderIsTransitiveAroundFloatPrecisionBoundaries() {
      final List<Number> pool = new ArrayList<>();
      final Random random = new Random(8252);
      for (final long base : new long[] { 1L << 24, 1L << 25, 33554448L, 1L << 30, 1L << 40, 1L << 53 })
        for (long delta = -6; delta <= 6; delta++) {
          final long v = base + delta;
          pool.add(v);
          pool.add((int) v);
          pool.add((float) v);
          pool.add((double) v);
          pool.add(v + 0.5);
          pool.add(BigDecimal.valueOf(v));
          pool.add(BigInteger.valueOf(v));
          pool.add(Math.nextUp((float) v));
          pool.add(Math.nextDown((float) v));
        }
      for (int i = 0; i < 200; i++)
        pool.add(random.nextFloat() * (1L << 35));

      int violations = 0;
      for (int i = 0; i < 300_000; i++) {
        final Number a = pool.get(random.nextInt(pool.size()));
        final Number b = pool.get(random.nextInt(pool.size()));
        final Number c = pool.get(random.nextInt(pool.size()));
        final int ab = Integer.signum(BinaryComparator.compareTo(a, b));
        final int bc = Integer.signum(BinaryComparator.compareTo(b, c));
        final int ac = Integer.signum(BinaryComparator.compareTo(a, c));
        if (ab != -Integer.signum(BinaryComparator.compareTo(b, a)))
          violations++;
        if (ab <= 0 && bc <= 0 && ac > 0)
          violations++;
        if (ab >= 0 && bc >= 0 && ac < 0)
          violations++;
      }
      assertThat(violations).isZero();
    }

    @Test
    void nativeSelectAgreesWithSqlAndOrderBySorts() throws Exception {
      TestHelper.executeInNewDatabase("./target/databases/testIssue8252FloatOrder", db -> {
        db.getSchema().createDocumentType("M").createProperty("f", Type.FLOAT);
        db.getSchema().createDocumentType("P");
        db.transaction(() -> {
          db.newDocument("M").set("f", FLOAT_VALUE).save();
          for (int i = 0; i < 60; i++) {
            final Number p = switch (i % 3) {
              case 0 -> LONG_VALUE;
              case 1 -> DOUBLE_VALUE;
              default -> FLOAT_VALUE;
            };
            db.newDocument("P").set("id", i).set("p", p).save();
          }
        });

        assertThat(db.query("sql", "SELECT FROM M WHERE f > 33554449").stream().count()).isEqualTo(1);
        assertThat(db.query("sql", "SELECT FROM M WHERE f < 33554449").stream().count()).isZero();
        assertThat(db.select().fromType("M").where().property("f").gt().value(LONG_VALUE).documents().toList()).hasSize(1);
        assertThat(db.select().fromType("M").where().property("f").lt().value(LONG_VALUE).documents().toList()).isEmpty();
        assertThat(db.select().fromType("M").where().property("f").gt().value(DOUBLE_VALUE).documents().toList()).hasSize(1);
        assertThat(db.select().fromType("M").where().property("f").lt().value(DOUBLE_VALUE).documents().toList()).isEmpty();

        final List<Number> sorted = new ArrayList<>();
        try (final ResultSet rs = db.query("sql", "SELECT id, p FROM P ORDER BY p")) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            sorted.add(row.getProperty("p"));
          }
        }
        assertThat(sorted).hasSize(60);
        // every Long and Double (33554449) sorts before every Float (33554450)
        for (int i = 0; i < 40; i++)
          assertThat(sorted.get(i)).isNotInstanceOf(Float.class);
        for (int i = 40; i < 60; i++)
          assertThat(sorted.get(i)).isInstanceOf(Float.class);
      });
    }
  }

  /**
   * Regression for #8921: a DATE/DATETIME operand the timestamp conversion has no case for answered null, which
   * the comparator unboxed to a {@code long} and failed with a {@code NullPointerException}. It now refuses the pair.
   */
  @Nested
  class UnsupportedTemporalOperand {

    @Test
    void anUnreadableDatetimeOperandIsRefusedNotUnboxed() {
      final BinaryComparator comparator = new BinaryComparator();

      assertThatThrownBy(() -> comparator.compare(new Object(), BinaryTypes.TYPE_DATETIME, 1L, BinaryTypes.TYPE_DATETIME))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("not supported");
    }

    @Test
    void readableDatetimeOperandsStillCompare() {
      final BinaryComparator comparator = new BinaryComparator();

      assertThat(comparator.compare(1L, BinaryTypes.TYPE_DATETIME, 2L, BinaryTypes.TYPE_DATETIME)).isNegative();
    }
  }
}
