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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.function.text.TextLevenshteinDistance;
import com.arcadedb.function.util.UtilCompress;
import com.arcadedb.function.util.UtilDecompress;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * Tests for security boundary conditions in Cypher functions.
 */
class CypherFunctionSecurityTest extends TestHelper {
  @Test
  void utilSleepMaxDuration() {
    final ResultSet rs = database.query("opencypher", "RETURN util.sleep(999999999999) AS result");
    // Test that sleep duration is limited to prevent DoS
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("Sleep duration exceeds maximum allowed")).isTrue();
  }

  @Test
  void utilSleepValidDuration() {
    // Test that valid sleep durations work (e.g., 100ms)
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(100) AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNull();
  }

  @Test
  void utilSleepNegativeDuration() {
    // Test that negative durations are handled gracefully
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(-100) AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNull();
  }

  @Test
  void utilSleepZeroDuration() {
    // Test that zero duration is handled
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(0) AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNull();
  }

  /**
   * Was {@code @Disabled} for "large parameter passing (11MB) has issues in Cypher query engine". That reason was
   * wrong: binding a multi-megabyte String parameter works, and the cap fires exactly as written. Issue #7142 asked
   * which of the two it was - the answer is that there was no engine limitation to record.
   */
  @Test
  void utilCompressInputSizeLimit() {
    final char[] largeChars = new char[UtilCompress.MAX_INPUT_SIZE + 1];
    Arrays.fill(largeChars, 'x');
    final String largeString = new String(largeChars);

    final ResultSet rs = database.query("opencypher", "RETURN util.compress($data) AS result", "data", largeString);

    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage()).contains("Input size exceeds maximum allowed");
  }

  /** The companion boundary: a payload of exactly the cap is compressed rather than refused. */
  @Test
  void utilCompressAcceptsExactlyTheCap() {
    final char[] atCap = new char[UtilCompress.MAX_INPUT_SIZE];
    Arrays.fill(atCap, 'x');

    final ResultSet rs = database.query("opencypher", "RETURN util.compress($data) AS result", "data", new String(atCap));

    assertThat(rs.next().<String>getProperty("result")).isNotEmpty();
  }

  @Test
  void utilCompressValidSize() {
    // Test that valid compression works
    final ResultSet resultSet = database.query("opencypher", "RETURN util.compress('Hello World') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNotNull();
  }

  @Test
  void utilDecompressRoundTrip() {
    // The control for the two zip-bomb tests below: a payload under the cap still round-trips.
    final ResultSet compressResult = database.query("opencypher", "RETURN util.compress('test') AS compressed");
    final String compressed = compressResult.next().getProperty("compressed").toString();

    final ResultSet decompressResult = database.query("opencypher",
        "RETURN util.decompress('" + compressed + "') AS result");
    assertThat(decompressResult.hasNext()).isTrue();
    assertThat(decompressResult.next().<String>getProperty("result")).isEqualTo("test");
  }

  /**
   * A real zip bomb: ~134KB of base64 that inflates past {@link UtilDecompress#MAX_OUTPUT_SIZE}. The test this
   * replaced round-tripped four bytes, so the guard it was named for was never reached and both of its branches
   * were uncovered (issue #7142).
   */
  @Test
  void utilDecompressGzipOutputSizeLimitRejectsAZipBomb() throws IOException {
    final ResultSet rs = database.query("opencypher", "RETURN util.decompress($data, 'gzip') AS result",
        "data", zipBomb("gzip"));

    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage()).contains("Decompressed output size exceeds maximum allowed", "zip bomb");
  }

  /** The deflate branch enforces the same cap - it used to carry its own copy of the read loop. */
  @Test
  void utilDecompressDeflateOutputSizeLimitRejectsAZipBomb() throws IOException {
    final ResultSet rs = database.query("opencypher", "RETURN util.decompress($data, 'deflate') AS result",
        "data", zipBomb("deflate"));

    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage()).contains("Decompressed output size exceeds maximum allowed", "zip bomb");
  }

  /**
   * The accepting half of the same boundary. It has to sit at exactly {@code MAX_OUTPUT_SIZE - 1}: any smaller
   * payload would still pass if the cap were lowered underneath it, so only the last accepted size pins the
   * contract from below the way the bomb pins it from above.
   */
  @Test
  void utilDecompressAcceptsAPayloadJustUnderTheCap() throws IOException {
    final int justUnder = UtilDecompress.MAX_OUTPUT_SIZE - 1;
    final ResultSet rs = database.query("opencypher", "RETURN util.decompress($data, 'gzip') AS result",
        "data", compressedRepeatedByte(justUnder, "gzip"));

    assertThat(rs.next().<String>getProperty("result")).hasSize(justUnder);
  }

  /**
   * Builds a payload that inflates to one byte past {@link UtilDecompress#MAX_OUTPUT_SIZE}, which is the first size
   * the guard refuses. Written a megabyte at a time so the bomb costs a megabyte of heap to build, not 100.
   */
  private static String zipBomb(final String algorithm) throws IOException {
    return compressedRepeatedByte(UtilDecompress.MAX_OUTPUT_SIZE + 1, algorithm);
  }

  /**
   * The algorithm is named rather than defaulted: a helper that quietly produced gzip for anything it did not
   * recognise would hand the deflate test a payload the deflate branch never sees, and the test would still pass.
   */
  private static String compressedRepeatedByte(final int size, final String algorithm) throws IOException {
    final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    final byte[] chunk = new byte[1024 * 1024];
    Arrays.fill(chunk, (byte) 'x');
    try (final OutputStream out = switch (algorithm) {
      case "gzip" -> new GZIPOutputStream(compressed);
      case "deflate" -> new DeflaterOutputStream(compressed);
      default -> throw new IllegalArgumentException("Unsupported compression algorithm: " + algorithm);
    }) {
      for (int written = 0; written < size; written += chunk.length)
        out.write(chunk, 0, Math.min(chunk.length, size - written));
    }
    return Base64.getEncoder().encodeToString(compressed.toByteArray());
  }

  @Test
  void textLpadMaxLength() {
    final ResultSet rs = database.query("opencypher", "RETURN text.lpad('x', 999999999, ' ') AS result");
    // Test that lpad has length limits to prevent excessive memory allocation
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("length exceeds maximum allowed") ||
        exception.getMessage().contains("Invalid length")).isTrue();
  }

  @Test
  void textLpadNegativeLength() {
    final ResultSet rs = database.query("opencypher", "RETURN text.lpad('x', -100, ' ') AS result");
    // Test that negative lengths are rejected
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("Invalid length") ||
        exception.getMessage().contains("negative")).isTrue();
  }

  @Test
  void textLpadValidLength() {
    // Test that valid padding works
    final ResultSet resultSet = database.query("opencypher", "RETURN text.lpad('x', 5, ' ') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("    x");
  }

  @Test
  void textRpadMaxLength() {
    final ResultSet rs = database.query("opencypher", "RETURN text.rpad('x', 999999999, ' ') AS result");
    // Test that rpad has length limits to prevent excessive memory allocation
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("length exceeds maximum allowed") ||
        exception.getMessage().contains("Invalid length")).isTrue();
  }

  @Test
  void textRpadNegativeLength() {
    final ResultSet rs = database.query("opencypher", "RETURN text.rpad('x', -100, ' ') AS result");
    // Test that negative lengths are rejected
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("Invalid length") ||
        exception.getMessage().contains("negative")).isTrue();
  }

  @Test
  void textRpadValidLength() {
    // Test that valid padding works
    final ResultSet resultSet = database.query("opencypher", "RETURN text.rpad('x', 5, ' ') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("x    ");
  }

  @Test
  void textRegexReplaceCatastrophicBacktracking() {
    // Test ReDoS protection - catastrophic backtracking pattern
    // Note: Java's regex engine may not cause stack overflow for short inputs
    // This test verifies that the function handles long backtracking patterns safely
    // by using a pattern that exceeds the max length limit instead
    final String longPattern = "(a+)+".repeat(200);
    final ResultSet rs = database.query("opencypher",
        "RETURN text.regexReplace('test', '" + longPattern + "', 'x') AS result"); // 1000 chars, exceeds 500 limit
    final Exception exception = assertThatExceptionOfType(Exception.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("pattern") ||
        exception.getMessage().contains("regex") ||
        exception.getMessage().contains("exceeds")).as("Expected regex-related error but got: " + exception.getMessage()).isTrue();
  }

  @Test
  void textRegexReplaceTooLongPattern() {
    // Test that excessively long patterns are rejected (MAX_PATTERN_LENGTH = 500)
    // Use literal value to avoid parameter handling issues
    final String longPattern = "a".repeat(600);
    final ResultSet rs = database.query("opencypher",
        "RETURN text.regexReplace('test', '" + longPattern + "', 'x') AS result");
    final Exception exception = assertThatExceptionOfType(Exception.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("pattern") ||
        exception.getMessage().contains("regex") ||
        exception.getMessage().contains("exceeds")).as("Expected pattern length error but got: " + exception.getMessage()).isTrue();
  }

  @Test
  void textRegexReplaceValidPattern() {
    // Test that valid patterns work correctly
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.regexReplace('hello world', 'world', 'universe') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("hello universe");
  }

  @Test
  void dateAddOverflow() {
    final ResultSet rs = database.query("opencypher", "RETURN date.add(9223372036854775807, 1, 'ms') AS result");
    // Test that integer overflow is caught in date arithmetic
    final Exception exception = assertThatExceptionOfType(Exception.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("overflow") ||
        exception.getMessage().contains("ArithmeticException")).as("Expected overflow error but got: " + exception.getMessage())
        .isTrue();
  }

  @Test
  void dateAddMultiplicationOverflow() {
    final ResultSet rs = database.query("opencypher", "RETURN date.add(0, 9223372036854775807, 'h') AS result");
    // Test that multiplication overflow is caught (large value * unit conversion)
    final Exception exception = assertThatExceptionOfType(Exception.class)
        .isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("overflow") ||
        exception.getMessage().contains("ArithmeticException")).as("Expected overflow error but got: " + exception.getMessage())
        .isTrue();
  }

  @Test
  void dateAddValidOperation() {
    // Test that valid date operations work
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.add(1000, 500, 'ms') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<Long>getProperty("result")).isEqualTo(1500L);
  }

  @Test
  void textFormatInvalidFormat() {
    final ResultSet rs = database.query("opencypher", "RETURN text.format('%s %s', 'only one arg') AS result");
    // Test that invalid format strings are handled gracefully
    final Exception exception = assertThatExceptionOfType(Exception.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("format") ||
        exception.getMessage().contains("MissingFormatArgumentException")).as(
        "Expected format error but got: " + exception.getMessage()).isTrue();
  }

  @Test
  void textFormatIllegalFormatConversion() {
    final ResultSet rs = database.query("opencypher", "RETURN text.format('%d', 'not a number') AS result");
    // Test that illegal format conversions are caught
    final Exception exception = assertThatExceptionOfType(Exception.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("format") ||
        exception.getMessage().contains("IllegalFormatConversionException")).as(
        "Expected format conversion error but got: " + exception.getMessage()).isTrue();
  }

  @Test
  void textFormatValidUsage() {
    // Test that valid formatting works
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.format('Hello %s, you are %d years old', 'Alice', 30) AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isEqualTo("Hello Alice, you are 30 years old");
  }

  /**
   * Was {@code @Disabled} for "large parameter passing (10KB+) has issues in Cypher query engine". Same verdict as
   * {@link #utilCompressInputSizeLimit()}: the parameter binds, the guard fires, there was no engine limitation.
   */
  @Test
  void textLevenshteinDistanceMaxLength() {
    final String longString = "a".repeat(TextLevenshteinDistance.MAX_STRING_LENGTH + 1);
    final ResultSet rs = database.query("opencypher", "RETURN text.levenshteinDistance($str1, $str2) AS result",
        "str1", longString,
        "str2", "test");
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext)
        .actual();
    assertThat(exception.getMessage()).contains("String length exceeds maximum allowed");
  }

  /** The second argument is capped too, and it is a separate check in the function. */
  @Test
  void textLevenshteinDistanceMaxLengthOnTheSecondArgument() {
    final String longString = "a".repeat(TextLevenshteinDistance.MAX_STRING_LENGTH + 1);
    final ResultSet rs = database.query("opencypher", "RETURN text.levenshteinDistance($str1, $str2) AS result",
        "str1", "test",
        "str2", longString);
    final IllegalArgumentException exception = assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(rs::hasNext)
        .actual();
    assertThat(exception.getMessage()).contains("String length exceeds maximum allowed");
  }

  /** A pair of strings at exactly the cap is computed rather than refused. */
  @Test
  void textLevenshteinDistanceAcceptsExactlyTheCap() {
    final String atCap = "a".repeat(TextLevenshteinDistance.MAX_STRING_LENGTH);
    final ResultSet rs = database.query("opencypher", "RETURN text.levenshteinDistance($str1, $str2) AS result",
        "str1", atCap,
        "str2", atCap);
    assertThat(rs.next().<Long>getProperty("result")).isZero();
  }

  @Test
  void textLevenshteinDistanceValidStrings() {
    // Test that valid string comparison works
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.levenshteinDistance('kitten', 'sitting') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<Long>getProperty("result")).isEqualTo(3L);
  }

  @Test
  void dateFieldsInvalidTimezone() {
    final ResultSet rs = database.query("opencypher",
        "RETURN date.fields('2024-01-15', 'yyyy-MM-dd', 'InvalidTimezone') AS result");
    // Test that invalid timezone IDs are rejected
    final Exception exception = assertThatExceptionOfType(Exception.class).isThrownBy(rs::hasNext).actual();
    assertThat(exception.getMessage().contains("timezone") ||
        exception.getMessage().contains("Invalid")).as("Expected timezone error but got: " + exception.getMessage()).isTrue();
  }

  @Test
  void dateFieldsValidTimezone() {
    // Test that valid timezone handling works
    // Note: date.fields requires a datetime string, not just date
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.fields('2024-01-15T10:30:00', 'yyyy-MM-dd\\'T\\'HH:mm:ss', 'UTC') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    final Object result = resultSet.next().getProperty("result");
    assertThat(result).isNotNull();
    assertThat(result).isInstanceOf(Map.class);
  }

  @Test
  void textRegexReplaceNullHandling() {
    // Test null handling in regex replace
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.regexReplace(null, 'pattern', 'replace') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNull();
  }

  @Test
  void dateAddNullHandling() {
    // Test null handling in date add
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.add(null, 100, 'ms') AS result");
    assertThat(resultSet.hasNext()).isTrue();
    assertThat(resultSet.next().<String>getProperty("result")).isNull();
  }
}
