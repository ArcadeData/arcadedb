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
package com.arcadedb.redis;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The edges of the small pure helpers behind the Redis commands: the canonical 64-bit integer text of INCR and friends, the
 * INCRBYFLOAT arithmetic, the key shapes {@code HDEL} and the hash commands tell apart, and the argument positions that keep their
 * bytes.
 */
class RedisHelpersBoundaryTest {
  private static void assertNotAnInteger(final String text) {
    assertThatThrownBy(() -> RedisCounterOperations.parseInteger(text)).as("'%s'", text).isInstanceOf(RedisException.class)
        .hasMessage("value is not an integer or out of range");
  }

  @Test
  void parseIntegerAcceptsTheCanonicalTextOfAnyLong() {
    assertThat(RedisCounterOperations.parseInteger("0")).isZero();
    assertThat(RedisCounterOperations.parseInteger("-1")).isEqualTo(-1L);
    assertThat(RedisCounterOperations.parseInteger("9")).isEqualTo(9L);
    assertThat(RedisCounterOperations.parseInteger("10")).isEqualTo(10L);
    // 19 digits: the longest text a long has
    assertThat(RedisCounterOperations.parseInteger("1000000000000000000")).isEqualTo(1_000_000_000_000_000_000L);
    assertThat(RedisCounterOperations.parseInteger("9223372036854775807")).isEqualTo(Long.MAX_VALUE);
    assertThat(RedisCounterOperations.parseInteger("-9223372036854775808")).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void parseIntegerRefusesEverythingThatIsNotThatCanonicalText() {
    for (final String text : new String[] { "", "-", "+1", "+", " 1", "1 ", "1.0", "1a", "a1", "a", "-a", "--1", "1-", "1e3", "0x1", "00", "01", "-0",
        "-01", "9223372036854775808", "-9223372036854775809", "99999999999999999999", "-99999999999999999999",
        "\u0663" /* an Arabic-Indic digit, which Long.parseLong would read */, "1\u0663", "/", ":", "1/", "1:" })
      assertNotAnInteger(text);
  }

  @Test
  void requireIntegralValueReadsTextAndRefusesWhatIsNotIntegral() {
    assertThat(RedisCounterOperations.requireIntegralValue(null)).isEqualTo(0L);
    assertThat(RedisCounterOperations.requireIntegralValue("41")).isEqualTo(41L);
    assertThat(RedisCounterOperations.requireIntegralValue(7)).isEqualTo(7);
    assertThat(RedisCounterOperations.requireIntegralValue((short) 7)).isEqualTo((short) 7);
    assertThat(RedisCounterOperations.requireIntegralValue((byte) 7)).isEqualTo((byte) 7);
    assertThat(RedisCounterOperations.requireIntegralValue(7L)).isEqualTo(7L);
    for (final Object value : new Object[] { 1.5d, 2.0f, new BigDecimal("1"), BigInteger.ONE, "1.5", "abc" })
      assertThatThrownBy(() -> RedisCounterOperations.requireIntegralValue(value)).as("%s", value).isInstanceOf(RedisException.class)
          .hasMessage("value is not an integer or out of range");
  }

  @Test
  void incrementByFloatKeepsSeventeenDecimalsAndNoTrailingZeros() {
    assertThat(RedisCounterOperations.incrementByFloat("0.123456789012345678").apply("0.1")).isEqualTo("0.22345678901234568");
    assertThat(RedisCounterOperations.incrementByFloat("123456789012345678.123456789012345678").apply(null))
        .isEqualTo("123456789012345678.12345678901234568");
    assertThat(RedisCounterOperations.incrementByFloat("1e-17").apply(null)).isEqualTo("0.00000000000000001");
    assertThat(RedisCounterOperations.incrementByFloat("6e-18").apply(null)).isEqualTo("0.00000000000000001");
    assertThat(RedisCounterOperations.incrementByFloat("4e-18").apply(null)).isEqualTo("0");
    // far below the 17th decimal: nothing survives, and no huge number is built for it
    assertThat(RedisCounterOperations.incrementByFloat("1e-18").apply(null)).isEqualTo("0");
    assertThat(RedisCounterOperations.incrementByFloat("1e-999999999").apply(null)).isEqualTo("0");
    assertThat(RedisCounterOperations.incrementByFloat("1.10").apply(null)).isEqualTo("1.1");
    assertThat(RedisCounterOperations.incrementByFloat("0.7").apply("3.3")).isEqualTo("4");
    assertThat(RedisCounterOperations.incrementByFloat("1").apply("1e3")).isEqualTo("1001");
    assertThat(RedisCounterOperations.incrementByFloat("-1").apply("1")).isEqualTo("0");
    assertThat(RedisCounterOperations.incrementByFloat("-0.0").apply(null)).isEqualTo("0");
    // a Float left in the store counts with its own shortest text
    assertThat(RedisCounterOperations.incrementByFloat("1").apply(0.1f)).isEqualTo("1.1");
    assertThat(RedisCounterOperations.incrementByFloat("0.5").apply(0.25d)).isEqualTo("0.75");
  }

  @Test
  void incrementByFloatRefusesWhatIsNotAFiniteNumberInRange() {
    final String notFloat = "value is not a valid float";
    final String infinite = "increment would produce NaN or Infinity";
    for (final String delta : new String[] { " 1", "1 ", "0x10", "1_0", "\u0661", "" })
      assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat(delta)).as("'%s'", delta).isInstanceOf(RedisException.class)
          .hasMessage(notFloat);
    for (final String delta : new String[] { "nan", "NaN", "-inf", "+Infinity", "inf", "1e4933", "-1e4933" })
      assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat(delta)).as("'%s'", delta).isInstanceOf(RedisException.class)
          .hasMessage(infinite);
    // the largest magnitude is 1e4932 (exclusive): 1e4931 is fine, a sum that crosses it is refused
    assertThat((String) RedisCounterOperations.incrementByFloat("1e4931").apply(null)).startsWith("1000").hasSize(4932);
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("9e4931").apply("9e4931")).isInstanceOf(RedisException.class)
        .hasMessage(infinite);
    // a stored value that is not a float, or not a finite one, is "not a valid float"
    for (final Object stored : new Object[] { "abc", "inf", "nan", "1,5", Double.NaN, Double.POSITIVE_INFINITY })
      assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("1").apply(stored)).as("%s", stored)
          .isInstanceOf(RedisException.class).hasMessage(notFloat);
    // text of 5 KB or more is refused whatever it holds, one character less is read
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("1".repeat(5120))).isInstanceOf(RedisException.class)
        .hasMessage(notFloat);
    assertThat(RedisCounterOperations.incrementByFloat("0".repeat(5119)).apply(null)).isEqualTo("0");
    // ASCII only: the last character BigDecimal would read as a digit but strtold does not is above 127
    assertThatThrownBy(() -> RedisCounterOperations.incrementByFloat("1").apply("\u0663")).isInstanceOf(RedisException.class)
        .hasMessage(notFloat);
    assertThat(RedisCounterOperations.incrementByFloat("1").apply("2")).isEqualTo("3");
  }

  @Test
  void incrementAndDecrementAreCheckedAtTheEdgesOfALong() {
    assertThat(RedisCounterOperations.incrementBy(1).apply(Long.MAX_VALUE - 1)).isEqualTo(Long.MAX_VALUE);
    assertThatThrownBy(() -> RedisCounterOperations.incrementBy(1).apply(Long.MAX_VALUE)).isInstanceOf(RedisException.class)
        .hasMessage("increment or decrement would overflow");
    assertThat(RedisCounterOperations.incrementBy(-1).apply(Long.MIN_VALUE + 1)).isEqualTo(Long.MIN_VALUE);
    assertThatThrownBy(() -> RedisCounterOperations.incrementBy(-1).apply(Long.MIN_VALUE)).isInstanceOf(RedisException.class);
    assertThat(RedisCounterOperations.decrementBy(1).apply(Long.MIN_VALUE + 1)).isEqualTo(Long.MIN_VALUE);
    assertThatThrownBy(() -> RedisCounterOperations.decrementBy(1).apply(Long.MIN_VALUE)).isInstanceOf(RedisException.class)
        .hasMessage("increment or decrement would overflow");
    // negating Long.MIN_VALUE would wrap to itself: DECRBY k MIN is refused on a positive value, not run as INCRBY k MIN
    assertThatThrownBy(() -> RedisCounterOperations.decrementBy(Long.MIN_VALUE).apply(1L)).isInstanceOf(RedisException.class);
    assertThat(RedisCounterOperations.decrementBy(Long.MIN_VALUE).apply(-1L)).isEqualTo(Long.MAX_VALUE);
    assertThat(RedisCounterOperations.incrementBy(5).apply(null)).isEqualTo(5L);
    assertThat(RedisCounterOperations.decrementBy(5).apply("3")).isEqualTo(-2L);
  }

  @Test
  void aRidKeyIsHashBucketColonPositionWithBothNonNegative() {
    for (final String key : new String[] { "#0:0", "#1:2", "#12:345", "#1:0" })
      assertThat(RedisRecords.isRid(key)).as(key).isTrue();
    for (final String key : new String[] { "", "#", "#1", "#1:", "1:2", "#:2", "#1:2:3", "#-1:0", "#1:-2", "# 1:2", "#1:a", "#a:1", "##1:2", "#1:2 ",
        "#12", ":", "#:", "#::" })
      assertThat(RedisRecords.isRid(key)).as(key).isFalse();
  }

  @Test
  void parseRidRefusesWhatIsNotARid() {
    assertThat(RedisRecords.parseRid("#3:7").getBucketId()).isEqualTo(3);
    assertThat(RedisRecords.parseRid("#3:7").getPosition()).isEqualTo(7L);
    assertThatThrownBy(() -> RedisRecords.parseRid("nope")).isInstanceOf(RedisException.class)
        .hasMessage("invalid RID 'nope', it must be #<bucket-id>:<bucket-position>");
  }

  @Test
  void anIndexKeyIsQuotedTextOrAJsonArrayOrTheTextItself() {
    assertThat(RedisIndexKeys.parse("")).containsExactly("");
    assertThat(RedisIndexKeys.parse("abc")).containsExactly("abc");
    assertThat(RedisIndexKeys.parse("\"abc\"")).containsExactly("abc");
    assertThat(RedisIndexKeys.parse("\"\"")).containsExactly("");
    // a lone or unterminated quote belongs to the value
    assertThat(RedisIndexKeys.parse("\"")).containsExactly("\"");
    assertThat(RedisIndexKeys.parse("\"abc")).containsExactly("\"abc");
    assertThat(RedisIndexKeys.parse("abc\"")).containsExactly("abc\"");
    assertThat(RedisIndexKeys.parse("'a'")).containsExactly("'a'");
    // only the outer pair is removed
    assertThat(RedisIndexKeys.parse("\"a\"b\"")).containsExactly("a\"b");
    assertThat(RedisIndexKeys.parse("[\"a\",1]")).containsExactly("a", 1);
    assertThatThrownBy(() -> RedisIndexKeys.parse("[]")).isInstanceOf(RedisException.class).hasMessageContaining("is empty");
    assertThatThrownBy(() -> RedisIndexKeys.parse("[1,")).isInstanceOf(RedisException.class).hasMessageContaining("not a valid JSON array");
  }

  @Test
  void theOpaqueArgumentPositionsAreInclusiveAndOpenEnded() {
    // GET <key>: argument 1 only
    final int[] get = RedisArguments.opaquePositions("GET");
    assertThat(RedisArguments.isOpaque(get, 0)).isFalse();
    assertThat(RedisArguments.isOpaque(get, 1)).isTrue();
    assertThat(RedisArguments.isOpaque(get, 2)).isFalse();
    // EXISTS <key>...: every argument from 1 on
    final int[] exists = RedisArguments.opaquePositions("EXISTS");
    assertThat(RedisArguments.isOpaque(exists, 0)).isFalse();
    assertThat(RedisArguments.isOpaque(exists, 1)).isTrue();
    assertThat(RedisArguments.isOpaque(exists, 500)).isTrue();
    // SET <key> <value> [options]: the key and the value, not the options
    final int[] set = RedisArguments.opaquePositions("SET");
    assertThat(RedisArguments.isOpaque(set, 1)).isTrue();
    assertThat(RedisArguments.isOpaque(set, 2)).isTrue();
    assertThat(RedisArguments.isOpaque(set, 3)).isFalse();
    // HGET <database> <key>: the database name is parsed, the key is not
    final int[] hget = RedisArguments.opaquePositions("HGET");
    assertThat(RedisArguments.isOpaque(hget, 1)).isFalse();
    assertThat(RedisArguments.isOpaque(hget, 2)).isTrue();
    assertThat(RedisArguments.isOpaque(hget, 3)).isFalse();
    // HMGET and HDEL: from the first key on
    assertThat(RedisArguments.isOpaque(RedisArguments.opaquePositions("HMGET"), 1)).isFalse();
    assertThat(RedisArguments.isOpaque(RedisArguments.opaquePositions("HMGET"), 2)).isTrue();
    assertThat(RedisArguments.isOpaque(RedisArguments.opaquePositions("HDEL"), 9)).isTrue();
    // ECHO and PING carry an opaque message; a command that is not listed has no opaque argument
    assertThat(RedisArguments.isOpaque(RedisArguments.opaquePositions("ECHO"), 1)).isTrue();
    assertThat(RedisArguments.isOpaque(RedisArguments.opaquePositions("PING"), 1)).isTrue();
    assertThat(RedisArguments.opaquePositions("SELECT")).isNull();
    assertThat(RedisArguments.isOpaque(null, 1)).isFalse();
    // a position of 0 means "none" in the merged table, argument 0 is the command name
    assertThat(RedisArguments.isOpaque(new int[] { 0, 0, 0, 0 }, 0)).isFalse();
  }

  @Test
  void theKeyCommandsAreTheOnesThatNameAKeyAndTheirPositionsAreCopies() {
    assertThat(RedisArguments.keyCommands()).contains("GET", "SET", "EXISTS", "HGET", "HMGET", "HDEL", "INCRBYFLOAT").doesNotContain("PING", "ECHO");
    assertThat(RedisArguments.keyPositions("EXISTS")).containsExactly(1, -1);
    assertThat(RedisArguments.keyPositions("HGET")).containsExactly(2, 2);
    assertThat(RedisArguments.keyPositions("PING")).isNull();
    // the caller may modify what it got
    RedisArguments.keyPositions("GET")[0] = 99;
    assertThat(RedisArguments.keyPositions("GET")).containsExactly(1, 1);
  }
}
