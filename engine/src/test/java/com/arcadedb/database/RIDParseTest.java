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
package com.arcadedb.database;

import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.NumberUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins how {@link RID#is(String)} and {@link RID#RID(String)} read a textual RID, token by token, including the shapes that are
 * not RIDs. The two do not agree on every string ({@code is} only checks the characters, the constructor also converts), and
 * callers such as the {@code filter} option of {@code vector.neighbors} rely on that exact split: a string {@code is} refuses is
 * reported as a parsing error, one it accepts but the constructor cannot convert surfaces as the conversion error.
 */
class RIDParseTest {

  @Test
  void canonicalRidsAreRecognisedAndParsed() {
    assertParsed("#1:2", 1, 2);
    assertParsed("#12:345", 12, 345);
    assertParsed("#0:0", 0, 0);
    assertParsed("#00001:00002", 1, 2);
    assertParsed("#2147483647:9223372036854775807", Integer.MAX_VALUE, Long.MAX_VALUE);
    assertParsed("#123456789:999999999999999999", 123456789, 999999999999999999L);
    assertParsed("#1234:5678901234567890", 1234, 5678901234567890L);
  }

  @Test
  void signsAreAcceptedAnywhereByIsAndByTheConstructorWhenTheyFormANumber() {
    assertParsed("#+1:+2", 1, 2);
    assertParsed("#-1:-2", -1, -2);
    assertParsed("#1:-2", 1, -2);
    assertParsed("#-1:2", -1, 2);
    assertParsed("#-2147483648:-9223372036854775808", Integer.MIN_VALUE, Long.MIN_VALUE);
  }

  @Test
  void nonAsciiDigitsAreDigits() {
    assertParsed("#\u0661:\u0662", 1, 2);
    assertParsed("#\u0660:\u0660", 0, 0);
    assertParsed("#1:\u0662\u0663", 1, 23);
  }

  @Test
  void aTrailingColonAfterThePositionIsIgnored() {
    assertParsed("#1:2:", 1, 2);
  }

  @Test
  void extraPartsAreRefusedByIsButTheConstructorReadsTheFirstTwo() {
    assertIsRefusedButParsed("#1:2:3", 1, 2);
    assertIsRefusedButParsed("#1:2::", 1, 2);
    assertIsRefusedButParsed("#1:2:x", 1, 2);
  }

  @Test
  void aShapeIsAcceptsButTheConstructorCannotConvertFailsWithNumberFormat() {
    for (final String token : new String[] { "#:12", "#--1:2", "#1+2:3", "#1:2-3", "#2147483648:1", "#99999999999:1",
        "#1:9223372036854775808", "#12345678901234567890:1", "#1:12345678901234567890" }) {
      assertThat(RID.is(token)).as("is(" + token + ")").isTrue();
      assertThatThrownBy(() -> new RID(token)).as("new RID(" + token + ")").isInstanceOf(NumberFormatException.class);
    }
  }

  @Test
  void notARidIsRefusedByIsAndFailsInTheConstructorInTheWayItAlwaysDid() {
    assertNotParsed("#1::2", NumberFormatException.class);
    assertNotParsed("#:", NumberFormatException.class);
    assertNotParsed("#:1:2", NumberFormatException.class);
    assertNotParsed("#a:b", NumberFormatException.class);
    assertNotParsed("#1:b", NumberFormatException.class);
    assertNotParsed("#a:2", NumberFormatException.class);
    assertNotParsed("#1:2 ", NumberFormatException.class);
    assertNotParsed("#1:2\n", NumberFormatException.class);
    assertNotParsed("##1:2", NumberFormatException.class);
    assertNotParsed("#1;2", NumberFormatException.class);
    assertNotParsed("#1:", IndexOutOfBoundsException.class);
    assertNotParsed("#1", IndexOutOfBoundsException.class);
    assertNotParsed("#", NoSuchElementException.class);
    assertNotParsed("", IllegalArgumentException.class);
    assertNotParsed(" #1:2", IllegalArgumentException.class);
    assertNotParsed("1:2", IllegalArgumentException.class);
  }

  @Test
  void isOnAnObjectAnswersForRidsAndStringsOnly() {
    assertThat(RID.is((Object) new RID(1, 2))).isTrue();
    assertThat(RID.is((Object) "#1:2")).isTrue();
    assertThat(RID.is((Object) "#1:2:3")).isFalse();
    assertThat(RID.is((Object) 12)).isFalse();
    assertThat(RID.is((Object) null)).isFalse();
  }

  @Test
  void aSweepOfCanonicalRidsRoundTripsThroughToString() {
    final List<RID> rids = new ArrayList<>();
    for (final int bucket : new int[] { 0, 1, 7, 99, 12345, 999_999_999, Integer.MAX_VALUE })
      for (final long position : new long[] { 0, 1, 9, 10, 2047, 2048, 65_535, 123_456_789L, 1L << 40, 999_999_999_999_999_999L,
          Long.MAX_VALUE })
        rids.add(new RID(bucket, position));
    for (final RID rid : rids) {
      final String text = rid.toString();
      assertThat(RID.is(text)).as(text).isTrue();
      final RID parsed = new RID(text);
      assertThat(parsed.getBucketId()).as(text).isEqualTo(rid.getBucketId());
      assertThat(parsed.getPosition()).as(text).isEqualTo(rid.getPosition());
      assertThat(parsed).as(text).isEqualTo(rid);
    }
  }

  private static void assertParsed(final String token, final int bucket, final long position) {
    assertThat(RID.is(token)).as("is(" + token + ")").isTrue();
    final RID rid = new RID(token);
    assertThat(rid.getBucketId()).as("bucket of " + token).isEqualTo(bucket);
    assertThat(rid.getPosition()).as("position of " + token).isEqualTo(position);
  }

  private static void assertIsRefusedButParsed(final String token, final int bucket, final long position) {
    assertThat(RID.is(token)).as("is(" + token + ")").isFalse();
    final RID rid = new RID(token);
    assertThat(rid.getBucketId()).as("bucket of " + token).isEqualTo(bucket);
    assertThat(rid.getPosition()).as("position of " + token).isEqualTo(position);
  }

  private static void assertNotParsed(final String token, final Class<? extends Throwable> failure) {
    assertThat(RID.is(token)).as("is(" + token + ")").isFalse();
    assertThatThrownBy(() -> new RID(token)).as("new RID(" + token + ")").isInstanceOf(failure);
  }

  /**
   * The general path as it was before the canonical form was read in place, kept here as the model: random strings over the
   * alphabet that matters (the sigil, the separator, signs, ASCII and non-ASCII digits, a letter, a blank) must give the
   * same {@code is} verdict and the same RID, or the same exception class, as the model.
   */
  @Test
  void randomStringsAgreeWithTheGeneralPath() {
    final char[] alphabet = { '#', ':', ':', '+', '-', '0', '1', '2', '7', '9', '9', '9', '\u0661', 'a', ' ' };
    final Random random = new Random(20261123L);
    for (int i = 0; i < 400_000; i++) {
      final StringBuilder sb = new StringBuilder();
      if (random.nextInt(10) > 0)
        sb.append('#');
      final int length = random.nextInt(24);
      for (int c = 0; c < length; c++)
        sb.append(random.nextInt(4) == 0 ? alphabet[random.nextInt(alphabet.length)] : (char) ('0' + random.nextInt(10)));
      if (random.nextInt(3) > 0 && sb.indexOf(":") < 0 && sb.length() > 1)
        sb.insert(1 + random.nextInt(sb.length() - 1), ':');
      final String text = sb.toString();
      assertThat(RID.is(text)).as("is(" + text + ")").isEqualTo(modelIs(text));

      long[] expected = null;
      Class<? extends Throwable> failure = null;
      try {
        expected = modelParse(text);
      } catch (final Throwable e) {
        failure = e.getClass();
      }
      if (failure != null) {
        final Class<? extends Throwable> expectedFailure = failure;
        assertThatThrownBy(() -> new RID(text)).as("new RID(" + text + ")").isInstanceOf(expectedFailure);
      } else {
        final RID rid = new RID(text);
        assertThat(new long[] { rid.getBucketId(), rid.getPosition() }).as("new RID(" + text + ")").isEqualTo(expected);
      }
    }
  }

  private static boolean modelIs(final String valueAsString) {
    if (valueAsString.length() > 3 && valueAsString.charAt(0) == '#') {
      final List<String> parts = CodeUtils.split(valueAsString.substring(1), ':', 3);
      return parts.size() == 2 && NumberUtils.isIntegerNumber(parts.getFirst()) && NumberUtils.isIntegerNumber(parts.get(1));
    }
    return false;
  }

  private static long[] modelParse(final String value) {
    if (!value.startsWith("#"))
      throw new IllegalArgumentException("The RID '" + value + "' is not valid");
    final List<String> parts = CodeUtils.split(value.substring(1), ':', 2);
    return new long[] { Integer.parseInt(parts.getFirst()), Long.parseLong(parts.get(1)) };
  }
}
