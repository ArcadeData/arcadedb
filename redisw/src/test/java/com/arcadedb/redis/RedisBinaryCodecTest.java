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

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct tests of the lossless bytes/String codec behind issue #9057.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisBinaryCodecTest {

  private static void roundTrip(final byte[] bytes) {
    final String s = RedisBinaryCodec.decode(bytes, bytes.length);
    assertThat(RedisBinaryCodec.encode(s)).isEqualTo(bytes);
    assertThat(RedisBinaryCodec.encodedLength(s)).isEqualTo(bytes.length);
  }

  @Test
  void everySingleByteRoundTrips() {
    for (int b = 0; b < 256; b++)
      roundTrip(new byte[] { (byte) b });
  }

  @Test
  void randomBytesRoundTrip() {
    final Random random = new Random(9057);
    for (int n = 0; n < 2000; n++) {
      final byte[] bytes = new byte[random.nextInt(64)];
      random.nextBytes(bytes);
      roundTrip(bytes);
    }
  }

  @Test
  void validAndInvalidSequencesMixed() {
    final byte[] valid = "é日😀".getBytes(StandardCharsets.UTF_8);
    final byte[] mixed = new byte[valid.length * 2 + 2];
    System.arraycopy(valid, 0, mixed, 0, valid.length);
    mixed[valid.length] = (byte) 0xff;
    mixed[valid.length + 1] = (byte) 0x80;
    System.arraycopy(valid, 0, mixed, valid.length + 2, valid.length);
    roundTrip(mixed);
    assertThat(RedisBinaryCodec.decode(valid, valid.length)).isEqualTo("é日😀");
  }

  @Test
  void sanitizeReplacesOnlyEscapedBytes() {
    final byte[] bytes = { 'a', (byte) 0xff, (byte) 0xc3, (byte) 0xa9 };
    assertThat(RedisBinaryCodec.sanitize(RedisBinaryCodec.decode(bytes, bytes.length))).isEqualTo("a�é");
    assertThat(RedisBinaryCodec.sanitize("😀 plain")).isEqualTo("😀 plain");
  }

  @Test
  void truncatedAndSurrogateSequencesRoundTrip() {
    roundTrip(new byte[] { (byte) 0xed, (byte) 0xa0, (byte) 0x80 }); // CESU-8 high surrogate
    roundTrip(new byte[] { (byte) 0xed, (byte) 0xb2, (byte) 0x80 }); // CESU-8 low surrogate
    roundTrip(new byte[] { (byte) 0xed, (byte) 0xa0, (byte) 0xbd, (byte) 0xed, (byte) 0xb8, (byte) 0x80 }); // CESU-8 pair
    roundTrip(new byte[] { 'a', (byte) 0xf0, (byte) 0x9f, (byte) 0x98 }); // truncated 4-byte sequence
    roundTrip(new byte[] { (byte) 0xc0, (byte) 0x80 }); // overlong NUL
  }

  @Test
  void aTextHoldingAnEscapeEncodesEveryOtherCharacterAsPlainUtf8() {
    // the escape-aware encoder is a second UTF-8 encoder, used only once a text holds an escaped byte: each length class of
    // UTF-8 (1, 2, 3, 4 bytes) must agree with String.getBytes at its boundaries
    final String escape = "\uDC80"; // the byte 0x80
    final String[] characters = { "\u007f", "\u0080", "\u07ff", "\u0800", "\ud7ff", "\ue000", "\uffff", "\ud83d\ude00", "\ud800\udc00",
        "\udbff\udfff" };
    for (final String character : characters) {
      final byte[] utf8 = character.getBytes(StandardCharsets.UTF_8);
      final byte[] expected = new byte[utf8.length * 2 + 1];
      System.arraycopy(utf8, 0, expected, 0, utf8.length);
      expected[utf8.length] = (byte) 0x80;
      System.arraycopy(utf8, 0, expected, utf8.length + 1, utf8.length);

      final String text = character + escape + character;
      assertThat(RedisBinaryCodec.encode(text)).as("encode of %s", character).isEqualTo(expected);
      assertThat(RedisBinaryCodec.encodedLength(text)).as("length of %s", character).isEqualTo(expected.length);
    }
  }

  @Test
  void anUnpairedSurrogateThatIsNotAnEscapeEncodesAsAQuestionMarkLikeGetBytes() {
    // a lone high surrogate, and a lone low surrogate below the escape range, next to an escaped byte
    final String text = "\ud800a\uDC80\udc00";
    final byte[] expected = { '?', 'a', (byte) 0x80, '?' };
    assertThat(RedisBinaryCodec.encode(text)).isEqualTo(expected);
    assertThat(RedisBinaryCodec.encodedLength(text)).isEqualTo(expected.length);
    assertThat("\ud800a\udc00".getBytes(StandardCharsets.UTF_8)).isEqualTo(new byte[] { '?', 'a', '?' });
  }

  @Test
  void encodeAsciiWritesAtTheOffsetAndRefusesAnythingElse() {
    final byte[] out = new byte[6];
    assertThat(RedisBinaryCodec.encodeAscii("abc", out, 2)).isEqualTo(3);
    assertThat(out).isEqualTo(new byte[] { 0, 0, 'a', 'b', 'c', 0 });
    assertThat(RedisBinaryCodec.encodeAscii("", out, 0)).isZero();
    assertThat(RedisBinaryCodec.encodeAscii("\u007f", out, 0)).isEqualTo(1);
    // 0x80 is the first byte that is not ASCII, and an escaped byte is above ASCII too
    assertThat(RedisBinaryCodec.encodeAscii("a\u0080", out, 0)).isEqualTo(-1);
    assertThat(RedisBinaryCodec.encodeAscii("a\uDC80", out, 0)).isEqualTo(-1);
    assertThat(RedisBinaryCodec.encodeAscii("\u00e9", out, 0)).isEqualTo(-1);
  }

  @Test
  void decodeOfAPrefixOfTheArrayIgnoresTheRest() {
    // the length argument bounds the read: the bytes after it are not part of the value, ASCII or not
    final byte[] bytes = { 'a', 'b', (byte) 0xc3, (byte) 0xa9, 'z' };
    assertThat(RedisBinaryCodec.decode(bytes, 2)).isEqualTo("ab");
    assertThat(RedisBinaryCodec.decode(bytes, 4)).isEqualTo("abé");
    assertThat(RedisBinaryCodec.decode(bytes, 0)).isEmpty();
    // the first non-ASCII byte is exactly 0x80
    assertThat(RedisBinaryCodec.decode(new byte[] { 'a', (byte) 0x80 }, 2)).isEqualTo("a\uDC80");
    assertThat(RedisBinaryCodec.decode(new byte[] { 'a', 0x7f }, 2)).isEqualTo("a\u007f");
  }
}
