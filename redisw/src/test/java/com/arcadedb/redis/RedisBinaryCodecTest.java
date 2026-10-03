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
}
