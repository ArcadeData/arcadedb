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
package com.arcadedb.engine.codec;

import org.junit.jupiter.api.Test;

import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the exact bytes the four shared codecs write: they are the time-series on-disk format, so moving them to another
 * package must not change a single byte. The expected values were recorded on the code before the move.
 */
class CodecByteCompatibilityTest {
  private static final int  DOD_LEN  = 429;
  private static final long DOD_CRC  = 0xFD2A3E50L;
  private static final int  XOR_LEN  = 782;
  private static final long XOR_CRC  = 0x39AAB033L;
  private static final int  S8B_LEN  = 2356;
  private static final long S8B_CRC  = 0x9CB915C8L;
  private static final int  S8BN_LEN = 2492;
  private static final long S8BN_CRC = 0x7B9B2BFDL;
  private static final int  DICT_LEN = 2149;
  private static final long DICT_CRC = 0x6E9A677AL;

  @Test
  void deltaOfDeltaBytesAreUnchanged() {
    final long[] t = new long[1000];
    for (int i = 0; i < t.length; i++)
      t[i] = 1_700_000_000_000L + i * 10_000L + (i % 7) * 3;
    assertEncoded(DeltaOfDeltaCodec.encode(t), DOD_LEN, DOD_CRC);
  }

  @Test
  void gorillaXorBytesAreUnchanged() {
    final double[] v = new double[1000];
    for (int i = 0; i < v.length; i++)
      v[i] = 20.0 + (i % 13) * 0.25;
    v[500] = Double.NaN;
    assertEncoded(GorillaXORCodec.encode(v), XOR_LEN, XOR_CRC);
  }

  @Test
  void simple8bBytesAreUnchanged() {
    final long[] v = new long[1000];
    for (int i = 0; i < v.length; i++)
      v[i] = (i % 2 == 0 ? i : -i) * 37L;
    assertEncoded(Simple8bCodec.encode(v), S8B_LEN, S8B_CRC);

    final long[] nulls = new long[(v.length + 63) / 64];
    nulls[0] |= 1L << 3;
    nulls[999 >>> 6] |= 1L << (999 & 63);
    assertEncoded(Simple8bCodec.encode(v, nulls), S8BN_LEN, S8BN_CRC);
  }

  @Test
  void dictionaryBytesAreUnchanged() {
    final String[] v = new String[1000];
    for (int i = 0; i < v.length; i++)
      v[i] = "host-" + (i % 17);
    assertEncoded(DictionaryCodec.encode(v), DICT_LEN, DICT_CRC);
  }

  private static void assertEncoded(final byte[] encoded, final int expectedLen, final long expectedCrc) {
    final CRC32 crc = new CRC32();
    crc.update(encoded);
    assertThat(encoded.length).as("encoded length").isEqualTo(expectedLen);
    assertThat(crc.getValue()).as("crc32 of encoded bytes").isEqualTo(expectedCrc);
  }
}
