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

/**
 * Picks, for an array of signed longs, the smallest of {@link BitPackedCodec}, {@link LinearCodec} and
 * {@link BlockwiseLinearCodec}, and dispatches reads on the codec id stored in the first byte of the encoding.
 * <p>
 * The choice is purely by {@code encodedSize}: each candidate is sized (one pass each) and only the winner is encoded.
 * Ties go to {@link BitPackedCodec}, then {@link LinearCodec}, then {@link BlockwiseLinearCodec}. An empty array is
 * always bit-packed, because the blockwise empty form (an 8-byte header) would otherwise win over nothing.
 */
public final class NumericCodecChooser {
  private NumericCodecChooser() {
  }

  public static byte[] encodeBest(final long[] values, final int offset, final int length) {
    if (length == 0)
      return BitPackedCodec.encode(values, offset, 0);
    final int bitPacked = BitPackedCodec.encodedSize(values, offset, length);
    final int linear = LinearCodec.encodedSize(values, offset, length);
    final int blockwise = BlockwiseLinearCodec.encodedSize(values, offset, length);
    if (bitPacked <= linear && bitPacked <= blockwise)
      return BitPackedCodec.encode(values, offset, length);
    if (linear <= blockwise)
      return LinearCodec.encode(values, offset, length);
    return BlockwiseLinearCodec.encode(values, offset, length);
  }

  public static long get(final byte[] encoded, final int index) {
    return switch (codecId(encoded)) {
      case BitPackedCodec.CODEC_ID -> BitPackedCodec.get(encoded, index);
      case LinearCodec.CODEC_ID -> LinearCodec.get(encoded, index);
      default -> BlockwiseLinearCodec.get(encoded, index);
    };
  }

  public static void decode(final byte[] encoded, final long[] out) {
    switch (codecId(encoded)) {
    case BitPackedCodec.CODEC_ID -> BitPackedCodec.decode(encoded, out);
    case LinearCodec.CODEC_ID -> LinearCodec.decode(encoded, out);
    default -> BlockwiseLinearCodec.decode(encoded, out);
    }
  }

  public static int count(final byte[] encoded) {
    return switch (codecId(encoded)) {
      case BitPackedCodec.CODEC_ID -> BitPackedCodec.count(encoded);
      case LinearCodec.CODEC_ID -> LinearCodec.count(encoded);
      default -> BlockwiseLinearCodec.count(encoded);
    };
  }

  private static byte codecId(final byte[] encoded) {
    if (encoded.length == 0)
      throw new IllegalArgumentException("Unknown numeric codec id: empty encoding");
    final byte id = encoded[0];
    if (id != BitPackedCodec.CODEC_ID && id != LinearCodec.CODEC_ID && id != BlockwiseLinearCodec.CODEC_ID)
      throw new IllegalArgumentException("Unknown numeric codec id " + (id & 0xFF));
    return id;
  }
}
