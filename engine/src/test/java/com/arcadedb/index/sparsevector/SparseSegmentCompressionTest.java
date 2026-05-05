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
package com.arcadedb.index.sparsevector;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 codec verification: VarInt is lossless for any 64-bit value, int8 quantization stays
 * within the documented error bound, and fp16 conversion roundtrips finite values close enough.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SparseSegmentCompressionTest {

  @Test
  void varIntUnsignedRoundtripsBoundaryValues() {
    final long[] cases = {
        0L, 1L, 127L, 128L, 16383L, 16384L,
        (1L << 21) - 1, (1L << 21), (1L << 28) - 1, (1L << 28),
        Integer.MAX_VALUE, ((long) Integer.MAX_VALUE) + 1L,
        (1L << 56) - 1, (1L << 63) - 1
    };
    final ByteBuffer buf = ByteBuffer.allocate(128);
    for (final long v : cases) {
      buf.clear();
      VarInt.writeUnsignedVarLong(buf, v);
      buf.flip();
      assertThat(VarInt.readUnsignedVarLong(buf)).isEqualTo(v);
    }
  }

  @Test
  void varIntSignedRoundtripsZigzag() {
    final long[] cases = { 0L, 1L, -1L, 2L, -2L, 1000L, -1000L, Long.MAX_VALUE, Long.MIN_VALUE + 1 };
    final ByteBuffer buf = ByteBuffer.allocate(128);
    for (final long v : cases) {
      buf.clear();
      VarInt.writeSignedVarLong(buf, v);
      buf.flip();
      assertThat(VarInt.readSignedVarLong(buf)).isEqualTo(v);
    }
  }

  @Test
  void varIntFuzzRoundtrip() {
    final Random rnd = new Random(0xF1A1L);
    final ByteBuffer buf = ByteBuffer.allocate(128);
    for (int i = 0; i < 5000; i++) {
      final long v = rnd.nextLong() & 0x7FFFFFFFFFFFFFFFL; // unsigned-safe
      buf.clear();
      VarInt.writeUnsignedVarLong(buf, v);
      buf.flip();
      assertThat(VarInt.readUnsignedVarLong(buf)).isEqualTo(v);
    }
  }

  @Test
  void int8QuantizationStaysWithinErrorBound() {
    final float[][] ranges = { { 0.0f, 1.0f }, { 0.5f, 0.7f }, { 0.0f, 0.001f }, { 100.0f, 200.0f } };
    final Random rnd = new Random(0xAB1EL);
    for (final float[] range : ranges) {
      final float min = range[0];
      final float max = range[1];
      final float bound = WeightCodec.maxQuantizationError(min, max);
      for (int i = 0; i < 1000; i++) {
        final float w = min + rnd.nextFloat() * (max - min);
        final byte b = WeightCodec.quantizeInt8(w, min, max);
        // Sentinel byte must never be emitted by quantization.
        assertThat(b).isNotEqualTo(SegmentFormat.INT8_TOMBSTONE_SENTINEL);
        final float decoded = WeightCodec.dequantizeInt8(b, min, max);
        assertThat(Math.abs(decoded - w)).isLessThanOrEqualTo(bound);
      }
    }
  }

  @Test
  void int8QuantizationDegenerateRange() {
    // min == max: every weight maps to min on decode.
    final byte b = WeightCodec.quantizeInt8(0.5f, 0.5f, 0.5f);
    assertThat(WeightCodec.dequantizeInt8(b, 0.5f, 0.5f)).isEqualTo(0.5f);
  }

  @Test
  void int8EndpointsAreExactlyRepresentable() {
    final float min = 0.1f;
    final float max = 0.9f;
    final byte bMin = WeightCodec.quantizeInt8(min, min, max);
    final byte bMax = WeightCodec.quantizeInt8(max, min, max);
    assertThat(WeightCodec.dequantizeInt8(bMin, min, max)).isEqualTo(min);
    assertThat(WeightCodec.dequantizeInt8(bMax, min, max)).isEqualTo(max);
  }

  @Test
  void int8SkipsTombstoneSentinel() {
    // Sentinel byte 0x80 must not be produced by valid quantization regardless of input value.
    for (int i = 0; i < 100_000; i++) {
      final float w = (float) i / 100_000.0f;
      final byte b = WeightCodec.quantizeInt8(w, 0.0f, 1.0f);
      assertThat(b).isNotEqualTo(SegmentFormat.INT8_TOMBSTONE_SENTINEL);
    }
  }

  @Test
  void fp16RoundtripsFiniteValues() {
    // fp16 has ~3 decimal digits of precision; check several common magnitudes.
    final float[] cases = { 0.0f, 1.0f, -1.0f, 0.5f, 65504.0f, -65504.0f, 1e-4f, 1e4f };
    for (final float v : cases) {
      final short s = WeightCodec.toFp16(v);
      assertThat(s).isNotEqualTo(SegmentFormat.FP16_TOMBSTONE_SENTINEL);
      final float back = WeightCodec.fromFp16(s);
      if (v == 0.0f) {
        assertThat(back).isEqualTo(0.0f);
      } else {
        // fp16 relative error is ~2^-10 = ~1e-3.
        final float relError = Math.abs(back - v) / Math.abs(v);
        assertThat(relError).isLessThan(1e-3f);
      }
    }
  }

  @Test
  void fp32TombstoneRoundTrip() {
    final int bits = WeightCodec.FP32_TOMBSTONE_BITS;
    assertThat(WeightCodec.isFp32Tombstone(bits)).isTrue();
    assertThat(WeightCodec.isFp32Tombstone(Float.floatToRawIntBits(0.5f))).isFalse();
  }
}
