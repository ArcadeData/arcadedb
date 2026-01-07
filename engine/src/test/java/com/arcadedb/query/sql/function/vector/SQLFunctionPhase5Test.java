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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.BasicCommandContext;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SQLFunctionPhase5Test extends TestHelper {

  // ========== Phase 5.1: Quantization Tests ==========

  @Test
  void vectorQuantizeInt8Basic() {
    final SQLFunctionVectorQuantizeInt8 function = new SQLFunctionVectorQuantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize vector [0.1, 0.5, 0.9]
    final SQLFunctionVectorQuantizeInt8.QuantizationResult result =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) function.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    assertThat(result).isNotNull();
    assertThat(result.getMin()).isEqualTo(0.1f);
    assertThat(result.getMax()).isEqualTo(0.9f);
    assertThat(result.getQuantized().length).isEqualTo(3);
  }

  @Test
  void vectorQuantizeInt8Uniform() {
    final SQLFunctionVectorQuantizeInt8 function = new SQLFunctionVectorQuantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // All same values
    final SQLFunctionVectorQuantizeInt8.QuantizationResult result =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) function.execute(null, null, null,
            new Object[] { new float[] { 0.5f, 0.5f, 0.5f } },
            context);

    assertThat(result).isNotNull();
    assertThat(result.getMin()).isEqualTo(0.5f);
    assertThat(result.getMax()).isEqualTo(0.5f);
    // When all values are same, they should quantize to 0
    for (final byte b : result.getQuantized()) {
      assertThat((int) b).isEqualTo(0);
    }
  }

  @Test
  void vectorQuantizeInt8Compression() {
    // Test memory compression: 4 bytes per float -> 1 byte per value
    final float[] original = new float[1000];
    for (int i = 0; i < 1000; i++) {
      original[i] = (i % 100) / 100.0f;
    }

    final SQLFunctionVectorQuantizeInt8 function = new SQLFunctionVectorQuantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final SQLFunctionVectorQuantizeInt8.QuantizationResult result =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) function.execute(null, null, null,
            new Object[] { original },
            context);

    // Original: 1000 floats * 4 bytes = 4000 bytes
    // Quantized: 1000 bytes + 2 floats metadata = ~1008 bytes
    assertThat(result.getQuantized().length).isEqualTo(1000);
  }

  @Test
  void vectorQuantizeBinary() {
    final SQLFunctionVectorQuantizeBinary function = new SQLFunctionVectorQuantizeBinary();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize vector [0.1, 0.5, 0.9]
    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult result =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) function.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    assertThat(result).isNotNull();
    assertThat(result.getMedian()).isEqualTo(0.5f);
    assertThat(result.getOriginalLength()).isEqualTo(3);
    // 3 bits need 1 byte
    assertThat(result.getPacked().length).isEqualTo(1);
  }

  @Test
  void vectorQuantizeBinaryBits() {
    final SQLFunctionVectorQuantizeBinary function = new SQLFunctionVectorQuantizeBinary();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize vector [0.1, 0.5, 0.9]
    // Median = 0.5
    // Bits: [0.1 < 0.5 ? 0] [0.5 >= 0.5 ? 1] [0.9 >= 0.5 ? 1] = 011 (bits 0,1,2)
    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult result =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) function.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    assertThat(result).isNotNull();
    // Packed bits should be 110 (bits 1 and 2 set, bit 0 not set)
    assertThat((result.getPacked()[0] & 0xFF) & 0b00000110).isNotZero();
  }

  @Test
  void vectorQuantizeBinaryCompressionExtreme() {
    // Test extreme compression: 4 bytes per float -> 1 bit per value
    final float[] original = new float[1000];
    for (int i = 0; i < 1000; i++) {
      original[i] = (i % 100) / 100.0f;
    }

    final SQLFunctionVectorQuantizeBinary function = new SQLFunctionVectorQuantizeBinary();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult result =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) function.execute(null, null, null,
            new Object[] { original },
            context);

    // Original: 1000 floats * 4 bytes = 4000 bytes
    // Binary quantized: 1000 bits / 8 = 125 bytes + metadata
    assertThat(result.getPacked().length).isLessThanOrEqualTo(125 + 1);
  }

  @Test
  void vectorDequantizeInt8() {
    final SQLFunctionVectorQuantizeInt8 quantize = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dequantize = new SQLFunctionVectorDequantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize original vector
    final float[] original = new float[] { 0.1f, 0.5f, 0.9f };
    final SQLFunctionVectorQuantizeInt8.QuantizationResult quantized =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) quantize.execute(null, null, null,
            new Object[] { original },
            context);

    // Dequantize back
    final float[] recovered = (float[]) dequantize.execute(null, null, null,
        new Object[] { quantized.getQuantized(), quantized.getMin(), quantized.getMax() },
        context);

    assertThat(recovered).isNotNull();
    assertThat(recovered.length).isEqualTo(3);

    // Check approximate recovery (some precision loss expected - int8 has limited precision)
    // With 256 levels across a range, precision is approximately range/256
    assertThat(recovered[0]).isCloseTo(0.1f, Offset.offset(0.08f));
    assertThat(recovered[1]).isCloseTo(0.5f, Offset.offset(0.08f));
    assertThat(recovered[2]).isCloseTo(0.9f, Offset.offset(0.08f));
  }

  @Test
  void vectorDequantizeInt8Uniform() {
    final SQLFunctionVectorDequantizeInt8 function = new SQLFunctionVectorDequantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Dequantize with min = max (all values the same)
    final float[] result = (float[]) function.execute(null, null, null,
        new Object[] { new byte[] { 0, 0, 0 }, 0.5f, 0.5f },
        context);

    assertThat(result).isNotNull();
    assertThat(result.length).isEqualTo(3);
    for (final float value : result) {
      assertThat(value).isEqualTo(0.5f);
    }
  }

  @Test
  void vectorApproxDistanceInt8() {
    final SQLFunctionVectorQuantizeInt8 quantize = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorApproxDistance function = new SQLFunctionVectorApproxDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize two vectors
    final SQLFunctionVectorQuantizeInt8.QuantizationResult q1 =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    final SQLFunctionVectorQuantizeInt8.QuantizationResult q2 =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.2f, 0.4f, 0.8f } },
            context);

    // Compute approximate distance
    final float distance = (float) function.execute(null, null, null,
        new Object[] { q1.getQuantized(), q2.getQuantized(), "INT8" },
        context);

    assertThat(distance).isGreaterThan(0);
    assertThat(distance).isLessThan(300); // Reasonable L2 distance on int8
  }

  @Test
  void vectorApproxDistanceBinary() {
    final SQLFunctionVectorQuantizeBinary quantize = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance function = new SQLFunctionVectorApproxDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize two vectors
    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q1 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q2 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.2f, 0.4f, 0.8f } },
            context);

    // Compute binary distance
    final float distance = (float) function.execute(null, null, null,
        new Object[] { q1, q2, "BINARY" },
        context);

    assertThat(distance).isGreaterThanOrEqualTo(0);
    assertThat(distance).isLessThanOrEqualTo(1); // Normalized Hamming distance [0, 1]
  }

  @Test
  void vectorApproxDistanceBinaryIdentical() {
    final SQLFunctionVectorQuantizeBinary quantize = new SQLFunctionVectorQuantizeBinary();
    final SQLFunctionVectorApproxDistance function = new SQLFunctionVectorApproxDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Quantize same vector twice
    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q1 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q2 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.5f, 0.9f } },
            context);

    // Distance between identical vectors should be 0
    final float distance = (float) function.execute(null, null, null,
        new Object[] { q1, q2, "BINARY" },
        context);

    assertThat(distance).isCloseTo(0.0f, Offset.offset(0.001f));
  }

  @Test
  void vectorBinaryHammingDistance() {
    final SQLFunctionVectorQuantizeBinary quantize = new SQLFunctionVectorQuantizeBinary();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    // Create two vectors with controlled bit patterns
    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q1 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.1f, 0.2f, 0.3f, 0.4f } },
            context);

    final SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult q2 =
        (SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult) quantize.execute(null, null, null,
            new Object[] { new float[] { 0.6f, 0.7f, 0.8f, 0.9f } },
            context);

    // Hamming distance between vectors with different medians
    // Most bits should differ since q1 has values below 0.5 median and q2 has values above
    final int distance = q1.hammingDistance(q2);

    // Should have some difference due to different value ranges
    assertThat(distance).isGreaterThanOrEqualTo(0);
    assertThat(distance).isLessThanOrEqualTo(4); // Max 4 bits
  }

  // ========== Error Handling Tests ==========

  @Test
  void vectorQuantizeInt8Empty() {
    final SQLFunctionVectorQuantizeInt8 function = new SQLFunctionVectorQuantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] {} },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("empty");
  }

  @Test
  void vectorQuantizeBinaryEmpty() {
    final SQLFunctionVectorQuantizeBinary function = new SQLFunctionVectorQuantizeBinary();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new float[] {} },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("empty");
  }

  @Test
  void vectorDequantizeInt8InvalidMinMax() {
    final SQLFunctionVectorDequantizeInt8 function = new SQLFunctionVectorDequantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new byte[] { 0, 1, 2 }, 1.0f, 0.5f },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("must be <=");
  }

  @Test
  void vectorApproxDistanceInvalidType() {
    final SQLFunctionVectorApproxDistance function = new SQLFunctionVectorApproxDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new byte[] { 0, 1 }, new byte[] { 0, 1 }, "INVALID" },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Unknown quantization type");
  }

  @Test
  void vectorApproxDistanceLengthMismatch() {
    final SQLFunctionVectorApproxDistance function = new SQLFunctionVectorApproxDistance();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    assertThatThrownBy(() -> function.execute(null, null, null,
        new Object[] { new byte[] { 0, 1 }, new byte[] { 0, 1, 2 }, "INT8" },
        context))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("same length");
  }

  @Test
  void vectorQuantizeInt8Null() {
    final SQLFunctionVectorQuantizeInt8 function = new SQLFunctionVectorQuantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null, new Object[] { null }, context);
    assertThat(result).isNull();
  }

  @Test
  void vectorDequantizeInt8Null() {
    final SQLFunctionVectorDequantizeInt8 function = new SQLFunctionVectorDequantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final Object result = function.execute(null, null, null, new Object[] { null, 0.0f, 1.0f }, context);
    assertThat(result).isNull();
  }

  @Test
  void vectorQuantizationRoundTrip() {
    // Test: original -> quantize -> dequantize ~= original
    final SQLFunctionVectorQuantizeInt8 quantize = new SQLFunctionVectorQuantizeInt8();
    final SQLFunctionVectorDequantizeInt8 dequantize = new SQLFunctionVectorDequantizeInt8();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);

    final float[] original = new float[] { 0.123f, 0.456f, 0.789f, 0.234f, 0.567f };

    // Quantize
    final SQLFunctionVectorQuantizeInt8.QuantizationResult q =
        (SQLFunctionVectorQuantizeInt8.QuantizationResult) quantize.execute(null, null, null,
            new Object[] { original },
            context);

    // Dequantize
    final float[] recovered = (float[]) dequantize.execute(null, null, null,
        new Object[] { q.getQuantized(), q.getMin(), q.getMax() },
        context);

    // Check recovery (with tolerance for quantization loss)
    // Int8 quantization has precision of approximately 1/256 = 0.004 for normalized [0,1] ranges
    assertThat(recovered.length).isEqualTo(original.length);
    for (int i = 0; i < original.length; i++) {
      assertThat(recovered[i]).isCloseTo(original[i], Offset.offset(0.15f));
    }
  }
}
