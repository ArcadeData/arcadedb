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

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Arrays;
import java.util.List;

/**
 * Quantizes a float vector to binary (1-bit per dimension) using median threshold.
 * Reduces memory usage from 4 bytes/value to 1 bit/value (32x compression).
 * Trade-off: Extreme loss of precision, but very fast Hamming distance calculation.
 *
 * Algorithm:
 * 1. Calculate median value
 * 2. Quantize each element: bit = 1 if value >= median, else 0
 * 3. Pack bits into byte array (8 bits per byte)
 * 4. Return byte array and median for hamming distance calculation
 *
 * Returns a BinaryQuantizationResult with:
 * - packed bits as byte array
 * - median value for distance calculation
 * - original vector length for unpacking
 *
 * Example: vectorQuantizeBinary([0.1, 0.5, 0.9]) → binary representation
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorQuantizeBinary extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorQuantizeBinary";

  public SQLFunctionVectorQuantizeBinary() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 1)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];

    if (vectorObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Calculate median
    final float median = calculateMedian(vector);

    // Quantize to binary
    final int byteCount = (vector.length + 7) / 8; // Round up to nearest byte
    final byte[] packed = new byte[byteCount];

    for (int i = 0; i < vector.length; i++) {
      if (vector[i] >= median) {
        // Set bit to 1
        final int byteIndex = i / 8;
        final int bitIndex = i % 8;
        packed[byteIndex] |= (1 << bitIndex);
      }
    }

    return new BinaryQuantizationResult(packed, median, vector.length);
  }

  /**
   * Calculate median of array.
   */
  private float calculateMedian(final float[] values) {
    final float[] sorted = values.clone();
    Arrays.sort(sorted);
    if (sorted.length % 2 == 0) {
      return (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0f;
    } else {
      return sorted[sorted.length / 2];
    }
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }

  /**
   * Result of binary quantization containing packed bits and metadata.
   */
  public static class BinaryQuantizationResult {
    public final byte[] packed;
    public final float median;
    public final int originalLength;

    public BinaryQuantizationResult(final byte[] packed, final float median, final int originalLength) {
      this.packed = packed;
      this.median = median;
      this.originalLength = originalLength;
    }

    public byte[] getPacked() {
      return packed;
    }

    public float getMedian() {
      return median;
    }

    public int getOriginalLength() {
      return originalLength;
    }

    /**
     * Calculate Hamming distance to another binary quantized vector.
     */
    public int hammingDistance(final BinaryQuantizationResult other) {
      if (this.originalLength != other.originalLength)
        throw new IllegalArgumentException("Vectors must have same length");

      int distance = 0;
      // Count differing bits
      for (int i = 0; i < Math.min(packed.length, other.packed.length); i++) {
        // XOR gives 1 for differing bits, count the 1s
        distance += Integer.bitCount(packed[i] ^ other.packed[i]);
      }

      return distance;
    }
  }
}
