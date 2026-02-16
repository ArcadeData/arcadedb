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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Quantizes a float vector to int8 (byte) representation using min-max scaling.
 * Reduces memory usage from 4 bytes/value to 1 byte/value (4x compression).
 *
 * Algorithm:
 * 1. Find min and max values in vector
 * 2. Scale to [-128, 127] range using: quantized = round((value - min) / (max - min) * 255 - 128)
 * 3. Return byte array
 *
 * Returns both the quantized bytes and metadata for dequantization:
 * - Result is a map with keys "quantized" (byte[]), "min" (float), "max" (float)
 *
 * Example: vectorQuantizeInt8([0.1, 0.5, 0.9]) → {quantized: [...], min: 0.1, max: 0.9}
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorQuantizeInt8 extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.quantizeInt8";

  public SQLFunctionVectorQuantizeInt8() {
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

    // Find min and max
    float min = vector[0];
    float max = vector[0];
    for (final float value : vector) {
      if (value < min)
        min = value;
      if (value > max)
        max = value;
    }

    // Quantize to int8 [-128, 127]
    final byte[] quantized = new byte[vector.length];

    if (min == max) {
      // All values are the same, map to 0
      for (int i = 0; i < vector.length; i++) {
        quantized[i] = 0;
      }
    } else {
      final float range = max - min;
      for (int i = 0; i < vector.length; i++) {
        // Scale to [0, 255] then shift to [-128, 127]
        final float normalized = (vector[i] - min) / range; // [0, 1]
        final int scaled = Math.round(normalized * 255.0f); // [0, 255]
        final byte shifted = (byte) (scaled - 128); // Shift to [-128, 127]
        quantized[i] = shifted;
      }
    }

    // Return result with metadata for dequantization
    final QuantizationResult result = new QuantizationResult(quantized, min, max);
    return result;
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }

  /**
     * Result object containing quantized bytes and metadata.
     */
    public record QuantizationResult(byte[] quantized, float min, float max) {
  }
}
