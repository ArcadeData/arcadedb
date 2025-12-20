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
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import java.util.List;

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
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorQuantizeInt8 extends SQLFunctionAbstract {
  public static final String NAME = "vectorQuantizeInt8";

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

  private float[] toFloatArray(final Object vector) {
    if (vector instanceof float[] floatArray) {
      return floatArray;
    } else if (vector instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (vector instanceof List<?> list) {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Vector must be an array or list, found: " + vector.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<vector>)";
  }

  /**
   * Result object containing quantized bytes and metadata.
   */
  public static class QuantizationResult {
    public final byte[] quantized;
    public final float min;
    public final float max;

    public QuantizationResult(final byte[] quantized, final float min, final float max) {
      this.quantized = quantized;
      this.min = min;
      this.max = max;
    }

    public byte[] getQuantized() {
      return quantized;
    }

    public float getMin() {
      return min;
    }

    public float getMax() {
      return max;
    }
  }
}
