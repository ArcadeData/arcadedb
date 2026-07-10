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
import com.arcadedb.function.sql.vector.SQLFunctionVectorQuantizeInt8.QuantizationResult;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.List;

/**
 * Dequantizes an int8 (byte) array back to float vector using min-max scaling.
 * Reverses the quantization applied by vectorQuantizeInt8().
 *
 * Algorithm:
 * Inverse of quantization: value = ((quantized + 128) / 255) * (max - min) + min
 *
 * Signatures:
 * - vectorDequantizeInt8(result)            - pass the result of vectorQuantizeInt8() directly (min/max
 *                                             come from the result, no need to unpack)
 * - vectorDequantizeInt8(quantized_bytes, min, max)
 *
 * A result's embedded min/max are authoritative (they were used at quantization time, so only they
 * reconstruct the original scale). When a result is passed to the 3-arg form, the explicit min/max must be
 * null or match the embedded values (the redundant call is supported); scalars that conflict are rejected,
 * since applying them would dequantize to a wrong scale.
 *
 * Note: Dequantized values are approximations due to precision loss during quantization.
 * Original vector cannot be perfectly recovered.
 *
 * Example: vectorDequantizeInt8(vectorQuantizeInt8([0.1, 0.5, 0.9])) → [0.1, 0.5, 0.9] (approximate)
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorDequantizeInt8 extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.dequantizeInt8";

  public SQLFunctionVectorDequantizeInt8() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1)
      throw new CommandSQLParsingException(getSyntax());

    // Object form: vectorDequantizeInt8(<result of vectorQuantizeInt8>) - min/max come from the result.
    if (params.length == 1) {
      final Object quantizedObj = params[0];
      if (quantizedObj == null)
        return null;
      if (!(quantizedObj instanceof QuantizationResult qr))
        throw new CommandSQLParsingException(
            "Single-argument form expects the result of vector.quantizeInt8(), found: " + quantizedObj.getClass()
                .getSimpleName() + ". Otherwise call vector.dequantizeInt8(<bytes>, <min>, <max>).");
      return dequantize(qr.quantized(), qr.min(), qr.max());
    }

    // 2-arg form is intentionally unsupported: either 1 (result object) or 3 (bytes, min, max).
    if (params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object quantizedObj = params[0];
    if (quantizedObj == null)
      return null;

    final Object minObj = params[1];
    final Object maxObj = params[2];

    // A QuantizationResult's own min/max are authoritative (computed at quantization time, so only they
    // reconstruct the original scale). Issue #3099 requires the redundant 3-arg call to succeed, so explicit
    // scalars that are null - or that match the embedded values - are accepted and the embedded values used.
    // Scalars that conflict with the embedded values would dequantize to a wrong scale and are almost
    // certainly a caller mistake, so they are rejected loudly rather than silently ignored. This runs before
    // the scalar null-guard below, otherwise (result, null, null) would wrongly short-circuit to null.
    if (quantizedObj instanceof QuantizationResult qr) {
      if (conflictsWithEmbedded(minObj, qr.min()))
        throw new CommandSQLParsingException("Explicit min (" + minObj + ") conflicts with the result's embedded min ("
            + qr.min() + "); omit min/max when passing a vector.quantizeInt8() result.");
      if (conflictsWithEmbedded(maxObj, qr.max()))
        throw new CommandSQLParsingException("Explicit max (" + maxObj + ") conflicts with the result's embedded max ("
            + qr.max() + "); omit min/max when passing a vector.quantizeInt8() result.");
      return dequantize(qr.quantized(), qr.min(), qr.max());
    }

    if (minObj == null || maxObj == null)
      return null;

    // Parse quantized bytes
    final byte[] quantized = toByteArray(quantizedObj);

    // Parse min and max
    final float min;
    if (minObj instanceof Number num1) {
      min = num1.floatValue();
    } else {
      throw new CommandSQLParsingException("Min must be a number, found: " + minObj.getClass().getSimpleName());
    }

    final float max;
    if (maxObj instanceof Number num2) {
      max = num2.floatValue();
    } else {
      throw new CommandSQLParsingException("Max must be a number, found: " + maxObj.getClass().getSimpleName());
    }

    return dequantize(quantized, min, max);
  }

  /**
   * A null explicit scalar is a no-op (the embedded value is used). A non-null scalar conflicts unless it is
   * numeric and equal to the embedded value within a small relative tolerance (absorbing float round-trip).
   */
  private static boolean conflictsWithEmbedded(final Object explicit, final float embedded) {
    if (explicit == null)
      return false;
    if (!(explicit instanceof Number num))
      return true;
    final float value = num.floatValue();
    // Relative tolerance because float rounding error scales with magnitude (it is measured in ULPs). 1e-5
    // is ~100x the float epsilon (~1.2e-7), comfortably absorbing a float->JSON-double->float round-trip of
    // the embedded value while still rejecting a min/max that genuinely differs. The Math.max(1, ...) floor
    // keeps the window from collapsing to ~0 for embedded values near zero.
    final float tolerance = 1e-5f * Math.max(1.0f, Math.max(Math.abs(value), Math.abs(embedded)));
    return Math.abs(value - embedded) > tolerance;
  }

  private float[] dequantize(final byte[] quantized, final float min, final float max) {
    if (quantized.length == 0)
      throw new CommandSQLParsingException("Quantized vector cannot be empty");

    if (min > max)
      throw new CommandSQLParsingException("Min (" + min + ") must be <= max (" + max + ")");

    final float[] result = new float[quantized.length];
    final float range = max - min;

    if (range == 0.0f) {
      // All values were the same, return min value for all
      for (int i = 0; i < quantized.length; i++) {
        result[i] = min;
      }
    } else {
      for (int i = 0; i < quantized.length; i++) {
        // Reverse quantization: value = (((quantized + 128) / 255) * range) + min
        // Convert signed byte [-128, 127] back to [0, 255] range by adding 128
        final int scaled = (int) quantized[i] + 128; // Convert to [0, 255]
        final float normalized = scaled / 255.0f; // [0, 1]
        result[i] = normalized * range + min;
      }
    }

    return result;
  }

  private byte[] toByteArray(final Object quantized) {
    if (quantized instanceof byte[] byteArray) {
      return byteArray;
    } else if (quantized instanceof Object[] objArray) {
      final byte[] result = new byte[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.byteValue();
        } else {
          throw new CommandSQLParsingException("Quantized elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (quantized instanceof List<?> list) {
      final byte[] result = new byte[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.byteValue();
        } else {
          throw new CommandSQLParsingException("Quantized elements must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    } else {
      throw new CommandSQLParsingException("Quantized vector must be an array or list, found: " + quantized.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<result>) | " + NAME + "(<quantized_bytes>, <min>, <max>)";
  }
}
