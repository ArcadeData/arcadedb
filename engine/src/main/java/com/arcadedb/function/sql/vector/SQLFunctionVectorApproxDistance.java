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
import com.arcadedb.function.sql.vector.SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult;
import com.arcadedb.function.sql.vector.SQLFunctionVectorQuantizeInt8.QuantizationResult;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Locale;

/**
 * Calculates approximate distance between quantized vectors without full dequantization.
 * Supports two modes:
 *
 * 1. INT8 Mode: Computes L2 distance directly on int8 bytes
 *    - Much faster than dequantizing and computing on floats
 *    - Result is approximate but preserves the top-k ordering of the true distances
 *
 * 2. BINARY Mode: Computes Hamming distance on binary quantized vectors
 *    - Very fast (count differing bits)
 *    - 1 operation per 8 dimensions
 *    - Returns normalized Hamming distance [0, 1]
 *
 * Signatures:
 * - vectorApproxDistance(result1, result2)  - the quantization type is inferred from the result objects
 *                                             returned by vectorQuantizeInt8()/vectorQuantizeBinary()
 * - vectorApproxDistance(quantized1, quantized2, 'INT8' | 'BINARY')
 *
 * INT8 accepts a QuantizationResult or a raw int8 byte array. BINARY requires the BinaryQuantizationResult
 * returned by vectorQuantizeBinary() (the packed bits alone are not enough - the Hamming distance needs the
 * original length carried by the result object).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorApproxDistance extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.approxDistance";

  public enum QuantizationType {
    INT8,
    BINARY
  }

  public SQLFunctionVectorApproxDistance() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || (params.length != 2 && params.length != 3))
      throw new CommandSQLParsingException(getSyntax());

    final Object q1Obj = params[0];
    final Object q2Obj = params[1];

    if (q1Obj == null || q2Obj == null)
      return null;

    final QuantizationType type;
    if (params.length == 3) {
      // Explicit type string (works with raw byte arrays as well as result objects).
      final Object typeObj = params[2];
      if (typeObj == null)
        return null;
      if (!(typeObj instanceof String str))
        throw new CommandSQLParsingException("Type must be a string, found: " + typeObj.getClass().getSimpleName());
      try {
        type = QuantizationType.valueOf(str.toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException e) {
        throw new CommandSQLParsingException("Unknown quantization type: " + str + ". Supported: INT8, BINARY");
      }
    } else {
      // Infer the quantization type from the result objects produced by vector.quantizeInt8/quantizeBinary.
      type = inferType(q1Obj, q2Obj);
    }

    return switch (type) {
      case INT8 -> computeInt8Distance(q1Obj, q2Obj);
      case BINARY -> computeBinaryDistance(q1Obj, q2Obj);
    };
  }

  private static QuantizationType inferType(final Object q1Obj, final Object q2Obj) {
    final boolean b1 = q1Obj instanceof BinaryQuantizationResult;
    final boolean b2 = q2Obj instanceof BinaryQuantizationResult;
    final boolean i1 = q1Obj instanceof QuantizationResult;
    final boolean i2 = q2Obj instanceof QuantizationResult;

    final boolean q1Recognized = b1 || i1;
    final boolean q2Recognized = b2 || i2;

    // The two-argument form infers the type from result objects: each argument must be a recognized result
    // object (no mixing a result object with a raw array, where inference would be unreliable).
    if (!q1Recognized || !q2Recognized)
      throw new CommandSQLParsingException(
          """
          Cannot infer the quantization type: the two-argument form requires the result objects of \
          vector.quantizeInt8()/vector.quantizeBinary() for both arguments. For raw byte arrays, specify \
          'INT8' / 'BINARY' as the third argument.""");
    if ((b1 || b2) && (i1 || i2))
      throw new CommandSQLParsingException(
          """
          Cannot mix INT8 and BINARY quantization results in vector.approxDistance(): both arguments must be \
          the same quantization type.""");
    return b1 || b2 ? QuantizationType.BINARY : QuantizationType.INT8;
  }

  /**
   * Compute L2 distance on int8 quantized vectors.
   */
  private float computeInt8Distance(final Object q1Obj, final Object q2Obj) {
    final byte[] q1 = toByteArray(q1Obj);
    final byte[] q2 = toByteArray(q2Obj);

    if (q1.length != q2.length)
      throw new CommandSQLParsingException("Quantized vectors must have same length: " + q1.length + " vs " + q2.length);

    // Compute L2 distance: sqrt(sum((q1[i] - q2[i])^2))
    double sumSquares = 0.0;
    for (int i = 0; i < q1.length; i++) {
      final int diff = (q1[i] & 0xFF) - (q2[i] & 0xFF);
      sumSquares += diff * diff;
    }

    return (float) Math.sqrt(sumSquares);
  }

  /**
   * Compute normalized Hamming distance on binary quantized vectors.
   */
  private float computeBinaryDistance(final Object q1Obj, final Object q2Obj) {
    if (!(q1Obj instanceof SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult bq1))
      throw new CommandSQLParsingException("Expected BinaryQuantizationResult, found: " + q1Obj.getClass().getSimpleName());

    if (!(q2Obj instanceof SQLFunctionVectorQuantizeBinary.BinaryQuantizationResult bq2))
      throw new CommandSQLParsingException("Expected BinaryQuantizationResult, found: " + q2Obj.getClass().getSimpleName());

    // Get Hamming distance
    final int hammingDistance = bq1.hammingDistance(bq2);

    // Normalize to [0, 1]: divide by max possible distance (original length)
    return (float) hammingDistance / bq1.originalLength();
  }

  private byte[] toByteArray(final Object quantized) {
    if (quantized instanceof QuantizationResult qr) {
      return qr.quantized();
    } else if (quantized instanceof byte[] byteArray) {
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
    } else {
      // Also reached when a BinaryQuantizationResult is passed to the INT8 path (mixed types in the
      // explicit 3-arg form), hence the explicit hint about the expected INT8 inputs.
      throw new CommandSQLParsingException(
          "INT8 distance expects a vector.quantizeInt8() result or a byte array, found: " + quantized.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<quantized1>, <quantized2> [, <type>])";
  }
}
