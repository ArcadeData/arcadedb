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

/**
 * Calculates approximate distance between quantized vectors without full dequantization.
 * Supports two modes:
 *
 * 1. INT8 Mode: Computes L2 distance directly on int8 bytes
 *    - Much faster than dequantizing and computing on floats
 *    - Result is approximate but preserves ranking order
 *
 * 2. BINARY Mode: Computes Hamming distance on binary quantized vectors
 *    - Very fast (count differing bits)
 *    - 1 operation per 8 dimensions
 *    - Returns normalized Hamming distance [0, 1]
 *
 * Signature: vectorApproxDistance(quantized1, quantized2, 'INT8' | 'BINARY')
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorApproxDistance extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorApproxDistance";

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
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object q1Obj = params[0];
    final Object q2Obj = params[1];
    final Object typeObj = params[2];

    if (q1Obj == null || q2Obj == null || typeObj == null)
      return null;

    // Parse type
    final String typeStr;
    if (typeObj instanceof String str) {
      typeStr = str.toUpperCase();
    } else {
      throw new CommandSQLParsingException("Type must be a string, found: " + typeObj.getClass().getSimpleName());
    }

    // Parse quantization type
    QuantizationType type;
    try {
      type = QuantizationType.valueOf(typeStr);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException("Unknown quantization type: " + typeStr + ". Supported: INT8, BINARY");
    }

    return switch (type) {
      case INT8 -> computeInt8Distance(q1Obj, q2Obj);
      case BINARY -> computeBinaryDistance(q1Obj, q2Obj);
    };
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
    return (float) hammingDistance / bq1.getOriginalLength();
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
    } else {
      throw new CommandSQLParsingException("Quantized vector must be a byte array, found: " + quantized.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<quantized1>, <quantized2>, <type>)";
  }
}
