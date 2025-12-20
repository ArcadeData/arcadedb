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
 * Dequantizes an int8 (byte) array back to float vector using min-max scaling.
 * Reverses the quantization applied by vectorQuantizeInt8().
 *
 * Algorithm:
 * Inverse of quantization: value = ((quantized + 128) / 255) * (max - min) + min
 *
 * Signature: vectorDequantizeInt8(quantized_bytes, min, max)
 *
 * Note: Dequantized values are approximations due to precision loss during quantization.
 * Original vector cannot be perfectly recovered.
 *
 * Example: vectorDequantizeInt8(quantized_bytes, 0.1, 0.9) → [0.1, 0.5, 0.9] (approximate)
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorDequantizeInt8 extends SQLFunctionAbstract {
  public static final String NAME = "VECTOR_DEQUANTIZE_INT8";

  public SQLFunctionVectorDequantizeInt8() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object quantizedObj = params[0];
    final Object minObj = params[1];
    final Object maxObj = params[2];

    if (quantizedObj == null || minObj == null || maxObj == null)
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

    if (quantized.length == 0)
      throw new CommandSQLParsingException("Quantized vector cannot be empty");

    if (min > max)
      throw new CommandSQLParsingException("Min (" + min + ") must be <= max (" + max + ")");

    // Dequantize
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
    return "VECTOR_DEQUANTIZE_INT8(<quantized_bytes>, <min>, <max>)";
  }
}
