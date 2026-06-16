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
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Reconstructs an approximate float vector from a binary-quantized result produced by
 * {@code vector.quantizeBinary()}. Binary quantization stores only one bit per dimension (whether the
 * original value was >= the median), so the inverse is necessarily lossy: it can only recover the sign
 * relative to the median. Each bit is mapped to {@code highValue} (default {@code 1.0}) when set and
 * {@code lowValue} (default {@code -1.0}) when clear; the {@code ±1} default is the convention used by
 * binary embeddings with dot-product / Hamming scoring.
 * <p>
 * Accepts the {@link BinaryQuantizationResult} object returned by {@code vector.quantizeBinary()}
 * directly, so {@code vectorDequantizeBinary(vectorQuantizeBinary([...]))} works without unpacking.
 *
 * Example: vectorDequantizeBinary(vectorQuantizeBinary([0.1, 0.5, 0.9])) -> [-1.0, 1.0, 1.0]
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorDequantizeBinary extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.dequantizeBinary";

  public SQLFunctionVectorDequantizeBinary() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1 || params.length > 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object quantizedObj = params[0];
    if (quantizedObj == null)
      return null;

    if (!(quantizedObj instanceof BinaryQuantizationResult quantized))
      throw new CommandSQLParsingException(
          "Quantized vector must be the result of vector.quantizeBinary(), found: " + quantizedObj.getClass().getSimpleName());

    final float lowValue = params.length >= 2 && params[1] != null ? toFloat(params[1], "lowValue") : -1.0f;
    final float highValue = params.length == 3 && params[2] != null ? toFloat(params[2], "highValue") : 1.0f;

    final byte[] packed = quantized.packed();
    final int length = quantized.originalLength();
    final float[] result = new float[length];
    for (int i = 0; i < length; i++) {
      final boolean bitSet = (packed[i / 8] & (1 << (i % 8))) != 0;
      result[i] = bitSet ? highValue : lowValue;
    }
    return result;
  }

  private static float toFloat(final Object value, final String name) {
    if (value instanceof Number num)
      return num.floatValue();
    throw new CommandSQLParsingException(name + " must be a number, found: " + value.getClass().getSimpleName());
  }

  public String getSyntax() {
    return NAME + "(<quantized> [, <lowValue>, <highValue>])";
  }
}
