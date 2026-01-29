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
import java.util.ArrayList;
import java.util.List;

/**
 * Converts a dense vector to sparse representation, filtering out values below threshold.
 * Optionally takes a threshold parameter (default 0.0, includes only non-zero values).
 *
 * Signatures:
 * - denseVectorToSparse(vector) - filters exact zeros
 * - denseVectorToSparse(vector, threshold) - filters values with |value| <= threshold
 *
 * Example: denseVectorToSparse([0.5, 0.0, 0.3], 0.0)
 * → SparseVector with indices=[0, 2], values=[0.5, 0.3]
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionDenseVectorToSparse extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.denseToSparse";

  public SQLFunctionDenseVectorToSparse() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1 || params.length > 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];

    if (vectorObj == null)
      return null;

    final float[] dense = toFloatArray(vectorObj);
    float threshold = 0.0f;

    if (params.length == 2) {
      final Object threshObj = params[1];
      if (threshObj != null) {
        if (threshObj instanceof Number num) {
          threshold = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Threshold must be a number, found: " + threshObj.getClass().getSimpleName());
        }
      }
    }

    // Collect non-zero or above-threshold values
    final List<Integer> indices = new ArrayList<>();
    final List<Float> values = new ArrayList<>();

    for (int i = 0; i < dense.length; i++) {
      final float value = dense[i];
      if (Math.abs(value) > threshold) {
        indices.add(i);
        values.add(value);
      }
    }

    // Convert lists to arrays
    final int[] indicesArray = new int[indices.size()];
    final float[] valuesArray = new float[values.size()];
    for (int i = 0; i < indices.size(); i++) {
      indicesArray[i] = indices.get(i);
      valuesArray[i] = values.get(i);
    }

    return new SparseVector(indicesArray, valuesArray, dense.length);
  }


  public String getSyntax() {
    return NAME + "(<vector> [, <threshold>])";
  }
}
