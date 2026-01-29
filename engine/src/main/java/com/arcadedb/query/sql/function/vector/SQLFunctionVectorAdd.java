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
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Performs element-wise vector addition.
 * Returns a new vector where each component is the sum of corresponding components.
 * <p>
 * Uses scalar implementation which is 7-11x faster than JVector for typical vector sizes (< 1024).
 * JVector overhead from object allocation and conversion dominates actual computation cost.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorAdd extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorAdd";

  public SQLFunctionVectorAdd() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    validateParameterCount(params, 2);
    validateNotNull(params[0], "Vector1");
    validateNotNull(params[1], "Vector2");

    final float[] v1 = toFloatArray(params[0]);
    final float[] v2 = toFloatArray(params[1]);
    validateSameDimension(v1, v2);

    // Scalar implementation - significantly faster than JVector for typical sizes
    final float[] result = new float[v1.length];
    for (int i = 0; i < v1.length; i++) {
      result[i] = v1[i] + v2[i];
    }
    return result;
  }

  public String getSyntax() {
    return NAME + "(<vector1>, <vector2>)";
  }
}
