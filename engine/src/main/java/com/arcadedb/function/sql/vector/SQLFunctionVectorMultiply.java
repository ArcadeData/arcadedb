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
 * Performs element-wise vector multiplication (Hadamard product).
 * Returns a new vector where each component is the product of corresponding components.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorMultiply extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.multiply";

  public SQLFunctionVectorMultiply() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector1 = params[0];
    final Object vector2 = params[1];

    if (vector1 == null || vector2 == null)
      throw new CommandSQLParsingException("Vectors cannot be null");

    final float[] v1 = toFloatArray(vector1);
    final float[] v2 = toFloatArray(vector2);

    if (v1.length != v2.length)
      throw new CommandSQLParsingException("Vectors must have the same dimension");

    final float[] result = new float[v1.length];
    for (int i = 0; i < v1.length; i++) {
      result[i] = v1[i] * v2[i];
    }

    return result;
  }


  public String getSyntax() {
    return NAME+"(<vector1>, <vector2>)";
  }
}
