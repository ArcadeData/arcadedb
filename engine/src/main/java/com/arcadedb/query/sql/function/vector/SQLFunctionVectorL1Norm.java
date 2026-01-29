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
import java.util.List;

/**
 * Calculates the L1 norm (Manhattan norm) of a vector.
 * Also known as taxicab norm or sum of absolute values.
 *
 * Formula: L1 = Σ|x_i|
 *
 * Example: vectorL1Norm([3, 4]) = 7
 *          vectorL1Norm([-1, -2, 3]) = 6
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorL1Norm extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.l1Norm";

  public SQLFunctionVectorL1Norm() {
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

    // Calculate L1 norm: sum of absolute values
    float norm = 0.0f;
    for (final float value : vector) {
      norm += Math.abs(value);
    }

    return norm;
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }
}
