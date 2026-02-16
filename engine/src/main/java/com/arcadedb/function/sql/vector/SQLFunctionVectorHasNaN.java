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
import java.util.List;

/**
 * Checks if a vector contains any NaN (Not a Number) values.
 * NaN values often result from invalid operations (0/0, sqrt(-1), etc.).
 *
 * Example: vectorHasNaN([1.0, 2.0, 3.0]) = false
 *          vectorHasNaN([1.0, NaN, 3.0]) = true
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorHasNaN extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.hasNaN";

  public SQLFunctionVectorHasNaN() {
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

    // Check for NaN values
    for (final float value : vector) {
      if (Float.isNaN(value)) {
        return true;
      }
    }

    return false;
  }


  public String getSyntax() {
    return NAME + "(<vector>)";
  }
}
