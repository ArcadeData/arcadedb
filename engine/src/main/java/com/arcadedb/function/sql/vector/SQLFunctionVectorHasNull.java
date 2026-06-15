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
 * Checks if a vector contains any NULL element.
 * <p>
 * Unlike {@code vector.hasNaN}, this distinguishes a genuine NULL (a missing element, e.g. produced by an
 * invalid SQL math op coerced to NULL inside a collection literal) from a NaN float value. Primitive
 * float[]/double[]/int[]/long[] inputs can never hold a NULL and therefore always return {@code false}.
 *
 * Example: vectorHasNull([1.0, 2.0, 3.0]) = false
 *          vectorHasNull([1.0, sqrt(-1.0), 3.0]) = true
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorHasNull extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.hasNull";

  public SQLFunctionVectorHasNull() {
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

    // Only object-typed collections can carry a NULL element; scan them directly.
    if (vectorObj instanceof List<?> list) {
      for (final Object elem : list)
        if (elem == null)
          return true;
      return false;
    }
    if (vectorObj instanceof Object[] array) {
      for (final Object elem : array)
        if (elem == null)
          return true;
      return false;
    }

    // Primitive arrays / strings cannot contain NULLs; toFloatArray validates the type and throws otherwise.
    toFloatArray(vectorObj);
    return false;
  }

  public String getSyntax() {
    return NAME + "(<vector>)";
  }
}
