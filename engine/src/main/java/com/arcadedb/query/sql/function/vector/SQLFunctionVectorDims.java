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

/**
 * Returns the number of dimensions in a vector.
 * Useful for validating vector dimensionality and compatibility.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorDims extends SQLFunctionAbstract {
  public static final String NAME = "vectorDims";

  public SQLFunctionVectorDims() {
    super(NAME);
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 1)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector = params[0];

    if (vector == null)
      throw new CommandSQLParsingException("Vector cannot be null");

    if (vector instanceof float[] floatArray) {
      return floatArray.length;
    } else if (vector instanceof Object[] objArray) {
      return objArray.length;
    } else if (vector instanceof java.util.List<?> list) {
      return list.size();
    } else {
      throw new CommandSQLParsingException("Vector must be an array or list, found: " + vector.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return "vectorDims(<vector>)";
  }
}
