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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Converts a vector to a human-readable string representation.
 * Supports different formats for different use cases (see {@link VectorUtils.StringFormat}):
 * COMPACT (default), PRETTY, PYTHON, MATLAB, JULIA, NUMPY.
 * <p>
 * Equivalent to the {@code asString()} SQL method on a vector value (e.g. {@code embedding.asString('PYTHON')});
 * both share {@link VectorUtils#formatVector(float[], VectorUtils.StringFormat)}.
 *
 * Example: vectorToString([0.5, 0.25, 0.75]) = "[0.5, 0.25, 0.75]"
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorToString extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.toString";

  public SQLFunctionVectorToString() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || (params.length != 1 && params.length != 2))
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    if (vectorObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Parse format (default: COMPACT)
    VectorUtils.StringFormat format = VectorUtils.StringFormat.COMPACT;
    if (params.length == 2 && params[1] != null) {
      if (!(params[1] instanceof String formatStr))
        throw new CommandSQLParsingException("Format must be a string, found: " + params[1].getClass().getSimpleName());
      try {
        format = VectorUtils.parseStringFormat(formatStr);
      } catch (final IllegalArgumentException e) {
        throw new CommandSQLParsingException(e.getMessage());
      }
    }

    return VectorUtils.formatVector(vector, format);
  }

  public String getSyntax() {
    return NAME + "(<vector> [, <format>])";
  }
}
