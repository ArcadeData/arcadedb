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
 * Converts a vector to a human-readable string representation.
 * Supports different formats for different use cases.
 *
 * Formats:
 * - 'COMPACT': Single line "[1.0, 2.0, 3.0]" (default)
 * - 'PRETTY': Multi-line with formatting
 * - 'PYTHON': Python list format
 * - 'MATLAB': MATLAB format
 *
 * Example: vectorToString([0.5, 0.25, 0.75]) = "[0.5, 0.25, 0.75]"
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLFunctionVectorToString extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorToString";

  public enum Format {
    COMPACT,
    PRETTY,
    PYTHON,
    MATLAB
  }

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
    Format format = Format.COMPACT;
    if (params.length == 2 && params[1] != null) {
      if (params[1] instanceof String formatStr) {
        try {
          format = Format.valueOf(formatStr.toUpperCase());
        } catch (final IllegalArgumentException e) {
          throw new CommandSQLParsingException("Unknown format: " + formatStr + ". Supported: COMPACT, PRETTY, PYTHON, MATLAB");
        }
      } else {
        throw new CommandSQLParsingException("Format must be a string, found: " + params[1].getClass().getSimpleName());
      }
    }

    return switch (format) {
      case COMPACT -> formatCompact(vector);
      case PRETTY -> formatPretty(vector);
      case PYTHON -> formatPython(vector);
      case MATLAB -> formatMatlab(vector);
    };
  }

  private String formatCompact(final float[] vector) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(vector[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  private String formatPretty(final float[] vector) {
    final StringBuilder sb = new StringBuilder("[\n");
    for (int i = 0; i < vector.length; i++) {
      sb.append("  ").append(vector[i]);
      if (i < vector.length - 1)
        sb.append(",");
      sb.append("\n");
    }
    sb.append("]");
    return sb.toString();
  }

  private String formatPython(final float[] vector) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append(vector[i]);
    }
    sb.append("]");
    return sb.toString();
  }

  private String formatMatlab(final float[] vector) {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0)
        sb.append(" ");
      sb.append(vector[i]);
    }
    sb.append("]");
    return sb.toString();
  }


  public String getSyntax() {
    return NAME + "(<vector> [, <format>])";
  }
}
