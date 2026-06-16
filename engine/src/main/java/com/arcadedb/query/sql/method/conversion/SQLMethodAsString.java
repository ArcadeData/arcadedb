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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.List;

/**
 * Converts a value to its string representation.
 * <p>
 * Numeric vectors (float[]/double[]/int[]/long[], or an array/list of numbers) are rendered with the same
 * formats as the {@code vector.toString()} SQL function via {@link VectorUtils#formatVector}. An optional
 * format argument selects the layout: COMPACT (default), PRETTY, PYTHON, MATLAB, JULIA, NUMPY. Example:
 * {@code embedding.asString('PYTHON')}. Non-vector values fall back to their natural string form.
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsString extends AbstractSQLMethod {

  public static final String NAME = "asstring";

  public SQLMethodAsString() {
    super(NAME, 0, 1);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;
    if (value instanceof byte[] bytes)
      return new String(bytes);

    // Render numeric vectors (primitive arrays or arrays/lists of numbers) using the shared vector formatter.
    if (value instanceof float[] || value instanceof double[] || value instanceof int[] || value instanceof long[]
        || value instanceof Object[] || value instanceof List) {
      try {
        final float[] vector = VectorUtils.toFloatArray(value);
        final VectorUtils.StringFormat format = parseFormat(params);
        return VectorUtils.formatVector(vector, format);
      } catch (final IllegalArgumentException e) {
        // Not a numeric vector (e.g. a list of strings/documents): fall back to the natural string form.
      }
    }

    return value.toString();
  }

  private static VectorUtils.StringFormat parseFormat(final Object[] params) {
    if (params != null && params.length > 0 && params[0] != null)
      return VectorUtils.parseStringFormat(params[0].toString());
    return VectorUtils.StringFormat.COMPACT;
  }
}
