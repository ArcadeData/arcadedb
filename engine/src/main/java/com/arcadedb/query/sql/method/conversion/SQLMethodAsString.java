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
 * A vector (a {@code float[]}) is rendered with the same formats as the {@code vector.toString()} SQL function via
 * {@link VectorUtils#formatVector}, and so is any array or list of numbers when a format argument asks for one (a list of
 * {@link Float} without a format takes its natural form, which is the same text as the COMPACT layout). An optional format
 * argument selects the layout: COMPACT (default), PRETTY, PYTHON, MATLAB, JULIA, NUMPY. Example:
 * {@code embedding.asString('PYTHON')}. Every other value, a list of integers, longs or doubles included, falls back to its
 * natural string form: the vector formatter converts to {@code float}, which would change the numbers (issue #12834).
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

    // Render vectors using the shared vector formatter, which holds floats: only a float[] or a caller that asked for a vector
    // format get there. A list of integers, longs or doubles has no float form that is the same numbers, and 1234567890123456
    // must not come back as 1.234568E15 (issue #12834). Without a format, the natural form below of a list or array of Floats
    // is byte-identical to the COMPACT layout, so it needs no special case; a float[] keeps the formatter, which renders it
    // without boxing every element.
    if (value instanceof float[] || (asksForVectorFormat(params) && isArrayOrList(value))) {
      try {
        final float[] vector = VectorUtils.toFloatArray(value);
        final VectorUtils.StringFormat format = parseFormat(params);
        return VectorUtils.formatVector(vector, format);
      } catch (final IllegalArgumentException e) {
        // Not a numeric vector (e.g. a list of strings/documents): fall back to the natural string form.
      }
    }

    if (value.getClass().isArray())
      // A NON-NUMERIC ARRAY HAS NO NATURAL STRING FORM OF ITS OWN: Object.toString() IS THE JVM IDENTITY
      // ("[Ljava.lang.String;@7a8fa663"), WHICH USED TO REACH THE CLIENT AS DATA (ISSUE #7027). RENDER IT AS THE
      // LIST OF ITS ELEMENTS, EXACTLY AS THE SAME VALUES IN A List WOULD BE. NEVER NULL FOR AN ARRAY: EVERY ARRAY IS
      // A MultiValue, SO THE HELPER ALWAYS ANSWERS A LIST HERE.
      return listReceiverOrNull(value).toString();

    return value.toString();
  }

  private static boolean asksForVectorFormat(final Object[] params) {
    return params != null && params.length > 0 && params[0] != null;
  }

  private static boolean isArrayOrList(final Object value) {
    return value instanceof double[] || value instanceof int[] || value instanceof long[] || value instanceof Object[]
        || value instanceof List;
  }

  private static VectorUtils.StringFormat parseFormat(final Object[] params) {
    if (params != null && params.length > 0 && params[0] != null)
      return VectorUtils.parseStringFormat(params[0].toString());
    return VectorUtils.StringFormat.COMPACT;
  }
}
