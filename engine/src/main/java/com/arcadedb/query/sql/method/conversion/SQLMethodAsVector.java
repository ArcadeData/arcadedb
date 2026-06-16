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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Parses a value into a vector ({@code float[]}). This is the inverse of {@code asString()} for vectors and
 * accepts every format that {@code asString()} / {@code vector.toString()} can emit: bracketed or bare,
 * comma-separated (COMPACT/PYTHON/JULIA/NUMPY), space-separated (MATLAB) and multi-line (PRETTY).
 * <p>
 * Also accepts an array/list of numbers (normalized to {@code float[]}) and a single number (wrapped into a
 * one-element vector). Delegates to {@link VectorUtils#toFloatArray(Object)}.
 *
 * Example: {@code '[1.0 2.0 3.0]'.asVector()} -> {@code float[]{1.0, 2.0, 3.0}}
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsVector extends AbstractSQLMethod {

  public static final String NAME = "asvector";

  public SQLMethodAsVector() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;
    if (value instanceof float[] floats)
      return floats;
    // A single number becomes a one-element vector, consistent with how asList() wraps a single value.
    if (value instanceof Number num)
      return new float[] { num.floatValue() };

    try {
      return VectorUtils.toFloatArray(value);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(e.getMessage());
    }
  }
}
