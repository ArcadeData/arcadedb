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
package com.arcadedb.function.sql;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.SQLFunction;

import java.util.List;
import java.util.Objects;

/**
 * Abstract class to extend to build Custom SQL Functions.
 * <p>
 * Extends the unified function system via {@link SQLFunction} which implements
 * {@link com.arcadedb.function.RecordFunction}.
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class SQLFunctionAbstract implements SQLFunction {
  protected final String name;

  public SQLFunctionAbstract(final String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * Returns the minimum number of arguments required.
   * Subclasses should override this to specify their requirements.
   *
   * @return minimum argument count (default: 0)
   */
  @Override
  public int getMinArgs() {
    return 0;
  }

  /**
   * Returns the maximum number of arguments allowed.
   * Subclasses should override this to specify their requirements.
   *
   * @return maximum argument count (default: Integer.MAX_VALUE)
   */
  @Override
  public int getMaxArgs() {
    return Integer.MAX_VALUE;
  }

  /**
   * Returns a description of the function for documentation.
   * Subclasses should override this to provide meaningful documentation.
   *
   * @return function description (default: the syntax)
   */
  @Override
  public String getDescription() {
    return getSyntax();
  }

  @Override
  public String toString() {
    return getSyntax();
  }

  @Override
  public SQLFunction config(final Object[] iConfiguredParameters) {
    return this;
  }

  /**
   * Converts various input types (float[], double[], Object[], List) to a float array.
   * Delegates to {@link com.arcadedb.index.vector.VectorUtils#toFloatArray(Object)}.
   *
   * @param vector The input vector (can be float[], double[], Object[], or List)
   *
   * @return float array representation
   *
   * @throws CommandSQLParsingException if input type is invalid or contains non-numeric elements
   */
  protected float[] toFloatArray(final Object vector) {
    try {
      return com.arcadedb.index.vector.VectorUtils.toFloatArray(vector);
    } catch (final IllegalArgumentException e) {
      throw new CommandSQLParsingException(e.getMessage());
    }
  }

}
