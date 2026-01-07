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
package com.arcadedb.query.sql.function;

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.SQLFunction;

import java.util.List;

/**
 * Abstract class to extend to build Custom SQL Functions.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
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

  @Override
  public String toString() {
    return getSyntax();
  }

  @Override
  public SQLFunction config(final Object[] iConfiguredParameters) {
    return this;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }

  /**
   * Converts various input types (float[], Object[], List) to a float array.
   * Handles type conversion and validation with helpful error messages.
   *
   * @param vector The input vector (can be float[], Object[], or List)
   *
   * @return float array representation
   *
   * @throws CommandSQLParsingException if input type is invalid or contains non-numeric elements
   */
  protected float[] toFloatArray(final Object vector) {
    switch (vector) {
    case float[] floatArray -> {
      return floatArray;
    }
    case Object[] objArray -> {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    }
    case List<?> list -> {
      final float[] result = new float[list.size()];
      for (int i = 0; i < list.size(); i++) {
        final Object elem = list.get(i);
        if (elem instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + elem.getClass().getSimpleName());
        }
      }
      return result;
    }
    default -> throw new CommandSQLParsingException("Vector must be an array or list, found: " + vector.getClass().getSimpleName());
    }
  }
}
