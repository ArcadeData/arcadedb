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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

/**
 * Abstract base class for vector SQL functions providing common utility methods.
 * All vector functions should extend this class to reuse conversion and validation logic.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public abstract class SQLFunctionVectorAbstract extends SQLFunctionAbstract {

  protected SQLFunctionVectorAbstract(final String name) {
    super(name);
  }

  /**
   * Converts various input types (float[], Object[], List) to a float array.
   * Handles type conversion and validation with helpful error messages.
   *
   * @param vector The input vector (can be float[], Object[], or List)
   * @return float array representation
   * @throws CommandSQLParsingException if input type is invalid or contains non-numeric elements
   */
  protected float[] toFloatArray(final Object vector) {
    if (vector instanceof float[] floatArray) {
      return floatArray;
    } else if (vector instanceof Object[] objArray) {
      final float[] result = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++) {
        if (objArray[i] instanceof Number num) {
          result[i] = num.floatValue();
        } else {
          throw new CommandSQLParsingException("Vector elements must be numbers, found: " + objArray[i].getClass().getSimpleName());
        }
      }
      return result;
    } else if (vector instanceof java.util.List<?> list) {
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
    } else {
      throw new CommandSQLParsingException("Vector must be an array or list, found: " + vector.getClass().getSimpleName());
    }
  }

  /**
   * Validates that two vectors have the same dimension.
   *
   * @param v1 First vector
   * @param v2 Second vector
   * @throws CommandSQLParsingException if dimensions don't match
   */
  protected void validateSameDimension(final float[] v1, final float[] v2) {
    if (v1.length != v2.length) {
      throw new CommandSQLParsingException("Vectors must have the same dimension, found: " + v1.length + " and " + v2.length);
    }
  }

  /**
   * Validates that a vector is not empty.
   *
   * @param vector Vector to validate
   * @throws CommandSQLParsingException if vector is empty
   */
  protected void validateNotEmpty(final float[] vector) {
    if (vector.length == 0) {
      throw new CommandSQLParsingException("Vector cannot be empty");
    }
  }

  /**
   * Validates that a parameter is not null.
   *
   * @param param Parameter to validate
   * @param paramName Parameter name for error message
   * @throws CommandSQLParsingException if parameter is null
   */
  protected void validateNotNull(final Object param, final String paramName) {
    if (param == null) {
      throw new CommandSQLParsingException(paramName + " cannot be null");
    }
  }

  /**
   * Validates that the number of parameters matches expected count.
   *
   * @param params Actual parameters
   * @param expectedCount Expected parameter count
   * @throws CommandSQLParsingException if count doesn't match
   */
  protected void validateParameterCount(final Object[] params, final int expectedCount) {
    if (params == null || params.length != expectedCount) {
      throw new CommandSQLParsingException(getSyntax());
    }
  }

  /**
   * Validates that the number of parameters is within a range.
   *
   * @param params Actual parameters
   * @param minCount Minimum parameter count
   * @param maxCount Maximum parameter count
   * @throws CommandSQLParsingException if count is out of range
   */
  protected void validateParameterCount(final Object[] params, final int minCount, final int maxCount) {
    if (params == null || params.length < minCount || params.length > maxCount) {
      throw new CommandSQLParsingException(getSyntax());
    }
  }

  /**
   * Extracts a float scalar from a parameter.
   *
   * @param param Parameter to extract from
   * @param paramName Parameter name for error message
   * @return float value
   * @throws CommandSQLParsingException if parameter is not a number
   */
  protected float toFloatScalar(final Object param, final String paramName) {
    if (param instanceof Number num) {
      return num.floatValue();
    } else {
      throw new CommandSQLParsingException(paramName + " must be a number, found: " + param.getClass().getSimpleName());
    }
  }

  /**
   * Extracts an integer scalar from a parameter.
   *
   * @param param Parameter to extract from
   * @param paramName Parameter name for error message
   * @return int value
   * @throws CommandSQLParsingException if parameter is not a number
   */
  protected int toIntScalar(final Object param, final String paramName) {
    if (param instanceof Number num) {
      return num.intValue();
    } else {
      throw new CommandSQLParsingException(paramName + " must be a number, found: " + param.getClass().getSimpleName());
    }
  }
}
