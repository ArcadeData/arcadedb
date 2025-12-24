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
   * Validates that two vectors have the same dimension.
   *
   * @param v1 First vector
   * @param v2 Second vector
   *
   * @throws CommandSQLParsingException if dimensions don't match
   */
  protected void validateSameDimension(final float[] v1, final float[] v2) {
    if (v1.length != v2.length) {
      throw new CommandSQLParsingException("Vectors must have the same dimension, found: " + v1.length + " and " + v2.length);
    }
  }

  /**
   * Validates that a parameter is not null.
   *
   * @param param     Parameter to validate
   * @param paramName Parameter name for error message
   *
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
   * @param params        Actual parameters
   * @param expectedCount Expected parameter count
   *
   * @throws CommandSQLParsingException if count doesn't match
   */
  protected void validateParameterCount(final Object[] params, final int expectedCount) {
    if (params == null || params.length != expectedCount) {
      throw new CommandSQLParsingException(getSyntax());
    }
  }
}
