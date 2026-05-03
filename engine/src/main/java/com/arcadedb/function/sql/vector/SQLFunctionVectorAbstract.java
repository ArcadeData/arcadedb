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

import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.function.sql.SQLFunctionAbstract;

import java.util.Map;

/**
 * Abstract base class for vector SQL functions providing common utility methods.
 * All vector functions should extend this class to reuse conversion and validation logic.
 * <p>
 * Functions with names starting with "vector." will automatically have an alias
 * generated for backward compatibility. For example, "vector.cosineSimilarity"
 * will also be available as "vectorCosineSimilarity".
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class SQLFunctionVectorAbstract extends SQLFunctionAbstract {
  private static final String VECTOR_PREFIX = "vector.";
  private final String alias;

  protected SQLFunctionVectorAbstract(final String name) {
    super(name);
    // Auto-generate alias for backward compatibility: vector.xxx -> vectorXxx
    if (name.startsWith(VECTOR_PREFIX)) {
      final String suffix = name.substring(VECTOR_PREFIX.length());
      this.alias = "vector" + Character.toUpperCase(suffix.charAt(0)) + suffix.substring(1);
    } else {
      this.alias = null;
    }
  }

  @Override
  public String getAlias() {
    return alias;
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

  /**
   * Reads a (possibly dotted) property path from a record and returns the leaf value.
   * <p>
   * Supports flat property names ({@code "source"}) and dotted nested paths
   * ({@code "metadata.author"}, {@code "provenance.source.id"}). Each segment after the first
   * descends one level via {@link Map#get(Object)} for {@code Map}-typed values or
   * {@link Document#get(String)} for embedded documents. A missing segment short-circuits to
   * {@code null}; a segment that lands on a non-traversable value (e.g. a primitive in the middle
   * of the path) also returns {@code null}.
   * <p>
   * Used by the {@code groupBy} option on {@code vector.neighbors}, {@code vector.sparseNeighbors},
   * and {@code vector.fuse} (issue #4072).
   *
   * @param record the record to read from; {@code null} returns {@code null}
   * @param path   the property name or dotted path; {@code null} returns {@code null}
   *
   * @return the leaf value, or {@code null} if any segment of the path resolves to {@code null}
   *         or to a non-traversable intermediate value
   */
  protected static Object readNestedField(final Document record, final String path) {
    if (record == null || path == null || path.isEmpty())
      return null;

    final int firstDot = path.indexOf('.');
    if (firstDot < 0)
      return record.get(path);

    final String[] parts = path.split("\\.");
    Object current = record.get(parts[0]);
    for (int i = 1; i < parts.length && current != null; i++) {
      if (current instanceof Document doc)
        current = doc.get(parts[i]);
      else if (current instanceof Map<?, ?> map)
        current = map.get(parts[i]);
      else
        return null;
    }
    return current;
  }
}
