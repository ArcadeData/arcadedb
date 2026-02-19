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
package com.arcadedb.query;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

@ExcludeFromJacocoGeneratedReport
public interface QueryEngine {
  interface AnalyzedQuery {
    boolean isIdempotent();

    boolean isDDL();

    /**
     * Returns the set of operation types this query performs.
     * Provides semantic, parser-based classification of query operations
     * for fine-grained permission checking.
     *
     * @return a non-empty set of {@link OperationType} values
     */
    default Set<OperationType> getOperationTypes() {
      if (isDDL())
        return CollectionUtils.singletonSet(OperationType.SCHEMA);
      if (isIdempotent())
        return CollectionUtils.singletonSet(OperationType.READ);
      // Fallback: non-idempotent, non-DDL commands that don't override this method
      return Set.of(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
    }

    /**
     * Executes this analyzed query, reusing the already-parsed AST to avoid double parsing.
     * Returns null if direct execution is not supported, in which case the caller should
     * fall back to the standard query/command methods.
     */
    default ResultSet execute(final Map<String, Object> parameters) {
      return null;
    }
  }

  @ExcludeFromJacocoGeneratedReport
  interface QueryEngineFactory {
    String getLanguage();

    QueryEngine getInstance(DatabaseInternal database);
  }

  String getLanguage();

  AnalyzedQuery analyze(String query);

  ResultSet query(String query, ContextConfiguration configuration, Map<String, Object> parameters);

  /**
   * Optimized overload for queries with no parameters - avoids varargs array allocation.
   */
  default ResultSet query(String query, ContextConfiguration configuration) {
    return query(query, configuration, Collections.emptyMap());
  }

  ResultSet query(String query, ContextConfiguration configuration, Object... parameters);

  ResultSet command(String query, ContextConfiguration configuration, Map<String, Object> parameters);

  /**
   * Optimized overload for commands with no parameters - avoids varargs array allocation.
   */
  default ResultSet command(String query, ContextConfiguration configuration) {
    return command(query, configuration, Collections.emptyMap());
  }

  ResultSet command(String query, ContextConfiguration configuration, Object... parameters);

  default QueryEngine registerFunctions(final String function) {
    return this;
  }

  default QueryEngine unregisterFunctions() {
    return this;
  }

  default boolean isExecutedByTheLeader() {
    return false;
  }

  default boolean isReusable() {
    return true;
  }

  default void close() {
    // NO OPERATIONS
  }
}
