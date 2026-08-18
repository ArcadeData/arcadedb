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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

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

  /**
   * What a language can say about a statement WITHOUT paying for work its execution will not reuse - see
   * {@link QueryEngine#classifyDDL(String)}.
   */
  enum DDLClassification {
    /** The statement is DDL in this language. */
    DDL,
    /** The statement is definitely not DDL. */
    NOT_DDL,
    /**
     * This engine does not classify statements ahead of running them, so the caller learns nothing and has to fall
     * back to whatever it does when it cannot tell. The honest default: a wrong guess costs a statement its routing,
     * and no answer is better than a plausible one.
     */
    UNKNOWN
  }

  String getLanguage();

  AnalyzedQuery analyze(String query);

  /**
   * Whether {@code query} is DDL, asked on the thread that DISPATCHED it rather than on the one that will run it
   * (issue #6324, item 5).
   * <p>
   * {@code database.async().command(...)} has to know before it schedules anything: the DDL that builds an index by
   * SCANNING the data must first make the async executor quiesce, and one of that executor's own workers cannot -
   * the quiescence enqueues a task on every worker including its own, and only that worker drains its queue. So a
   * dispatched DDL statement runs on {@code AsyncCommandPool} instead, and everything else stays on the workers,
   * where a worker's batch transaction and its pinned bucket are worth keeping.
   * <p>
   * <b>Why this is a hook and not a branch in the dispatcher.</b> It used to be SQL's private knowledge, so
   * {@code CREATE INDEX} sent with {@code awaitResponse=false} worked in SQL and was refused in Cypher - an asymmetry
   * a user meets without warning. It is a hook rather than a keyword match because the DECISION has to be a parse: a
   * keyword can appear in a string literal, and routing an ordinary write off the workers costs it the bucket pinning
   * that keeps concurrent writers apart.
   * <p>
   * <b>And why {@link DDLClassification#UNKNOWN} is a legitimate answer.</b> Only a language whose parse is already
   * paid for - a statement cache the execution is about to read again - can answer this for free, and paying for a
   * full parse (for Gremlin, a Groovy compile) on the submitting thread is exactly what a caller passing
   * {@code awaitResponse=false} asked not to wait for. An engine that cannot answer cheaply says so, and its
   * statements keep the behaviour they have always had, #6281's refusal included.
   */
  default DDLClassification classifyDDL(final String query) {
    return DDLClassification.UNKNOWN;
  }

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
