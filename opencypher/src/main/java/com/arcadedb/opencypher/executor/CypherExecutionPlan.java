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
package com.arcadedb.opencypher.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Execution plan for a Cypher query.
 * Contains the chain of execution steps and executes them.
 */
public class CypherExecutionPlan {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;
  private final ContextConfiguration configuration;

  public CypherExecutionPlan(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters, final ContextConfiguration configuration) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
    this.configuration = configuration;
  }

  /**
   * Executes the query plan and returns results.
   * Phase 1: Stub implementation that returns an empty result set.
   * TODO: Implement actual execution step chain.
   *
   * @return result set
   */
  public ResultSet execute() {
    // Phase 1: Return empty result set as placeholder
    // TODO: Build and execute execution step chain
    final List<ResultInternal> results = new ArrayList<>();

    // Placeholder: add a simple result showing the query was parsed
    final ResultInternal result = new ResultInternal();
    result.setProperty("message", "OpenCypher query parsed successfully (Phase 1 - execution not yet implemented)");
    result.setProperty("query", statement.getClass().getSimpleName());
    results.add(result);

    return new IteratorResultSet(results.iterator());
  }
}
