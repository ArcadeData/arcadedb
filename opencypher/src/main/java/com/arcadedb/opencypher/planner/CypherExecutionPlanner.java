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
package com.arcadedb.opencypher.planner;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.opencypher.ast.CypherStatement;
import com.arcadedb.opencypher.executor.CypherExecutionPlan;

import java.util.Map;

/**
 * Creates optimized execution plans for Cypher statements.
 * Analyzes the query and determines the best execution strategy.
 */
public class CypherExecutionPlanner {
  private final DatabaseInternal database;
  private final CypherStatement statement;
  private final Map<String, Object> parameters;

  public CypherExecutionPlanner(final DatabaseInternal database, final CypherStatement statement,
      final Map<String, Object> parameters) {
    this.database = database;
    this.statement = statement;
    this.parameters = parameters;
  }

  /**
   * Creates an execution plan for the Cypher statement.
   * Phase 1: Basic plan without optimization.
   * TODO: Implement index selection, filter pushdown, and join optimization.
   *
   * @param configuration context configuration
   * @return execution plan
   */
  public CypherExecutionPlan createExecutionPlan(final ContextConfiguration configuration) {
    return new CypherExecutionPlan(database, statement, parameters, configuration);
  }
}
