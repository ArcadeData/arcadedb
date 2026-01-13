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
package com.arcadedb.opencypher.executor.operators;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

/**
 * Base interface for physical operators in the query execution plan.
 * Physical operators represent "how" operations are executed with specific algorithms.
 *
 * Operators form a tree structure where each operator:
 * - Receives input from child operators (or data sources)
 * - Performs a specific operation (scan, seek, join, filter, etc.)
 * - Produces output for parent operators
 *
 * This design is inspired by the Volcano/Cascade iterator model used by
 * other advanced query optimizers.
 */
public interface PhysicalOperator {
  /**
   * Executes this operator and returns results.
   *
   * @param context command context with database access
   * @param nRecords number of records to fetch (-1 for all)
   * @return result set with operator output
   */
  ResultSet execute(CommandContext context, int nRecords);

  /**
   * Returns the estimated cost of executing this operator.
   * Cost is computed during optimization and used for plan selection.
   *
   * @return estimated cost (lower is better)
   */
  double getEstimatedCost();

  /**
   * Returns the estimated cardinality (number of output rows).
   * Used for cost estimation of parent operators.
   *
   * @return estimated output row count
   */
  long getEstimatedCardinality();

  /**
   * Returns the type of this operator (for EXPLAIN output).
   *
   * @return operator type name (e.g., "NodeByLabelScan", "NodeIndexSeek")
   */
  String getOperatorType();

  /**
   * Generates human-readable explanation of this operator's execution plan.
   * Used by EXPLAIN command to show query plan with costs.
   *
   * @param depth indentation depth for tree visualization
   * @return formatted explanation string
   */
  String explain(int depth);

  /**
   * Returns the child operator that provides input to this operator.
   * Null if this is a leaf operator (data source).
   *
   * @return child operator or null
   */
  PhysicalOperator getChild();

  /**
   * Sets the child operator that will provide input to this operator.
   *
   * @param child the child operator
   */
  void setChild(PhysicalOperator child);
}
