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
package com.arcadedb.query.sql.executor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

/**
 * Basic interface for commands. Manages the context variables during execution.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface CommandContext {
  Object getVariablePath(String name);

  Object getVariablePath(String name, Object iDefault);

  Object getVariable(String name);

  Object getVariable(String name, Object iDefaultValue);

  CommandContext setVariable(String name, Object iValue);

  CommandContext incrementVariable(String getNeighbors);

  Map<String, Object> getVariables();

  CommandContext getParent();

  CommandContext setParent(CommandContext parentContext);

  CommandContext setChild(CommandContext context);

  boolean isProfiling();

  CommandContext setProfiling(boolean profilingEnabled);

  Map<String, Object> getInputParameters();

  void setInputParameters(Map<String, Object> inputParameters);

  /**
   * Creates a copy of execution context.
   */
  CommandContext copy();

  DatabaseInternal getDatabase();

  void declareScriptVariable(String varName);

  boolean isScriptVariableDeclared(String varName);

  ContextConfiguration getConfiguration();

  void setConfiguration(ContextConfiguration configuration);

  CommandContext getContextDeclaredVariable(String varName);

  /** Context variable name set to {@code true} when CSR (Graph Analytical View) acceleration was used during execution. */
  String CSR_ACCELERATED_VAR = "_csrAccelerated";

  /** Context variable set by algorithm procedures to the total number of results they will yield.
   *  Used by CallStep to optimize count-only queries by skipping per-row Result object creation. */
  String RESULT_COUNT_HINT_VAR = "_resultCountHint";

  /**
   * Context variable holding the partition-pruned bucket file ids derived by
   * {@code SelectExecutionPlanner.derivePartitionPrunedClusters}. Set when a SELECT's WHERE
   * clause binds every partition property of the FROM type to a literal, so any downstream
   * consumer (e.g. {@code vector.neighbors} / {@code vector.sparseNeighbors}) that does its
   * own per-bucket fan-out can intersect this set with its full per-type bucket allow-list and
   * skip indexes outside the partition.
   * <p>
   * Value type: {@link com.arcadedb.utility.IntHashSet} of bucket file ids.
   * Companion variable: {@link #PARTITION_PRUNED_TYPE_NAME_VAR}.
   * <p>
   * <b>NOTE (multi-type queries).</b> The slot is single-valued: a query that triggers pruning
   * on more than one partitioned type (e.g. a future projection that calls
   * {@code vector.neighbors} on type A while the FROM clause prunes type B) will have its first
   * type's hint silently overwritten by the second derivation. The companion type-name variable
   * blocks cross-type misapplication on the read side, but the first type's hint is lost. Today
   * the planner only calls this from a single FROM-type context so this is not reachable; if
   * multi-type pruning is ever added, swap the two scalars for a {@code Map<String, IntHashSet>}.
   */
  String PARTITION_PRUNED_BUCKET_FILE_IDS_VAR = "_partitionPrunedBucketFileIds";

  /**
   * Companion to {@link #PARTITION_PRUNED_BUCKET_FILE_IDS_VAR}: the FROM type name the prune was
   * derived from. Consumers must verify their target type matches before applying the prune so
   * a vector-function call against an unrelated type in the same query (rare but possible) is
   * not accidentally narrowed.
   */
  String PARTITION_PRUNED_TYPE_NAME_VAR = "_partitionPrunedTypeName";
}
