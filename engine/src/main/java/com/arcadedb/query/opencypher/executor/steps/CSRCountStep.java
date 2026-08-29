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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.DenseNodeIdProvider;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.List;

/**
 * Unified execution step for all CSR count-push-down optimizations.
 * Handles provider lookup, CSR/OLTP dispatch, profiling, and result packaging.
 * Delegates the actual counting logic to a {@link CountOp} implementation.
 */
public final class CSRCountStep extends AbstractExecutionStep {
  private final CountOp op;
  private final String countAlias;

  public CSRCountStep(final CountOp op, final String countAlias, final CommandContext context) {
    super(context);
    this.op = op;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Database db = context.getDatabase();
      GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, op.edgeTypes());

      // An operator anchored on "every vertex" can only be answered off a provider whose node domain is every
      // vertex; one built over a subset of the vertex types would count a subset of the graph (issue #5757).
      if (provider != null && op.requiresFullVertexCoverage() && !provider.coversVertexType(null))
        provider = null;

      // Every CountOp indexes its scratch arrays by node id and iterates the id space from the node count, so
      // the two have to be the same number. They are not for a provider holding overlay deletions, which keeps
      // the slot of every deleted node and allocates the id of every added one above the base mapping: the
      // count then names neither the size of the space nor the ids in it, and the operator both skips the
      // highest live nodes and indexes past the end of its own arrays (issue #6792). Renumbering here hands the
      // operators the compact space they are written against; it is a no-op when there are no holes.
      provider = DenseNodeIdProvider.wrap(provider);

      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      final long count;
      if (provider != null)
        count = op.execute(provider, db, guard);
      else
        count = op.executeOLTP(db, guard);

      if (context.isProfiling()) {
        cost = System.nanoTime() - begin;
        rowCount = 1;
        context.setVariable(CommandContext.CSR_ACCELERATED_VAR, provider != null);
      }

      final ResultInternal result = new ResultInternal();
      result.setProperty(countAlias, count);
      return new IteratorResultSet(List.of((Result) result).iterator());
    } finally {
      if (context.isProfiling() && cost < 0)
        cost = System.nanoTime() - begin;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return op.describe(depth, indent);
  }
}
