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
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

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
      final GraphTraversalProvider provider = GraphTraversalProviderRegistry.findProvider(db, op.edgeTypes());

      final long count;
      if (provider != null)
        count = op.execute(provider, db);
      else
        count = op.executeOLTP(db);

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
