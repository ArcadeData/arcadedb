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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.LabelReplacements;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Execution step for SET clause.
 * Supports: SET n.prop = value, SET n[key] = value, SET n = {map}, SET n += {map}, SET n:Label
 * <p>
 * The clause semantics live in {@link SetClauseApplier}, which {@link MergeStep} shares for ON CREATE SET /
 * ON MATCH SET; this step only owns the row loop and the implicit transaction around it.
 */
public class SetStep extends AbstractExecutionStep {
  private final SetClause         setClause;
  private final SetClauseApplier  applier;

  public SetStep(final SetClause setClause, final CommandContext context,
                 final CypherFunctionFactory functionFactory) {
    super(context);
    this.setClause = setClause;
    this.applier = SetClauseApplier.forSetClause(context, new ExpressionEvaluator(functionFactory));
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("SetStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;
      // Tracks the latest written MutableDocument per RID so that self-referential
      // expressions (e.g. SET p.age = p.age + i) accumulate correctly when the same
      // node is hit across multiple rows (e.g. via UNWIND). For the per-row-tx path,
      // MutableDocument retains its in-memory property state after commit (unsetDirty()
      // clears the dirty flag but not the map), so subsequent rows can read through the
      // stored instance without reloading from storage.
      private final Map<RID, MutableDocument> writtenDocs = new HashMap<>();
      // Tracks the vertices SET n:Label replaced so that when the same node appears on a later row
      // (row fanout), the operation is redirected to the already-replaced vertex and the idempotency
      // check returns early. Statement-scoped, so a SET inside a CALL { } body - re-planned once per outer
      // row - still recognises what an earlier row moved (issue #6977).
      private final LabelReplacements labelReplacements = LabelReplacements.of(context);

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;
        if (prevResults == null)
          prevResults = prev.syncPull(context, nRecords);
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            applySetOperations(inputResult, writtenDocs, labelReplacements);
            buffer.add(inputResult);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        SetStep.this.close();
      }
    };
  }

  private void applySetOperations(final Result result, final Map<RID, MutableDocument> writtenDocs,
      final LabelReplacements labelReplacements) {
    if (setClause == null || setClause.isEmpty())
      return;

    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      if (!wasInTransaction)
        context.getDatabase().begin();

      applier.apply(setClause, result, writtenDocs, labelReplacements);

      if (!wasInTransaction)
        context.getDatabase().commit();
    } catch (final Exception e) {
      if (!wasInTransaction && context.getDatabase().isTransactionActive())
        context.getDatabase().rollback();
      throw e;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ SET");
    if (setClause != null && !setClause.isEmpty())
      builder.append(" (").append(setClause.getItems().size()).append(" items)");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
