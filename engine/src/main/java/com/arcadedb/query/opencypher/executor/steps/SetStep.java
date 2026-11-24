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
import com.arcadedb.query.opencypher.executor.SelectionPredicates;
import com.arcadedb.query.opencypher.executor.WrittenCopy;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

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
  // THE ROWS A DRAINING WRITE HOLDS UNTIL IT HANDS THEM OUT, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES,
  // AS EAGERSTEP HOLDS THEM (ISSUE #12656). ON THE STEP: THE CLOSE() OF A QUERY REACHES THE STEPS, NOT THEIR RESULT SETS
  private       OperationHeapLimit heapLimit;
  // No clause after this one reads the rows: they are handed out as they are written, one pull batch at a time (issue #12656)
  private       boolean            streamsRows;
  private final SetClause          setClause;
  private final SetClauseApplier   applier;

  public SetStep(final SetClause setClause, final CommandContext context,
                 final CypherFunctionFactory functionFactory) {
    this(setClause, context, functionFactory, SelectionPredicates.NONE);
  }

  /**
   * @param selection the predicates that selected the records the rows bind, evaluated again on a target a concurrent commit
   *                  changed since the row read it (issue #11818)
   */
  public SetStep(final SetClause setClause, final CommandContext context, final CypherFunctionFactory functionFactory,
      final SelectionPredicates selection) {
    super(context);
    this.setClause = setClause;
    this.applier = SetClauseApplier.forSetClause(context, new ExpressionEvaluator(functionFactory), selection);
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
        fetchMore();
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = buffer.get(bufferIndex++);
        // A LATER row of the same batch may have replaced a record this row holds after this row was written: served with
        // the live records, not the deleted ones (issue #10614). Free while the statement replaced nothing
        labelReplacements.redirect(result);
        if (heapLimit != null && bufferIndex == buffer.size()) {
          // EVERY ROW WAS SERVED: THE BUFFER IS NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
          buffer.clear();
          bufferIndex = 0;
          releaseHeap();
        }
        return result;
      }

      private void fetchMore() {
        buffer.clear();
        bufferIndex = 0;
        if (prevResults == null)
          prevResults = prev.syncPull(context, nRecords);
        // The SET runs for every row before it hands out the first one, as Neo4j's Eager does: when several rows bind the
        // same node, the clauses after the SET read the write of every row, however many pull batches the input takes
        // (issue #12656). The drain is one region the statement-level deadline check cannot reach (#6266)
        final boolean drains = !streamsRows;
        final WorkGuard guard = drains ? WorkGuard.forCommandDeadline(context) : null;
        if (drains)
          heapLimit = OperationHeapLimit.of(context, "SET read/write barrier");
        try {
          while ((drains || buffer.size() < nRecords) && prevResults.hasNext()) {
            if (guard != null)
              guard.check();
            final Result inputResult = prevResults.next();
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              applySetOperations(inputResult, writtenDocs, labelReplacements);
              buffer.add(inputResult);
              if (heapLimit != null)
                heapLimit.add(buffer.size(), inputResult);
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
          }
        } catch (final RuntimeException | Error e) {
          // A FAILED DRAIN HANDS OUT NOTHING: ITS ROWS AND THEIR CHARGE GO NOW, NOT WHEN THE QUERY IS CLOSED
          buffer.clear();
          releaseHeap();
          throw e;
        }
        if (!prevResults.hasNext())
          finished = true;
        // A row holding its own copy of a record a later row wrote is lined up with the copy the transaction wrote
        if (drains)
          WrittenCopy.resolveRows(context, buffer);
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

    // In the statement's transaction, which OpenCypherQueryEngine opens when the caller has none (issue #10190): one of
    // its own per row committed the rows before a failing one, and the statement was not atomic any more
    applier.apply(setClause, result, writtenDocs, labelReplacements);
  }

  /** Called when this step ends the statement: nothing reads its rows, so it does not hold them until every row has written. */
  public void streamRows() {
    streamsRows = true;
  }

  private void releaseHeap() {
    if (heapLimit != null) {
      heapLimit.release();
      heapLimit = null;
    }
  }

  @Override
  public void close() {
    releaseHeap();
    super.close();
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
