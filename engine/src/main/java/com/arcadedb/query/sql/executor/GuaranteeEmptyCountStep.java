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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;

import java.util.NoSuchElementException;

/**
 * Guarantees that a no-GROUP-BY aggregation (count(*), sum(), avg(), min(), max(), ...) always produces exactly one
 * row, even when the upstream produced none. Without this step {@link AggregateProjectionCalculationStep} emits no
 * row at all over an empty input, which is inconsistent with the single-row-with-identity-value semantics SQL callers
 * expect from a scalar aggregate (issue #6680).
 */
public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final Projection aggregateProjection;
  private final Projection preAggregateProjection;
  private       boolean    executed = false;

  public GuaranteeEmptyCountStep(final Projection aggregateProjection, final Projection preAggregateProjection,
      final CommandContext context) {
    super(context);
    this.aggregateProjection = aggregateProjection;
    this.preAggregateProjection = preAggregateProjection;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();
    final ResultSet upstream = prev.syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        if (!executed)
          return true;

        return upstream.hasNext();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();

        try {
          if (upstream.hasNext())
            return upstream.next();

          final ResultInternal result = new ResultInternal(context.getDatabase());
          for (final ProjectionItem item : aggregateProjection.getItems()) {
            final Object value = item.isAggregate(context) ?
                // No row was ever aggregated: read the aggregate function's identity value (e.g. 0 for count()/sum(),
                // null for min()/max()/avg()) without ever calling apply(), consistent with a group whose rows are
                // all-null.
                item.getAggregationContext(context).getFinalValue() :
                // A non-aggregate item mixed into a GROUP-BY-less aggregation projection (e.g. a constant) has no row
                // to evaluate against.
                item.execute((Result) null, context);
            result.setProperty(item.getProjectionAliasAsString(), value);
          }
          if (preAggregateProjection != null) {
            for (final ProjectionItem preAggItem : preAggregateProjection.getItems()) {
              result.setProperty(preAggItem.getProjectionAliasAsString(), preAggItem.execute((Result) null, context));
            }
          }
          return result;
        } finally {
          executed = true;
        }
      }

      @Override
      public void close() {
        prev.close();
      }
    };
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new GuaranteeEmptyCountStep(aggregateProjection.copy(), preAggregateProjection != null ? preAggregateProjection.copy() : null,
        context);
  }

  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ GUARANTEE SINGLE ROW FOR EMPTY AGGREGATION ";
  }
}
