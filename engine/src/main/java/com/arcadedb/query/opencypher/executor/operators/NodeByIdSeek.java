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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.executor.IdSeekValues;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * The anchor of a cost-based plan whose node the {@code WHERE} clause pins by id: {@code elementId(n) = $id}, {@code id(n) = $id}
 * or the {@code IN} of either (issue #12134). It fetches each record by its RID and keeps the vertices that carry the pattern's
 * labels, instead of scanning the whole label for the few records the predicate names.
 * <p>
 * The predicate is still evaluated on every vertex fetched (it is pushed down here, or left to the Filter above): the lookup reads
 * the values more loosely than Cypher's equality does (see {@link IdSeekValues}).
 */
public class NodeByIdSeek extends AbstractPhysicalOperator {
  private final String       variable;
  private final List<String> labels;
  private final boolean      labelDisjunction;
  private final InListValues ids;
  /** Predicates reading only {@link #variable}, evaluated once per fetched vertex. */
  private BooleanExpression whereFilter;

  public NodeByIdSeek(final String variable, final List<String> labels, final boolean labelDisjunction, final InListValues ids,
      final double estimatedCost, final long estimatedCardinality) {
    super(estimatedCost, estimatedCardinality);
    this.variable = variable;
    this.labels = labels == null ? List.of() : labels;
    this.labelDisjunction = labelDisjunction;
    this.ids = ids;
  }

  public void pushDownFilter(final BooleanExpression filter) {
    this.whereFilter = whereFilter == null ? filter : new LogicalExpression(LogicalExpression.Operator.AND, whereFilter, filter);
  }

  public BooleanExpression getWhereFilter() {
    return whereFilter;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {
      private List<RID>          rids;
      private int                ridIndex    = 0;
      private final List<Result> buffer      = new ArrayList<>();
      private int                bufferIndex = 0;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (rids == null)
          rids = IdSeekValues.resolve(ids, context);
        if (ridIndex >= rids.size())
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
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
        while (buffer.size() < n && ridIndex < rids.size()) {
          guard.check();
          final RID rid = rids.get(ridIndex++);
          final Record record;
          try {
            record = context.getDatabase().lookupByRID(rid, true);
          } catch (final RecordNotFoundException e) {
            // no record at that address: no node has that id
            continue;
          }
          if (!(record instanceof Vertex vertex) || !Labels.matches(vertex, labels, labelDisjunction))
            continue;

          final ResultInternal result = new ResultInternal();
          result.setProperty(variable, vertex);
          if (whereFilter != null && !whereFilter.evaluate(result, context))
            continue;
          buffer.add(result);
        }
      }

      @Override
      public void close() {
        // holds no cursor
      }
    };
  }

  @Override
  public String getOperatorType() {
    return "NodeByIdSeek";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ NodeByIdSeek(").append(variable);
    if (!labels.isEmpty())
      sb.append(':').append(String.join(labelDisjunction ? "|" : ":", labels));
    sb.append(") [ids=").append(ids);
    if (whereFilter != null)
      sb.append(", filter: ").append(whereFilter.getText());
    sb.append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");
    return sb.toString();
  }

  public String getVariable() {
    return variable;
  }

  public InListValues getIds() {
    return ids;
  }
}
