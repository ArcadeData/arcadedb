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

import com.arcadedb.database.Document;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A combined scan + filter step that evaluates the WHERE predicate immediately after loading each record,
 * avoiding the overhead of creating ResultInternal wrappers for records that don't match the filter.
 * <p>
 * When {@code projectedProperties} is set, matching records are returned with only the projected properties
 * deserialized, using a two-phase approach:
 * <ol>
 *   <li>Filter phase: WHERE predicate accesses only the fields it references (lazy deserialization)</li>
 *   <li>Projection phase: only the SELECT-listed properties are deserialized from the binary buffer</li>
 * </ol>
 * Fields that appear in both WHERE and SELECT are deserialized only once (the lazy cache in ImmutableDocument).
 */
public class ScanWithFilterStep extends AbstractExecutionStep {

  private final int         bucketId;
  private final WhereClause whereClause;
  private final Set<String> projectedProperties;
  private       Object      order;
  private       long        totalFetched  = 0L;
  private       long        totalFiltered = 0L;

  private Iterator<Record> iterator;

  public ScanWithFilterStep(final int bucketId, final WhereClause whereClause, final Set<String> projectedProperties,
      final CommandContext context) {
    super(context);
    this.bucketId = bucketId;
    this.whereClause = whereClause;
    this.projectedProperties = projectedProperties;
  }

  public ScanWithFilterStep(final int bucketId, final WhereClause whereClause, final CommandContext context) {
    this(bucketId, whereClause, null, context);
  }

  public void setOrder(final Object order) {
    this.order = order;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        if (FetchFromClusterExecutionStep.ORDER_DESC == order)
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).inverseIterator();
        else
          iterator = context.getDatabase().getSchema().getBucketById(bucketId).iterator();
      }

      return new ResultSet() {
        int nFetched = 0;
        Result nextItem = null;

        private void fetchNextItem() {
          nextItem = null;
          while (iterator.hasNext()) {
            final Record record = iterator.next();
            totalFetched++;

            // Phase 1: Evaluate WHERE on the full document (uses lazy per-field deserialization)
            final ResultInternal candidate = new ResultInternal(record);

            final long filterBegin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (whereClause.matchesFilters(candidate, context)) {
                // Phase 2: If projectedProperties is set, extract only needed columns
                if (projectedProperties != null && record instanceof ImmutableDocument immutableDoc) {
                  final String[] fieldNames = projectedProperties.toArray(new String[0]);
                  final Map<String, Object> props = immutableDoc.propertiesAsMap(fieldNames);
                  final ResultInternal projected = new ResultInternal(context.getDatabase());
                  projected.setElement(immutableDoc);
                  for (final Map.Entry<String, Object> entry : props.entrySet())
                    projected.setProperty(entry.getKey(), entry.getValue());
                  nextItem = projected;
                } else
                  nextItem = candidate;

                context.setVariable("current", nextItem);
                return;
              }
              totalFiltered++;
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - filterBegin);
            }
          }
        }

        @Override
        public boolean hasNext() {
          if (nFetched >= nRecords)
            return false;
          if (nextItem == null)
            fetchNextItem();
          return nextItem != null;
        }

        @Override
        public Result next() {
          if (nFetched >= nRecords)
            throw new NoSuchElementException();
          if (nextItem == null)
            fetchNextItem();
          if (nextItem == null)
            throw new NoSuchElementException();

          final Result result = nextItem;
          nextItem = null;
          nFetched++;
          return result;
        }
      };
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ SCAN WITH FILTER BUCKET " + bucketId
        + " (" + context.getDatabase().getSchema().getBucketById(bucketId).getName() + ")";
    if (projectedProperties != null)
      result += " [projected: " + String.join(", ", projectedProperties) + "]";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + whereClause;
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    final ScanWithFilterStep copy = new ScanWithFilterStep(this.bucketId, this.whereClause.copy(), this.projectedProperties,
        context);
    copy.setOrder(this.order);
    return copy;
  }
}
