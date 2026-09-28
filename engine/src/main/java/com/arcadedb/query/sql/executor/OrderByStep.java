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
import com.arcadedb.query.sql.parser.OrderBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by luigidellaquila on 11/07/16.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderBy            orderBy;
  private       Integer            maxResults;
  private final long               timeoutMillis;
  // THE ROWS BUFFERED, UNDER THE PER-OPERATION CAP AND THE HEAP BUDGET OF ALL THE QUERIES (ISSUES #8585, #8591)
  private       OperationHeapLimit limit;

  List<Result> cachedResult = null;
  int          nextElement  = 0;

  public OrderByStep(final OrderBy orderBy, final CommandContext context, final long timeoutMillis) {
    this(orderBy, null, context, timeoutMillis);
  }

  public OrderByStep(final OrderBy orderBy, final Integer maxResults, final CommandContext context, final long timeoutMillis) {
    super(context);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (cachedResult == null) {
      cachedResult = new ArrayList<>();
      limit = OperationHeapLimit.of(context, "ORDER BY");
      if (prev != null) {
        try {
          init(prev, context);
        } catch (final RuntimeException e) {
          releaseBuffer();
          throw e;
        }
      }
    }

    return new ResultSet() {
      private int currentBatchReturned = 0;
      private final int offset = nextElement;

      @Override
      public boolean hasNext() {
        if (currentBatchReturned >= nRecords) {
          return false;
        }
        return cachedResult.size() > nextElement;
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (currentBatchReturned >= nRecords) {
            throw new NoSuchElementException();
          }
          if (cachedResult.size() <= nextElement) {
            throw new NoSuchElementException();
          }
          final Result result = cachedResult.get(offset + currentBatchReturned);
          nextElement++;
          currentBatchReturned++;
          if (nextElement == cachedResult.size())
            // EVERY ROW WAS SERVED: THE BUFFER IS NOT NEEDED ANYMORE, EVEN IF THE CONSUMER KEEPS THE RESULT SET OPEN
            releaseBuffer();
          return result;
        } finally {
          if( context.isProfiling() ) {
            cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        if (prev != null)
          prev.close();
      }
    };
  }

  private void init(final ExecutionStepInternal p, final CommandContext context) {
    final long timeoutBegin = System.currentTimeMillis();
    boolean sorted = true;
    do {
      final ResultSet lastBatch = p.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
      if (!lastBatch.hasNext())
        break;

      while (lastBatch.hasNext()) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis())
          sendTimeout();

        if (this.timedOut)
          break;

        final Result item = lastBatch.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          cachedResult.add(item);
          limit.add(cachedResult.size(), item);
          sorted = false;
          // compact, only at twice as the buffer, to avoid to do it at each add
          if (this.maxResults != null) {
            final long compactThreshold = 2L * maxResults;
            if (compactThreshold < cachedResult.size()) {
              keepTopResults(context);
              sorted = true;
            }
          }
        } finally {
          if( context.isProfiling() ) {
            cost += System.nanoTime() - begin;
          }
        }
      }
      if (timedOut) {
        break;
      }
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        // compact at each batch, if needed
        if (!sorted && this.maxResults != null && maxResults < cachedResult.size()) {
          keepTopResults(context);
          sorted = true;
        }
      } finally {
        if( context.isProfiling() ) {
          cost += System.nanoTime() - begin;
        }
      }
    } while (true);
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      if (!sorted) {
        cachedResult.sort((a, b) -> orderBy.compare(a, b, context));
      }
    } finally {
      if( context.isProfiling() ) {
        cost += System.nanoTime() - begin;
      }
    }
  }

  /**
   * Sorts the buffer and keeps its first {@code maxResults} rows, giving back the heap of the others: the kept rows are
   * estimated again, since the rows dropped are not of the size of the average one.
   */
  private void keepTopResults(final CommandContext context) {
    cachedResult.sort((a, b) -> orderBy.compare(a, b, context));
    cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
    limit.rechargeAll(cachedResult);
  }

  private void releaseBuffer() {
    cachedResult = Collections.emptyList();
    if (limit != null)
      limit.release();
  }

  @Override
  public void close() {
    if (cachedResult != null)
      releaseBuffer();
    super.close();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    result += maxResults != null ? "\n  (buffer size: " + maxResults + ")" : "";
    return result;
  }

}
