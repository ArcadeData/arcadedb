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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.OrderBy;

import java.util.*;

/**
 * Created by luigidellaquila on 11/07/16.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OrderBy orderBy;
  private       Integer maxResults;
  private final long    timeoutMillis;

  private long cost = 0;

  List<Result> cachedResult = null;
  int          nextElement  = 0;

  public OrderByStep(OrderBy orderBy, CommandContext ctx, long timeoutMillis, boolean profilingEnabled) {
    this(orderBy, null, ctx, timeoutMillis, profilingEnabled);
  }

  public OrderByStep(OrderBy orderBy, Integer maxResults, CommandContext ctx, long timeoutMillis, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (cachedResult == null) {
      cachedResult = new ArrayList<>();
      prev.ifPresent(p -> init(p, ctx));
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
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (currentBatchReturned >= nRecords) {
            throw new IllegalStateException();
          }
          if (cachedResult.size() <= nextElement) {
            throw new IllegalStateException();
          }
          Result result = cachedResult.get(offset + currentBatchReturned);
          nextElement++;
          currentBatchReturned++;
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        prev.ifPresent(p -> p.close());
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return new HashMap<>();
      }
    };
  }

  private void init(ExecutionStepInternal p, CommandContext ctx) {
    long timeoutBegin = System.currentTimeMillis();
    final long maxElementsAllowed = GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    boolean sorted = true;
    do {
      ResultSet lastBatch = p.syncPull(ctx, 100);
      if (!lastBatch.hasNext()) {
        break;
      }
      while (lastBatch.hasNext()) {
        if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
          sendTimeout();
        }

        if (this.timedOut) {
          break;
        }
        Result item = lastBatch.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          cachedResult.add(item);
          if (maxElementsAllowed >= 0 && maxElementsAllowed < cachedResult.size()) {
            this.cachedResult.clear();
            throw new CommandExecutionException(
                "Limit of allowed elements for in-heap ORDER BY in a single query exceeded (" + maxElementsAllowed + ") . You can set "
                    + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey() + " to increase this limit");
          }
          sorted = false;
          // compact, only at twice as the buffer, to avoid to do it at each add
          if (this.maxResults != null) {
            long compactThreshold = 2L * maxResults;
            if (compactThreshold < cachedResult.size()) {
              cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
              cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
              sorted = true;
            }
          }
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }
      if (timedOut) {
        break;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        // compact at each batch, if needed
        if (!sorted && this.maxResults != null && maxResults < cachedResult.size()) {
          cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
          cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
          sorted = true;
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    } while (true);
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (!sorted) {
        cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (maxResults != null ? "\n  (buffer size: " + maxResults + ")" : "");
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
