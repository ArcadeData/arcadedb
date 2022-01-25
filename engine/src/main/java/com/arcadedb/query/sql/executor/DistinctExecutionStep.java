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
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class DistinctExecutionStep extends AbstractExecutionStep {

  final Set<Result> pastItems = new HashSet<>();
  final RidSet      pastRids  = new RidSet();

  ResultSet lastResult = null;
  Result    nextValue;

  private       long cost = 0;
  private final long maxElementsAllowed;

  public DistinctExecutionStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    Database db = ctx == null ? null : ctx.getDatabase();
    maxElementsAllowed = db == null ?
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong() :
        db.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {

    return new ResultSet() {
      int nextLocal = 0;

      @Override
      public boolean hasNext() {
        if (nextLocal >= nRecords) {
          return false;
        }
        if (nextValue != null) {
          return true;
        }
        fetchNext(nRecords);
        return nextValue != null;
      }

      @Override
      public Result next() {
        if (nextLocal >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextValue == null) {
          fetchNext(nRecords);
        }
        if (nextValue == null) {
          throw new IllegalStateException();
        }
        Result result1 = nextValue;
        nextValue = null;
        nextLocal++;
        return result1;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNext(int nRecords) {
    while (true) {
      if (nextValue != null) {
        return;
      }
      if (lastResult == null || !lastResult.hasNext()) {
        lastResult = getPrev().get().syncPull(ctx, nRecords);
      }
      if (lastResult == null || !lastResult.hasNext()) {
        return;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        nextValue = lastResult.next();
        if (alreadyVisited(nextValue)) {
          nextValue = null;
        } else {
          markAsVisited(nextValue);
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
  }

  private void markAsVisited(Result nextValue) {
    if (nextValue.isElement()) {
      RID identity = nextValue.getElement().get().getIdentity();
      int bucket = identity.getBucketId();
      long pos = identity.getPosition();
      if (bucket >= 0 && pos >= 0) {
        pastRids.add(identity);
        return;
      }
    }
    pastItems.add(nextValue);
    if (maxElementsAllowed > 0 && maxElementsAllowed < pastItems.size()) {
      this.pastItems.clear();
      throw new CommandExecutionException(
          "Limit of allowed elements for in-heap DISTINCT in a single query exceeded (" + maxElementsAllowed + ") . You can set "
              + GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey() + " to increase this limit");
    }
  }

  private boolean alreadyVisited(Result nextValue) {
    if (nextValue.isElement()) {
      RID identity = nextValue.getElement().get().getIdentity();
      int bucket = identity.getBucketId();
      long pos = identity.getPosition();
      if (bucket >= 0 && pos >= 0) {
        return pastRids.contains(identity);
      }
    }
    return pastItems.contains(nextValue);
  }

  @Override
  public void sendTimeout() {

  }

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ DISTINCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
