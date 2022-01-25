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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Expands a result-set.
 * The pre-requisite is that the input element contains only one field (no matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {

  private long cost = 0;

  ResultSet lastResult      = null;
  Iterator  nextSubsequence = null;
  Result    nextElement     = null;

  public ExpandStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    if (prev == null || prev.isEmpty()) {
      throw new CommandExecutionException("Cannot expand without a target");
    }
    return new ResultSet() {
      long localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        return nextElement != null;
      }

      @Override
      public Result next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextElement == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextElement == null) {
          throw new IllegalStateException();
        }

        Result result = nextElement;
        localCount++;
        nextElement = null;
        fetchNext(ctx, nRecords);
        return result;
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

  private void fetchNext(CommandContext ctx, int n) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Object nextElementObj = nextSubsequence.next();
          if (nextElementObj instanceof Result) {
            nextElement = (Result) nextElementObj;
          } else if (nextElementObj instanceof Identifiable) {
            Record record = ((Identifiable) nextElementObj).getRecord();
            if (record == null) {
              continue;
            }
            nextElement = new ResultInternal();
            ((ResultInternal) nextElement).setElement((Document) record);
          } else {
            nextElement = new ResultInternal();
            ((ResultInternal) nextElement).setProperty("value", nextElementObj);
          }
          break;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (lastResult == null || !lastResult.hasNext()) {
          lastResult = getPrev().get().syncPull(ctx, n);
        }
        if (!lastResult.hasNext()) {
          return;
        }
      }

      Result nextAggregateItem = lastResult.next();
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        if (nextAggregateItem.getPropertyNames().size() == 0) {
          continue;
        }
        if (nextAggregateItem.getPropertyNames().size() > 1) {
          throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
        }

        String propName = nextAggregateItem.getPropertyNames().iterator().next();
        Object projValue = nextAggregateItem.getProperty(propName);
        if (projValue == null) {
          continue;
        }
        if (projValue instanceof Identifiable) {
          Record rec = ((Identifiable) projValue).getRecord();
          if (rec == null) {
            continue;
          }
          ResultInternal res = new ResultInternal();
          res.setElement((Document) rec);

          nextSubsequence = Collections.singleton(res).iterator();
        } else if (projValue instanceof Result) {
          nextSubsequence = Collections.singleton((Result) projValue).iterator();
        } else if (projValue instanceof Iterator) {
          nextSubsequence = (Iterator) projValue;
        } else if (projValue instanceof Iterable) {
          nextSubsequence = ((Iterable) projValue).iterator();
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    } while (true);

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXPAND";
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
