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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private final Collection<RID> rids;

  private Iterator<RID> iterator;

  private Result nextResult = null;

  public FetchFromRidsStep(Collection<RID> rids, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.rids = rids;
    reset();
  }

  public void reset() {
    iterator = rids.iterator();
    nextResult = null;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new ResultSet() {
      int internalNext = 0;

      private void fetchNext() {
        if (nextResult != null) {
          return;
        }
        while (iterator.hasNext()) {
          RID nextRid = iterator.next();
          if (nextRid == null) {
            continue;
          }
          Identifiable nextDoc = null;
          try {
            nextDoc = ctx.getDatabase().lookupByRID(nextRid, true);
          } catch (RecordNotFoundException e) {
            // IGNORE HERE< HANDLED BELOW
          }
          if (nextDoc == null) {
            continue;
          }
          nextResult = new ResultInternal();
          ((ResultInternal) nextResult).setElement((Document) nextDoc);
          return;
        }
      }

      @Override
      public boolean hasNext() {
        if (internalNext >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext();
        }
        return nextResult != null;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }

        internalNext++;
        Result result = nextResult;
        nextResult = null;
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

  @Override
  public String prettyPrint(int depth, int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM RIDs\n" + ExecutionStepInternal.getIndent(depth, indent) + "  " + rids;
  }

  @Override
  public Result serialize() {
    ResultInternal result = ExecutionStepInternal.basicSerialize(this);
    if (rids != null) {
      result.setProperty("rids", rids.stream().map(x -> x.toString()).collect(Collectors.toList()));
    }
    return result;
  }

  @Override
  public void deserialize(Result fromResult) {
    try {
      ExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("rids") != null) {
//        List<String> ser = fromResult.getProperty("rids");
        throw new UnsupportedOperationException();
//        rids = ser.stream().map(x -> new PRID(x)).collect(Collectors.toList());
      }
      reset();
    } catch (Exception e) {
      throw new CommandExecutionException(e);
    }
  }
}
