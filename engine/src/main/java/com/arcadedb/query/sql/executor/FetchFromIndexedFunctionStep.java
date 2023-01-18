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
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.FromClause;

import java.util.*;

/**
 * Created by luigidellaquila on 06/08/16.
 */
public class FetchFromIndexedFunctionStep extends AbstractExecutionStep {
  private BinaryCondition functionCondition;
  private FromClause      queryTarget;


  //runtime0
  Iterator<Record> fullResult = null;

  public FetchFromIndexedFunctionStep(final BinaryCondition functionCondition, final FromClause queryTarget, final CommandContext ctx,
      final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init(ctx);

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        return fullResult.hasNext();
      }

      @Override
      public Result next() {
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (localCount >= nRecords) {
            throw new NoSuchElementException();
          }
          if (!fullResult.hasNext()) {
            throw new NoSuchElementException();
          }
          final ResultInternal result = new ResultInternal();
          result.setElement((Document) fullResult.next().getRecord());
          localCount++;
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }






    };
  }

  private void init(final CommandContext ctx) {
    if (fullResult == null) {
      final long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        fullResult = functionCondition.executeIndexedFunction(queryTarget, ctx).iterator();
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEXED FUNCTION " + functionCondition.toString();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public void reset() {
    this.fullResult = null;
  }

}
