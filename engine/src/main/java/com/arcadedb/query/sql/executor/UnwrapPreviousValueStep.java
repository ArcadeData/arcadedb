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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  public UnwrapPreviousValueStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();

    final ResultSet upstream = prev.syncPull(context, nRecords);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          Result prevResult = upstream.next();
          if (prevResult instanceof UpdatableResult) {
            prevResult = ((UpdatableResult) prevResult).previousValue;
            if (prevResult == null) {
              throw new CommandExecutionException("Invalid status of record: no previous value available");
            }
            return prevResult;
          } else {
            throw new CommandExecutionException("Invalid status of record: no previous value available");
          }
        } finally {
          if( context.isProfiling() ) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ UNWRAP PREVIOUS VALUE";
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
