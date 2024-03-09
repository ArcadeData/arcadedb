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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

/**
 * <p>Reads an upstream result set and returns a new result set that contains copies of the original OResult instances
 * </p>
 * <p>This is mainly used from statements that need to copy of the original data before modifying it,
 * eg. UPDATE ... RETURN BEFORE</p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CopyRecordContentBeforeUpdateStep extends AbstractExecutionStep {

  public CopyRecordContentBeforeUpdateStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet lastFetched = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override
      public Result next() {
        final Result result = lastFetched.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {

          if (result instanceof UpdatableResult) {
            final ResultInternal prevValue = new ResultInternal();
            final Record rec = result.getElement().get().getRecord();
            prevValue.setProperty("@rid", rec.getIdentity());
            if (rec instanceof Document) {
              prevValue.setProperty("@type", ((Document) rec).getTypeName());
            }
            for (final String propName : result.getPropertyNames()) {
              prevValue.setProperty(propName, result.getProperty(propName));
            }
            ((UpdatableResult) result).previousValue = prevValue;
          } else {
            throw new CommandExecutionException("Cannot fetch previous value: " + result);
          }
          return result;
        } finally {
          if( context.isProfiling() ) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        lastFetched.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY RECORD CONTENT BEFORE UPDATE");
    if( context.isProfiling() ) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

}
