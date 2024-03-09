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
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * <p>
 * takes a result set made of OUpdatableRecord instances and transforms it in another result set made of normal OResultInternal
 * instances.
 * </p>
 * <p>This is the opposite of ConvertToUpdatableResultStep</p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ConvertToResultInternalStep extends AbstractExecutionStep {

  ResultSet prevResult = null;

  public ConvertToResultInternalStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    return new ResultSet() {
      public boolean finished = false;

      Result nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished)
          return;

        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (nextItem instanceof UpdatableResult) {
              final Document element = nextItem.getElement().get();
              if (element != null)
                nextItem = new ResultInternal(element);
              break;
            }
          } finally {
            if ( context.isProfiling() )
              cost += (System.nanoTime() - begin);
          }
          nextItem = null;
        }
      }

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished)
          return false;

        if (nextItem == null)
          fetchNextItem();

        return nextItem != null;
      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished)
          throw new NoSuchElementException();

        if (nextItem == null)
          fetchNextItem();

        if (nextItem == null)
          throw new NoSuchElementException();

        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        ConvertToResultInternalStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ CONVERT TO REGULAR RESULT ITEM";
    if( context.isProfiling() ) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
