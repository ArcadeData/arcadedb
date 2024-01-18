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
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Created by luigidellaquila on 01/03/17.
 */
public class FilterByTypeStep extends AbstractExecutionStep {
  private final Identifier identifier;

  //runtime

  ResultSet prevResult = null;

  public FilterByTypeStep(final Identifier identifier, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.identifier = identifier;
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
        if (finished) {
          return;
        }
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
          final long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nextItem.isElement()) {
              final DocumentType typez = nextItem.getElement().get().getType();
              if (typez != null && typez.isSubTypeOf(identifier.getStringValue())) {
                break;
              }
            }
            nextItem = null;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
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
        FilterByTypeStep.this.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    result.append(ExecutionStepInternal.getIndent(depth, indent));
    result.append("+ FILTER ITEMS BY TYPE");
    if (profilingEnabled)
      result.append(" (").append(getCostFormatted()).append(")");

    result.append(" \n");
    result.append(ExecutionStepInternal.getIndent(depth, indent));
    result.append("  ");
    result.append(identifier.getStringValue());
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FilterByTypeStep(this.identifier.copy(), context, this.profilingEnabled);
  }
}
