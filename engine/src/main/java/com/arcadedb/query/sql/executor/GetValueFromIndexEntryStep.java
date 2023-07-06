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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;

import java.util.*;
import java.util.logging.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 16/03/17.
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final int[] filterClusterIds;

  // runtime

  private ResultSet prevResult = null;

  /**
   * @param context          the execution context
   * @param filterClusterIds only extract values from these clusters. Pass null if no filtering is needed
   * @param profilingEnabled enable profiling
   */
  public GetValueFromIndexEntryStep(final CommandContext context, final int[] filterClusterIds, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.filterClusterIds = filterClusterIds;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    return new ResultSet() {

      public boolean finished = false;

      Result nextItem = null;
      int fetched = 0;

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
          final Result val = prevResult.next();
          final long begin = profilingEnabled ? System.nanoTime() : 0;

          try {
            final Object finalVal = val.getProperty("rid");
            if (filterClusterIds != null) {
              if (!(finalVal instanceof Identifiable)) {
                continue;
              }
              final RID rid = ((Identifiable) finalVal).getIdentity();
              boolean found = false;
              for (final int filterClusterId : filterClusterIds) {
                if (rid.getBucketId() < 0 || filterClusterId == rid.getBucketId()) {
                  found = true;
                  break;
                }
              }
              if (!found) {
                continue;
              }
            }

            if (finalVal instanceof RID) {
              try {
                nextItem = new ResultInternal(((RID) finalVal).asDocument());
              } catch (final RecordNotFoundException e) {
                LogManager.instance().log(this, Level.WARNING, "Record %s not found. Skip it from the result set", null, finalVal);
                continue;
              }
            } else if (finalVal instanceof Document) {
              nextItem = new ResultInternal((Document) finalVal);
            } else if (finalVal instanceof Result) {
              nextItem = (Result) finalVal;
            }
            break;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXTRACT VALUE FROM INDEX ENTRY";

    if (profilingEnabled)
      result += " (" + getCostFormatted() + ")";

    if (filterClusterIds != null) {
      result += "\n";
      result += spaces;
      result += "  filtering clusters [";
      result += Arrays.stream(filterClusterIds).boxed().map(x -> "" + x).collect(Collectors.joining(","));
      result += "]";
    }

    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new GetValueFromIndexEntryStep(context, this.filterClusterIds, this.profilingEnabled);
  }
}
