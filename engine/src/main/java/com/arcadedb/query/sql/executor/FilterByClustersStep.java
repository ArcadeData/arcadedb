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

import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 01/03/17.
 */
public class FilterByClustersStep extends AbstractExecutionStep {
  private final Set<String>  clusters;
  private       Set<Integer> bucketIds;

  ResultSet prevResult = null;

  public FilterByClustersStep(final Set<String> filterClusters, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.clusters = filterClusters;
    final Database db = context.getDatabase();
    init(db);

  }

  private void init(final Database db) {
    if (this.bucketIds == null) {
      this.bucketIds = clusters.stream().filter(x -> x != null).map(x -> db.getSchema().getBucketByName(x).getFileId()).collect(Collectors.toSet());
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    init(context.getDatabase());
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
          if (nextItem.isElement()) {
            final int bucketId = nextItem.getIdentity().get().getBucketId();
            if (bucketId < 0) {
              // this record comes from a TX, it still doesn't have a bucket assigned
              break;
            }
            if (bucketIds.contains(bucketId)) {
              break;
            }
          }
          nextItem = null;
        }
      }

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        return nextItem != null;

      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished) {
          throw new NoSuchElementException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new NoSuchElementException();
        }
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterByClustersStep.this.close();
      }

    };

  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FILTER ITEMS BY CLUSTERS \n" + ExecutionStepInternal.getIndent(depth, indent) + "  "
        + String.join(", ", clusters);
  }

}
