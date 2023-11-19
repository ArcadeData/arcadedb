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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;
import java.util.stream.*;

/**
 * Returns a Result containing metadata regarding the available buckets in the database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaBucketsStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaBucketsStep(final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        final Schema schema = context.getDatabase().getSchema();

        final List<String> orderedBuckets = schema.getBuckets().stream().map(x -> x.getName()).sorted(String::compareToIgnoreCase)
            .collect(Collectors.toList());
        for (final String bucketName : orderedBuckets) {
          final Bucket bucket = schema.getBucketByName(bucketName);

          final ResultInternal r = new ResultInternal();
          result.add(r);

          r.setProperty("name", bucket.getName());
          r.setProperty("fileId", bucket.getFileId());
          r.setProperty("records", context.getDatabase().countBucket(bucketName));

          context.setVariable("current", r);
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        result.clear();
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA BUCKETS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
