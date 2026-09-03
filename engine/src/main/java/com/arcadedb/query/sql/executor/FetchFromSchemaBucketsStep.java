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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Returns a Result containing metadata regarding the available buckets in the database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaBucketsStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaBucketsStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final Schema schema = context.getDatabase().getSchema();

        final SecurityDatabaseUser currentUser = currentUser(context);

        final List<String> orderedBuckets = schema.getBuckets().stream().map(x -> x.getName()).sorted(String::compareToIgnoreCase)
            .collect(Collectors.toList());
        for (final String bucketName : orderedBuckets) {
          final Bucket bucket = schema.getBucketByName(bucketName);

          // Hide buckets the current user cannot read instead of throwing, the same way schema:types hides
          // restricted types (issue #4238): countBucket() below checks the same permission and throws on the
          // first denied bucket, which aborted the whole listing - and with it every remote-client call that
          // goes through it - for a user allowed to see all the others. A null user (embedded usage, or no
          // security context) sees everything, and so does a bucket the security map does not cover.
          if (currentUser != null && !currentUser.requestAccessOnFile(bucket.getFileId(),
              SecurityDatabaseUser.ACCESS.READ_RECORD))
            continue;

          final ResultInternal r = new ResultInternal(context.getDatabase());
          result.add(r);

          r.setProperty("name", bucket.getName());
          r.setProperty("fileId", bucket.getFileId());
          r.setProperty("records", context.getDatabase().countBucket(bucketName));
          // The bucket's purpose lets tooling (Studio etc.) hide or label internal buckets like paired
          // external-property buckets. Filter via `WHERE purpose = 'PRIMARY'` to see only user-targetable ones.
          if (bucket instanceof LocalBucket lb)
            r.setProperty("purpose", lb.getPurpose().name());

          context.setVariable("current", r);
        }
      } finally {
        if (context.isProfiling()) {
          cost += System.nanoTime() - begin;
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

  private static SecurityDatabaseUser currentUser(final CommandContext context) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final DatabaseContext.DatabaseContextTL dbContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    return dbContext != null ? dbContext.getCurrentUser() : null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA BUCKETS";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
