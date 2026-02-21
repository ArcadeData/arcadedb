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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * Returns a single result containing detailed information about a specific bucket, including
 * internal storage statistics from {@link LocalBucket#check(int, boolean)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaBucketDetailStep extends AbstractExecutionStep {

  private final String  bucketName;
  private       boolean served = false;

  public FetchFromSchemaBucketDetailStep(final String bucketName, final CommandContext context) {
    super(context);
    this.bucketName = bucketName;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (served)
      return new InternalResultSet();

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Schema schema = context.getDatabase().getSchema();
      final LocalBucket bucket = (LocalBucket) schema.getBucketByName(bucketName);

      final ResultInternal r = new ResultInternal(context.getDatabase());
      r.setProperty("name", bucket.getName());
      r.setProperty("fileId", bucket.getFileId());
      r.setProperty("pageSize", bucket.getPageSize());
      r.setProperty("totalPages", bucket.getTotalPages());

      final Map<String, Object> checkResult = bucket.check(0, false);
      for (final Map.Entry<String, Object> entry : checkResult.entrySet())
        r.setProperty(entry.getKey(), entry.getValue());

      served = true;

      return new InternalResultSet(r);
    } finally {
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH BUCKET DETAIL '" + bucketName + "'";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
