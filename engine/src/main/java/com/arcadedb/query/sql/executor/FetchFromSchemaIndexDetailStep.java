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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.util.*;

/**
 * Returns a single result containing detailed information about a specific index, including
 * internal statistics from {@link IndexInternal#getStats()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaIndexDetailStep extends AbstractExecutionStep {

  private final String  indexName;
  private       boolean served = false;

  public FetchFromSchemaIndexDetailStep(final String indexName, final CommandContext context) {
    super(context);
    this.indexName = indexName;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (served)
      return new InternalResultSet();

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final Schema schema = context.getDatabase().getSchema();
      final Index index = schema.getIndexByName(indexName);
      final IndexInternal indexInternal = (IndexInternal) index;

      final ResultInternal r = new ResultInternal(context.getDatabase());
      r.setProperty("name", index.getName());
      r.setProperty("indexType", index.getType());
      r.setProperty("typeName", index.getTypeName());

      if (index.getPropertyNames() != null)
        r.setProperty("properties", Collections.singletonList(index.getPropertyNames()));

      final List<String> keyTypes = new ArrayList<>();
      if (indexInternal.getKeyTypes() != null)
        for (final Type k : indexInternal.getKeyTypes())
          keyTypes.add(k.name());
      r.setProperty("keyTypes", keyTypes);

      r.setProperty("unique", index.isUnique());
      r.setProperty("automatic", index.isAutomatic());
      r.setProperty("compacting", indexInternal.isCompacting());
      r.setProperty("valid", indexInternal.isValid());
      r.setProperty("supportsOrderedIterations", index.supportsOrderedIterations());
      r.setProperty("nullStrategy", index.getNullStrategy());

      if (index.getAssociatedBucketId() > -1)
        r.setProperty("associatedBucketId", index.getAssociatedBucketId());

      final int fileId = indexInternal.getFileId();
      if (fileId > -1) {
        r.setProperty("fileId", fileId);
        try {
          r.setProperty("size", FileUtils.getSizeAsString(context.getDatabase().getFileManager().getFile(fileId).getSize()));
        } catch (final IOException e) {
          // IGNORE IT, NO SIZE AVAILABLE
        }
      }

      final Map<String, Long> stats = indexInternal.getStats();
      if (stats != null)
        for (final Map.Entry<String, Long> entry : stats.entrySet())
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
    String result = spaces + "+ FETCH INDEX DETAIL '" + indexName + "'";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
