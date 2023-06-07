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
 * Returns an OResult containing metadata regarding the schema indexes.
 *
 * @author Luca Garulli
 */
public class FetchFromSchemaIndexesStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaIndexesStep(final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        final Schema schema = context.getDatabase().getSchema();

        for (final Index index : schema.getIndexes()) {
          final ResultInternal r = new ResultInternal();
          result.add(r);

          final int fileId = ((IndexInternal) index).getFileId();

          r.setProperty("name", index.getName());
          r.setProperty("typeName", index.getTypeName());
          if (index.getPropertyNames() != null)
            r.setProperty("properties", Arrays.asList(index.getPropertyNames()));

          // KEY TYPES
          final List<String> keyTypes = new ArrayList<>();
          if (((IndexInternal) index).getKeyTypes() != null)
            for (final Type k : ((IndexInternal) index).getKeyTypes())
              keyTypes.add(k.name());
          r.setProperty("keyTypes", keyTypes);

          r.setProperty("unique", index.isUnique());
          r.setProperty("automatic", index.isAutomatic());
          if (index instanceof IndexInternal)
            r.setProperty("compacting", ((IndexInternal) index).isCompacting());

          if (fileId > -1) {
            r.setProperty("fileId", fileId);
            try {
              r.setProperty("size", FileUtils.getSizeAsString(context.getDatabase().getFileManager().getFile(((IndexInternal) index).getFileId()).getSize()));
            } catch (final IOException e) {
              // IGNORE IT, NO SIZE AVAILABLE
            }
          }
          r.setProperty("supportsOrderedIterations", index.supportsOrderedIterations());
          if (index.getAssociatedBucketId() > -1)
            r.setProperty("associatedBucketId", index.getAssociatedBucketId());
          r.setProperty("nullStrategy", index.getNullStrategy());
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
    String result = spaces + "+ FETCH DATABASE METADATA INDEXES";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

}
