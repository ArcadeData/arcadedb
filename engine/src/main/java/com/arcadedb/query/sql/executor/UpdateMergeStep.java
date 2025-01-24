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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Json;

import java.util.*;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateMergeStep extends AbstractExecutionStep {
  private final Json json;

  public UpdateMergeStep(final Json json, final CommandContext context) {
    super(context);
    this.json = json;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final Result result = upstream.next();
        if (result instanceof ResultInternal internal) {
          if (!(result.getElement().orElse(null) instanceof Document)) {
            internal.setElement((Document) result.getElement().get().getRecord());
          }
          if (!(result.getElement().orElse(null) instanceof Document)) {
            return result;
          }
          handleMerge(result.getElement().orElse(null), context);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  private void handleMerge(final Record record, final CommandContext context) {
    final MutableDocument doc = ((Document) record).modify();
    final Map<String, Object> map = json.toMap(record, context);
    for (final Map.Entry<String, Object> entry : map.entrySet())
      doc.set(entry.getKey(), entry.getValue());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final String result = spaces + "+ UPDATE MERGE\n" + spaces + "  " + json;
    return result;
  }
}
