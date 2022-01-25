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

import java.util.Map;
import java.util.Optional;

/**
 * <p>Reads an upstream result set and returns a new result set that contains copies of the original OResult instances
 * </p>
 * <p>This is mainly used from statements that need to copy of the original data to save it somewhere else,
 * eg. INSERT ... FROM SELECT</p>
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CopyDocumentStep extends AbstractExecutionStep {

  private final String targetType;
  private       long   cost = 0;

  public CopyDocumentStep(CommandContext ctx, String targetType, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetType = targetType;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    ResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        Result toCopy = upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Record resultDoc = null;
          if (toCopy.isElement()) {

            Record docToCopy = toCopy.getElement().get().getRecord();

            if (docToCopy instanceof Document) {
              if (targetType != null) {
                resultDoc = getContext().getDatabase().newDocument(targetType);
              } else {
                resultDoc = getContext().getDatabase().newDocument(((Document) docToCopy).getTypeName());
              }
              ((MutableDocument) resultDoc).set(((Document) docToCopy).toMap());
            }
          } else {
            resultDoc = toCopy.toElement().getRecord();
          }
          return new UpdatableResult((MutableDocument) resultDoc);
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        upstream.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY DOCUMENT");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
