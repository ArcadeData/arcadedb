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
import com.arcadedb.schema.DocumentType;

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

  public CopyDocumentStep(final CommandContext context, final String targetType) {
    super(context);
    this.targetType = targetType;
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
        final Result toCopy = upstream.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          Record resultDoc = null;
          if (toCopy.isElement()) {
            final Record docToCopy = toCopy.getElement().get().getRecord();

            if (docToCopy instanceof Document document) {
              if (targetType != null) {
                resultDoc = getContext().getDatabase().newDocument(targetType);
              } else {
                resultDoc = getContext().getDatabase().newDocument(document.getTypeName());
              }
              ((MutableDocument) resultDoc).set(document.toMap(false));
            }
          } else {
            final DocumentType type = context.getDatabase().getSchema().getType(targetType);
            resultDoc = type.newRecord().set(toCopy.toMap()).save();
          }
          return new UpdatableResult((MutableDocument) resultDoc);
        } finally {
          if (context.isProfiling())
            cost += (System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {
        upstream.close();
      }

    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY DOCUMENT");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    return result.toString();
  }

}
