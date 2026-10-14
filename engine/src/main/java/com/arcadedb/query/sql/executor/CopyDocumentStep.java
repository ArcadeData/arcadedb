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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;

import java.util.Map;

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
            // The record decides the kind of the copy (vertex or document), the row its properties: a projection that
            // excluded or overrode a column has already decided what the row contains, and copying the record's own
            // properties wrote back an excluded column and the stored value of an overridden one (issue #9965)
            final Record docToCopy = toCopy.getElement().get().getRecord();
            final Map<String, Object> properties = propertiesToWrite(toCopy);

            if (docToCopy instanceof Vertex vertex) {
              if (targetType != null) {
                resultDoc = getContext().getDatabase().newVertex(targetType);
              } else {
                resultDoc = getContext().getDatabase().newVertex(vertex.getTypeName());
              }
              ((MutableVertex) resultDoc).set(properties);
            } else if (docToCopy instanceof Document document) {
              if (targetType != null) {
                resultDoc = getContext().getDatabase().newDocument(targetType);
              } else {
                resultDoc = getContext().getDatabase().newDocument(document.getTypeName());
              }
              ((MutableDocument) resultDoc).set(properties);
            }
          } else {
            final DocumentType type = context.getDatabase().getSchema().getType(targetType);
            resultDoc = type.newRecord().set(propertiesToWrite(toCopy)).save();
          }
          return new UpdatableResult((MutableDocument) resultDoc);
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public void close() {
        upstream.close();
      }

    };
  }

  /**
   * The properties a statement writes from a row ({@code INSERT ... FROM SELECT}, {@code UPDATE ... MERGE} of a sub-query): the
   * row's own properties ({@link Result#toPropertyMap(boolean)}), less the names that are not properties of a record -
   * metadata ({@code @rid}, {@code @type}, ...) and the {@code $} variables a query projects ({@code $score},
   * {@code $distance}, ...) - so neither ends up stored on the target record. A {@code $} name the row's own record stores
   * is a property like any other, and is written.
   */
  static Map<String, Object> propertiesToWrite(final Result row) {
    final Map<String, Object> properties = row.toPropertyMap(false);
    // The source record is asked only about a $ name, which most rows do not have
    properties.keySet().removeIf(name -> name.isEmpty() || name.charAt(0) == '@' ||//
        (name.charAt(0) == '$' && !(row.isElement() && row.toElement().has(name))));
    return properties;
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
