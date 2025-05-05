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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SaveElementStep extends AbstractExecutionStep {
  private final Identifier bucket;
  private final boolean    createAlways;

  public SaveElementStep(final CommandContext context, final Identifier bucket, final boolean createAlways) {
    super(context);
    this.bucket = bucket;
    this.createAlways = createAlways;
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
        if (result != null && result.isElement()) {
          final Document doc = result.getElement().orElse(null);

          if (doc == null)
            throw new IllegalArgumentException("Cannot save a null document");

          final MutableDocument modifiableDoc;
//          if (createAlways) {
//            // STRIPE OFF ANY IDENTITY TO FORCE AN INSERT. THIS IS NECESSARY IF THE RECORD IS COMING FROM A SELECT
//            if (doc instanceof Vertex)
//              modifiableDoc = context.getDatabase().newVertex(doc.getTypeName()).fromMap(doc.toMap(false));
//            else if (doc instanceof Edge)
//              throw new IllegalArgumentException("Cannot duplicate an edge");
//            else
//              modifiableDoc = context.getDatabase().newDocument(doc.getTypeName()).fromMap(doc.toMap(false));
//          } else
          modifiableDoc = doc.modify();

          if (bucket == null)
            modifiableDoc.save();
          else
            modifiableDoc.save(bucket.getStringValue());
        }
        return result;
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
    result.append("+ SAVE RECORD");
    if (bucket != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on bucket ").append(bucket);
    }
    return result.toString();
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new SaveElementStep(context, bucket == null ? null : bucket.copy(), createAlways);
  }
}
