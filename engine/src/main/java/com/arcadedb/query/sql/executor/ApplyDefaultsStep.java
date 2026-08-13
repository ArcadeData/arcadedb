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
import com.arcadedb.schema.DocumentType;

/**
 * Execution step that applies default values to null/missing properties.
 * Used with UPDATE ... APPLY DEFAULTS to trigger default value application during updates.
 *
 * @since Issue #1814
 */
public class ApplyDefaultsStep extends AbstractExecutionStep {

  public ApplyDefaultsStep(final CommandContext context) {
    super(context);
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
          applyDefaults(result.getElement().orElse(null));
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  /**
   * Apply default values to properties that are null or missing.
   * <p>
   * Issue #6134: this used to be a second, independent copy of {@code LocalDatabase.setDefaultValues()} that disagreed
   * with it on null defaults. Both now go through {@link DocumentType#applyDefaultValues(MutableDocument)}.
   */
  private void applyDefaults(final Document doc) {
    if (doc == null) {
      return;
    }

    final MutableDocument mutableDoc = doc.modify();
    final DocumentType type = mutableDoc.getType();

    if (type == null) {
      return;
    }

    type.applyDefaultValues(mutableDoc);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ APPLY DEFAULTS";
  }
}
