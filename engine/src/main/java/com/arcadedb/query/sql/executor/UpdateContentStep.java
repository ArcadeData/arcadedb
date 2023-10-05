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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.Json;
import com.arcadedb.query.sql.parser.JsonArray;
import com.arcadedb.serializer.json.JSONArray;

import java.util.*;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateContentStep extends AbstractExecutionStep {
  private Json           json;
  private JsonArray      jsonArray;
  private int            arrayIndex = 0;
  private InputParameter inputParameter;

  public UpdateContentStep(final Json json, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.json = json;
  }

  public UpdateContentStep(final JsonArray jsonArray, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.jsonArray = jsonArray;
  }

  public UpdateContentStep(final InputParameter inputParameter, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.inputParameter = inputParameter;
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

        if (result instanceof ResultInternal) {
          if (!(result.getElement().get() instanceof Document))
            ((ResultInternal) result).setElement((Document) result.getElement().get().getRecord());

          if (!(result.getElement().get() instanceof Document))
            return result;

          handleContent(result.getElement().get(), context);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  private void handleContent(final Document record, final CommandContext context) {
    // REPLACE ALL THE CONTENT
    final MutableDocument doc = record.modify();

    if (json != null) {
      doc.fromMap(json.toMap(record, context));
    } else if (jsonArray != null && arrayIndex < jsonArray.items.size()) {
      final Json jsonItem = jsonArray.items.get(arrayIndex++);
      doc.fromMap(jsonItem.toMap(record, context));
    } else if (inputParameter != null) {
      final Object val = inputParameter.getValue(context.getInputParameters());
      if (val instanceof Document) {
        doc.fromMap(((Document) val).getRecord().toJSON().toMap());
      } else if (val instanceof Map) {
        doc.fromMap((Map<String, Object>) val);
      } else {
        throw new CommandExecutionException("Invalid value for UPDATE CONTENT: " + val);
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    if (json != null)
      result.append(json);
    else
      result.append(inputParameter);
    return result.toString();
  }
}
