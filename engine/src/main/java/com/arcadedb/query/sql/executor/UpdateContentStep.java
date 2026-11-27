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
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.Json;
import com.arcadedb.query.sql.parser.JsonArray;
import com.arcadedb.query.sql.parser.UpdateOperations;

import java.util.List;
import java.util.Map;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateContentStep extends AbstractExecutionStep {
  private Json           json;
  private JsonArray      jsonArray;
  private int            arrayIndex = 0;
  private InputParameter inputParameter;
  private List<?>        parameterArray; // For handling List parameters
  private String         statement; // the statement a CONTENT parameter belongs to, for the error message

  public UpdateContentStep(final Json json, final CommandContext context) {
    super(context);
    this.json = json;
  }

  public UpdateContentStep(final JsonArray jsonArray, final CommandContext context) {
    super(context);
    this.jsonArray = jsonArray;
  }

  /**
   * @param statement the statement name, for the error message a parameter of the wrong kind is refused with
   */
  public UpdateContentStep(final InputParameter inputParameter, final String statement, final CommandContext context) {
    super(context);
    this.inputParameter = inputParameter;
    this.statement = statement;
  }

  /**
   * Builds the step for a {@code CONTENT} operation from whichever payload form it was written with: a JSON object, a JSON
   * array or an input parameter. The single definition every statement taking {@code CONTENT} goes through, so a payload form
   * cannot be honoured by one statement and dropped by another (issue #12326: {@code MOVE VERTEX ... CONTENT :param} read only the
   * JSON object and ignored the parameter).
   *
   * @param statement the statement name, for the error message
   *
   * @throws CommandExecutionException if the operation carries no payload
   */
  public static UpdateContentStep of(final UpdateOperations op, final String statement, final CommandContext context) {
    if (op.getJson() != null)
      return new UpdateContentStep(op.getJson(), context);
    if (op.getJsonArray() != null)
      return new UpdateContentStep(op.getJsonArray(), context);
    if (op.getInputParam() != null)
      return new UpdateContentStep(op.getInputParam(), statement, context);
    throw new CommandExecutionException("Missing payload for " + statement + " ... CONTENT: " + op);
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
          if (!(result.getElement().get() instanceof Document))
            internal.setElement((Document) result.getElement().get().getRecord());

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
    } else if (parameterArray != null && arrayIndex < parameterArray.size()) {
      // Handle List parameter like jsonArray
      final Object item = parameterArray.get(arrayIndex++);
      if (item instanceof Document document) {
        doc.fromMap(document.getRecord().toJSON().toMap());
      } else if (item instanceof Map) {
        doc.fromMap((Map<String, Object>) item);
      } else {
        throw invalidContent("array item", item, "the items of a CONTENT list must be maps or documents");
      }
    } else if (inputParameter != null && parameterArray == null) {
      // Initialize parameterArray on first call if the parameter is a List
      final Object val = inputParameter.getValue(context.getInputParameters());
      if (val instanceof List) {
        parameterArray = (List<?>) val;
        // Process first item
        handleContent(record, context);
      } else if (val instanceof Document document) {
        doc.fromMap(document.getRecord().toJSON().toMap());
      } else if (val instanceof Map) {
        doc.fromMap((Map<String, Object>) val);
      } else {
        throw invalidContent("value", val, "a CONTENT parameter must be a map, a document or a list of them");
      }
    }
  }

  /**
   * A CONTENT parameter bound to a value of the wrong kind: the caller's mistake, a client error (issue #12795, where it answered
   * 500 and named UPDATE for an INSERT too).
   */
  private TypeMismatchException invalidContent(final String what, final Object value, final String expected) {
    final String kind = value != null ? " (" + value.getClass().getSimpleName() + ")" : "";
    return new TypeMismatchException(
        "Invalid " + what + " for " + statement + " CONTENT: " + InvalidArgumentException.quote(value) + kind + ", " + expected);
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
    else if (jsonArray != null)
      result.append(jsonArray);
    else
      result.append(inputParameter);
    return result.toString();
  }
}
