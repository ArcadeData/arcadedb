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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;

import java.util.*;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class InsertValuesStep extends AbstractExecutionStep {
  private final List<Identifier>       identifiers;
  private final List<List<Expression>> values;
  private       int                    nextValueSet = 0;

  public InsertValuesStep(final List<Identifier> identifierList, final List<List<Expression>> valueExpressions, final CommandContext context,
      final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.identifiers = identifierList;
    this.values = valueExpressions;
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
        Result result = upstream.next();
        if (!(result instanceof ResultInternal)) {
          if (!result.isElement()) {
            throw new CommandExecutionException("Error executing INSERT, cannot modify element: " + result);
          }
          result = new UpdatableResult((MutableDocument) result.getElement().get());
        }
        final List<Expression> currentValues = values.get(nextValueSet++);
        if (currentValues.size() != identifiers.size()) {
          throw new CommandExecutionException(
              "Cannot execute INSERT, the number of fields is different from the number of expressions: " + identifiers + " " + currentValues);
        }
        nextValueSet %= values.size();
        for (int i = 0; i < currentValues.size(); i++) {
          final Identifier identifier = identifiers.get(i);
          final Object value = currentValues.get(i).execute(result, context);
          ((ResultInternal) result).setProperty(identifier.getStringValue(), value);
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
    result.append("+ SET VALUES \n");
    result.append(spaces);
    result.append("  (");
    for (int i = 0; i < identifiers.size(); i++) {
      if (i > 0) {
        result.append(", ");
      }
      result.append(identifiers.get(i));
    }
    result.append(")\n");

    result.append(spaces);
    result.append("  VALUES\n");

    for (int c = 0; c < this.values.size(); c++) {
      if (c > 0) {
        result.append("\n");
      }
      final List<Expression> exprs = this.values.get(c);
      result.append(spaces);
      result.append("  (");
      for (int i = 0; i < exprs.size() && i < 3; i++) {
        if (i > 0) {
          result.append(", ");
        }
        result.append(exprs.get(i));
      }
      result.append(")");
    }
    if (this.values.size() >= 3) {
      result.append(spaces);
      result.append("  ...");

    }
    return result.toString();
  }
}
