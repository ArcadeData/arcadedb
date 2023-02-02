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
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetExpressionStep extends AbstractExecutionStep {
  private final Identifier varName;
  private final Expression expression;

  public LetExpressionStep(final Identifier varName, final Expression expression, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.varName = varName;
    this.expression = expression;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("Cannot execute a local LET on a query without a target");

    return new ResultSet() {
      final ResultSet source = getPrev().syncPull(context, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        final ResultInternal result = (ResultInternal) source.next();
        final Object value = expression.execute(result, context);
        result.setMetadata(varName.getStringValue(), value);
        context.setVariable(varName.getStringValue(), value);
        return result;
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = " + expression;
  }
}
