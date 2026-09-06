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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.LoadCSVRowContext;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Simple projection step that keeps only specified variable names in the result.
 * Used after ORDER BY + LIMIT to strip non-projected variables that were kept
 * for ORDER BY evaluation.
 * <p>
 * "Non-projected variables" means the query's own variables. The LOAD CSV row context is not one of them: it
 * describes the row rather than naming a value the query bound, so it survives the strip the same way it survives
 * the projection {@code WithStep} does (issue #6402). Without that, a {@code WITH} carrying an {@code ORDER BY}
 * was the one projection form that turned {@code file()} and {@code linenumber()} off for the rest of the query -
 * {@code WITH row ORDER BY row RETURN file()} answered {@code null} while the very same {@code WITH} without the
 * {@code ORDER BY} answered the file (issue #7182). Neo4j sorts the rows it was handed rather than building new
 * ones, so the context rides through its sort untouched.
 */
public class VariableProjectionStep extends AbstractExecutionStep {
  private final Set<String> keepVariables;

  public VariableProjectionStep(final Set<String> keepVariables, final CommandContext context) {
    super(context);
    this.keepVariables = keepVariables;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("VariableProjectionStep requires a previous step");

    final ResultSet prevResults = prev.syncPull(context, nRecords);

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return prevResults.hasNext();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result input = prevResults.next();
        final ResultInternal projected = new ResultInternal();
        for (final String var : keepVariables) {
          if (input.getPropertyNames().contains(var))
            projected.setProperty(var, input.getProperty(var));
        }
        LoadCSVRowContext.carryOver(input, projected);
        return projected;
      }

      @Override
      public void close() {
        prevResults.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent)) + "+ PROJECT " + keepVariables;
  }
}
