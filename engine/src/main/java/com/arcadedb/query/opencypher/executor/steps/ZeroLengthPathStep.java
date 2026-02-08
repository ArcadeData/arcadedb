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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.NoSuchElementException;

/**
 * Execution step for zero-length named paths (e.g., p = (n)).
 * Creates a TraversalPath containing just the source vertex.
 */
public class ZeroLengthPathStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String pathVariable;

  public ZeroLengthPathStep(final String sourceVariable, final String pathVariable, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.pathVariable = pathVariable;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ZeroLengthPathStep requires a previous step");

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
        final ResultInternal result = new ResultInternal();

        // Copy all properties from input
        for (final String prop : input.getPropertyNames())
          result.setProperty(prop, input.getProperty(prop));

        // Create zero-length path from source vertex
        final Object sourceObj = input.getProperty(sourceVariable);
        if (sourceObj instanceof Vertex)
          result.setProperty(pathVariable, new TraversalPath((Vertex) sourceObj));
        else
          result.setProperty(pathVariable, null);

        return result;
      }

      @Override
      public void close() {
        prevResults.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent)) + "+ ZERO-LENGTH PATH (" + sourceVariable + ") as " + pathVariable;
  }
}
