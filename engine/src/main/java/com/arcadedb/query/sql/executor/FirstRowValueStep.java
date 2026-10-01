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

import java.util.NoSuchElementException;

/**
 * Turns the first row of an ordered fetch into the one row of a {@code min()} / {@code max()}: the value of {@code
 * source} published under {@code alias}, or a null value when the fetch returned nothing. It is what lets
 * {@code SELECT min(a) FROM T WHERE a > ?} be answered as {@code ORDER BY a LIMIT 1} over an index (issue #8812): the
 * aggregate always yields one row, an empty ordered fetch yields none.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FirstRowValueStep extends AbstractExecutionStep {
  private final String  source;
  private final String  alias;
  private       boolean executed = false;

  public FirstRowValueStep(final String source, final String alias, final CommandContext context) {
    super(context);
    this.source = source;
    this.alias = alias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed)
          throw new NoSuchElementException();
        executed = true;

        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final ResultSet upstream = prev.syncPull(context, 1);
          Object value = null;
          if (upstream.hasNext())
            value = upstream.next().getProperty(source);

          final ResultInternal result = new ResultInternal(context.getDatabase());
          result.setProperty(alias, value);
          return result;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public void close() {
        prev.close();
      }

      @Override
      public void reset() {
        FirstRowValueStep.this.reset();
      }
    };
  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new FirstRowValueStep(source, alias, context);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ FIRST ROW VALUE " + source + " AS " + alias;
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
