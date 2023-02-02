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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Returns the number of records contained in a class (including subTypes) Executes a count(*) on a class and returns a single
 * record that contains that value (with a specific alias).
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - gmail.com)
 */
public class CountFromClassStep extends AbstractExecutionStep {
  private final Identifier target;
  private final String     alias;

  private boolean executed = false;

  /**
   * @param targetClass      An identifier containing the name of the class to count
   * @param alias            the name of the property returned in the result-set
   * @param context          the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountFromClassStep(final Identifier targetClass, final String alias, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.target = targetClass;
    this.alias = alias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Result next() {
        if (executed) {
          throw new NoSuchElementException();
        }
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          final DocumentType typez = context.getDatabase().getSchema().getType(target.getStringValue());
          if (typez == null) {
            throw new CommandExecutionException("Type " + target.getStringValue() + " does not exist in the database schema");
          }

          final long size = context.getDatabase().countType(target.getStringValue(), true);
          executed = true;
          final ResultInternal result = new ResultInternal();
          result.setProperty(alias, size);
          return result;

        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void reset() {
        CountFromClassStep.this.reset();
      }
    };
  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE USERTYPE SIZE: " + target;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
