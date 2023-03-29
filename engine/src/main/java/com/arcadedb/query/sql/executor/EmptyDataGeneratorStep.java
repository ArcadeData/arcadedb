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

import java.util.*;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {
  final int size;
  int served = 0;

  public EmptyDataGeneratorStep(final int size, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.size = size;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return served < size;
      }

      @Override
      public Result next() {
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (served < size) {
            served++;
            final ResultInternal result = new ResultInternal();
            context.setVariable("current", result);
            return result;
          }
          throw new NoSuchElementException();
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void reset() {
        served = 0;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
