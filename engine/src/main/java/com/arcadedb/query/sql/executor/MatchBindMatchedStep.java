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

/**
 * Binds the {@code matched} context variable to every row leaving the pattern-matching part of a MATCH plan, so that
 * {@code $matched.<alias>.<property>} in {@code RETURN} reads the row being projected.
 * <p>
 * The pattern-matching steps themselves bind the variable only while they evaluate a node's {@code where:} - to the partial
 * match the hop starts from, or to the outer tuple a correlated sub-plan runs for - and restore it afterwards. Binding it
 * on emit, as they used to, shadowed the partial match a correlated filter upstream was still reading, since the pipeline
 * pulls rows lazily: the row emitted for one candidate was what the filter saw for the next (issue #7434).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MatchBindMatchedStep extends AbstractExecutionStep {
  /**
   * Name of the context variable behind {@code $matched}. Every step that binds it does so only for the span in which it
   * evaluates something, and restores it afterwards; this step is the one that binds it for the RETURN clause.
   */
  public static final String MATCHED_VARIABLE = "matched";


  public MatchBindMatchedStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious();

    final ResultSet upstream = prev.syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final Result row = upstream.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          context.setVariable(MATCHED_VARIABLE, row);
          return row;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result = ExecutionStepInternal.getIndent(depth, indent) + "+ BIND $matched";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
