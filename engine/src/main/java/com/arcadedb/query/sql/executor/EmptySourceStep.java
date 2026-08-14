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
 * Row source of a statement the planner proved empty by reading the statement alone: a filter that is false for every
 * record ({@code WHERE 1=0}) or a {@code LIMIT 0}. It takes the place of the target fetch, which is why nothing
 * downstream ever asks the storage for a page.
 * <p>
 * It differs from {@link EmptyStep}, which is what the planner chains when the <i>data</i> turns out to make a fetch
 * pointless (an empty bucket list, a variable that resolved to no record), in being cacheable: "this statement cannot
 * return a row" is a property of its text and holds for every execution that reuses the plan, whereas
 * {@link EmptyStep} encodes one execution's data.
 * <p>
 * Like {@link EmptyStep} it does pull its previous step once, so a {@code LET} evaluated once per statement still
 * runs. That costs nothing here because the steps it can follow are exactly those: the fetch is not part of the
 * chain.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EmptySourceStep extends AbstractExecutionStep {
  private final String reason;

  public EmptySourceStep(final CommandContext context, final String reason) {
    super(context);
    this.reason = reason;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    return new InternalResultSet();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new EmptySourceStep(context, reason);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ EMPTY RESULT (" + reason + ")";
  }
}
