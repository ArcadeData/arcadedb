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

import java.util.Collections;
import java.util.List;

/**
 * Execution plan returned by {@code EXPLAIN <statement>} when it is not run through
 * {@code Statement.execute()} directly, but instead chained into an enclosing plan - a script
 * ({@code SQLScriptQueryEngine}), or a nested block (FOREACH, IF, WHILE, RETRY, LET). All of
 * those callers treat whatever {@code Statement.createExecutionPlan(CommandContext)} returns as
 * an executable plan and chain/pull it to completion, so an EXPLAIN whose {@code
 * createExecutionPlan()} passed the wrapped statement's own plan straight through - as it used
 * to - handed them something that, once pulled, actually ran the wrapped statement. For a write
 * statement (UPDATE/INSERT/DELETE/...) that silently performed the write EXPLAIN promised not to
 * (issue #6648).
 * <p>
 * This plan wraps the built-but-not-pulled plan of the wrapped statement and, on
 * {@link #fetchNext(int)}, reports its description without ever pulling it - the same contract
 * {@link com.arcadedb.query.sql.parser.ExplainResultSet} gives callers that go through {@code
 * execute()}. It also deliberately is not an {@code InsertExecutionPlan}/{@code
 * DeleteExecutionPlan}/{@code UpdateExecutionPlan}/{@code DDLExecutionPlan}/{@code
 * SingleOpExecutionPlan}: {@link ScriptLineStep#syncPull} special-cases exactly those types to
 * eagerly run them, so staying outside that set is what keeps a chained EXPLAIN from being run.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ExplainExecutionPlan implements InternalExecutionPlan {
  private final CommandContext context;
  private final ExecutionPlan  wrappedPlan;
  private       boolean        executed = false;

  public ExplainExecutionPlan(final CommandContext context, final ExecutionPlan wrappedPlan) {
    this.context = context;
    this.wrappedPlan = wrappedPlan;
  }

  public ExecutionPlan getWrappedPlan() {
    return wrappedPlan;
  }

  @Override
  public ResultSet fetchNext(final int n) {
    if (executed)
      return new InternalResultSet();

    executed = true;

    final ResultInternal result = new ResultInternal();
    result.setProperty("executionPlan", wrappedPlan.toResult());
    result.setProperty("executionPlanAsString", wrappedPlan.prettyPrint(0, 3));

    final InternalResultSet resultSet = new InternalResultSet(result);
    resultSet.setPlan(this);
    return resultSet;
  }

  @Override
  public void reset(final CommandContext context) {
    executed = false;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return wrappedPlan.prettyPrint(depth, indent);
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("type", "ExplainExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("wrapped", wrappedPlan.toResult());
    return result;
  }
}
