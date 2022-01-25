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
package com.arcadedb.query.sql.parser;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.*;

import java.util.List;

public class WhileStep extends AbstractExecutionStep {
  private final BooleanExpression condition;
  private final List<Statement>   statements;

  private ExecutionStepInternal finalResult = null;

  public WhileStep(BooleanExpression condition, List<Statement> statements, CommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.condition = condition;
    this.statements = statements;
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
    prev.ifPresent(x -> x.syncPull(ctx, nRecords));
    if (finalResult != null) {
      return finalResult.syncPull(ctx, nRecords);
    }

    while (condition.evaluate(new ResultInternal(), ctx)) {

      ScriptExecutionPlan plan = initPlan(ctx);
      ExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        this.finalResult = result;
        return result.syncPull(ctx, nRecords);
      }
    }
    finalResult = new EmptyStep(ctx, false);
    return finalResult.syncPull(ctx, nRecords);

  }

  public ScriptExecutionPlan initPlan(CommandContext ctx) {
    BasicCommandContext subCtx1 = new BasicCommandContext();
    subCtx1.setParent(ctx);
    ScriptExecutionPlan plan = new ScriptExecutionPlan(subCtx1);
    for (Statement stm : statements) {
      if (stm.originalStatement == null) {
        stm.originalStatement = stm.toString();
      }
      InternalExecutionPlan subPlan = stm.createExecutionPlan(subCtx1, profilingEnabled);
      plan.chain(subPlan, profilingEnabled);
    }
    return plan;
  }

  public boolean containsReturn() {
    for (Statement stm : this.statements) {
      if (stm instanceof ReturnStatement) {
        return true;
      }
      if (stm instanceof ForEachBlock && ((ForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof IfStatement && ((IfStatement) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof WhileBlock && ((WhileBlock) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
