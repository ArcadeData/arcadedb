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
import com.arcadedb.query.sql.parser.BreakStatement;
import com.arcadedb.query.sql.parser.IfStatement;
import com.arcadedb.query.sql.parser.ReturnStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 * <p>
 * This step represents the execution plan of an instruction instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  protected final InternalExecutionPlan plan;

  boolean executed = false;

  public ScriptLineStep(final InternalExecutionPlan nextPlan, final CommandContext context) {
    super(context);
    this.plan = nextPlan;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (!executed) {
      if (plan instanceof InsertExecutionPlan executionPlan)
        executionPlan.executeInternal();
      else if (plan instanceof DeleteExecutionPlan executionPlan)
        executionPlan.executeInternal();
      else if (plan instanceof UpdateExecutionPlan executionPlan)
        executionPlan.executeInternal();
      else if (plan instanceof DDLExecutionPlan executionPlan)
        executionPlan.executeInternal();
      else if (plan instanceof SingleOpExecutionPlan executionPlan) {
        final ResultSet res = executionPlan.executeInternal();
        if (res == BreakStatement.BREAK_RESULTSET)
          return res;
      }

      executed = true;
    }
    return plan.fetchNext(nRecords);
  }

  public boolean containsReturn() {
    if (plan instanceof ScriptExecutionPlan executionPlan)
      return executionPlan.containsReturn();

    else if (plan instanceof SingleOpExecutionPlan executionPlan) {
      return executionPlan.statement instanceof ReturnStatement;

    } else if (plan instanceof IfExecutionPlan) {
      final IfStep step = (IfStep) plan.getSteps().getFirst();
      if (step.positivePlan != null && step.positivePlan.containsReturn())
        return true;
      else if (step.positiveStatements != null) {
        for (final Statement stm : step.positiveStatements) {
          if (containsReturn(stm))
            return true;
        }
      }
    } else if (plan instanceof ForEachExecutionPlan executionPlan)
      return executionPlan.containsReturn();

    return false;
  }

  private boolean containsReturn(final Statement stm) {
    if (stm instanceof ReturnStatement)
      return true;

    if (stm instanceof IfStatement statement) {
      for (final Statement o : statement.getStatements()) {
        if (containsReturn(o))
          return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    plan.close();
    super.close();
  }

  public ExecutionStepInternal executeUntilReturn(final CommandContext context) {
    if (plan instanceof ScriptExecutionPlan executionPlan)
      return executionPlan.executeUntilReturn();
    else if (plan instanceof SingleOpExecutionPlan executionPlan) {
      if (executionPlan.statement instanceof ReturnStatement)
        return new ReturnStep(executionPlan.statement, context);
    } else if (plan instanceof IfExecutionPlan executionPlan)
      return executionPlan.executeUntilReturn();
    else if (plan instanceof BreakStatement)
      return new BreakStep(context);

    throw new NoSuchElementException();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    if (plan == null)
      return "Script Line";
    return plan.prettyPrint(depth, indent);
  }
}
