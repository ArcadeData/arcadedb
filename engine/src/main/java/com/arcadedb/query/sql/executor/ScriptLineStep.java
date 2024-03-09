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
      if (plan instanceof InsertExecutionPlan) {
        ((InsertExecutionPlan) plan).executeInternal();
      } else if (plan instanceof DeleteExecutionPlan) {
        ((DeleteExecutionPlan) plan).executeInternal();
      } else if (plan instanceof UpdateExecutionPlan) {
        ((UpdateExecutionPlan) plan).executeInternal();
      } else if (plan instanceof DDLExecutionPlan) {
        ((DDLExecutionPlan) plan).executeInternal();
      } else if (plan instanceof SingleOpExecutionPlan) {
        ((SingleOpExecutionPlan) plan).executeInternal();
      }
      executed = true;
    }
    return plan.fetchNext(nRecords);
  }

  public boolean containsReturn() {
    if (plan instanceof ScriptExecutionPlan) {
      return ((ScriptExecutionPlan) plan).containsReturn();
    }
    if (plan instanceof SingleOpExecutionPlan) {
      if (((SingleOpExecutionPlan) plan).statement instanceof ReturnStatement) {
        return true;
      }
    }
    if (plan instanceof IfExecutionPlan) {
      final IfStep step = (IfStep) plan.getSteps().get(0);
      if (step.positivePlan != null && step.positivePlan.containsReturn()) {
        return true;
      } else if (step.positiveStatements != null) {
        for (final Statement stm : step.positiveStatements) {
          if (containsReturn(stm)) {
            return true;
          }
        }
      }
    }

    if (plan instanceof ForEachExecutionPlan) {
      return ((ForEachExecutionPlan) plan).containsReturn();
    }
    return false;
  }

  private boolean containsReturn(final Statement stm) {
    if (stm instanceof ReturnStatement)
      return true;

    if (stm instanceof IfStatement) {
      for (final Statement o : ((IfStatement) stm).getStatements()) {
        if (containsReturn(o)) {
          return true;
        }
      }
    }
    return false;
  }

  public ExecutionStepInternal executeUntilReturn(final CommandContext context) {
    if (plan instanceof ScriptExecutionPlan)
      return ((ScriptExecutionPlan) plan).executeUntilReturn();

    if (plan instanceof SingleOpExecutionPlan) {
      if (((SingleOpExecutionPlan) plan).statement instanceof ReturnStatement) {
        return new ReturnStep(((SingleOpExecutionPlan) plan).statement, context);
      }
    }
    if (plan instanceof IfExecutionPlan)
      return ((IfExecutionPlan) plan).executeUntilReturn();

    throw new NoSuchElementException();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (plan == null)
      return "Script Line";

    return plan.prettyPrint(depth, indent);
  }
}
