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

/**
 * Created by luigidellaquila on 08/08/16.
 */

import com.arcadedb.query.sql.parser.Statement;

import java.util.*;
import java.util.stream.*;

import static com.arcadedb.query.sql.executor.AbstractExecutionStep.DEFAULT_FETCH_RECORDS_PER_PULL;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ScriptExecutionPlan implements InternalExecutionPlan {
  private final CommandContext        context;
  private       boolean               executed    = false;
  protected     List<ScriptLineStep>  steps       = new ArrayList<>();
  private       ExecutionStepInternal lastStep    = null;
  private       ResultSet             finalResult = null;
  private       List<Statement>       statements;
  private       String                statementAsString;

  public ScriptExecutionPlan(final CommandContext context) {
    this.context = context;
  }

  @Override
  public void reset(final CommandContext context) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public ResultSet fetchNext(final int n) {
    doExecute(n);
    return new ResultSet() {

      @Override
      public boolean hasNext() {
        final int totalFetched = 0;
        return finalResult.hasNext() && totalFetched < n;
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return finalResult.next();
      }

      @Override
      public void close() {
        finalResult.close();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return finalResult == null ? Optional.empty() : finalResult.getExecutionPlan();
      }

    };
  }

  private void doExecute(final int n) {
    if (!executed) {
      executeUntilReturn();
      executed = true;
      finalResult = new InternalResultSet();
      if (lastStep != null) {
        ResultSet partial = lastStep.syncPull(context, n);
        while (partial.hasNext()) {
          while (partial.hasNext()) {
            ((InternalResultSet) finalResult).add(partial.next());
          }
          partial = lastStep.syncPull(context, n);
        }
        if (lastStep instanceof ScriptLineStep step) {
          ((InternalResultSet) finalResult).setPlan(step.plan);
        }
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    for (int i = 0; i < steps.size(); i++) {
      final ExecutionStepInternal step = steps.get(i);
      result.append(step.prettyPrint(depth, indent));
      if (i < steps.size() - 1) {
        result.append("\n");
      }
    }
    return result.toString();
  }

  public void chain(final InternalExecutionPlan nextPlan) {
    final ScriptLineStep lastStep = steps.isEmpty() ? null : steps.getLast();
    final ScriptLineStep nextStep = new ScriptLineStep(nextPlan, context);
    if (lastStep != null)
      nextStep.setPrevious(lastStep);

    steps.add(nextStep);
    this.lastStep = nextStep;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(final List<ExecutionStepInternal> steps) {
    this.steps = (List) steps;
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("type", "ScriptExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", steps == null ? null : steps.stream().map(x -> x.toResult()).collect(Collectors.toList()));
    return result;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public boolean containsReturn() {
    for (final ExecutionStepInternal step : steps) {
      if (step instanceof ReturnStep) {
        return true;
      }
      if (step instanceof ScriptLineStep lineStep) {
        return lineStep.containsReturn();
      }
    }

    return false;
  }

  /**
   * executes all the script and returns last statement execution step, so that it can be executed
   * from outside
   *
   * @return
   */
  public ExecutionStepInternal executeUntilReturn() {
    if (steps.size() > 0) {
      lastStep = steps.getLast();
      for (int i = 0; i < steps.size() - 1; i++) {
        final ScriptLineStep step = steps.get(i);
        if (step.containsReturn()) {
          final ExecutionStepInternal returnStep = step.executeUntilReturn(context);
          if (returnStep != null) {
            lastStep = returnStep;
            return lastStep;
          }
        }
        ResultSet lastResult = step.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);

        while (lastResult.hasNext()) {
          while (lastResult.hasNext()) {
            lastResult.next();
          }
          lastResult = step.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
        }
      }
      this.lastStep = steps.getLast();
    }
    return lastStep;
  }

  /**
   * executes the whole script and returns last statement ONLY if it's a RETURN, otherwise it
   * returns null;
   *
   * @return
   */
  public ExecutionStepInternal executeFull() {
    for (final ScriptLineStep step : steps) {
      if (step.containsReturn()) {
        final ExecutionStepInternal returnStep = step.executeUntilReturn(context);
        if (returnStep != null) {
          return returnStep;
        }
      }
      ResultSet lastResult = step.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);

      while (lastResult.hasNext()) {
        while (lastResult.hasNext()) {
          lastResult.next();
        }
        lastResult = step.syncPull(context, DEFAULT_FETCH_RECORDS_PER_PULL);
      }
    }

    return null;
  }

  @Override
  public String getStatement() {
    if (statementAsString == null)
      statementAsString = statements.stream().map(Statement::toString).collect(Collectors.joining(";"));
    return statementAsString;
  }

  @Override
  public void setStatements(final List<Statement> statements) {
    this.statements = statements;
  }
}
