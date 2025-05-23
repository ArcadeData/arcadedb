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

import java.util.List;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class IfExecutionPlan implements InternalExecutionPlan {

  private final CommandContext context;

  protected IfStep step;

  public IfExecutionPlan(final CommandContext context) {
    this.context = context;
  }

  @Override
  public void reset(final CommandContext context) {
    //TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    step.close();
  }

  @Override
  public ResultSet fetchNext(final int n) {
    return step.syncPull(context, n);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return step.prettyPrint(depth, indent);
  }

  public void chain(final IfStep step) {
    this.step = step;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    //TODO do a copy of the steps
    return List.of(step);
  }

  public void setSteps(final List<ExecutionStepInternal> steps) {
    this.step = (IfStep) steps.getFirst();
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("type", "IfExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", List.of(step.toResult()));
    return result;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public ExecutionStepInternal executeUntilReturn() {
    if (step.evaluate(context)) {
      step.initPositivePlan(context);
      return step.positivePlan.executeUntilReturn();
    } else {
      step.initNegativePlan(context);
      if (step.negativePlan != null) {
        return step.negativePlan.executeUntilReturn();
      }
    }
    return null;
  }
}
