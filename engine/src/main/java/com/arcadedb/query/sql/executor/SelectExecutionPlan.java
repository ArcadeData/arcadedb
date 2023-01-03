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

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class SelectExecutionPlan implements InternalExecutionPlan {
  private       String                      location;
  private final CommandContext              ctx;
  protected     List<ExecutionStepInternal> steps    = new ArrayList<>();
  private       ExecutionStepInternal       lastStep = null;

  public SelectExecutionPlan(final CommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public ResultSet fetchNext(final int n) {
    return lastStep.syncPull(ctx, n);
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

  @Override
  public void reset(final CommandContext ctx) {
    steps.forEach(ExecutionStepInternal::reset);
  }

  public void chain(final ExecutionStepInternal nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override
  public List<ExecutionStep> getSteps() {
    //TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(final List<ExecutionStepInternal> steps) {
    this.steps = steps;
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    } else {
      lastStep = null;
    }
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", steps == null ? null : steps.stream().map(x -> x.toResult()).collect(Collectors.toList()));
    return result;
  }

  public Result serialize() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", steps == null ? null : steps.stream().map(x -> x.serialize()).collect(Collectors.toList()));
    return result;
  }

  public void deserialize(final Result serializedExecutionPlan) {
    final List<Result> serializedSteps = serializedExecutionPlan.getProperty("steps");
    for (final Result serializedStep : serializedSteps) {
      try {
        final String className = serializedStep.getProperty(JAVA_TYPE);
        final ExecutionStepInternal step = (ExecutionStepInternal) Class.forName(className).getConstructor().newInstance();
        step.deserialize(serializedStep);
        chain(step);
      } catch (final Exception e) {
        throw new CommandExecutionException("Cannot deserialize execution step:" + serializedStep, e);
      }
    }
  }

  @Override
  public InternalExecutionPlan copy(final CommandContext ctx) {
    final SelectExecutionPlan copy = new SelectExecutionPlan(ctx);

    ExecutionStepInternal lastStep = null;
    for (final ExecutionStepInternal step : this.steps) {
      final ExecutionStepInternal newStep = (ExecutionStepInternal) step.copy(ctx);
      newStep.setPrevious(lastStep);
      if (lastStep != null) {
        lastStep.setNext(newStep);
      }
      lastStep = newStep;
      copy.getSteps().add(newStep);
    }
    copy.lastStep = copy.steps.get(copy.steps.size() - 1);
    copy.location = location;
    return copy;
  }

  @Override
  public boolean canBeCached() {
    for (final ExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }
}
