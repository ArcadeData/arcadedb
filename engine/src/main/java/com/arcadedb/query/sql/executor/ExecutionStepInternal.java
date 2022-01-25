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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>Execution Steps are the building blocks of a query execution plan</p> <p>Typically an execution plan is made of a chain of
 * steps. The execution is pull-based, meaning that the result set that the client iterates is conceptually the one returned by
 * <i>last</i> step of the execution plan</p> <p>At each `next()` invocation, the step typically fetches a record from the previous
 * (upstream) step, does its elaboration (eg. for a filtering step, it can discard the record and fetch another one if it doesn't
 * match the conditions) and returns the elaborated step</p>
 * <br>
 * <p>The invocation of {@literal syncPull(ctx, nResults)} has to return a result set of at most nResults records. If the upstream
 * (the previous steps) return more records, they have to be returned by next call of {@literal syncPull()}. The returned result
 * set can have less than nResults records ONLY if current step cannot produce any more records (eg. the upstream does not have any
 * more records)</p>
 *
 * @author Luigi Dell'Aquila luigi.dellaquila-(at)-gmail.com
 */
public interface ExecutionStepInternal extends ExecutionStep {

  static String getIndent(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < indent; j++) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  static ResultInternal basicSerialize(ExecutionStepInternal step) {
    ResultInternal result = new ResultInternal();
    result.setProperty(InternalExecutionPlan.JAVA_TYPE, step.getClass().getName());
    if (step.getSubSteps() != null && step.getSubSteps().size() > 0) {
      List<Result> serializedSubsteps = new ArrayList<>();
      for (ExecutionStep substep : step.getSubSteps()) {
        serializedSubsteps.add(((ExecutionStepInternal) substep).serialize());
      }
      result.setProperty("subSteps", serializedSubsteps);
    }

    if (step.getSubExecutionPlans() != null && step.getSubExecutionPlans().size() > 0) {
      List<Result> serializedSubPlans = new ArrayList<>();
      for (ExecutionPlan substep : step.getSubExecutionPlans()) {
        serializedSubPlans.add(((InternalExecutionPlan) substep).serialize());
      }
      result.setProperty("subExecutionPlans", serializedSubPlans);
    }
    return result;
  }

  static void basicDeserialize(Result serialized, ExecutionStepInternal step)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
    List<Result> serializedSubsteps = serialized.getProperty("subSteps");
    if (serializedSubsteps != null) {
      for (Result serializedSub : serializedSubsteps) {
        String className = serializedSub.getProperty(InternalExecutionPlan.JAVA_TYPE);
        ExecutionStepInternal subStep = (ExecutionStepInternal) Class.forName(className).getConstructor().newInstance();
        subStep.deserialize(serializedSub);
        step.getSubSteps().add(subStep);
      }
    }

    List<Result> serializedPlans = serialized.getProperty("subExecutionPlans");
    if (serializedSubsteps != null) {
      for (Result serializedSub : serializedPlans) {
        String className = serializedSub.getProperty(InternalExecutionPlan.JAVA_TYPE);
        InternalExecutionPlan subStep = (InternalExecutionPlan) Class.forName(className).getConstructor().newInstance();
        subStep.deserialize(serializedSub);
        step.getSubExecutionPlans().add(subStep);
      }
    }
  }

  ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException;

  void sendTimeout();

  boolean isTimedOut();

  void setPrevious(ExecutionStepInternal step);

  void setNext(ExecutionStepInternal step);

  void close();

  default String prettyPrint(int depth, int indent) {
    String spaces = getIndent(depth, indent);
    return spaces + getClass().getSimpleName();
  }

  default String getName() {
    return getClass().getSimpleName();
  }

  default String getType() {
    return getClass().getSimpleName();
  }

  default String getDescription() {
    return prettyPrint(0, 3);
  }

  default String getTargetNode() {
    return "<local>";
  }

  default List<ExecutionStep> getSubSteps() {
    return Collections.emptyList();
  }

  default List<ExecutionPlan> getSubExecutionPlans() {
    return Collections.emptyList();
  }

  default void reset() {
    //do nothing
  }

  default Result serialize() {
    throw new UnsupportedOperationException();
  }

  default void deserialize(Result fromResult) {
    throw new UnsupportedOperationException();
  }

  default ExecutionStep copy(CommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  default boolean canBeCached() {
    return false;
  }
}
