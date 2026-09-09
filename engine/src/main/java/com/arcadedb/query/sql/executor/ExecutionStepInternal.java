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
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.Collections;
import java.util.List;

/**
 * <p>Execution Steps are the building blocks of a query execution plan</p> <p>Typically an execution plan is made of
 * a chain of
 * steps. The execution is pull-based, meaning that the result set that the client iterates is conceptually the one
 * returned by
 * <i>last</i> step of the execution plan</p> <p>At each `next()` invocation, the step typically fetches a record
 * from the previous
 * (upstream) step, does its elaboration (eg. for a filtering step, it can discard the record and fetch another one
 * if it doesn't
 * match the conditions) and returns the elaborated step</p>
 * <br>
 * <p>The invocation of {@literal syncPull(context, nResults)} has to return a result set of at most nResults records
 * . If the upstream
 * (the previous steps) return more records, they have to be returned by next call of {@literal syncPull()}. The
 * returned result
 * set can have less than nResults records ONLY if current step cannot produce any more records (eg. the upstream
 * does not have any
 * more records)</p>
 *
 * @author Luigi Dell'Aquila luigi.dellaquila-(at)-gmail.com
 */
@ExcludeFromJacocoGeneratedReport
public interface ExecutionStepInternal extends ExecutionStep {

  static String getIndent(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    for (int i = 0; i < depth; i++)
      result.append(" ".repeat(Math.max(0, indent)));

    return result.toString();
  }

  ResultSet syncPull(CommandContext context, int nRecords) throws TimeoutException;

  void sendTimeout();

  boolean isTimedOut();

  void setPrevious(ExecutionStepInternal step);

  /**
   * Releases whatever resources this step is holding (open cursors, buffered pages, ...). Implementations MUST be
   * idempotent: a second call after the step is already closed has to be a no-op, never a double-free or an
   * exception. This is not just a defensive nicety - {@code AbstractExecutionStep.close()} propagates to {@code
   * prev} by default, so closing any step in a chain closes the whole chain, and callers legitimately close a chain
   * more than once (e.g. a DML plan self-closes as soon as it is done executing - see {@code
   * UpdateExecutionPlan#executeInternal()} - and then closes again, harmlessly, if the caller also closes the
   * returned {@code ResultSet}).
   */
  void close();

  default String prettyPrint(final int depth, final int indent) {
    final String spaces = getIndent(depth, indent);
    return spaces + getClass().getSimpleName();
  }

  default String getName() {
    return simpleNameOf(getClass());
  }

  default String getType() {
    return simpleNameOf(getClass());
  }

  /**
   * The step class' simple name, falling back to the first named ancestor for an anonymous step. {@code
   * Class.getSimpleName()} answers the empty string for an anonymous class, and a step chain built out of anonymous
   * steps (the OpenCypher plan builds several) therefore published nameless rows in the EXPLAIN/PROFILE plan and in
   * Studio's Query Profiler step table (issue #7291).
   */
  static String simpleNameOf(final Class<?> stepClass) {
    for (Class<?> c = stepClass; c != null; c = c.getSuperclass()) {
      final String name = c.getSimpleName();
      if (!name.isEmpty())
        return name;
    }
    return "ExecutionStep";
  }

  default String getDescription() {
    return prettyPrint(0, 3);
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

  default ExecutionStep copy(final CommandContext context) {
    throw new UnsupportedOperationException();
  }

  default boolean canBeCached() {
    return false;
  }
}
