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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple ExecutionPlan implementation for OpenCypher EXPLAIN/PROFILE output.
 * Wraps a pre-formatted text string so it can be returned via the standard
 * {@link com.arcadedb.query.sql.parser.ExplainResultSet} mechanism used by the server HTTP handler.
 */
public class OpenCypherExplainExecutionPlan implements ExecutionPlan {
  private final String planText;
  private final List<ExecutionStep> steps;
  private final long cost;

  public OpenCypherExplainExecutionPlan(final String planText) {
    this(planText, Collections.emptyList(), -1);
  }

  public OpenCypherExplainExecutionPlan(final String planText, final List<ExecutionStep> steps, final long cost) {
    this.planText = planText;
    this.steps = steps != null ? steps : Collections.emptyList();
    this.cost = cost;
  }

  @Override
  public List<ExecutionStep> getSteps() {
    return steps;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return planText;
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("type", "OpenCypherExecutionPlan");
    result.setProperty("prettyPrint", planText);
    result.setProperty("cost", cost);
    result.setProperty("steps", steps.isEmpty() ? null :
        steps.stream().map(ExecutionStep::toResult).collect(Collectors.toList()));
    return result;
  }
}
