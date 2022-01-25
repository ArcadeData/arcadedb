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

import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class ExplainResultSet implements ResultSet {
  private final ExecutionPlan executionPlan;
  boolean hasNext = true;

  public ExplainResultSet(ExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public Result next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }
    ResultInternal result = new ResultInternal();
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.toResult()));
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlanAsString", x.prettyPrint(0, 3)));
    hasNext = false;
    return result;
  }

  @Override
  public void close() {

  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
