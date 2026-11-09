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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.InternalExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11459: the constructor of {@link LocalResultSet} fetches the first rows. When that fetch throws (timeout, conflict, a row a
 * step refuses) the caller never receives the result set, so nobody can close it and what the plan's steps hold (a FACETS step's memory
 * reservations and projection snapshots) was released only by the garbage collector, with a warning.
 */
class LocalResultSetClosesPlanOnFailedFirstFetchTest extends TestHelper {

  private static final class FailingPlan implements InternalExecutionPlan {
    private int closes;

    @Override
    public ResultSet fetchNext(final int n) {
      throw new CommandExecutionException("boom");
    }

    @Override
    public void reset(final CommandContext context) {
    }

    @Override
    public boolean canBeCached() {
      return false;
    }

    @Override
    public void close() {
      closes++;
    }

    @Override
    public List<ExecutionStep> getSteps() {
      return List.of();
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return "failing";
    }

    @Override
    public Result toResult() {
      return null;
    }
  }

  @Test
  void aFailedFirstFetchClosesThePlan() {
    final FailingPlan plan = new FailingPlan();
    assertThatThrownBy(() -> new LocalResultSet(plan)).isInstanceOf(CommandExecutionException.class).hasMessage("boom");
    assertThat(plan.closes).as("the plan of a result set nobody can close is closed by the constructor").isEqualTo(1);
  }
}
