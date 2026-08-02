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

import com.arcadedb.query.sql.parser.BreakStatement;
import com.arcadedb.query.sql.parser.ReturnStatement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A script that hits a RETURN or a BREAK swaps the plan's {@code lastStep} for a step outside the chain of
 * script lines. Releasing the plan must still release every line that ran before it, and must do so
 * iteratively so a large batch does not blow the stack.
 */
public class ScriptExecutionPlanCloseTest {
  private static final int LINES = 20_000;

  /** Minimal per-statement plan that records its own release and can stand in for a BREAK. */
  private static class RecordingPlan implements InternalExecutionPlan {
    private final String       name;
    private final List<String> closed;
    private final boolean      breaks;

    RecordingPlan(final String name, final List<String> closed, final boolean breaks) {
      this.name = name;
      this.closed = closed;
      this.breaks = breaks;
    }

    @Override
    public void close() {
      closed.add(name);
    }

    @Override
    public ResultSet fetchNext(final int n) {
      return breaks ? BreakStatement.BREAK_RESULTSET : new InternalResultSet();
    }

    @Override
    public void reset(final CommandContext context) {
    }

    @Override
    public boolean canBeCached() {
      return false;
    }

    @Override
    public List<ExecutionStep> getSteps() {
      return List.of();
    }

    @Override
    public String prettyPrint(final int depth, final int indent) {
      return name;
    }

    @Override
    public Result toResult() {
      return null;
    }
  }

  @Test
  void aScriptThatReturnsEarlyStillReleasesEveryLineBeforeIt() {
    final CommandContext context = new BasicCommandContext();
    final List<String> closed = new ArrayList<>();

    final ScriptExecutionPlan plan = new ScriptExecutionPlan(context);
    plan.chain(new RecordingPlan("line0", closed, false));
    plan.chain(new SingleOpExecutionPlan(context, new ReturnStatement(-1)));
    plan.chain(new RecordingPlan("line2", closed, false));

    assertThat(plan.executeUntilReturn()).isInstanceOf(ReturnStep.class);

    plan.close();

    assertThat(closed).containsExactly("line2", "line0");
  }

  @Test
  void aScriptThatBreaksStillReleasesEveryLineBeforeIt() {
    final CommandContext context = new BasicCommandContext();
    final List<String> closed = new ArrayList<>();

    final ScriptExecutionPlan plan = new ScriptExecutionPlan(context);
    plan.chain(new RecordingPlan("line0", closed, false));
    plan.chain(new RecordingPlan("line1", closed, true));
    plan.chain(new RecordingPlan("line2", closed, false));

    assertThat(plan.executeUntilReturn()).isInstanceOf(BreakStep.class);

    plan.close();

    assertThat(closed).containsExactly("line2", "line1", "line0");
  }

  /**
   * The chain of script lines is as long as the user's batch, so the release walk has to stay iterative: an
   * explicit 1 MB stack keeps the verdict independent of the JVM's default {@code -Xss}.
   */
  @Test
  void closingALargeScriptThatReturnedEarlyDoesNotOverflowTheStack() throws InterruptedException {
    final CommandContext context = new BasicCommandContext();
    final List<String> closed = new ArrayList<>();

    final ScriptExecutionPlan plan = new ScriptExecutionPlan(context);
    plan.chain(new SingleOpExecutionPlan(context, new ReturnStatement(-1)));
    for (int i = 0; i < LINES; i++)
      plan.chain(new RecordingPlan("line" + i, closed, false));

    assertThat(plan.executeUntilReturn()).isInstanceOf(ReturnStep.class);

    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread runner = new Thread(null, () -> {
      try {
        plan.close();
      } catch (final Throwable t) {
        failure.set(t);
      }
    }, "script-return-close", 1024 * 1024);

    runner.start();
    runner.join();

    assertThat(failure.get()).isNull();
    assertThat(closed).hasSize(LINES);
  }
}
