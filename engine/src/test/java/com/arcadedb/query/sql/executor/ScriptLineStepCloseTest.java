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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the contract of the iterative walk in {@link ScriptLineStep#close()}: every line is released, tail to
 * head, and a plan that fails to close does not strand the ones behind it.
 */
public class ScriptLineStepCloseTest {
  /** Minimal plan that records its own release and can be told to fail while doing so. */
  private static class RecordingPlan implements InternalExecutionPlan {
    private final String            name;
    private final List<String>      closed;
    private final RuntimeException  failure;

    RecordingPlan(final String name, final List<String> closed, final RuntimeException failure) {
      this.name = name;
      this.closed = closed;
      this.failure = failure;
    }

    @Override
    public void close() {
      closed.add(name);
      if (failure != null)
        throw failure;
    }

    @Override
    public ResultSet fetchNext(final int n) {
      return new InternalResultSet();
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

  /**
   * Builds a chain of script lines in script order and returns the tail, which is what
   * {@link ScriptExecutionPlan#close()} closes.
   */
  private ScriptLineStep chainOf(final CommandContext context, final List<InternalExecutionPlan> plans) {
    ScriptLineStep previous = null;
    for (final InternalExecutionPlan plan : plans) {
      final ScriptLineStep step = new ScriptLineStep(plan, context);
      if (previous != null)
        step.setPrevious(previous);
      previous = step;
    }
    return previous;
  }

  @Test
  void closingTheTailReleasesEveryLineFromTailToHead() {
    final CommandContext context = new BasicCommandContext();
    final List<String> closed = new ArrayList<>();

    chainOf(context, List.of(
        new RecordingPlan("line0", closed, null),
        new RecordingPlan("line1", closed, null),
        new RecordingPlan("line2", closed, null))).close();

    assertThat(closed).containsExactly("line2", "line1", "line0");
  }

  @Test
  void aPlanThatFailsToCloseDoesNotStrandTheLinesBehindIt() {
    final CommandContext context = new BasicCommandContext();
    final List<String> closed = new ArrayList<>();
    final RuntimeException firstFailure = new IllegalStateException("line2 failed");
    final RuntimeException laterFailure = new IllegalStateException("line0 failed");

    final ScriptLineStep tail = chainOf(context, List.of(
        new RecordingPlan("line0", closed, laterFailure),
        new RecordingPlan("line1", closed, null),
        new RecordingPlan("line2", closed, firstFailure)));

    // the tail-most failure is the one that surfaces, and it surfaces only after the whole chain is released
    assertThatThrownBy(tail::close).isSameAs(firstFailure);

    assertThat(closed).containsExactly("line2", "line1", "line0");
  }
}
