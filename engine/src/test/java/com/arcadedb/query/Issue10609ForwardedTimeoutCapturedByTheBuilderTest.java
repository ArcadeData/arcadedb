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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10609: the budget a request publishes with {@link CommandTimeoutOverride} (the Postgres {@code statement_timeout}, the
 * budget a follower forwards) lives in a thread-local, and a command context resolves it lazily, on the first thread that asks.
 * A parallel scan hands the context to workers that carry no budget, so a worker that was first to ask resolved "no timeout" for
 * the whole command: a 1 ms {@code statement_timeout} was answered with the result of a 9 second statement. The context captures
 * the budget when the thread that serves the request builds it, as it does for the registry entry of the statement (#9680).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10609ForwardedTimeoutCapturedByTheBuilderTest extends TestHelper {

  @AfterEach
  void clear() {
    CommandTimeoutOverride.clear();
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @Test
  void aWorkerThatResolvesTheContextFirstSeesTheBudgetOfTheThreadThatBuiltIt() throws Exception {
    CommandTimeoutOverride.set(1_234L);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    // the request ends on its own thread without anything having asked the context for its budget
    CommandTimeoutOverride.clear();

    final AtomicLong timeout = new AtomicLong();
    final AtomicLong deadline = new AtomicLong();
    final Thread worker = new Thread(() -> {
      timeout.set(context.getCommandTimeout());
      deadline.set(context.getCommandDeadline());
    });
    worker.start();
    worker.join();

    assertThat(timeout.get()).isEqualTo(1_234L);
    assertThat(deadline.get()).as("a deadline, not 'no deadline'").isNotEqualTo(Long.MAX_VALUE);
    assertThat(context.getCommandTimeout()).as("one value for every thread afterwards").isEqualTo(1_234L);
  }

  @Test
  void aCopyMadeByAWorkerKeepsTheBudgetToo() throws Exception {
    CommandTimeoutOverride.set(2_345L);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    CommandTimeoutOverride.clear();

    final AtomicLong copyTimeout = new AtomicLong();
    final Thread worker = new Thread(() -> {
      final CommandContext copy = context.copy();
      copyTimeout.set(copy.getCommandTimeout());
    });
    worker.start();
    worker.join();

    assertThat(copyTimeout.get()).isEqualTo(2_345L);
  }

  @Test
  void aContextBuiltWithoutABudgetFallsBackToTheDatabaseSetting() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);

    final AtomicLong timeout = new AtomicLong();
    final Thread worker = new Thread(() -> timeout.set(context.getCommandTimeout()));
    worker.start();
    worker.join();

    assertThat(timeout.get()).isEqualTo(60_000L);
  }

  @Test
  void theBudgetOfTheBuilderBeatsTheLaterBudgetOfTheResolvingThread() throws Exception {
    CommandTimeoutOverride.set(500L);
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);

    final AtomicLong timeout = new AtomicLong();
    final Thread worker = new Thread(() -> {
      CommandTimeoutOverride.set(9_999L);
      try {
        timeout.set(context.getCommandTimeout());
      } finally {
        CommandTimeoutOverride.clear();
      }
    });
    worker.start();
    worker.join();

    assertThat(timeout.get()).as("the command is the request of the thread that built it, whoever asks").isEqualTo(500L);
  }
}
