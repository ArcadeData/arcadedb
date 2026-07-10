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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for {@code DatabaseAsyncExecutorImpl.kill()} returning before worker threads have
 * actually stopped. Without the fix, threads continued running (polling the queue, mutating the
 * database) after {@code kill()} returned, causing spurious errors when
 * {@code LocalDatabase.closeInternal()} closed internal components while async workers were still
 * active.
 */
class DatabaseAsyncExecutorKillTest extends TestHelper {

  @Test
  void killWaitsForWorkerThreadsToStop() throws Exception {
    final int parallelLevel = 2;
    database.async().setParallelLevel(parallelLevel);

    final String threadPrefix = "AsyncExecutor-" + database.getName() + "-";

    // Confirm the worker threads are alive before kill.
    final Set<Thread> workersBefore = Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(threadPrefix))
        .collect(Collectors.toSet());
    assertThat(workersBefore).as("expected %d async worker threads before kill()", parallelLevel)
        .hasSize(parallelLevel);

    // kill() must block until all workers have terminated.
    ((DatabaseInternal) database).kill();

    // After kill() returns, no AsyncExecutor-* thread for this database may remain.
    // Thread.getAllStackTraces() only reports live threads, so a matching name here means alive.
    final Set<Thread> workersAfter = Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(threadPrefix))
        .collect(Collectors.toSet());
    assertThat(workersAfter).as("async worker threads still alive after kill()").isEmpty();

    // kill() leaves the database closed and unusable - the defined post-kill invariant.
    assertThat(database.isOpen()).as("database must be closed after kill()").isFalse();
  }
}
