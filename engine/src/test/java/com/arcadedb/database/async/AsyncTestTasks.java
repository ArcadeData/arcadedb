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

import com.arcadedb.database.DatabaseInternal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Shared task factories for the async executor regression tests (#5072 review, point 3).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AsyncTestTasks {
  private AsyncTestTasks() {
  }

  /**
   * A task that signals its start and then blocks until released (20s ceiling), restoring the
   * interrupt flag: simulates a worker busy inside user code.
   */
  static DatabaseAsyncTask awaitTask(final CountDownLatch started, final CountDownLatch release) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        started.countDown();
        try {
          release.await(20, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  /** A task that counts the given latch down when executed. */
  static DatabaseAsyncTask countingTask(final CountDownLatch latch) {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        latch.countDown();
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }

  /** A task with no effects, used to fill queues. */
  static DatabaseAsyncTask noOpTask() {
    return new DatabaseAsyncTask() {
      @Override
      public void execute(final DatabaseAsyncExecutorImpl.AsyncThread asyncThread, final DatabaseInternal database) {
        // NO ACTIONS
      }

      @Override
      public boolean requiresActiveTx() {
        return false;
      }
    };
  }
}
