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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseOperationException;
import org.junit.jupiter.api.Test;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #8944: a ClosedChannelException surfacing from inside {@code executeInReadLock} must not make the
 * database close itself while the calling thread holds the read lock (the write lock close() waits for is never granted
 * to a reader), and a channel closed by an interrupt must not close it from {@code executeInWriteLock} either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8944ClosedChannelInLockTest extends TestHelper {

  @Test
  void readLockClosedChannelDoesNotDeadlockClose() throws Exception {
    assertThatThrownBy(() -> database.executeInReadLock(() -> {
      throw new ClosedByInterruptException();
    })).isInstanceOf(DatabaseOperationException.class).hasCauseInstanceOf(ClosedChannelException.class);

    assertCloseReturns();
  }

  @Test
  void writeLockClosedByInterruptKeepsTheDatabaseOpen() {
    assertThatThrownBy(() -> database.executeInWriteLock(() -> {
      throw new ClosedByInterruptException();
    })).isInstanceOf(DatabaseOperationException.class).hasCauseInstanceOf(ClosedByInterruptException.class);

    assertThat(database.isOpen()).isTrue();
  }

  private void assertCloseReturns() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
      final Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    });
    try {
      final Future<?> close = executor.submit(database::close);
      close.get(30, TimeUnit.SECONDS);
      assertThat(database.isOpen()).isFalse();
    } finally {
      executor.shutdownNow();
    }
  }
}
