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
import com.arcadedb.exception.DatabaseOperationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #4955: {@code getBestSlot}/{@code getRandomSlot} snapshotted {@code executorThreads}
 * without a null check, so a compaction (or random-slot task) scheduled concurrently with {@code close()}/
 * {@code kill()} threw a raw {@code NullPointerException} instead of the intended
 * {@code DatabaseOperationException("Async executor has been shut down")} that {@code scheduleTask} already
 * produced for the same race.
 */
class AsyncSlotShutdownRaceTest extends TestHelper {

  @Test
  void slotSelectionAfterShutdownThrowsTheIntendedException() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();

    // Force the executor into its post-shutdown state (executorThreads = null).
    async.kill();

    assertThatThrownBy(async::getBestSlot)
        .as("getBestSlot must fail with the intended shutdown error, not an NPE")
        .isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("shut down");
    assertThatThrownBy(async::getRandomSlot)
        .as("getRandomSlot must fail with the intended shutdown error, not an NPE")
        .isInstanceOf(DatabaseOperationException.class)
        .hasMessageContaining("shut down");
  }
}
