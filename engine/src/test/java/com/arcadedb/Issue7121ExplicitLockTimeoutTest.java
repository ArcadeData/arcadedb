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
package com.arcadedb;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7121: {@code arcadedb.explicitLockTimeout} was declared, documented and settable, and read by nothing.
 * Both the implicit commit-time locking and an application's explicit {@code LOCK} went through one helper that
 * always used {@link GlobalConfiguration#COMMIT_LOCK_TIMEOUT}, so an application that asked for a short explicit
 * lock budget waited the commit budget instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7121ExplicitLockTimeoutTest extends TestHelper {
  private static final long EXPLICIT_TIMEOUT_MS = 500L;
  private static final long COMMIT_TIMEOUT_MS   = 60_000L;
  private static final long HOLD_MS             = 10_000L;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.EXPLICIT_LOCK_TIMEOUT, EXPLICIT_TIMEOUT_MS);
    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, COMMIT_TIMEOUT_MS);
    database.getSchema().createDocumentType("Locked");
  }

  @Test
  void anExplicitLockGivesUpOnItsOwnBudgetNotTheCommitOne() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final List<Integer> fileIds = database.getSchema().getType("Locked").getBuckets(false).stream()
        .map(b -> b.getFileId()).toList();

    final LockHoldingThread holder = new LockHoldingThread(db, fileIds, HOLD_MS);
    holder.start();
    holder.lockAcquired.await();

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> database.transaction(() -> database.acquireLock().type("Locked").lock()))
        .isInstanceOf(LockTimeoutException.class)
        .as("the budget the explicit lock actually waited on")
        .hasMessageContaining("timeout=" + EXPLICIT_TIMEOUT_MS + "ms");

    // The tripwire between "waited its own 500ms budget" and "waited the 60s commit budget". Generous on purpose:
    // a wider bound cannot turn a passing run red, and anything under the commit budget proves the right setting
    // was read.
    stopwatch.assertGaveUpWithin(COMMIT_TIMEOUT_MS / 2,
        "the explicit-lock budget (" + EXPLICIT_TIMEOUT_MS + "ms) from the commit budget (" + COMMIT_TIMEOUT_MS + "ms)");

    holder.interrupt();
    holder.join(HOLD_MS);
  }
}
