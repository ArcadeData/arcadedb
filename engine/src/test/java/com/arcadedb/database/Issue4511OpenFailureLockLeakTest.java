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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4511: {@code LocalDatabase.openInternal} leaks the JVM {@link java.nio.channels.FileLock}
 * (and the underlying lock-file I/O channels) when recovery throws while reopening a database that was not closed
 * cleanly. Once the lock leaks, the database is permanently unopenable within the same JVM (and on Windows the lock
 * file cannot be removed by external tools either).
 *
 * <p>The scenario is reproduced by killing the database to leave the {@code database.lck} marker on disk (so the next
 * open enters the recovery path and acquires the file lock) and then forcing recovery to throw via a one-shot
 * {@code DB_NOT_CLOSED} callback. After that failed open, a subsequent open must succeed: that only happens if the
 * failed open released its file lock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4511OpenFailureLockLeakTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Issue4511Type");
  }

  @Test
  void failedRecoveryReleasesFileLockSoDatabaseCanReopen() {
    // Produce some content and crash without a clean close so the next open runs recovery on a present lock file.
    database.transaction(() -> database.newDocument("Issue4511Type").set("k", 1).save());
    ((DatabaseInternal) database).kill();
    // Deregister the killed instance from the active-instances registry (the database.lck marker stays on disk).
    database.close();

    // Force the first recovery attempt to throw AFTER the file lock has been acquired.
    final AtomicBoolean failNextRecovery = new AtomicBoolean(true);
    factory.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_NOT_CLOSED, () -> {
      if (failNextRecovery.compareAndSet(true, false))
        throw new RuntimeException("Simulated recovery failure (issue #4511)");
      return null;
    });

    // The first open must fail because recovery threw.
    assertThatThrownBy(() -> factory.open()).isInstanceOf(Exception.class);

    // If the failed open leaked the file lock, this second open fails with a LockException
    // ("locked by another process"). With the fix the lock was released, so the open succeeds.
    database = factory.open();
    assertThat(database.isOpen()).isTrue();
    assertThat(database.getSchema().existsType("Issue4511Type")).isTrue();
  }
}
