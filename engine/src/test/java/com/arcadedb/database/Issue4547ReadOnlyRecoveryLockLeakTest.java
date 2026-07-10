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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseMetadataException;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4547: a {@code READ_ONLY} open of a not-cleanly-closed database (its
 * {@code database.lck} marker present, so recovery is required) must be rejected and must not leak
 * the OS file lock, so a subsequent recovery-capable {@code READ_WRITE} open still succeeds.
 */
class Issue4547ReadOnlyRecoveryLockLeakTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Issue4547Type");
  }

  @Test
  void readOnlyOpenOfRecoveringDatabaseIsRejectedAndDoesNotLeakLock() {
    // Produce some content and crash without a clean close so the next open runs recovery on a present lock file.
    database.transaction(() -> database.newDocument("Issue4547Type").set("k", 1).save());
    ((DatabaseInternal) database).kill();
    // Deregister the killed instance from the active-instances registry (the database.lck marker stays on disk).
    database.close();

    final File lockFile = new File(getDatabasePath() + "/database.lck");
    assertThat(lockFile.exists()).as("lock marker must be present so recovery is required").isTrue();

    // A READ_ONLY open of a database that needs recovery must be rejected.
    assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_ONLY))
        .isInstanceOf(DatabaseMetadataException.class);

    // If the rejected READ_ONLY open leaked the file lock, this READ_WRITE open fails with a
    // LockException ("locked by another process"). With the fix the lock was never leaked, so the
    // recovery-capable READ_WRITE open succeeds.
    database = factory.open();
    assertThat(database.isOpen()).isTrue();
    assertThat(database.getSchema().existsType("Issue4547Type")).isTrue();
  }
}
