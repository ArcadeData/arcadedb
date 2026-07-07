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
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.utility.LockManager;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5067 (item 1): {@code TransactionContext.kill()} released the explicit lock
 * set but nulled {@code lockedFiles} WITHOUT unlocking, so killing a transaction caught in
 * COMMIT_1ST_PHASE (its file locks acquired by {@code commit1stPhase}) leaked the {@code LockManager}
 * entries until {@code TransactionManager.kill()} closed the whole lock manager. Test-only API, but the
 * release must be symmetric with {@code reset()}.
 */
class Issue5067KillLockSymmetryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Issue5067Type");
  }

  @Test
  void killAfterCommit1stPhaseReleasesFileLocks() {
    database.begin();
    database.newDocument("Issue5067Type").set("k", 1).save();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    // Bring the transaction into COMMIT_1ST_PHASE: this is where lockFilesInOrder populates lockedFiles.
    assertThat(tx.commit1stPhase(true)).isNotNull();

    tx.kill();
    // Leave the killed context inactive so the teardown close does not try to roll back its nulled state.
    tx.setStatus(TransactionContext.STATUS.INACTIVE);

    // The file locks acquired by commit1stPhase must have been released by kill(): a foreign requester
    // must be able to lock the same bucket file within the (short) timeout instead of timing out on a
    // lock still keyed to the killed transaction's requester.
    final int bucketFileId = database.getSchema().getType("Issue5067Type").getBuckets(false).getFirst().getFileId();
    final TransactionManager transactionManager = ((DatabaseInternal) database).getTransactionManager();
    final Object foreignRequester = new Object();

    final LockManager.LOCK_STATUS status = transactionManager.tryLockFile(bucketFileId, 1_000L, foreignRequester);
    assertThat(status).as("kill() must release the COMMIT_1ST_PHASE file locks (symmetry with reset())")
        .isEqualTo(LockManager.LOCK_STATUS.YES);

    transactionManager.unlockFile(bucketFileId, foreignRequester);
  }
}
