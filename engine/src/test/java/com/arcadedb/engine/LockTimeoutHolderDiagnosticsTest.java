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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@link LockTimeoutException} must name WHO held the file, not only which file could not be locked.
 * <p>
 * The holder is knowable only while the lock is still held. A lock leaked by a thread that is still
 * alive is never reclaimed — the abandoned-lock sweep requires the owner to have died — so it survives
 * until the process restarts, and by the time an operator reads the log the owner is unrecoverable. The
 * message is the only place the evidence can be captured at the moment it still exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LockTimeoutHolderDiagnosticsTest extends TestHelper {

  @Test
  void lockTimeoutMessageNamesTheHolder() {
    final DocumentType type = database.getSchema().createDocumentType("Locked");
    final int fileId = type.getBuckets(false).getFirst().getFileId();

    final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();

    // tryLockFiles keys ownership by the REQUESTER, not the calling thread, so a second requester on
    // this same thread contends exactly as a competing transaction would.
    final List<Integer> held = txManager.tryLockFiles(List.of(fileId), 1_000, "holder-tx-A");
    assertThat(held).containsExactly(fileId);

    try {
      assertThatThrownBy(() -> txManager.tryLockFiles(List.of(fileId), 50, "waiter-tx-B"))
          .isInstanceOf(LockTimeoutException.class)
          .hasMessageContaining("holder-tx-A")
          .hasMessageContaining("heldBy")
          .hasMessageContaining("waiters");
    } finally {
      txManager.unlockFilesInOrder(held, "holder-tx-A");
    }
  }

  @Test
  void lockStatsAreReadableFromTheTransactionManager() {
    final DocumentType type = database.getSchema().createDocumentType("Observed");
    final int fileId = type.getBuckets(false).getFirst().getFileId();

    final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();

    // Nothing held: the normal state, and the answer that rules a database out during an incident.
    assertThat(txManager.getLockStats()).isEmpty();

    final List<Integer> held = txManager.tryLockFiles(List.of(fileId), 1_000, "holder-tx-A");
    try {
      assertThat(txManager.getLockStats()).singleElement().satisfies(stats -> {
        assertThat(stats.resource()).isEqualTo(String.valueOf(fileId));
        assertThat(stats.owner()).isEqualTo("holder-tx-A");
        assertThat(stats.heldForMs()).isNotNegative();
        assertThat(stats.waiters()).isZero();
      });
    } finally {
      txManager.unlockFilesInOrder(held, "holder-tx-A");
    }

    assertThat(txManager.getLockStats()).isEmpty();
  }

  @Test
  void diagnosticsDoNotChangeTheOutcomeWhenTheLockIsFree() {
    final DocumentType type = database.getSchema().createDocumentType("Free");
    final int fileId = type.getBuckets(false).getFirst().getFileId();

    final TransactionManager txManager = ((DatabaseInternal) database).getTransactionManager();

    // Uncontended acquisition must still succeed and release cleanly — the diagnostics hang off the
    // failure path only.
    final List<Integer> held = txManager.tryLockFiles(List.of(fileId), 1_000, "tx-A");
    assertThat(held).containsExactly(fileId);
    txManager.unlockFilesInOrder(held, "tx-A");

    final List<Integer> again = txManager.tryLockFiles(List.of(fileId), 1_000, "tx-B");
    assertThat(again).containsExactly(fileId);
    txManager.unlockFilesInOrder(again, "tx-B");
  }
}
