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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12251: {@link TransactionContext#concludeReadOnlyCommitDeferringCallbacks()} ends a transaction with nothing to write as a
 * commit, but hands its callbacks back instead of running them, so the caller can run them outside its database lock.
 */
class Issue12251ReadOnlyCommitDeferredCallbacksTest extends TestHelper {

  @Test
  void theCommitIsCountedAndBothKindsOfCallbackAreDeferredInOrder() {
    final List<String> events = new ArrayList<>();

    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    tx.addAfterCommitCallback(() -> events.add("after-commit"));
    tx.addAfterCompletionCallbackIfAbsent("issue12251", () -> events.add("after-completion"));
    final long commitCountBefore = tx.getCommitCount();

    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();
    final Runnable deferred = tx.concludeReadOnlyCommitDeferringCallbacks();

    assertThat(deferred).isNotNull();
    assertThat(events).as("nothing runs before the caller runs the deferred work").isEmpty();
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    assertThat(database.isTransactionActive()).isFalse();

    deferred.run();

    assertThat(events).containsExactly("after-commit", "after-completion");
  }

  @Test
  void aTransactionWithNoCallbacksDefersNothing() {
    database.begin();
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final long commitCountBefore = tx.getCommitCount();

    assertThat(tx.commit1stPhase(true)).as("nothing to write").isNull();

    assertThat(tx.concludeReadOnlyCommitDeferringCallbacks()).isNull();
    assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    assertThat(database.isTransactionActive()).isFalse();
  }
}
