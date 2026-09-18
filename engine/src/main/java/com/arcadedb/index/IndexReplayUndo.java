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
package com.arcadedb.index;

/**
 * Compensation for the NON-transactional side effects an index produced while its queued operations were replayed
 * during {@code TransactionContext.commit1stPhase()} (issue #7931).
 * <p>
 * The replay runs BEFORE the page versions are validated, so every index operation of a transaction that then loses
 * the MVCC check has already been applied. For an ordinary index that ordering is harmless: the replay only writes
 * transaction-local mutable pages, which the rollback discards wholesale, and nothing else of the index changes.
 * An index that ALSO keeps process-wide in-memory state - {@code LSMVectorIndex} and its location index, delta
 * buffer and rebuild counters - has no such guarantee, and must hand the transaction one of these so the abort can
 * put that state back.
 * <p>
 * Registered with {@code TransactionContext.addIndexReplayUndo} at the first mutation of the replay and run by
 * {@code TransactionContext.rollback()} while the transaction still holds its file locks, so no other transaction
 * can have touched the same index in between. It is run ONLY from {@code rollback()}: the other non-committed
 * conclusions of a transaction (a failure past the WAL append, a locally-failed but remotely-committed apply) leave
 * the changes durable and must therefore leave the in-memory state alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface IndexReplayUndo {
  /**
   * Reverses everything the replay published outside the transaction's own pages. Called at most once, on the
   * transaction's own thread, and must not throw for anything a rollback can encounter: the caller degrades a
   * failure here to a warning rather than let it replace the exception that caused the rollback.
   */
  void undoIndexReplay();
}
