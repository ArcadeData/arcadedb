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
 * What the conclusion of a transaction does to the NON-transactional state an index touched while its queued
 * operations were replayed during {@code TransactionContext.commit1stPhase()} (issues #7931, #7933).
 * <p>
 * The replay runs BEFORE the page versions are validated, so every index operation of a transaction that then loses
 * the MVCC check has already been carried out. For an ordinary index that ordering is harmless: the replay only
 * writes transaction-local mutable pages, which the rollback discards wholesale, and nothing else of the index
 * changes. An index that ALSO keeps process-wide in-memory state has no such guarantee, and hands the transaction
 * one of these so its conclusion can be applied to that state too.
 * <p>
 * <b>Exactly one of the two methods runs, always.</b> {@code rollback()} calls {@link #undoIndexReplay()} - and
 * nothing else does, because the other non-committed conclusions of a transaction (a failure past the WAL append, a
 * locally-failed but remotely-committed apply) leave the changes durable and must therefore NOT unwind. Every one
 * of those, and the committed path, reaches {@code reset()}, which calls {@link #publishIndexReplay()}. The
 * registration is dropped by whichever of the two ran first, so neither can run twice.
 * <p>
 * <b>The two shapes, and why both exist.</b>
 * <ul>
 *   <li>{@code LSMVectorIndex} publishes EAGERLY, during the replay, and hands back a journal of what it published
 *       so {@link #undoIndexReplay()} can take it back. It has no choice: the replay allocates vector ids and
 *       writes them onto index pages, and the ids have to be decided before the pages are written.</li>
 *   <li>{@code LSMSparseVectorIndex} publishes LAZILY: the replay only fills a buffer, and
 *       {@link #publishIndexReplay()} is what writes it into the engine's shared memtable. Its
 *       {@link #undoIndexReplay()} therefore has nothing to reverse - it just drops the buffer. Deferring is not a
 *       stylistic preference there but the only correct option (issue #7933): the memtable seals itself into an
 *       on-disk segment on its own schedule, so an eagerly-published posting can be baked into a sealed segment
 *       BEFORE the rollback runs, where no in-memory compensation can reach it.</li>
 * </ul>
 * <p>
 * Registered with {@code TransactionContext.addIndexReplayConclusion} at the first mutation of the replay, one per
 * index per transaction. Both methods run on the transaction's own thread while it still holds its file locks -
 * {@code reset()} is what releases those, and it releases them after calling {@link #publishIndexReplay()} - so no
 * other transaction can have touched the same index in between.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface IndexReplayConclusion {
  /**
   * The transaction rolled back: reverse everything the replay published outside the transaction's own pages, or
   * discard what it had not published yet. Called at most once, and must not throw for anything a rollback can
   * encounter: the caller degrades a failure here to a warning rather than let it replace the exception that
   * caused the rollback.
   */
  void undoIndexReplay();

  /**
   * The transaction's changes stand - it committed, or it failed somewhere its changes are durable anyway. Publish
   * whatever the replay deferred. Called at most once, and never for a transaction that rolled back. The default is
   * for an index that published eagerly during the replay and therefore has nothing left to do.
   * <p>
   * Must not throw for anything a commit conclusion can encounter, for the same reason
   * {@link #undoIndexReplay()} must not: the caller degrades a failure here to a warning, and a transaction that
   * has already committed cannot be un-committed by it.
   */
  default void publishIndexReplay() {
    // NOTHING TO DO: THE REPLAY ALREADY PUBLISHED ITS WORK.
  }
}
