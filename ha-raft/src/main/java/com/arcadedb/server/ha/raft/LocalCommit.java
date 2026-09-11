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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.TransactionContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One transaction this node originated and is replicating through Raft, between phase 1 (WAL captured, file locks
 * held) and the publication of its pages (issue #6965).
 * <p>
 * The committing thread registers it before dispatching the entry and the Raft apply thread claims it when the entry
 * reaches its position in the log: the apply thread then publishes the prepared pages right there, so the leader's
 * pages change in exactly the order the log dictates, like every follower's. Whichever side moves first decides:
 * <ul>
 *   <li>the apply thread {@link #claim() claims} first - it publishes the pages and records the outcome; the
 *       committing thread, once the entry is acknowledged, only finishes the bookkeeping. A replication result that
 *       arrives as "unknown" after a claim is therefore known after all: the entry committed;</li>
 *   <li>the committing thread {@link #withdraw() withdraws} first - replication failed or its outcome is unknown, so
 *       it rolls the transaction back; if the entry commits regardless, the apply thread finds no claim and applies
 *       the entry's own WAL bytes, exactly as a follower would.</li>
 * </ul>
 * Only one of the two transitions can win, so the pages of an entry are written exactly once on this node, and never
 * by a thread that does not know whether the entry committed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LocalCommit {
  /** What the apply thread did with the claimed transaction. */
  enum Outcome {
    /** Not concluded yet (only ever returned when the wait for it timed out). */
    PENDING,
    /** The prepared pages were published at the entry's log position. */
    PUBLISHED,
    /** Publishing failed; the pages were reconciled from the entry's WAL bytes where possible. */
    FAILED
  }

  private enum State {REGISTERED, CLAIMED, WITHDRAWN}

  private final String                            databaseName;
  private final long                              walTxId;
  private final TransactionContext                tx;
  private final TransactionContext.TransactionPhase1 phase1;
  private final byte[]                            walData;
  private final long                              registeredAtMs = System.currentTimeMillis();
  private final AtomicReference<State>            state          = new AtomicReference<>(State.REGISTERED);
  private final CountDownLatch                    concluded      = new CountDownLatch(1);
  private volatile Throwable                      failure;
  private volatile boolean                        reconciled;

  LocalCommit(final String databaseName, final long walTxId, final TransactionContext tx,
      final TransactionContext.TransactionPhase1 phase1, final byte[] walData) {
    this.databaseName = databaseName;
    this.walTxId = walTxId;
    this.tx = tx;
    this.phase1 = phase1;
    this.walData = walData;
  }

  String databaseName() {
    return databaseName;
  }

  long walTxId() {
    return walTxId;
  }

  TransactionContext transaction() {
    return tx;
  }

  TransactionContext.TransactionPhase1 phase1() {
    return phase1;
  }

  /** The WAL bytes the transaction shipped: what identifies its entry when the apply thread reaches it. */
  byte[] walData() {
    return walData;
  }

  long registeredAtMs() {
    return registeredAtMs;
  }

  /** The apply thread takes ownership of publishing the pages. */
  boolean claim() {
    return state.compareAndSet(State.REGISTERED, State.CLAIMED);
  }

  /** The committing thread takes the transaction back, leaving the entry to be applied from its WAL bytes if it commits. */
  boolean withdraw() {
    return state.compareAndSet(State.REGISTERED, State.WITHDRAWN);
  }

  boolean isClaimed() {
    return state.get() == State.CLAIMED;
  }

  void published() {
    concluded.countDown();
  }

  void failed(final Throwable cause, final boolean pagesReconciled) {
    failure = cause;
    reconciled = pagesReconciled;
    concluded.countDown();
  }

  /**
   * Waits for the apply thread to conclude a claimed transaction.
   *
   * @return {@link Outcome#PENDING} only when the wait timed out
   */
  Outcome awaitOutcome(final long timeoutMs) throws InterruptedException {
    if (!concluded.await(timeoutMs, TimeUnit.MILLISECONDS))
      return Outcome.PENDING;
    return failure == null ? Outcome.PUBLISHED : Outcome.FAILED;
  }

  Throwable failure() {
    return failure;
  }

  /** Whether, after a failed publication, the entry's pages were reconciled from its WAL bytes. */
  boolean reconciled() {
    return reconciled;
  }

  @Override
  public String toString() {
    return "LocalCommit{db=" + databaseName + ", walTxId=" + walTxId + ", state=" + state.get() + "}";
  }
}
