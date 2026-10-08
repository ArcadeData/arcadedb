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

import org.apache.ratis.server.protocol.TermIndex;

/**
 * A real {@link ArcadeStateMachine}, never wired to a Raft server, whose recovery flags and last applied position are set
 * by the test (issue #9464). It replaces a Mockito mock that stubbed them to describe a follower mid-recovery, which a
 * fresh state machine cannot be without replaying a log.
 * <p>
 * Every overridden getter starts where a fresh state machine does: not catching up, no snapshot download pending, no
 * resync in progress, not halted, and the base state machine's own last applied position. A test that needs only
 * those defaults uses a plain {@code new ArcadeStateMachine()}.
 */
public class FakeArcadeStateMachine extends ArcadeStateMachine {
  private volatile boolean   catchingUp;
  private volatile boolean   snapshotDownloadPending;
  private volatile boolean   resyncInProgress;
  private volatile boolean   haltedAfterCriticalError;
  private volatile TermIndex lastAppliedTermIndex;

  public FakeArcadeStateMachine catchingUp(final boolean catchingUp) {
    this.catchingUp = catchingUp;
    return this;
  }

  public FakeArcadeStateMachine snapshotDownloadPending(final boolean snapshotDownloadPending) {
    this.snapshotDownloadPending = snapshotDownloadPending;
    return this;
  }

  public FakeArcadeStateMachine resyncInProgress(final boolean resyncInProgress) {
    this.resyncInProgress = resyncInProgress;
    return this;
  }

  public FakeArcadeStateMachine haltedAfterCriticalError(final boolean haltedAfterCriticalError) {
    this.haltedAfterCriticalError = haltedAfterCriticalError;
    return this;
  }

  public FakeArcadeStateMachine lastAppliedTermIndex(final TermIndex lastAppliedTermIndex) {
    this.lastAppliedTermIndex = lastAppliedTermIndex;
    return this;
  }

  @Override
  public boolean isCatchingUp() {
    return catchingUp;
  }

  @Override
  public boolean isSnapshotDownloadPending() {
    return snapshotDownloadPending;
  }

  @Override
  public boolean isResyncInProgress() {
    return resyncInProgress;
  }

  @Override
  boolean isHaltedAfterCriticalError() {
    return haltedAfterCriticalError;
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    final TermIndex set = lastAppliedTermIndex;
    return set != null ? set : super.getLastAppliedTermIndex();
  }
}
