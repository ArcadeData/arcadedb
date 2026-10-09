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

import com.arcadedb.server.CallLog;
import org.apache.ratis.server.protocol.TermIndex;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A real {@link ArcadeStateMachine}, never wired to a Raft server, whose recovery flags and last applied position are set
 * by the test (issue #9464). It replaces a Mockito mock that stubbed them to describe a follower mid-recovery, which a
 * fresh state machine cannot be without replaying a log.
 * <p>
 * Every overridden getter starts where a fresh state machine does: not catching up, no snapshot download pending, no
 * resync in progress, not halted, and the base state machine's own last applied position. A test that needs only
 * those defaults uses a plain {@code new ArcadeStateMachine()}.
 * <p>
 * A few calls are recorded on a {@link CallLog} and answered through {@link #returns}, {@link #fails} and {@link #on}.
 * The two queries, {@code hasLeaderServiceGap} and {@code isBootstrapInstallInFlight}, answer for real when unanswered.
 * The two actions, {@code handOffLeadershipWhileReplacingDatabase} and {@code resyncDatabaseFromLeader}, only record
 * when unanswered: a state machine wired to no Raft server could not carry either out, so the call itself is the
 * effect a test observes.
 */
public class FakeArcadeStateMachine extends ArcadeStateMachine {
  private volatile boolean   catchingUp;
  private volatile boolean   snapshotDownloadPending;
  private volatile boolean   resyncInProgress;
  private volatile boolean   haltedAfterCriticalError;
  private volatile TermIndex lastAppliedTermIndex;
  private volatile boolean   closed;

  private static final Set<String> RECORDED = Set.of("hasLeaderServiceGap", "isBootstrapInstallInFlight",
      "handOffLeadershipWhileReplacingDatabase", "resyncDatabaseFromLeader");
  private volatile CallLog         log     = new CallLog();
  private final    CallLog.Answers answers = new CallLog.Answers(RECORDED);

  /** Records on {@code log} from now on, which other fakes may share. Call it before the fake is used. */
  public FakeArcadeStateMachine recordingOn(final CallLog log) {
    this.log = log;
    return this;
  }

  /** The argument lists of every call to the recorded {@code method}, in arrival order. */
  public List<List<Object>> calls(final String method) {
    return log.argsOf(this, method);
  }

  /** The recorded {@code method} answers {@code value} from now on. */
  public FakeArcadeStateMachine returns(final String method, final Object value) {
    return on(method, args -> value);
  }

  /** The recorded {@code method} throws {@code failure} from now on. */
  public FakeArcadeStateMachine fails(final String method, final RuntimeException failure) {
    return on(method, args -> {
      throw failure;
    });
  }

  /** The recorded {@code method} runs {@code answer} on its arguments from now on; a {@code void} one ignores the value. */
  public FakeArcadeStateMachine on(final String method, final Function<Object[], Object> answer) {
    answers.set(method, answer);
    return this;
  }

  private Object call(final String method, final Supplier<Object> fallback, final Object... args) {
    // The real constructor runs before this class's fields exist and may call an overridden method: it runs for real
    if (log == null || answers == null)
      return fallback.get();
    log.record(this, method, args);
    final Function<Object[], Object> answer = answers.get(method);
    return answer != null ? answer.apply(args) : fallback.get();
  }

  private static boolean bool(final String method, final Object answer) {
    if (!(answer instanceof Boolean value))
      throw new IllegalStateException("The answer set for '" + method + "' must be a Boolean, it gave " + answer);
    return value;
  }

  @Override
  boolean hasLeaderServiceGap() {
    return bool("hasLeaderServiceGap", call("hasLeaderServiceGap", super::hasLeaderServiceGap));
  }

  @Override
  public boolean isBootstrapInstallInFlight(final String dbName) {
    return bool("isBootstrapInstallInFlight",
        call("isBootstrapInstallInFlight", () -> super.isBootstrapInstallInFlight(dbName), dbName));
  }

  /** Recorded; unanswered, no hand-off happens and none is reported. */
  @Override
  public boolean handOffLeadershipWhileReplacingDatabase() {
    return bool("handOffLeadershipWhileReplacingDatabase", call("handOffLeadershipWhileReplacingDatabase", () -> false));
  }

  /** Recorded once, here: the one-argument form delegates with a {@code null} order. Unanswered, nothing is installed. */
  @Override
  public void resyncDatabaseFromLeader(final String dbName, final StalledResyncOrder order) {
    call("resyncDatabaseFromLeader", () -> null, dbName, order);
  }

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

  /** A state machine whose {@code close()} ran - as one a Ratis restart replaced - without closing anything. */
  public FakeArcadeStateMachine closed(final boolean closed) {
    this.closed = closed;
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
  boolean isClosed() {
    return closed || super.isClosed();
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    final TermIndex set = lastAppliedTermIndex;
    return set != null ? set : super.getLastAppliedTermIndex();
  }
}
