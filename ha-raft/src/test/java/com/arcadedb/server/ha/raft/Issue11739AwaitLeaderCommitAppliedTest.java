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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: a follower that forwarded {@code create database} or {@code drop database} answers once it has applied what the
 * leader had committed. {@link RaftHAPlugin#awaitLeaderCommitApplied()} is that wait: it asks the leader for its commit index, then
 * waits for the local applied index to reach it. It is best effort, so it answers {@code false} where it cannot confirm, and never
 * fails the command, which the leader has already made. The 3-node case is {@code RaftCreateDatabaseOnFollowerIT}; these tests pin
 * the outcomes that one cannot reach cheaply: nothing to wait for, a leader that cannot be asked or never answers, a follower that
 * catches up while waiting, one that does not, and one inside a snapshot resync gap.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue11739AwaitLeaderCommitAppliedTest {
  private static final long LEADER_COMMIT_INDEX = 500L;

  /** Short, so the test that waits it out is quick. */
  private static final long QUORUM_TIMEOUT_MS = 400L;

  /** The tripwire between a wait that gave up and one that is unbounded, or one that sat out a timeout it should have skipped. */
  private static final long TRIPWIRE_MS = 30_000L;

  @Test
  void aPluginWithoutARaftServerHasNothingToWaitFor() {
    final RaftHAPlugin plugin = new RaftHAPlugin();

    assertThat(plugin.awaitLeaderCommitApplied()).isTrue();
    assertThat(plugin.localWaiters()).isZero();
  }

  @Test
  void aLeaderThatCannotBeAskedIsAnsweredFalseAndLeavesNoWaiter() {
    final LaggingFollower raft = new LaggingFollower(QUORUM_TIMEOUT_MS);
    raft.readIndexFails = true;
    final RaftHAPlugin plugin = pluginOf(raft);

    assertThat(plugin.awaitLeaderCommitApplied()).as("the leader's commit index could not be read: nothing is confirmed").isFalse();
    assertThat(plugin.localWaiters()).as("the waiter count is released on the failure path too").isZero();
  }

  @Test
  void aFollowerThatAlreadyAppliedTheLeadersCommitIndexConfirmsAtOnce() {
    final LaggingFollower raft = new LaggingFollower(TRIPWIRE_MS);
    raft.applied.set(LEADER_COMMIT_INDEX);
    final RaftHAPlugin plugin = pluginOf(raft);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(plugin.awaitLeaderCommitApplied()).isTrue();
    watch.assertGaveUpWithin(TRIPWIRE_MS / 2, "an index already applied from one that waits out the quorum timeout");
  }

  @Test
  void aFollowerThatCatchesUpWhileWaitingConfirms() throws Exception {
    final LaggingFollower raft = new LaggingFollower(TRIPWIRE_MS);
    raft.applied.set(LEADER_COMMIT_INDEX - 10);
    final RaftHAPlugin plugin = pluginOf(raft);

    final Thread applier = new Thread(() -> {
      try {
        Thread.sleep(200L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      raft.applied.set(LEADER_COMMIT_INDEX);
      raft.notifyApplied();
    }, "issue11739-applier");
    applier.start();
    try {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThat(plugin.awaitLeaderCommitApplied()).as("the follower applied the leader's commit index while the call waited")
          .isTrue();
      watch.assertGaveUpWithin(TRIPWIRE_MS / 2, "a wait that ends once the index is applied from one that sits out the timeout");
    } finally {
      applier.join(TRIPWIRE_MS);
    }
  }

  @Test
  void aFollowerThatDoesNotCatchUpIsAnsweredFalseAfterTheQuorumTimeout() {
    final LaggingFollower raft = new LaggingFollower(QUORUM_TIMEOUT_MS);
    raft.applied.set(LEADER_COMMIT_INDEX - 10);
    final RaftHAPlugin plugin = pluginOf(raft);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(plugin.awaitLeaderCommitApplied()).as("the follower never reached the leader's commit index").isFalse();
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait bounded by arcadedb.ha.quorumTimeout from one that never ends");
    assertThat(plugin.localWaiters()).isZero();
  }

  /**
   * The case the first version of the fix burned the whole quorum timeout on: a node inside a snapshot resync gap advertises an
   * applied index that covers entries it does not hold, and waiting cannot close the gap. The lenient apply wait gives up at once.
   */
  @Test
  void aFollowerInsideASnapshotResyncGapIsAnsweredFalseWithoutSittingOutTheQuorumTimeout() {
    final LaggingFollower raft = new LaggingFollower(TRIPWIRE_MS * 2);
    // Ratis reports the applied index of the snapshot marker, above the leader's commit index...
    raft.applied.set(LEADER_COMMIT_INDEX + 1_000);
    // ...but the entries up to the floor are all this node really holds
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    sm.setServer(TestServerHelper.unstartedServer("localhost"));
    sm.staleSnapshotAppliedFloor.set(LEADER_COMMIT_INDEX - 100);
    raft.stateMachine = sm;
    final RaftHAPlugin plugin = pluginOf(raft);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(plugin.awaitLeaderCommitApplied()).as("the gap is still open: the change is not confirmed applied here").isFalse();
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a wait that skips a gap only a resync can fill from one that sits out the quorum timeout");
  }

  /**
   * The ReadIndex round is bounded by the quorum timeout too: the shared Raft client retries up to 60 times, one second apart, when
   * no leader answers, and a forwarded command must not hold its HTTP worker thread that long.
   */
  @Test
  void aLeaderThatNeverAnswersTheReadIndexIsGivenUpOnAtTheQuorumTimeoutAndTheRequestIsCancelled() {
    final LaggingFollower raft = new LaggingFollower(QUORUM_TIMEOUT_MS);
    final CompletableFuture<RaftClientReply> request = new CompletableFuture<>();
    raft.raftClient = clientSending(request);
    raft.askTheClient = true;
    final RaftHAPlugin plugin = pluginOf(raft);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(plugin.awaitLeaderCommitApplied()).as("no leader answered: nothing is confirmed").isFalse();
    watch.assertGaveUpWithin(TRIPWIRE_MS, "a ReadIndex round bounded by arcadedb.ha.quorumTimeout from the minute of the client's retries");
    assertThat(request.isCancelled()).as("the request stops retrying once the wait gave up").isTrue();
    assertThat(plugin.localWaiters()).isZero();
  }

  /** A Raft client whose unordered read-only request is {@code request}, which a test completes, or leaves pending. */
  private static RaftClient clientSending(final CompletableFuture<RaftClientReply> request) {
    final AsyncApi async = (AsyncApi) Proxy.newProxyInstance(AsyncApi.class.getClassLoader(), new Class<?>[] { AsyncApi.class },
        (proxy, method, args) -> {
          if ("sendReadOnlyUnordered".equals(method.getName()))
            return request;
          if ("toString".equals(method.getName()))
            return "AsyncApi stand-in of issue #11739";
          throw new UnsupportedOperationException(method.getName());
        });
    return (RaftClient) Proxy.newProxyInstance(RaftClient.class.getClassLoader(), new Class<?>[] { RaftClient.class },
        (proxy, method, args) -> {
          if ("async".equals(method.getName()))
            return async;
          if ("toString".equals(method.getName()))
            return "RaftClient stand-in of issue #11739";
          throw new UnsupportedOperationException(method.getName());
        });
  }

  private static RaftHAPlugin pluginOf(final RaftHAServer raft) {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.setRaftHAServer(raft);
    return plugin;
  }

  /** A real, detached {@link RaftHAServer} whose leader commit index is {@link #LEADER_COMMIT_INDEX} and whose applied index a test sets. */
  private static final class LaggingFollower extends RaftHAServer {
    final    AtomicLong applied = new AtomicLong(-1L);
    volatile boolean    readIndexFails;
    /** Reads the leader's commit index through {@link #raftClient}, as the real server does, instead of answering it. */
    volatile boolean    askTheClient;

    LaggingFollower(final long quorumTimeoutMs) {
      super(TestServerHelper.unstartedServer("localhost"), configuration(quorumTimeoutMs));
    }

    private static ContextConfiguration configuration(final long quorumTimeoutMs) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
      configuration.setValue(GlobalConfiguration.HA_QUORUM_TIMEOUT, quorumTimeoutMs);
      return configuration;
    }

    @Override
    public long fetchReadIndex(final boolean expectSelfIsLeader, final long timeoutMs) {
      if (askTheClient)
        return super.fetchReadIndex(expectSelfIsLeader, timeoutMs);
      if (readIndexFails)
        throw new ReplicationException("ReadIndex failed: leader unavailable");
      return LEADER_COMMIT_INDEX;
    }

    @Override
    public long getLastAppliedIndex() {
      return applied.get();
    }
  }
}
