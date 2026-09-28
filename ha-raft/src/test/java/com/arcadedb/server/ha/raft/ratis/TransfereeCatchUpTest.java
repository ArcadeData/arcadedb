/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TransfereeCatchUp} (issue #8533): the target of a leadership transfer holds its
 * {@code StartLeaderElection} until it knows the leader's last data entry to be committed and holds what the leader
 * appended after it, and only in that shape.
 */
class TransfereeCatchUpTest {

  private static final long D    = 724;
  private static final long TERM = 15;

  /** A local log whose commit and last index a test moves, holding data entries up to {@link #lastIndex}. */
  private static final class FakeLog implements TransfereeCatchUp.LocalLog {
    final AtomicLong commit = new AtomicLong();
    final AtomicLong last   = new AtomicLong();
    long             metadataIndex = -1;

    FakeLog(final long commit, final long last) {
      this.commit.set(commit);
      this.last.set(last);
    }

    @Override
    public long commitIndex() {
      return commit.get();
    }

    @Override
    public long lastIndex() {
      return last.get();
    }

    @Override
    public boolean isDataEntry(final long index, final long term) {
      return index <= last.get() && term == TERM && index != metadataIndex;
    }
  }

  @Test
  void theTargetOfTheRaceWaitsForTheCommitAndTheMetadataEntryAfterIt() throws Exception {
    // The target acked D, which is what completed the majority: D is not committed on it yet, and the leader is about to
    // append the metadata entry D+1 and send it together with commit=D.
    final FakeLog log = new FakeLog(D - 1, D);
    final Thread leader = new Thread(() -> {
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        return;
      }
      log.last.set(D + 1);
      log.commit.set(D);
    });
    leader.start();

    final boolean waited = TransfereeCatchUp.awaitCatchUp(log, D, TERM, 10_000, 5_000);
    leader.join();

    assertThat(waited).isTrue();
    // Returned only once it could campaign with a log at least as long as the voters': D committed and D+1 held.
    assertThat(log.commitIndex()).isGreaterThanOrEqualTo(D);
    assertThat(log.lastIndex()).isGreaterThan(D);
  }

  @Test
  void aCommitArrivingBeforeTheEntryAfterItStillWaitsForThatEntry() throws Exception {
    // The leader read its commit index for a request before appending the metadata entry: commit=D arrives first.
    final FakeLog log = new FakeLog(D - 1, D);
    final Thread leader = new Thread(() -> {
      try {
        Thread.sleep(50);
        log.commit.set(D);
        Thread.sleep(50);
        log.last.set(D + 1);
      } catch (final InterruptedException e) {
        // ends the thread
      }
    });
    leader.start();

    final boolean waited = TransfereeCatchUp.awaitCatchUp(log, D, TERM, 10_000, 5_000);
    leader.join();

    assertThat(waited).isTrue();
    assertThat(log.lastIndex()).isGreaterThan(D);
  }

  @Test
  void aLastEntryThatIsAMetadataEntryNeedsNoWait() throws Exception {
    // The quiescent case: committing a metadata entry appends nothing further, so there is nothing to wait for.
    final FakeLog log = new FakeLog(D - 1, D);
    log.metadataIndex = D;
    assertThat(TransfereeCatchUp.awaitCatchUp(log, D, TERM, 10_000, 5_000)).isFalse();
  }

  @Test
  void anAlreadyCommittedLastEntryNeedsNoWait() throws Exception {
    final FakeLog log = new FakeLog(D, D);
    assertThat(TransfereeCatchUp.awaitCatchUp(log, D, TERM, 10_000, 5_000)).isFalse();
  }

  @Test
  void anEntryOfAnotherTermOrAMissingEntryNeedsNoWait() throws Exception {
    assertThat(TransfereeCatchUp.awaitCatchUp(new FakeLog(D - 1, D), D, TERM + 1, 10_000, 5_000)).isFalse();
    assertThat(TransfereeCatchUp.awaitCatchUp(new FakeLog(D - 2, D - 1), D, TERM, 10_000, 5_000)).isFalse();
  }

  @Test
  void theFirstEntryOfTheLogNeedsNoWait() throws Exception {
    // BootstrapElection's transfer on a fresh cluster: the last entry is the initial configuration at index 0, whose
    // commit Ratis never follows with a metadata entry, so there is no race and nothing worth waiting the bound for.
    final FakeLog log = new FakeLog(-1, 0);
    assertThat(TransfereeCatchUp.awaitCatchUp(log, 0, TERM, 10_000, 5_000)).isFalse();
  }

  @Test
  void aCommitThatNeverComesIsWaitedForOnlyUpToTheBound() throws Exception {
    final FakeLog log = new FakeLog(D - 1, D);
    final long start = System.nanoTime();
    assertThat(TransfereeCatchUp.awaitCatchUp(log, D, TERM, 100, 50)).isTrue();
    // A lower bound only: the wait is what it gave up after, never a latency claim.
    assertThat((System.nanoTime() - start) / 1_000_000).isGreaterThanOrEqualTo(100);
  }

  @Test
  void theWaitStaysUnderHalfTheMinimumElectionTimeout() {
    // A transfer Ratis starts on its own gives up after the minimum election timeout: a target still waiting then would
    // campaign against a leader that has resumed.
    assertThat(TransfereeCatchUp.maxWaitMs(serverWithElectionTimeoutMin(5_000))).isEqualTo(TransfereeCatchUp.MAX_WAIT_MS);
    assertThat(TransfereeCatchUp.maxWaitMs(serverWithElectionTimeoutMin(1_000))).isEqualTo(500);
  }

  private static RaftServer serverWithElectionTimeoutMin(final long ms) {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(ms, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(ms * 2, TimeUnit.MILLISECONDS));
    return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> "getProperties".equals(method.getName()) ? properties : null);
  }

  @Test
  void theWrappedServerHandsEveryCallToTheRealOneAndItsFailuresBack() throws Exception {
    final List<String> calls = new ArrayList<>();
    final StartLeaderElectionReplyProto reply = StartLeaderElectionReplyProto.getDefaultInstance();
    final RaftServer real = (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          calls.add(method.getName());
          return switch (method.getName()) {
            case "getId" -> RaftPeerId.valueOf("n1");
            case "startLeaderElection" -> reply;
            // getDivision fails: the gate must let the request through as asked rather than fail the election.
            case "getDivision" -> throw new IOException("no such group");
            case "close" -> throw new IOException("close failed");
            default -> null;
          };
        });

    final RaftServer wrapped = TransfereeCatchUp.wrap(real);

    assertThat(wrapped.getId()).isEqualTo(RaftPeerId.valueOf("n1"));

    final StartLeaderElectionRequestProto request = StartLeaderElectionRequestProto.newBuilder()
        .setServerRequest(RaftRpcRequestProto.newBuilder()
            .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(RaftGroupId.randomId().toByteString())))
        .setLeaderLastEntry(TermIndexProto.newBuilder().setTerm(TERM).setIndex(D)).build();
    assertThat(wrapped.startLeaderElection(request)).isSameAs(reply);
    assertThat(calls).containsSubsequence("getDivision", "startLeaderElection");

    // A checked exception of the real server comes out as itself, not as an UndeclaredThrowableException.
    assertThatThrownBy(wrapped::close).isInstanceOf(IOException.class).hasMessage("close failed");
  }
}
