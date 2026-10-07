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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #8784: a transaction that committed cluster-wide must end, on the node that originated it, as a commit - its
 * after-commit callbacks fired, its commit counted, its records clean - whichever thread published its pages. The
 * paths where the state machine publishes them from the entry's WAL bytes (a replica, #5503; a leader whose entry was
 * acknowledged unclaimed while its state machine lives, #8781; the MAJORITY-committed recovery of both) and the
 * read-only arms (nothing to publish) used to end it with a bare {@code reset()}, which drops all three.
 * <p>
 * Driven through {@link RaftReplicatedDatabase#commit()} against a REAL {@link LocalDatabase} and a real
 * {@link TransactionContext}, because what is under test is what the context does at the end. The Raft server and the
 * broker are the only mocks: the broker answers the way the cluster would, and nothing applies the entry, which is
 * irrelevant here - the callbacks and the counter belong to the committing thread.
 */
class Issue8784ReplicatedCommitFiresAfterCommitCallbacksTest {
  private static final String TYPE = "Doc8784";

  @TempDir
  Path tempDir;

  private LocalDatabase          proxied;
  private RaftHAServer           raftServer;
  private RaftTransactionBroker  broker;
  private RaftReplicatedDatabase database;
  private ThreadLocal<Boolean>   schemaCommitThread;
  private final AtomicInteger    fired = new AtomicInteger();

  @BeforeEach
  void setUp() throws Exception {
    // Schema and property names in place BEFORE the wrapper exists, so no DDL or dictionary commit takes the Raft path.
    proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue8784").toString()).create();
    proxied.getSchema().createDocumentType(TYPE, 1).createProperty("name", Type.STRING);
    proxied.transaction(() -> proxied.newDocument(TYPE).set("name", "seed").save());

    broker = SubclassMocks.mock(RaftTransactionBroker.class);
    raftServer = SubclassMocks.mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.getTransactionBroker()).thenReturn(broker);
    when(raftServer.getQuorumTimeout()).thenReturn(1_000L);
    when(raftServer.getStateMachine()).thenReturn(new ArcadeStateMachine());
    when(raftServer.isShutdownRequested()).thenReturn(false);
    when(raftServer.canStateTxPreparedAt()).thenReturn(false);

    database = new RaftReplicatedDatabase(null, proxied, raftServer);

    final Field field = RaftReplicatedDatabase.class.getDeclaredField("isSchemaCommitThread");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    final ThreadLocal<Boolean> tl = (ThreadLocal<Boolean>) field.get(null);
    schemaCommitThread = tl;
  }

  @AfterEach
  void tearDown() {
    schemaCommitThread.remove();
    if (proxied != null && proxied.isOpen()) {
      proxied.rollbackAllNested();
      proxied.drop();
    }
  }

  /** The replica commit tail (#5503): the entry is acknowledged and this replica's state machine publishes the pages. */
  @Test
  void aReplicaCommitFiresItsCallbacksAndIsCounted() {
    asReplica();
    acknowledgeAt(7L);

    final Begun begun = beginUpdate();
    database.commit();

    begun.assertEndedAsACommit();
  }

  /** The replica's MAJORITY-committed / ALL-watch-failed recovery: the commit stands, the watch failure is reported. */
  @Test
  void aReplicaMajorityCommittedCommitFiresItsCallbacks() {
    asReplica();
    when(broker.replicateTransaction(anyString(), any(), any())).thenThrow(
        new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit at logIndex=9"));

    final Begun begun = beginUpdate();
    assertThatThrownBy(database::commit).isInstanceOf(MajorityCommittedAllFailedException.class);

    begun.assertEndedAsACommit();
  }

  /**
   * Issue #8781's path: a leader deposed mid-commit has its entry acknowledged with the registration unclaimed while its
   * state machine lives, so the state machine applies it from the WAL bytes and this thread only waits and releases.
   */
  @Test
  void aLeaderCommitAcknowledgedUnclaimedWhileTheStateMachineLivesFiresItsCallbacks() {
    asLeader();
    acknowledgeAt(7L);

    final Begun begun = beginUpdate();
    database.commit();

    begun.assertEndedAsACommit();
  }

  /** The leader's MAJORITY-committed recovery with a live state machine: the same wait-and-release tail. */
  @Test
  void aLeaderMajorityCommittedCommitWhileTheStateMachineLivesFiresItsCallbacks() {
    asLeader();
    when(broker.replicateTransaction(anyString(), any(), any())).thenThrow(
        new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));

    final Begun begun = beginUpdate();
    assertThatThrownBy(database::commit).isInstanceOf(MajorityCommittedAllFailedException.class);

    begun.assertEndedAsACommit();
  }

  /** The ordinary read-only arm on a replica: nothing to replicate is still a commit, as {@code commit()} has it off HA. */
  @Test
  void aReadOnlyCommitOnAReplicaFiresItsCallbacks() {
    asReplica();

    final Begun begun = beginReadOnly();
    database.commit();

    begun.assertEndedAsACommit();
  }

  /** The ordinary read-only arm on the leader. */
  @Test
  void aReadOnlyCommitOnTheLeaderFiresItsCallbacks() {
    asLeader();

    final Begun begun = beginReadOnly();
    database.commit();

    begun.assertEndedAsACommit();
  }

  /** The schema-commit arm's read-only branch: a commit inside a DDL callback on the leader that wrote nothing. */
  @Test
  void aReadOnlyCommitOnTheSchemaCommitArmFiresItsCallbacks() {
    asLeader();

    final Begun begun = beginReadOnly();
    schemaCommitThread.set(Boolean.TRUE);
    database.commit();
    schemaCommitThread.remove();

    begun.assertEndedAsACommit();
  }

  /**
   * A callback runs while the concluded context is still on the thread's stack, as it does after {@code commit()}: one that
   * opens and commits a transaction of its own - what a materialized-view refresh does - must work, and leave nothing
   * open behind.
   */
  @Test
  void aCallbackThatCommitsItsOwnTransactionOnAReplicaLeavesNothingOpen() {
    asReplica();
    acknowledgeAt(7L);

    final AtomicInteger innerCommitted = new AtomicInteger();
    final Begun begun = beginUpdate();
    begun.tx.addAfterCommitCallback(() -> {
      database.begin();
      proxied.newDocument(TYPE).set("name", "from-callback").save();
      database.commit();
      innerCommitted.incrementAndGet();
    });
    database.commit();

    assertThat(fired.get()).isEqualTo(1);
    assertThat(innerCommitted.get()).as("the callback's own transaction committed").isEqualTo(1);
    // The callback's begin() reuses the concluded context, as it does after commit() off HA, so both commits count on it.
    assertThat(begun.tx.getCommitCount()).isEqualTo(begun.commitCountBefore + 2);
    verify(broker, times(2)).replicateTransaction(anyString(), any(), any());
    assertThat(proxied.isTransactionActive()).as("nothing is left open on the thread").isFalse();
  }

  /** The other side of the contract: a commit the cluster refused is rolled back, and neither fires nor counts. */
  @Test
  void aRefusedReplicaCommitFiresNothing() {
    asReplica();
    when(broker.replicateTransaction(anyString(), any(), any()))
        .thenThrow(new ConcurrentModificationException("Concurrent modification on page 3/0"));

    final Begun begun = beginUpdate();
    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);

    assertThat(fired.get()).as("a refused commit must not fire the after-commit callbacks").isZero();
    assertThat(begun.tx.getCommitCount()).as("a refused commit is not counted").isEqualTo(begun.commitCountBefore);
    assertThat(proxied.isTransactionActive()).isFalse();
  }

  private void asReplica() {
    when(raftServer.isLeader()).thenReturn(false);
  }

  private void asLeader() {
    when(raftServer.isLeader()).thenReturn(true);
  }

  private void acknowledgeAt(final long logIndex) {
    when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(logIndex);
    when(broker.replicateTransaction(anyString(), any(), any(), anyLong())).thenReturn(logIndex);
  }

  private Begun beginUpdate() {
    database.begin();
    final MutableDocument doc = proxied.iterateType(TYPE, false).next().asDocument().modify();
    doc.set("name", "updated").save();
    return new Begun(doc);
  }

  private Begun beginReadOnly() {
    database.begin();
    return new Begun(null);
  }

  private final class Begun {
    final TransactionContext tx;
    final long               commitCountBefore;
    final MutableDocument    doc;

    Begun(final MutableDocument doc) {
      this.doc = doc;
      this.tx = proxied.getTransaction();
      this.commitCountBefore = tx.getCommitCount();
      tx.addAfterCommitCallback(fired::incrementAndGet);
    }

    void assertEndedAsACommit() {
      assertThat(fired.get()).as("the after-commit callback fires exactly once on the originating node").isEqualTo(1);
      assertThat(tx.getCommitCount()).as("the commit is counted, or a retry loop replays a durable block (#7916)")
          .isEqualTo(commitCountBefore + 1);
      if (doc != null)
        assertThat(doc.isDirty()).as("the committed record is clean").isFalse();
      assertThat(tx.isActive()).isFalse();
      assertThat(proxied.isTransactionActive()).isFalse();
    }
  }
}
