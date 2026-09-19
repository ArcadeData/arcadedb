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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7602: a locally-originated entry whose page publication AND whose reconciliation from the replicated
 * payload both fail must not be recorded as applied.
 * <p>
 * The #6965 rework of the local-commit path removed the #5407 replay floor - {@code lowestPendingLocalPhase2Floor}
 * and the {@code takeSnapshot()} clamp built on it - on the argument that pages are now published before the
 * applied index moves past the entry. That argument holds only while an entry whose pages did NOT reach the disk
 * also fails to advance the index, and it did not: {@code publishLocalCommit} logged the reconcile failure and
 * returned, so {@code applyTransaction} advanced {@code lastAppliedIndex}, answered OK, and the next checkpoint
 * recorded a position covering an entry this node does not hold. The node that ORIGINATED the write was the one
 * that lost it - permanently, because a restart replays nothing past a checkpoint - while every follower had it,
 * and nothing quarantined the database or armed a resync.
 * <p>
 * The asymmetry is what makes it a defect rather than a design: {@code applyReplicatedTransaction} throws on the
 * identical double failure and reaches the per-database quarantine of issue #4797. The fix makes the local path
 * answer the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7602LocalCommitNotAppliedTest {
  private static final long ENTRY_INDEX = 7L;

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine sm;
  private RaftStorage        storage;
  /** Set by the phase-2 fault, so only the reconcile that FOLLOWS a failed publication is broken. */
  private volatile boolean   publishFailed;

  @AfterEach
  void tearDown() throws IOException {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    if (db != null && db.isOpen()) {
      // The fixture leaves a phase-1 transaction open on the database (the committing thread never gets to
      // completeCommit here), and drop() refuses inside a transaction.
      if (db.isTransactionActive())
        db.rollback();
      db.drop();
    }
  }

  /**
   * Both halves fail. The entry must be reported as not applied, the database quarantined for a resync, and the
   * applied index left where it was - so a checkpoint cannot record a position that covers the entry and the
   * entry stays replayable.
   */
  @Test
  void anEntryThatCouldNeitherBePublishedNorReconciledIsNotRecordedAsApplied() throws Exception {
    final Fixture fixture = newFixture(true);

    // The publication fails: the fault fires where publishCommittedPages sits in the apply path.
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
      publishFailed = true;
      throw new IllegalStateException("the data volume is read-only");
    };

    assertThatThrownBy(() -> sm.applyTransaction(fixture.ratisContext(sm)).get())
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(ReplicationException.class);

    assertThat(fixture.local.awaitOutcome(1_000))
        .as("the committing thread is still woken, whatever the apply thread does next")
        .isEqualTo(LocalCommit.Outcome.FAILED);
    assertThat(fixture.local.reconciled()).as("and told the pages were NOT reconciled either").isFalse();

    assertThat(sm.isDatabaseDiverged(db.getName()))
        .as("the database is quarantined, exactly as the follower path quarantines it for the same failure")
        .isTrue();
    assertThat(indexOf(sm.getLastAppliedTermIndex()))
        .as("an entry this node does not hold must not move the applied position")
        .isLessThan(ENTRY_INDEX);
    assertThat(sm.takeSnapshot())
        .as("and no checkpoint may cover it, which is what keeps it replayable")
        .isLessThan(ENTRY_INDEX);
  }

  /**
   * The other side of the branch, which the fix must not disturb: when the publication fails but the reconcile
   * from the replicated payload SUCCEEDS, the entry IS applied here - the pages are on disk, from the same bytes
   * every other node applied - so the index advances and the database is not quarantined.
   */
  @Test
  void anEntryReconciledFromTheReplicatedPayloadIsStillApplied() throws Exception {
    final Fixture fixture = newFixture(false);

    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> {
      publishFailed = true;
      throw new IllegalStateException("the prepared pages could not be published");
    };

    sm.applyTransaction(fixture.ratisContext(sm)).get();

    assertThat(fixture.local.awaitOutcome(1_000)).isEqualTo(LocalCommit.Outcome.FAILED);
    assertThat(fixture.local.reconciled()).as("the replicated payload got the pages down").isTrue();
    assertThat(sm.isDatabaseDiverged(db.getName())).as("nothing to quarantine: the entry is applied").isFalse();
    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
    assertThat(sm.takeSnapshot()).isEqualTo(ENTRY_INDEX);
  }

  /** The registered transaction and the Raft entry that carries it. */
  private record Fixture(LocalCommit local, LogEntryProto logEntry) {
    org.apache.ratis.statemachine.TransactionContext ratisContext(final ArcadeStateMachine sm) {
      return org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry)
          .build();
    }
  }

  /**
   * A state machine over a real database with a real phase-1 transaction registered against it, as the
   * committing thread leaves it before dispatching the entry.
   *
   * @param breakTheReconcile when true, {@code databaseFor} refuses once the publication has failed - the second
   *                          failure. Gated on {@link #publishFailed} rather than unconditional, so nothing the
   *                          apply path resolves a database for BEFORE the publication is affected by it.
   */
  private Fixture newFixture(final boolean breakTheReconcile) throws Exception {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
    db.getSchema().createDocumentType("Doc", 1);
    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT).build();
    sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        if (breakTheReconcile && publishFailed)
          throw new IllegalStateException("the database cannot be reached to reconcile from the replicated payload");
        return db;
      }
    };
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

    db.begin();
    db.newDocument("Doc").set("v", 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    final LocalCommit local = new LocalCommit(db.getName(), ArcadeStateMachine.peekWalTransactionId(walData), tx,
        phase1, walData);
    assertThat(sm.registerLocalCommit(local)).isTrue();

    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(db.getName(), walData, tx.getBucketRecordDelta());
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(1L).setIndex(ENTRY_INDEX)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();

    return new Fixture(local, logEntry);
  }

  private static long indexOf(final TermIndex termIndex) {
    return termIndex != null ? termIndex.getIndex() : -1L;
  }

  private RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { RaftServer.class },
        (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
