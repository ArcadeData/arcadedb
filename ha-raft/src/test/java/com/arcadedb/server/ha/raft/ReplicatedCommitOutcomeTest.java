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
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas.CorpusDelta;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Type;
import com.arcadedb.server.ha.raft.ratis.RatisRefusedEntryErrorFilter;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What a locally originated transaction ends as once its entry is committed in the Raft log: reconciliation of the leader's pages,
 * refusal of a rollback-only transaction, after-commit callbacks, reservation release and applied-entry failures.
 */
class ReplicatedCommitOutcomeTest {

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

   */
  @Nested
  class LocalCommitNotApplied {
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
    // Regression for #7602: LocalCommitNotApplied
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
    // Regression for #7602: LocalCommitNotApplied
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
      final LocalCommit local = new LocalCommit(db.getName(), CommittedEntryDecoding.peekWalTransactionId(walData), tx,
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

  /**
   * Issue #8053: the rollback-only marker of issue #7467 must stop the REPLICATED commit too.
   * <p>
   * {@link RaftReplicatedDatabase#commit()} never calls {@code TransactionContext.commit()} - it drives
   * {@code commit1stPhase}/{@code commit2ndPhase} itself so it can put the WAL bytes on the wire between the two
   * phases - and the marker's only read used to be at the top of {@code commit()}. A transaction that could not
   * take back a half-written record was therefore refused on a standalone server and published on an HA leader,
   * where it does not merely land locally but ships as a {@code TX_ENTRY} and is applied on every follower.
   * <p>
   * Both of the wrapper's phase-1 call sites are driven here against a REAL {@link LocalDatabase} and a real
   * {@link TransactionContext}, because the guard lives in the transaction and a mocked one cannot refuse
   * anything. The Raft server is the only mock: what matters is that the broker is never asked to replicate.

   */
  @Nested
  class RollbackOnlyRefusedOnTheReplicatedCommitPath {
    private static final String TYPE   = "Doc8053";
    private static final String REASON = "record #1:0 could not be taken back after its indexing refused it";

    @TempDir
    Path tempDir;

    private LocalDatabase          proxied;
    private FakeRaftTransactionBroker  broker;
    private RaftReplicatedDatabase database;

    @BeforeEach
    void setUp() {
      // The type is created BEFORE the wrapper is installed: a DDL statement issued afterwards would route
      // through the Raft schema path, which needs a real cluster and is not what is under test here.
      proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue8053").toString()).create();
      proxied.getSchema().createDocumentType(TYPE, 1).createProperty("name", Type.STRING);

      // Same reason, one level down: the FIRST serialization of a property name the dictionary has not seen
      // opens its own nested transaction (Dictionary.getIdByName) and commits it, which after the wrapper is
      // installed would be a Raft commit of the dictionary page rather than of the record under test. Warm the
      // name in, then take the record back out so the counts below start from zero.
      final MutableDocument[] warmUp = new MutableDocument[1];
      proxied.transaction(() -> warmUp[0] = proxied.newDocument(TYPE).set("name", "warm-up").save());
      proxied.transaction(() -> warmUp[0].delete());

      broker = new FakeRaftTransactionBroker();
      final RaftHAServer raftServer = FakeRaftHAServer.detached().leader(true).transactionBroker(broker);

      // The constructor points the database's wrapped instance at the replicated wrapper, which is what makes
      // this the commit path a write on a replicated database really takes.
      database = new RaftReplicatedDatabase(null, proxied, raftServer);
    }

    @AfterEach
    void tearDown() {
      if (proxied != null && proxied.isOpen()) {
        if (proxied.isTransactionActive())
          proxied.rollback();
        proxied.drop();
      }
    }

    /**
     * The ordinary phase-1 arm - every user write on a replicated database. The commit must be refused and the
     * broker must never be asked to replicate anything, because the WAL bytes are produced by the very call that
     * now refuses.
     */
    // Regression for #8053: RollbackOnlyRefusedOnTheReplicatedCommitPath
    @Test
    void theReplicatedCommitRefusesARollbackOnlyTransaction() {
      database.begin();
      proxied.newDocument(TYPE).set("name", "in-flight").save();
      proxied.getTransaction().setRollbackOnly(REASON);

      assertThatThrownBy(database::commit)
          .isInstanceOf(TransactionException.class)
          .hasMessageContaining("could not be taken back")
          .hasMessageContaining("Roll it back");

      assertThat(broker.calls("replicateTransaction")).as("nothing was submitted to Raft").isEmpty();
      assertThat(proxied.isTransactionActive())
          .as("the wrapper's own phase-1 error handling rolled it back")
          .isFalse();
      assertThat(proxied.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
    }

    /**
     * The schema-commit arm, the second of the wrapper's two direct phase-1 calls: it buffers the WAL bytes for
     * the {@code SCHEMA_ENTRY} instead of shipping a {@code TX_ENTRY}, and used to buffer them for a transaction
     * that could not be published either.
     * <p>
     * Reached by setting the thread-local the arm keys on, which is what {@code recordFileChanges} sets around a
     * DDL callback. Reflection rather than a real DDL statement: the alternative needs a live Raft group, and
     * what is under test is which branch of {@code commit()} runs, not how the branch is selected.
     */
    // Regression for #8053: RollbackOnlyRefusedOnTheReplicatedCommitPath
    @Test
    void theSchemaCommitArmRefusesItToo() throws Exception {
      final ThreadLocal<Boolean> schemaCommitThread = threadLocal("isSchemaCommitThread");
      final ThreadLocal<List<byte[]>> schemaWalBuffer = threadLocal("schemaWalBuffer");

      database.begin();
      proxied.newDocument(TYPE).set("name", "in-flight").save();
      proxied.getTransaction().setRollbackOnly(REASON);

      schemaCommitThread.set(Boolean.TRUE);
      try {
        assertThatThrownBy(database::commit)
            .isInstanceOf(TransactionException.class)
            .hasMessageContaining("could not be taken back");

        assertThat(schemaWalBuffer.get())
            .as("no WAL bytes of an unpublishable transaction may be buffered for the SCHEMA_ENTRY")
            .isEmpty();

        // Since #8149 this arm rolls back what phase 1 refused, the way the ordinary arm above always has.
        assertThat(proxied.isTransactionActive())
            .as("the schema-commit arm rolls back what phase 1 refused (#8149)")
            .isFalse();
      } finally {
        schemaCommitThread.remove();
        schemaWalBuffer.remove();
        if (proxied.isTransactionActive())
          proxied.rollback();
      }

      assertThat(proxied.countType(TYPE, false)).as("nothing of the refused transaction may be durable").isZero();
    }

    @SuppressWarnings("unchecked")
    private static <T> ThreadLocal<T> threadLocal(final String name) throws Exception {
      final Field field = RaftReplicatedDatabase.class.getDeclaredField(name);
      field.setAccessible(true);
      return (ThreadLocal<T>) field.get(null);
    }
  }

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
  @Nested
  class ReplicatedCommitFiresAfterCommitCallbacks {
    private static final String TYPE = "Doc8784";

    @TempDir
    Path tempDir;

    private LocalDatabase             proxied;
    private FakeRaftHAServer          raftServer;
    private FakeRaftTransactionBroker broker;
    private RaftReplicatedDatabase    database;
    private ThreadLocal<Boolean>      schemaCommitThread;
    private final AtomicInteger       fired = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
      // Schema and property names in place BEFORE the wrapper exists, so no DDL or dictionary commit takes the Raft path.
      proxied = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue8784").toString()).create();
      proxied.getSchema().createDocumentType(TYPE, 1).createProperty("name", Type.STRING);
      proxied.transaction(() -> proxied.newDocument(TYPE).set("name", "seed").save());

      broker = new FakeRaftTransactionBroker();
      raftServer = FakeRaftHAServer.detached();
      raftServer.transactionBroker(broker);
      raftServer.quorumTimeout(1_000L);
      raftServer.stateMachine(new ArcadeStateMachine());
      raftServer.shutdownRequested(false);
      raftServer.txPreparedAtCapable(false);

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
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aReplicaCommitFiresItsCallbacksAndIsCounted() {
      asReplica();
      acknowledgeAt(7L);

      final Begun begun = beginUpdate();
      database.commit();

      begun.assertEndedAsACommit();
    }

    /** The replica's MAJORITY-committed / ALL-watch-failed recovery: the commit stands, the watch failure is reported. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aReplicaMajorityCommittedCommitFiresItsCallbacks() {
      asReplica();
      broker.fails("replicateTransaction",
          new MajorityCommittedAllFailedException("ALL quorum not reached after MAJORITY commit at logIndex=9"));

      final Begun begun = beginUpdate();
      assertThatThrownBy(database::commit).isInstanceOf(MajorityCommittedAllFailedException.class);

      begun.assertEndedAsACommit();
    }

    /**
     * Issue #8781's path: a leader deposed mid-commit has its entry acknowledged with the registration unclaimed while its
     * state machine lives, so the state machine applies it from the WAL bytes and this thread only waits and releases.
     */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aLeaderCommitAcknowledgedUnclaimedWhileTheStateMachineLivesFiresItsCallbacks() {
      asLeader();
      acknowledgeAt(7L);

      final Begun begun = beginUpdate();
      database.commit();

      begun.assertEndedAsACommit();
    }

    /** The leader's MAJORITY-committed recovery with a live state machine: the same wait-and-release tail. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aLeaderMajorityCommittedCommitWhileTheStateMachineLivesFiresItsCallbacks() {
      asLeader();
      broker.fails("replicateTransaction",
          new MajorityCommittedAllFailedException("ALL quorum not reached", null, 7L));

      final Begun begun = beginUpdate();
      assertThatThrownBy(database::commit).isInstanceOf(MajorityCommittedAllFailedException.class);

      begun.assertEndedAsACommit();
    }

    /** The ordinary read-only arm on a replica: nothing to replicate is still a commit, as {@code commit()} has it off HA. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aReadOnlyCommitOnAReplicaFiresItsCallbacks() {
      asReplica();

      final Begun begun = beginReadOnly();
      database.commit();

      begun.assertEndedAsACommit();
    }

    /** The ordinary read-only arm on the leader. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aReadOnlyCommitOnTheLeaderFiresItsCallbacks() {
      asLeader();

      final Begun begun = beginReadOnly();
      database.commit();

      begun.assertEndedAsACommit();
    }

    /** The schema-commit arm's read-only branch: a commit inside a DDL callback on the leader that wrote nothing. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
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
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
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
      assertThat(broker.calls("replicateTransaction")).hasSize(2);
      assertThat(proxied.isTransactionActive()).as("nothing is left open on the thread").isFalse();
    }

    /** The other side of the contract: a commit the cluster refused is rolled back, and neither fires nor counts. */
    // Regression for #8784: ReplicatedCommitFiresAfterCommitCallbacks
    @Test
    void aRefusedReplicaCommitFiresNothing() {
      asReplica();
      broker.fails("replicateTransaction", new ConcurrentModificationException("Concurrent modification on page 3/0"));

      final Begun begun = beginUpdate();
      assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);

      assertThat(fired.get()).as("a refused commit must not fire the after-commit callbacks").isZero();
      assertThat(begun.tx.getCommitCount()).as("a refused commit is not counted").isEqualTo(begun.commitCountBefore);
      assertThat(proxied.isTransactionActive()).isFalse();
    }

    private void asReplica() {
      raftServer.leader(false);
    }

    private void asLeader() {
      raftServer.leader(true);
    }

    private void acknowledgeAt(final long logIndex) {
      broker.returns("replicateTransaction", logIndex);
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

  /**
   * Issue #10051 (#10018): after {@code commit2ndPhase} failed, the leader brings its pages in line with the committed Raft entry by
   * applying the entry's WAL with {@code applyChanges}, which folds the BM25 corpus deltas of the entry like the record counts. The
   * invariant that keeps the leader from counting a transaction twice is the one the replay relies on: the fold happens only when
   * a page advances, and the local commit folds as the last step of publishing its pages. Both shapes of that failure are
   * driven here against a real database, with the same WAL bytes the entry carries.

   */
  @Nested
  class ReconcileAppliesDeltasOnce {
    private static final String TYPE  = "Doc10051R";
    private static final String INDEX = TYPE + "[content]";
    private static final int    SEED  = 4;

    @TempDir
    Path tempDir;

    private LocalDatabase database;

    @BeforeEach
    void setUp() {
      database = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue10051r").toString()).create();
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
        database.command("sql", "CREATE PROPERTY " + TYPE + ".content STRING");
        database.command("sql", "CREATE INDEX ON " + TYPE + " (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      });
      database.transaction(() -> {
        for (int i = 0; i < SEED; i++)
          database.command("sql", "INSERT INTO " + TYPE + " SET content = 'alpha beta gamma'");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen()) {
        database.rollbackAllNested();
        database.drop();
      }
    }

    private FullTextIndexMetadata metadata() {
      return LSMTreeFullTextIndex.metadataOf(database.getSchema().getIndexByName(INDEX));
    }

    /** What phase 1 of a replicated commit hands to the entry: the WAL bytes, the record deltas and the corpus deltas. */
    private record Phase1Entry(TransactionContext tx, TransactionContext.TransactionPhase1 phase1, byte[] walData,
                               Map<Integer, Integer> bucketDeltas, List<CorpusDelta> corpusDeltas) {
    }

    private Phase1Entry preparedTransactionInserting(final int count) {
      database.begin();
      for (int i = 0; i < count; i++)
        database.newDocument(TYPE).set("content", "alpha beta gamma delta").save();
      final TransactionContext tx = database.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      final FullTextCorpusDeltas deltas = FullTextCorpusDeltas.peek(tx);
      assertThat(deltas).isNotNull();
      return new Phase1Entry(tx, phase1, phase1.result.toByteArray(), tx.getBucketRecordDelta(), deltas.snapshot());
    }

    /** The local commit published every page and folded the deltas; the failure came after, and reconciling must fold nothing. */
    // Regression for #10051: ReconcileAppliesDeltasOnce
    @Test
    void reconcilingAfterTheLocalCommitPublishedItsPagesCountsTheTransactionOnce() {
      final Phase1Entry entry = preparedTransactionInserting(3);
      entry.tx().commit2ndPhase(entry.phase1());
      assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);

      final WALFile.WALTransaction walTx = CommittedEntryDecoding.deserializeWalTransaction(entry.walData());
      database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);

      assertThat(metadata().getTotalDocs()).as("the entry's pages were already published").isEqualTo(SEED + 3L);
      assertThat(metadata().getSumDocLength()).isEqualTo(SEED * 3L + 3 * 4L);
    }

    /** The failure came before any page was published: reconciling applies the pages and folds the deltas, once. */
    // Regression for #10051: ReconcileAppliesDeltasOnce
    @Test
    void reconcilingWhenNoPageWasPublishedCountsTheTransactionOnce() {
      final Phase1Entry entry = preparedTransactionInserting(3);
      entry.tx().rollback();
      assertThat(metadata().getTotalDocs()).as("a transaction that published nothing counted nothing").isEqualTo(SEED);

      final WALFile.WALTransaction walTx = CommittedEntryDecoding.deserializeWalTransaction(entry.walData());
      database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);

      assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);
      assertThat(metadata().getSumDocLength()).isEqualTo(SEED * 3L + 3 * 4L);

      // and a second reconcile of the same entry finds every page at its version
      database.getTransactionManager().applyChanges(walTx, entry.bucketDeltas(), entry.corpusDeltas(), true);
      assertThat(metadata().getTotalDocs()).isEqualTo(SEED + 3L);
    }
  }

  /**
   * Issue #7984: the release of an entry's page-version reservations must never be the thing that reports a WAL
   * payload this node cannot read.
   * <p>
   * {@code applyTxEntry} releases the reservations in its {@code finally}, and with no {@code Pages} in hand
   * {@link PageVersionLedger#release} used to decode them out of the payload a third time. A payload that cannot be
   * decoded is exactly the state the apply has just failed on, so the release raised a second
   * {@link ReplicationException} from inside a {@code finally} - replacing, and later merely riding along on, the
   * {@link RaftLogEntryDecodeException} whose whole purpose is to quarantine the database for a targeted snapshot
   * resync (issues #7495, #7678). The release is bookkeeping; the apply result is not.
   * <p>
   * There is nothing for the release to report, either: every reservation enters the ledger through
   * {@link PageVersionLedger#validateAndReserve}, whose only two callers
   * ({@code ArcadeStateMachine.startTransaction} and {@code validateBeforeAppend(DatabaseInternal, byte[], EntryId)})
   * parse the very same bytes before they call it, so a payload {@code parse()} rejects is one this node never
   * reserved anything from.

   */
  @Nested
  class ReleaseNeverReportsDecodeFailure {
    private static final String TYPE = "Counter";

    /**
     * Long enough for {@code peekWalTransactionId} to read the entry's transaction id (8 bytes), so the apply gets
     * past the claim and into the {@code try}/{@code finally}, and short of the 24-byte transaction header both
     * {@code deserializeWalTransaction} and {@link PageVersionLedger#parse} need. The apply therefore fails on the
     * decode it is supposed to fail on, and the release meets the same unreadable bytes.
     */
    private static final byte[] PEEKABLE_BUT_UNPARSEABLE = new byte[16];

    /** Too short even for the transaction id: what {@code releaseReservedVersions} can be handed directly. */
    private static final byte[] TRUNCATED = new byte[3];

    @TempDir
    Path tempDir;

    private LocalDatabase      db;
    private ArcadeStateMachine stateMachine;
    private RID                counter;

    @BeforeEach
    void setUp() {
      db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue7984").toString()).create();
      db.getSchema().createDocumentType(TYPE, 1);
      db.transaction(() -> counter = db.newDocument(TYPE).set("value", 0L).save().getIdentity());
      stateMachine = new DatabaseBoundStateMachine(db);
    }

    @AfterEach
    void tearDown() {
      if (db != null && db.isOpen())
        db.drop();
    }

    /**
     * Entry point 1: {@code ArcadeStateMachine.releaseReservedVersions(String, byte[])}, the seam the apply's
     * {@code finally} and the #6965 unit tests both go through. With reservations in the ledger it used to decode the
     * payload and throw.
     */
    // Regression for #7984: ReleaseNeverReportsDecodeFailure
    @Test
    void theReleaseSeamDoesNotReportAPayloadItCannotParse() {
      stateMachine.validateBeforeAppend(db, prepareIncrement(), entry(1));
      final int reserved = stateMachine.reservedPageVersions(db.getName());
      assertThat(reserved).isGreaterThan(0);

      assertThatCode(() -> stateMachine.releaseReservedVersions(db.getName(), TRUNCATED)).doesNotThrowAnyException();

      assertThat(stateMachine.reservedPageVersions(db.getName()))
          .as("a payload no reservation was taken from releases nothing, and must take nothing away")
          .isEqualTo(reserved);
    }

    /**
     * Entry point 2: the apply path itself. A committed entry this node cannot decode must be reported as the
     * {@link RaftLogEntryDecodeException} that routes it to the per-database quarantine, with nothing from the release
     * bookkeeping attached to it - not as the failure, and not as a suppressed rider on it.
     */
    // Regression for #7984: ReleaseNeverReportsDecodeFailure
    @Test
    void aCorruptCommittedEntryReportsTheApplyFailureAlone() {
      stateMachine.validateBeforeAppend(db, prepareIncrement(), entry(1));
      final int reserved = stateMachine.reservedPageVersions(db.getName());
      assertThat(reserved).isGreaterThan(0);

      final CompletableFuture<Message> future = stateMachine.applyTransaction(
          txEntry(stateMachine, db.getName(), PEEKABLE_BUT_UNPARSEABLE, 1L, 5L));

      assertThat(future.isCompletedExceptionally()).isTrue();
      final Throwable applyFailure = rootApplyFailure(catchThrowable(future::join));
      assertThat(applyFailure)
          .as("the entry is undecodable, which is what the per-database quarantine exists for")
          .isInstanceOf(RaftLogEntryDecodeException.class);
      assertThat(applyFailure.getSuppressed())
          .as("the release bookkeeping must not report the decode failure, not even alongside the apply's own")
          .isEmpty();

      assertThat(stateMachine.isDatabaseDiverged(db.getName())).isTrue();
      assertThat(stateMachine.isHaltedAfterCriticalError()).isFalse();
      assertThat(stateMachine.reservedPageVersions(db.getName()))
          .as("the reservations of the OTHER in-flight entry are not this entry's to discard")
          .isEqualTo(reserved);
    }

    /**
     * Entry point 3: {@link PageVersionLedger#release} on its own. The ledger has to hold reservations for the
     * database before the decode is reached at all - {@code release} returns early on an empty ledger, which is why a
     * follower never met this - so the unit pins the contract rather than the caller's guard.
     */
    // Regression for #7984: ReleaseNeverReportsDecodeFailure
    @Test
    void theLedgerReleaseIsTotalOverTheWalPayload() throws IOException {
      final PageVersionLedger ledger = new PageVersionLedger();
      ledger.validateAndReserve("ledger", PageVersionLedger.parse(wal(1L, 3, 0, 1)), entry(1), (fileId, pageNumber) -> 0);
      assertThat(ledger.reservedPages("ledger")).isEqualTo(1);

      assertThatCode(() -> ledger.release("ledger", null, PEEKABLE_BUT_UNPARSEABLE)).doesNotThrowAnyException();
      assertThatCode(() -> ledger.release("ledger", null, TRUNCATED)).doesNotThrowAnyException();
      assertThatCode(() -> ledger.release("ledger", null, null)).doesNotThrowAnyException();

      assertThat(ledger.reservedPages("ledger")).isEqualTo(1);

      // A payload it CAN parse still releases, so the quiet return is scoped to the bytes it cannot read.
      ledger.release("ledger", null, wal(1L, 3, 0, 1));
      assertThat(ledger.reservedPages("ledger")).isZero();
    }

    /**
     * The release counter must be bumped before the decode is attempted, not after. It is what tells a validation
     * whether the local page versions it read outside the ledger lock may have moved under it
     * ({@code validateAndReserve}'s early-read optimisation); an apply moves the local copy whether or not this ledger
     * still holds the entry's reservation, so skipping the bump on an unreadable payload would let a validation trust
     * a stale read. Reading the counter needs reflection, the same way {@code PageVersionLedgerTest} ages a
     * reservation.
     */
    // Regression for #7984: ReleaseNeverReportsDecodeFailure
    @Test
    void aReleaseItCannotParseIsStillCounted() throws Exception {
      final PageVersionLedger ledger = new PageVersionLedger();
      ledger.validateAndReserve("ledger", PageVersionLedger.parse(wal(1L, 3, 0, 1)), entry(1), (fileId, pageNumber) -> 0);
      final long before = releaseCount(ledger, "ledger");

      ledger.release("ledger", null, PEEKABLE_BUT_UNPARSEABLE);

      assertThat(releaseCount(ledger, "ledger")).isEqualTo(before + 1);
    }

    /**
     * The claim the quiet return rests on, from the other side: giving up on a payload is safe only while
     * {@link PageVersionLedger#parse} is no stricter than the decode the apply itself runs. If it ever became
     * stricter, a payload that applied cleanly could fail the release, and the reservations of an entry that really
     * did move the local copies would be dropped silently rather than reported. The two read the same layout with the
     * same bounds checks; {@code PageVersionLedgerTest.theHeaderWalkAgreesWithTheFullDeserialization} pins that they
     * agree page for page on a well-formed entry, and this pins that they agree on a malformed one.
     */
    // Regression for #7984: ReleaseNeverReportsDecodeFailure
    @Test
    void theHeaderWalkIsNoStricterThanTheDecodeTheApplyRuns() {
      final byte[] tooManyPages = wal(1L, 3, 0, 1);
      ByteBuffer.wrap(tooManyPages).putInt(2 * Long.BYTES, 1_000_000);
      final byte[] negativePageCount = wal(1L, 3, 0, 1);
      ByteBuffer.wrap(negativePageCount).putInt(2 * Long.BYTES, -7);
      final byte[] invalidDeltaRange = wal(1L, 3, 0, 1);
      ByteBuffer.wrap(invalidDeltaRange).putInt(2 * Long.BYTES + 2 * Integer.BYTES + 3 * Integer.BYTES, -1);

      for (final byte[] payload : new byte[][] { PEEKABLE_BUT_UNPARSEABLE, TRUNCATED, tooManyPages, negativePageCount,
          invalidDeltaRange, wal(1L, 3, 0, 1) }) {
        final boolean headerWalkRejects = catchThrowable(() -> PageVersionLedger.parse(payload)) != null;
        final boolean applyDecodeRejects = catchThrowable(() -> CommittedEntryDecoding.deserializeWalTransaction(payload)) != null;
        assertThat(headerWalkRejects)
            .as("parse() and deserializeWalTransaction() must agree on whether these %d bytes are readable", payload.length)
            .isEqualTo(applyDecodeRejects);
      }
    }

    private static Throwable rootApplyFailure(final Throwable thrown) {
      // join() wraps in a CompletionException, and handleUnexpectedApplyError wraps the apply's own failure in the
      // ReplicationException that tells the caller a per-database resync is under way.
      Throwable t = thrown;
      while (t.getCause() != null && !(t instanceof RaftLogEntryDecodeException))
        t = t.getCause();
      return t;
    }

    private static long releaseCount(final PageVersionLedger ledger, final String database) throws Exception {
      final Field byDatabase = PageVersionLedger.class.getDeclaredField("byDatabase");
      byDatabase.setAccessible(true);
      final Object databaseLedger = ((Map<?, ?>) byDatabase.get(ledger)).get(database);
      final Field releases = databaseLedger.getClass().getDeclaredField("releases");
      releases.setAccessible(true);
      return ((AtomicLong) releases.get(databaseLedger)).get();
    }

    private static PageVersionLedger.EntryId entry(final long callId) {
      return new PageVersionLedger.EntryId("client", callId);
    }

    private static org.apache.ratis.statemachine.TransactionContext txEntry(final ArcadeStateMachine sm,
        final String databaseName, final byte[] walData, final long term, final long index) {
      final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
      final LogEntryProto logEntry = LogEntryProto.newBuilder()
          .setTerm(term)
          .setIndex(index)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
          .build();
      return org.apache.ratis.statemachine.TransactionContext.newBuilder()
          .setStateMachine(sm)
          .setLogEntry(logEntry)
          .build();
    }

    /** The wire layout {@code deserializeWalTransaction} reads, with a single page and a 4-byte delta. */
    private static byte[] wal(final long txId, final int fileId, final int pageNumber, final int targetVersion) {
      final int deltaSize = 4;
      final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + 6 * Integer.BYTES + deltaSize);
      buf.putLong(txId);
      buf.putLong(System.currentTimeMillis());
      buf.putInt(1);
      buf.putInt(6 * Integer.BYTES + deltaSize);
      buf.putInt(fileId);
      buf.putInt(pageNumber);
      buf.putInt(0);              // changesFrom
      buf.putInt(deltaSize - 1);  // changesTo
      buf.putInt(targetVersion);
      buf.putInt(4096);           // currentPageSize
      buf.put(new byte[deltaSize]);
      return buf.array();
    }

    private byte[] prepareIncrement() {
      db.begin();
      final Document doc = db.lookupByRID(counter, true).asDocument();
      doc.modify().set("value", doc.getLong("value") + 1).save();
      final TransactionContext tx = db.getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      final byte[] walData = phase1.result.toByteArray();
      tx.rollback();
      return walData;
    }

    /** A state machine with no server attached, whose only database is the one this test opened. */
    private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
      private final DatabaseInternal database;

      DatabaseBoundStateMachine(final DatabaseInternal database) {
        this.database = database;
      }

      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return database;
      }
    }
  }

  /**
   * Issue #12365: a client was answered a {@link ConcurrentModificationException} for a transaction every node applied. The
   * submitter tells "the leader refused the entry before appending it" (a definite no, so the commit is withdrawn) from any other
   * failure by a {@link StateMachineException} whose direct cause is a retryable exception, and Ratis wraps a failure of
   * {@code applyTransaction} - an entry already in the log - in the same exception. A retryable exception escaping the apply of a
   * committed entry therefore read as a refusal. The apply now carries it one level down, where the refusal test does not look.

   */
  @Nested
  class AppliedEntryFailure {
    private static final RaftGroupMemberId MEMBER = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("peer-0"), RaftGroupId.randomId());

    // Regression for #12365: AppliedEntryFailure
    @Test
    void aRetryableFailureOfACommittedEntryIsNotReadAsARefusalBeforeTheAppend() {
      final ConcurrentModificationException conflict = new ConcurrentModificationException("Concurrent modification on page 3/0");

      // what Ratis hands the submitter for a failed apply: the state machine's exception wrapped in a StateMachineException
      final Exception applied = ArcadeStateMachine.appliedEntryFailure(42L, conflict);
      assertThat(applied).isInstanceOf(ReplicationException.class).hasCause(conflict).hasMessageContaining("index 42");
      assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(new CompletionException(new StateMachineException(MEMBER, applied))))
          .as("an entry that is in the log is never answered as refused").isNull();

      // without the guard the same failure was the refusal shape
      assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(new CompletionException(new StateMachineException(MEMBER, conflict))))
          .isSameAs(conflict);
    }

    // Regression for #12365: AppliedEntryFailure
    @Test
    void theRefusalBeforeTheAppendIsStillADefiniteRefusal() {
      final ConcurrentModificationException conflict = new ConcurrentModificationException("refused before the append");
      assertThat(RatisRefusedEntryErrorFilter.refusedBeforeAppend(
          new CompletionException(new StateMachineException(conflict.getMessage(), conflict, false)))).isSameAs(conflict);
    }

    // Regression for #12365: AppliedEntryFailure
    @Test
    void anyOtherFailureIsCarriedAsItIs() {
      final IOException io = new IOException("disk");
      final ReplicationException replication = new ReplicationException("resync required");
      final IllegalStateException state = new IllegalStateException("bug");
      assertThat(ArcadeStateMachine.appliedEntryFailure(1L, io)).isSameAs(io);
      assertThat(ArcadeStateMachine.appliedEntryFailure(1L, replication)).isSameAs(replication);
      assertThat(ArcadeStateMachine.appliedEntryFailure(1L, state)).isSameAs(state);
      final OutOfMemoryError oom = new OutOfMemoryError("heap");
      assertThat(ArcadeStateMachine.appliedEntryFailure(1L, oom)).isInstanceOf(RuntimeException.class).hasCause(oom);
    }
  }
}
