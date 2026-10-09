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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9547: during a chaos soak two committed schema entries gave file id 69 to two different compacted index files,
 * and the file-id collision check (#6063) quarantined the database on EVERY node at once, leaving no healthy peer to
 * resync from.
 * <p>
 * A schema entry allocates its file ids from its proposer's own file manager. The Ratis client resends a request it got
 * no answer for to whichever node leads now, so a compaction an isolated leader submitted reached the next leader after
 * that leader had committed its own compaction under the same id - and each node held one of the two files. The same
 * collision follows from a freshly elected leader that allocates before applying the entries its predecessor committed.
 * <p>
 * The leader now refuses, before anything is appended, a schema entry another node submitted, and one of its own whose
 * session is over or was bound to an earlier term; and a schema session only starts on a leader that Ratis reports
 * ready, which means every entry committed before its term has been applied.
 */
class Issue9547StaleSchemaProposalRefusedTest {
  private static final String TYPE = "Counter";

  /** The compacted file the stale proposer allocated, as the soak logged it. */
  private static final Map<Integer, String> STALE_COMPACTION = Map.of(69,
      "ChaosOp_8_5952892520929971.69.262144.v1.uctidx");

  @TempDir
  Path tempDir;

  private final ClientId   localClient = ClientId.randomId();
  private final AtomicLong term        = new AtomicLong(7L);

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;
  private RID                counter;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue9547").toString()).create();
    db.getSchema().createDocumentType(TYPE, 1);
    db.transaction(() -> db.newDocument(TYPE).set("value", 0L).save());
    counter = db.iterateType(TYPE, false).next().getIdentity();
    stateMachine = stateMachine(localClient);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  /**
   * The reported path: the ex-leader's compaction, resent by its Ratis client to the new leader. It used to be appended
   * after the new leader's own compaction of id 69. Refused even while the new leader has a session of its own open.
   */
  @Test
  void aSchemaEntryProposedByAnotherNodeIsRefusedBeforeAppend() throws Exception {
    stateMachine.bindSchemaSession(db.getName(), term.get());

    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(
        request(ClientId.randomId(), schemaEntry(STALE_COMPACTION), 1));

    assertThat(context.getException()).isInstanceOf(NeedRetryException.class);
    assertThat(context.getException().getMessage()).contains(db.getName()).contains("another node");
  }

  @Test
  void anEntryOfThisLeadersOwnSessionInItsTermIsAccepted() throws Exception {
    stateMachine.bindSchemaSession(db.getName(), term.get());

    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(
        request(localClient, schemaEntry(STALE_COMPACTION), 2));

    assertThat(context.getException()).isNull();
    assertThat(context.getStateMachineContext()).as("still marked as originated here, so apply skips it")
        .isEqualTo(Boolean.TRUE);
  }

  /** This node's own resend, landing on it again once it is re-elected, with the interim leader's entries ahead of it. */
  @Test
  void anEntryOfThisLeaderAllocatedInAnEarlierTermIsRefused() throws Exception {
    stateMachine.bindSchemaSession(db.getName(), 7L);
    term.set(9L);

    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(
        request(localClient, schemaEntry(STALE_COMPACTION), 3));

    assertThat(context.getException()).isInstanceOf(NeedRetryException.class);
    assertThat(context.getException().getMessage()).contains("term 7").contains("term 9");
  }

  /** A resend that outlived its session: the ids it allocated may have been handed out again since. */
  @Test
  void anEntryOfThisLeaderWithNoSessionOpenIsRefused() throws Exception {
    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(
        request(localClient, schemaEntry(STALE_COMPACTION), 4));

    assertThat(context.getException()).isInstanceOf(NeedRetryException.class);
    assertThat(context.getException().getMessage()).contains("outside of a schema session");
  }

  @Test
  void endingABindingOfAnotherTermLeavesTheCurrentOneInPlace() throws Exception {
    stateMachine.bindSchemaSession(db.getName(), 7L);
    stateMachine.unbindSchemaSession(db.getName(), 6L);

    assertThat(stateMachine.staleSchemaProposalRefusal(db.getName(), true)).isNull();

    stateMachine.unbindSchemaSession(db.getName(), 7L);
    assertThat(stateMachine.staleSchemaProposalRefusal(db.getName(), true)).isNotNull();
  }

  /** Transactions of the other nodes are how replicas write: this check must not touch them. */
  @Test
  void aTransactionEntryOfAnotherNodeIsNotRefusedByThisCheck() throws Exception {
    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(
        request(ClientId.randomId(), RaftLogEntryCodec.encodeTxEntry(db.getName(), prepareIncrement(), Collections.emptyMap()),
            5));

    assertThat(context.getException()).isNull();
  }

  /** Without a Raft client of its own the leader cannot tell who proposed the entry, and refuses nothing for it. */
  @Test
  void withoutALocalClientNoSchemaEntryIsRefused() throws Exception {
    final ArcadeStateMachine unwired = stateMachine(null);

    assertThat(unwired.startTransaction(request(ClientId.randomId(), schemaEntry(STALE_COMPACTION), 6)).getException())
        .isNull();
  }

  /**
   * The other way to the same collision: a freshly elected leader has not applied every entry its predecessor committed,
   * so its file manager can hand out an id one of them already uses. A compaction waits for the next schedule.
   */
  @Test
  void aCompactionOnALeaderThatIsNotReadyIsDeferred() throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).leaderReady(false)
        .currentTerm(7L).transactionBroker(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
    final AtomicBoolean ran = new AtomicBoolean();

    assertThat(replicated.runWithCompactionReplication(() -> {
      ran.set(true);
      return true;
    })).isFalse();

    assertThat(ran).as("no file id may be allocated before the leader is ready").isFalse();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
  }

  /** A DDL waits for readiness, but not for ever: past the quorum timeout it fails with a retryable error. */
  @Test
  void aSchemaChangeOnALeaderThatDoesNotBecomeReadyIsRefused() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).leaderReady(false)
        .currentTerm(7L).quorumTimeout(100L).transactionBroker(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
    final AtomicBoolean ran = new AtomicBoolean();

    assertThatThrownBy(() -> replicated.recordFileChanges(() -> {
      ran.set(true);
      return null;
    })).isInstanceOf(NeedRetryException.class).hasMessageContaining("has not applied every entry");

    assertThat(ran).isFalse();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
  }

  /** The binding lives exactly as long as the session: open while its entries go out, gone once it ends. */
  @Test
  void aSchemaChangeIsBoundToItsTermForTheWholeSessionAndOnlyThen() {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true)
        .leaderReady(false, true).currentTerm(7L).quorumTimeout(5_000L).transactionBroker(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
    final AtomicReference<NeedRetryException> duringSession = new AtomicReference<>(new NeedRetryException("not run"));

    replicated.recordFileChanges(() -> {
      duringSession.set(stateMachine.staleSchemaProposalRefusal(db.getName(), true));
      return null;
    });

    assertThat(duringSession.get()).as("the session's own entries are accepted in its term").isNull();
    assertThat(stateMachine.staleSchemaProposalRefusal(db.getName(), true)).as("a resend after the session is not")
        .isNotNull();
  }

  @Test
  void aCompactionIsBoundToItsTermForTheWholeSessionAndOnlyThen() throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).currentTerm(7L)
        .transactionBroker(new FakeRaftTransactionBroker());
    final RaftReplicatedDatabase replicated = new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
    final AtomicReference<NeedRetryException> duringSession = new AtomicReference<>(new NeedRetryException("not run"));

    replicated.runWithCompactionReplication(() -> {
      duringSession.set(stateMachine.staleSchemaProposalRefusal(db.getName(), true));
      return true;
    });

    assertThat(duringSession.get()).isNull();
    assertThat(stateMachine.staleSchemaProposalRefusal(db.getName(), true)).isNotNull();
  }

  private ArcadeStateMachine stateMachine(final ClientId clientId) {
    return new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }

      @Override
      ClientId localClientId() {
        return clientId;
      }

      @Override
      long currentRaftTerm() {
        return term.get();
      }
    };
  }

  private ByteString schemaEntry(final Map<Integer, String> filesToAdd) {
    return RaftLogEntryCodec.encodeSchemaEntry(db.getName(), "", filesToAdd, Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList());
  }

  private static RaftClientRequest request(final ClientId clientId, final ByteString entry, final long callId) {
    return RaftClientRequest.newBuilder()
        .setClientId(clientId)
        .setServerId(RaftPeerId.valueOf("peer-0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(callId)
        .setMessage(Message.valueOf(entry))
        .setType(RaftClientRequest.writeRequestType())
        .build();
  }

  /** Phase 1 of an increment, abandoned: the WAL bytes a replica would ship for it. */
  private byte[] prepareIncrement() {
    db.begin();
    final Document doc = db.lookupByRID(counter, true).asDocument();
    doc.modify().set("value", doc.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final byte[] walData = tx.commit1stPhase(true).result.toByteArray();
    tx.rollback();
    return walData;
  }
}
