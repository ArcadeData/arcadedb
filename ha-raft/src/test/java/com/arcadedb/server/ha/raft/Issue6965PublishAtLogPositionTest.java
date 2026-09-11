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
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ordering invariant that replaced the #5407 snapshot clamp, driven through the real
 * {@link ArcadeStateMachine#applyTransaction} and {@link ArcadeStateMachine#takeSnapshot} path: a transaction this
 * node originated is published at its entry's log position, BEFORE the applied index moves past the entry, so a
 * checkpoint taken afterwards may cover the entry and a restart never has to replay it to recover its pages.
 * <p>
 * The entry is fed back with a fresh context and no origin marker of any kind, as Ratis does after a step-down: the
 * state machine recognises its own transaction by the bytes it registered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6965PublishAtLogPositionTest {
  private static final long ENTRY_INDEX = 7L;

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine sm;
  private RaftStorage        storage;

  @AfterEach
  void tearDown() throws IOException {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    if (sm != null)
      sm.close();
    if (storage != null)
      storage.close();
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void theApplyThreadPublishesBeforeTheAppliedIndexMovesPastTheEntry() throws Exception {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
    db.getSchema().createDocumentType("Doc", 1);
    storage = RaftStorage.newBuilder().setDirectory(tempDir.resolve("raft").toFile())
        .setOption(RaftStorage.StartupOption.FORMAT).build();
    sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }
    };
    sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), storage);

    // Phase 1 of a real transaction, registered exactly as the committing thread does before dispatching it.
    db.begin();
    db.newDocument("Doc").set("v", 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    final LocalCommit local = new LocalCommit(db.getName(), ArcadeStateMachine.peekWalTransactionId(walData), tx, phase1, walData);
    assertThat(sm.registerLocalCommit(local)).isTrue();

    // The entry as Ratis feeds it to the state machine.
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(db.getName(), walData, tx.getBucketRecordDelta());
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(1L).setIndex(ENTRY_INDEX)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();

    // Observed from inside the publication: the applied index must still be behind the entry at that moment.
    final AtomicLong appliedWhilePublishing = new AtomicLong(Long.MIN_VALUE);
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = name -> appliedWhilePublishing.set(indexOf(sm.getLastAppliedTermIndex()));

    sm.applyTransaction(org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build())
        .get();

    assertThat(local.awaitOutcome(1_000)).as("the apply thread claimed and published the registered transaction")
        .isEqualTo(LocalCommit.Outcome.PUBLISHED);
    assertThat(appliedWhilePublishing.get()).as("the pages were published while the applied index was still behind the entry")
        .isLessThan(ENTRY_INDEX);
    assertThat(indexOf(sm.getLastAppliedTermIndex())).isEqualTo(ENTRY_INDEX);
    assertThat(sm.takeSnapshot()).as("with the pages published, the checkpoint may cover the entry").isEqualTo(ENTRY_INDEX);
    assertThat(sm.pendingLocalCommits()).isZero();
    assertThat(sm.reservedPageVersions(db.getName())).isZero();

    // The committing thread's half, once the entry is acknowledged.
    tx.completeCommit();
    assertThat(db.countType("Doc", true)).isEqualTo(1L);
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
