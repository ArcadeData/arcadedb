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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.TransactionContext.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7438: the window a leader opens for a drop or an install must be closed on EVERY exit of
 * {@code dropInReplicas} and {@code createInReplicas}. A registration that leaked would refuse every replica write on the
 * database until the node restarted, and an unbalanced end is a silent no-op, so nothing else would ever say so.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7438ExclusiveWindowReleaseTest {
  private static final String TYPE = "Counter";

  @TempDir
  Path tempDir;

  private LocalDatabase          db;
  private ArcadeStateMachine     stateMachine;
  private RaftReplicatedDatabase replicated;
  private RID                    counter;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue7438release").toString()).create();
    db.getSchema().createDocumentType(TYPE, 1);
    db.transaction(() -> db.newDocument(TYPE).set("value", 0L).save());
    counter = db.iterateType(TYPE, false).next().getIdentity();

    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }
    };
    // No transaction broker: every replicate call fails, which is the exit these tests take.
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getStateMachine()).thenReturn(stateMachine);
    replicated = new RaftReplicatedDatabase(mock(ArcadeDBServer.class), db, raft);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aFailedInstallReleasesTheWindow() throws Exception {
    assertThatThrownBy(() -> replicated.createInReplicas()).isInstanceOf(RuntimeException.class);
    assertReplicaEntryAccepted(1);
  }

  @Test
  void aFailedForcedInstallReleasesTheWindow() throws Exception {
    assertThatThrownBy(() -> replicated.createInReplicas(true)).isInstanceOf(RuntimeException.class);
    assertReplicaEntryAccepted(2);
  }

  @Test
  void aFailedDropReleasesTheWindow() throws Exception {
    assertThatThrownBy(() -> replicated.dropInReplicas()).isInstanceOf(RuntimeException.class);
    assertReplicaEntryAccepted(3);
  }

  private void assertReplicaEntryAccepted(final long callId) throws Exception {
    db.begin();
    final Document doc = db.lookupByRID(counter, true).asDocument();
    doc.modify().set("value", doc.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final byte[] walData = tx.commit1stPhase(true).result.toByteArray();
    tx.rollback();

    final RaftClientRequest request = RaftClientRequest.newBuilder()
        .setClientId(ClientId.randomId())
        .setServerId(RaftPeerId.valueOf("peer-0"))
        .setGroupId(RaftGroupId.randomId())
        .setCallId(callId)
        .setMessage(Message.valueOf(RaftLogEntryCodec.encodeTxEntry(db.getName(), walData, Collections.emptyMap())))
        .setType(RaftClientRequest.writeRequestType())
        .build();
    final org.apache.ratis.statemachine.TransactionContext context = stateMachine.startTransaction(request);

    assertThat(context.getException()).as("a leaked registration would refuse this entry").isNull();
  }
}
