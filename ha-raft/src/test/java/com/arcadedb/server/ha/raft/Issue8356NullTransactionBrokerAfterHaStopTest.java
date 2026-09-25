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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8356: {@code RaftHAServer.stop()} nulls its {@code transactionBroker} field while
 * {@code raftHAServer} itself - {@code RaftReplicatedDatabase}'s own, {@code final} reference to it - stays
 * non-null, so {@code requireRaftServer()} alone cannot detect the window. A write that reaches
 * {@code replicateAndConclude} after the broker has been cleared but before the HTTP server stops accepting
 * requests used to dereference the null broker directly: {@code catch (ArcadeDBException e)} does not match a
 * {@code NullPointerException}, so it fell through to the generic {@code catch (Exception e)} and came out the
 * other side as a plain {@code TransactionException} - not retryable, so {@code AbstractServerHttpHandler}
 * reports it as a 500 instead of the clean, retryable 503 a {@code NeedRetryException} gets. Either shape is
 * wrong for a write that only failed because the server is shutting down; this test checks the caller now sees
 * a retryable failure, not either non-retryable one.
 * <p>
 * Mirrors the construction and commit-handshake style of {@code Issue6965LocalCommitHandshakeTest}, with the
 * broker cleared instead of stubbed.
 */
class Issue8356NullTransactionBrokerAfterHaStopTest {
  private static final String DB_NAME   = "issue8356";
  private static final long   WAL_TX_ID = 4242L;

  private final String dbPath = "issue8356-null-transaction-broker-" + UUID.randomUUID();

  private LocalDatabase          proxied;
  private RaftHAServer           raftServer;
  private TransactionContext     tx;
  private ArcadeStateMachine     stateMachine;
  private RaftReplicatedDatabase database;
  private RaftReplicatedDatabase.ReplicationPayload payload;

  @BeforeEach
  void setUp() {
    proxied = mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(dbPath);
    when(proxied.getName()).thenReturn(DB_NAME);
    when(proxied.getTransactionManager()).thenReturn(mock(TransactionManager.class));
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));
    when(proxied.executeInReadLock(any())).thenAnswer(inv -> ((Callable<?>) inv.getArgument(0)).call());

    // The race this issue is about: RaftHAServer.stop() has already cleared the broker, but this
    // RaftReplicatedDatabase's own (final) reference to the RaftHAServer instance is still non-null, so
    // requireRaftServer() alone would not catch the window.
    raftServer = mock(RaftHAServer.class, RETURNS_DEEP_STUBS);
    when(raftServer.isLeader()).thenReturn(true);
    when(raftServer.getTransactionBroker()).thenReturn(null);
    stateMachine = new ArcadeStateMachine();
    when(raftServer.getStateMachine()).thenReturn(stateMachine);

    tx = mock(TransactionContext.class);
    DatabaseContext.INSTANCE.init(proxied, tx);
    database = new RaftReplicatedDatabase(null, proxied, raftServer);
    payload = new RaftReplicatedDatabase.ReplicationPayload(tx, null, walTransactionBytes(WAL_TX_ID), Map.of());
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(dbPath);
  }

  @Test
  void aNullTransactionBrokerIsReportedAsRetryableInsteadOfThrowingNPE() {
    assertThatThrownBy(() -> database.replicateAndCommitLocally(payload, true, stateMachine))
        .isInstanceOf(NeedRetryException.class)
        .isNotInstanceOf(NullPointerException.class);

    // A caught, retryable failure - not a half-applied local commit left dangling.
    verify(proxied).rollback();
    verify(tx, never()).completeCommit();
    assertThat(stateMachine.pendingLocalCommits()).as("the withdrawal removed the registration").isZero();
  }

  /** The wire layout {@code ArcadeStateMachine.deserializeWalTransaction} reads: id, timestamp, no pages. */
  private static byte[] walTransactionBytes(final long txId) {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(0);
    buf.putInt(0);
    return buf.array();
  }
}
