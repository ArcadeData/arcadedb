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
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * #5064: the ALL-quorum recovery path. When the MAJORITY durably committed a transaction but the
 * ALL-quorum watch failed, {@code applyLocallyAfterMajorityCommit} applies phase 2 locally. A failure
 * there must (a) have set {@code remotelyCommitted} on the transaction BEFORE the apply, so that
 * {@code commit2ndPhase}'s finally releases resources without the #4940 identity rollback (the flag's
 * engine-side semantics are pinned by {@code WalCommitOrderingTest}), (b) stay SILENT to the caller
 * (unlike the main phase-2 path this is background recovery: the caller already receives
 * {@code MajorityCommittedAllFailedException}; the reconcile + step-down below are the whole remedy),
 * and (c) still fire the reconcile + step-down remedy.
 * <p>
 * Unit-level by design: the real trigger needs an ALL-quorum cluster whose Ratis watch fails after
 * MAJORITY commit plus a concurrent local-apply fault, which hinges on Ratis watch timeouts and is
 * not deterministically reproducible in an IT. The failure is injected through the payload's
 * transaction instead, exercising the REAL {@code applyLocallyAfterMajorityCommit} body.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5064ApplyLocallyAfterMajorityCommitTest {

  private static final String DB_PATH = "/tmp/issue5064-apply-locally-test";

  private LocalDatabase      proxied;
  private RaftHAServer       raftServer;
  private TransactionContext tx;

  @BeforeEach
  void setUp() throws Exception {
    proxied = mock(LocalDatabase.class);
    when(proxied.getDatabasePath()).thenReturn(DB_PATH);
    when(proxied.getName()).thenReturn("issue5064");
    when(proxied.getTransactionManager()).thenReturn(mock(TransactionManager.class));
    when(proxied.executeInReadLock(any())).thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(0)).call());

    raftServer = mock(RaftHAServer.class);
    when(raftServer.isLeader()).thenReturn(true);

    tx = mock(TransactionContext.class);
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.removeContext(DB_PATH);
  }

  @Test
  void localApplyFailureStaysSilentSetsFlagBeforeApplyAndStepsDown() {
    final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, raftServer);
    // Bind the thread-local context the method resolves via DatabaseContext.getContext().
    DatabaseContext.INSTANCE.init(proxied, tx);

    doThrow(new IllegalStateException("simulated local apply failure")).when(tx).commit2ndPhase(any());

    // 24 zero bytes deserialize to an empty WAL transaction (txId=0, 0 pages), so the reconcile
    // remedy runs deterministically against the mocked TransactionManager.
    final RaftReplicatedDatabase.ReplicationPayload payload =
        new RaftReplicatedDatabase.ReplicationPayload(tx, null, new byte[24], Map.of());

    // (b) SILENT: the local-apply failure must not surface - the caller of commit() already gets
    // MajorityCommittedAllFailedException; surfacing a second exception here would replace it.
    assertThatCode(() -> database.applyLocallyAfterMajorityCommit(payload)).doesNotThrowAnyException();

    // (a) The durability-boundary shift is active DURING the apply: the flag is set before
    // commit2ndPhase, so its finally takes the remotelyCommitted reset() branch instead of the
    // #4940 identity rollback (branch semantics pinned by the engine's WalCommitOrderingTest).
    final InOrder order = inOrder(tx);
    order.verify(tx).setRemotelyCommitted(true);
    order.verify(tx).commit2ndPhase(any());

    // ...and this path itself must never add an identity rollback around the failed apply.
    verify(tx, never()).rollback();

    // (c) The remedy fires: the leader steps down so a node with correct state takes over.
    verify(raftServer).stepDown();
  }

  @Test
  void successfulApplyDoesNotStepDown() {
    final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, raftServer);
    DatabaseContext.INSTANCE.init(proxied, tx);
    when(proxied.getSchema()).thenReturn(mock(Schema.class, RETURNS_DEEP_STUBS));

    final RaftReplicatedDatabase.ReplicationPayload payload =
        new RaftReplicatedDatabase.ReplicationPayload(tx, null, new byte[24], Map.of());

    assertThatCode(() -> database.applyLocallyAfterMajorityCommit(payload)).doesNotThrowAnyException();

    // The boundary shift precedes the apply on the success path too.
    final InOrder order = inOrder(tx);
    order.verify(tx).setRemotelyCommitted(true);
    order.verify(tx).commit2ndPhase(any());

    verify(raftServer, never()).stepDown();
  }
}
