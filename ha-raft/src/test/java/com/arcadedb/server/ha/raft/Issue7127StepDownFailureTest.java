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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Issue #7127: exercise the real step-down loop, including recovery through a real database commit.
 * Only the replication/leadership-transfer boundary and process stop are simulated, so failures are
 * deterministic without waiting for a cluster's network timeouts.
 */
class Issue7127StepDownFailureTest {
  private final ContextConfiguration config = configuration();
  private final ArcadeDBServer server = mock(ArcadeDBServer.class);
  private final CountDownLatch stopped = new CountDownLatch(1);
  private ControlledTransfers raft;

  @BeforeEach
  void setUp() {
    when(server.getServerName()).thenReturn("ArcadeDB_0");
    when(server.getConfiguration()).thenReturn(config);
    doAnswer(invocation -> {
      stopped.countDown();
      return null;
    }).when(server).stop();
    raft = new ControlledTransfers(server, config);
  }

  @AfterEach
  void tearDown() {
    RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
    raft.stop();
  }

  @Test
  void exhaustedTransfersMustNotReportSuccessfulStepDown() {
    assertThatThrownBy(raft::stepDown)
        .as("a leader that could not transfer leadership must report failure to phase-2 recovery")
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("Cannot step down");
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  @Test
  void leadershipLostDuringFallbackMustNotBeReportedAsTransferFailure() {
    raft.loseLeadershipOnFallbackAttempt = 1;

    assertThatThrownBy(raft::stepDown)
        .as("phase-2 recovery must stop retrying when this node is already a follower")
        .isInstanceOf(NotTheLeaderRefusalException.class);
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  @Test
  void noEligibleCandidatesAndFailedFallbackMustReportFailure() {
    raft.stop();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    raft = new ControlledTransfers(server, config);

    assertThatThrownBy(raft::stepDown).isInstanceOf(ReplicationException.class);
    assertThat(raft.targetedAttempts).isZero();
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  @Test
  void successfulTargetedTransferMustNotTryTheFallback() {
    raft.targetedTransferSucceeds = true;

    assertThatCode(raft::stepDown).doesNotThrowAnyException();
    assertThat(raft.targetedAttempts).isEqualTo(1);
    assertThat(raft.fallbackAttempts).isZero();
  }

  @Test
  void successfulFallbackMustNotReportFailure() {
    raft.successfulFallbackAttempt = 1;

    assertThatCode(raft::stepDown).doesNotThrowAnyException();
    assertThat(raft.targetedAttempts).isEqualTo(2);
    assertThat(raft.fallbackAttempts).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void phase2FailureRetriesRealStepDownAndHonorsStopSetting(final boolean stopOnFailure) throws InterruptedException {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, stopOnFailure);

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).as("all three real step-down attempts must be exhausted").isEqualTo(3);
    assertThat(raft.targetedAttempts).isEqualTo(6);
    if (stopOnFailure) {
      assertThat(stopped.await(10, TimeUnit.SECONDS)).as("emergency server stop must be reached").isTrue();
      verify(server).stop();
    } else
      verify(server, never()).stop();
  }

  @Test
  void phase2RecoveryStopsRetryingAfterSuccessfulTransfer() {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
    raft.successfulFallbackAttempt = 2;

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).isEqualTo(2);
    assertThat(raft.targetedAttempts).isEqualTo(4);
    verify(server, never()).stop();
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 3 })
  void phase2RecoveryMustNotStopANodeThatLostLeadership(final int demotionAttempt) {
    config.setValue(GlobalConfiguration.HA_STOP_SERVER_ON_REPLICATION_FAILURE, true);
    raft.loseLeadershipOnFallbackAttempt = demotionAttempt;

    commitWithPhase2Failure();

    assertThat(raft.fallbackAttempts).isEqualTo(demotionAttempt);
    assertThat(raft.targetedAttempts).isEqualTo(2 * demotionAttempt);
    assertThat(raft.isLeader()).isFalse();
    verify(server, never()).stop();
  }

  private void commitWithPhase2Failure() {
    final String path = "target/databases/issue7127-" + UUID.randomUUID();
    final LocalDatabase local = (LocalDatabase) new DatabaseFactory(path).create();
    try {
      local.getSchema().createDocumentType("Item", 1);
      local.transaction(() -> local.newDocument("Item").set("value", 0).save());

      final RaftReplicatedDatabase database = new RaftReplicatedDatabase(server, local, raft);
      database.begin();
      final MutableDocument document = database.iterateType("Item", false).next().asDocument().modify();
      document.set("value", 1).save();

      final IllegalStateException fault = new IllegalStateException("simulated local phase-2 apply failure");
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = dbName -> {
        throw fault;
      };
      assertThatThrownBy(database::commit)
          .isInstanceOf(TransactionCommittedRemotelyException.class)
          .hasMessageContaining("Do NOT retry")
          .hasCause(fault);
      verify(raft.broker).replicateTransaction(anyString(), any(), any());
    } finally {
      RaftReplicatedDatabase.TEST_PHASE2_COMMIT_FAULT = null;
      local.setWrappedDatabaseInstance(local);
      local.rollbackAllNested();
      local.drop();
    }
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");
    return config;
  }

  private static class ControlledTransfers extends RaftHAServer {
    private final RaftTransactionBroker broker = mock(RaftTransactionBroker.class);
    private int targetedAttempts;
    private int fallbackAttempts;
    private boolean leader = true;
    private boolean targetedTransferSucceeds;
    private int successfulFallbackAttempt = Integer.MAX_VALUE;
    private int loseLeadershipOnFallbackAttempt = Integer.MAX_VALUE;

    private ControlledTransfers(final ArcadeDBServer server, final ContextConfiguration config) {
      super(server, config);
      when(broker.replicateTransaction(anyString(), any(), any())).thenReturn(1L);
    }

    @Override
    public RaftTransactionBroker getTransactionBroker() {
      return broker;
    }

    @Override
    public boolean isLeader() {
      return leader;
    }

    @Override
    public void transferLeadership(final String targetPeerId, final long timeoutMs) {
      targetedAttempts++;
      if (targetedTransferSucceeds) {
        leader = false;
        return;
      }
      throw new ReplicationException("Transfer timed out: " + targetPeerId);
    }

    @Override
    public boolean transferLeadership(final long timeoutMs) {
      fallbackAttempts++;
      if (fallbackAttempts == successfulFallbackAttempt) {
        leader = false;
        return true;
      }
      if (fallbackAttempts == loseLeadershipOnFallbackAttempt)
        leader = false;
      return false;
    }
  }
}
