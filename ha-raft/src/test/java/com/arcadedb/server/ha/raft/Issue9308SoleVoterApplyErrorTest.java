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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #9308: the sole-voter escape #8940 added to the forceSnapshot replay guard was not applied
 * to {@code handleUnexpectedApplyError}, the other site that raises a quarantine on a leader. A quarantine is lifted only
 * by a leadership hand-off and a targeted resync from the next leader, and a sole voter has neither, so one apply error
 * left the node permanently not-ready and its Raft log permanently un-checkpointed.
 * <p>
 * The failure routing under test is the real one: {@link ArcadeStateMachine#applyWithRetry(long, String, Runnable)} is
 * the wrapper {@code applyTransaction} runs every per-database dispatch in. The HA server is a mock whose voter
 * configuration the test sets; no {@link com.arcadedb.server.ArcadeDBServer} is wired, so a targeted resync would be a
 * no-op either way and only the quarantine bookkeeping is observed.
 */
class Issue9308SoleVoterApplyErrorTest {
  private static final String DB_NAME = "db9308";

  private final AtomicBoolean      soleVoter = new AtomicBoolean(true);
  private       RaftHAServer       raft;
  private       ArcadeStateMachine sm;
  private       int                prevRetries;
  private       int                prevDelay;

  @BeforeEach
  void setUp() {
    prevRetries = GlobalConfiguration.TX_RETRIES.getValueAsInteger();
    prevDelay = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRIES.setValue(0);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(0);

    raft = mock(RaftHAServer.class);
    when(raft.isLeader()).thenReturn(true);
    when(raft.isSoleVoter()).thenAnswer(inv -> soleVoter.get());
    sm = new ArcadeStateMachine();
    sm.setRaftHAServer(raft);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.TX_RETRIES.setValue(prevRetries);
    GlobalConfiguration.TX_RETRY_DELAY.setValue(prevDelay);
  }

  /**
   * The issue as reported: a sole voter hits an unexpected apply error on a named database. It used to quarantine it,
   * which no hand-off and no resync could ever lift here. The entry still fails - its submitter must learn the apply did
   * not happen - but the database is not quarantined, so the node stays ready and its log stays checkpointable.
   */
  @Test
  void aSoleVoterApplyErrorIsNotQuarantined() {
    final IllegalStateException boom = new IllegalStateException("apply failed on the only copy");

    assertThatThrownBy(() -> sm.applyWithRetry(10L, DB_NAME, () -> {
      throw boom;
    }))
        .as("the failed entry is still reported as failed, never skipped silently")
        .isInstanceOf(ReplicationException.class)
        .hasCause(boom)
        .hasMessageContaining("only voter");

    assertThat(sm.isDatabaseDiverged(DB_NAME)).as("nothing could ever lift this quarantine").isFalse();
    assertThat(sm.isResyncInProgress()).as("the node must stay in the ready set").isFalse();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
    verify(raft, never()).handOffLeadershipToResync(anyString());
  }

  /**
   * The undecodable-WAL flavour (issue #7495) reaches the same handler with a different cause, and has the same lack of
   * a peer to reinstall from.
   */
  @Test
  void aSoleVoterUndecodableEntryIsNotQuarantined() {
    final RaftLogEntryDecodeException decode = new RaftLogEntryDecodeException("truncated WAL payload",
        RaftLogEntryType.TX_ENTRY, DB_NAME, null);

    assertThatThrownBy(() -> sm.applyWithRetry(11L, DB_NAME, () -> {
      throw decode;
    })).isInstanceOf(ReplicationException.class).hasCause(decode);

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.isResyncInProgress()).isFalse();
  }

  /**
   * Without a quarantine there is no resync the error is waiting on, so the swallow budget - which exists to halt a node
   * that "can never resync" instead of degrading silently - is not charged: each failure is reported to its own
   * submitter and logged, and halting the only voter would take the whole cluster down into the same state on restart.
   */
  @Test
  void aSoleVoterIsNotHaltedByRepeatedApplyErrors() {
    for (int i = 0; i <= ArcadeStateMachine.MAX_DIVERGED_SWALLOWED_ERRORS + 1; i++) {
      final long index = 100L + i;
      assertThatThrownBy(() -> sm.applyWithRetry(index, DB_NAME, () -> {
        throw new IllegalStateException("apply failed at " + index);
      })).isInstanceOf(ReplicationException.class);
    }
    assertThat(sm.isDatabaseDiverged(DB_NAME)).isFalse();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  /** The counter-case: a leader with peers keeps the #4797 quarantine and the #8483 hand-off, exactly as before. */
  @Test
  void aLeaderWithPeersStillQuarantinesAndHandsOff() {
    soleVoter.set(false);

    assertThatThrownBy(() -> sm.applyWithRetry(12L, DB_NAME, () -> {
      throw new IllegalStateException("apply failed with peers around");
    })).isInstanceOf(ReplicationException.class).hasMessageContaining("snapshot resync");

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
    assertThat(sm.quarantineCause(DB_NAME)).isEqualTo(DivergenceCause.APPLY_ERROR);
    verify(raft).handOffLeadershipToResync(contains("'" + DB_NAME + "'"));
  }

  /**
   * A quarantine that already stands (restored from disk by #7735, or raised while the node still had peers) is not
   * this fix's to lift: the error keeps the resync-condition routing it always had.
   */
  @Test
  void anAlreadyQuarantinedDatabaseKeepsItsQuarantineOnASoleVoter() {
    sm.markStateDiverged(DB_NAME);

    assertThatThrownBy(() -> sm.applyWithRetry(13L, DB_NAME, () -> {
      throw new IllegalStateException("apply failed on a quarantined database");
    })).isInstanceOf(ReplicationException.class).hasMessageContaining("snapshot resync");

    assertThat(sm.isDatabaseDiverged(DB_NAME)).isTrue();
  }

  /**
   * The hand-off's "no peer" report used to promise a retry "as soon as a peer is eligible", which on a one-voter
   * configuration is never. It must say the state is terminal and name what it costs.
   */
  @Test
  void theNoPeerReportSaysASoleVoterIsTerminal() {
    final String soleVoterReport = RaftHAServer.noHandoffPeerReport("quarantined database 'x'", true);
    assertThat(soleVoterReport)
        .contains("only voter")
        .contains("terminal")
        .contains("not-ready")
        .contains("checkpoint")
        .doesNotContain("retried as soon as a peer is eligible");

    final String withPeersReport = RaftHAServer.noHandoffPeerReport("quarantined database 'x'", false);
    assertThat(withPeersReport).contains("retried as soon as a peer is eligible");
  }
}
