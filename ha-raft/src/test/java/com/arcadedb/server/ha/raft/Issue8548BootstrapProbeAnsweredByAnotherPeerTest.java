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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8548: a bootstrap-state probe answered by a node other than the one it was sent to must not be counted as
 * that peer's state.
 * <p>
 * The election files every answer under the id of the peer it meant to ask. When the address it dialled belongs to
 * another node - a stale or shifted {@code http} port in {@code arcadedb.ha.serverList}, another cluster's server on
 * the same host - the answering node's {@code lastTxId} and fingerprint were elected from as if they were that peer's.
 * In {@code RaftBootstrapLeadershipTransferIT} that picked a peer holding {@code lastTxId=100} over the one holding
 * 500, and on the reporter's run left two nodes with different copies of the schema. The answer names the node that
 * wrote it ({@code peerId}, present since the endpoint was introduced), so the probe can refuse it.
 */
class Issue8548BootstrapProbeAnsweredByAnotherPeerTest {

  private static final RaftPeerId ASKED = RaftPeerId.valueOf("localhost_31485");

  private static String answerFrom(final String peerId, final long lastTxId) {
    return """
        {"peerId":"%s","databases":[{"name":"graph","fingerprint":"ffff","lastTxId":%d}]}""".formatted(peerId, lastTxId);
  }

  @Test
  void theAnswerOfThePeerThatWasAskedIsItsState() {
    final BootstrapElection.ProbeOutcome outcome = BootstrapElection.probeOutcomeOf(ASKED, 200,
        answerFrom("localhost_31485", 500L), Set.of("graph"));

    assertThat(outcome.result()).isEqualTo(BootstrapElection.ProbeResult.OK);
    assertThat(outcome.states().get("graph").peerId()).isEqualTo(ASKED);
    assertThat(outcome.states().get("graph").lastTxId()).isEqualTo(500L);
  }

  @Test
  void anAnswerWrittenByAnotherPeerIsNotFiledUnderTheAskedOne() {
    // The defect: the stranger's lastTxId=100 was recorded as the asked peer's, and the election chose from it.
    final BootstrapElection.ProbeOutcome outcome = BootstrapElection.probeOutcomeOf(ASKED, 200,
        answerFrom("localhost_24110", 100L), Set.of("graph"));

    assertThat(outcome.result())
        .as("the dialled address does not identify the peer; the answer cannot become right by asking it again")
        .isEqualTo(BootstrapElection.ProbeResult.FATAL);
    assertThat(outcome.states()).isNull();
    assertThat(outcome.detail()).contains("localhost_24110").contains("localhost_31485");
  }

  @Test
  void anAnswerThatNamesNoPeerIsNotFiledUnderTheAskedOne() {
    // Every build that serves /bootstrap-state has named itself in the answer, so one that does not is not a peer.
    final BootstrapElection.ProbeOutcome outcome = BootstrapElection.probeOutcomeOf(ASKED, 200,
        """
            {"databases":[{"name":"graph","fingerprint":"ffff","lastTxId":100}]}""", Set.of("graph"));

    assertThat(outcome.result()).isEqualTo(BootstrapElection.ProbeResult.FATAL);
    assertThat(outcome.states()).isNull();
  }

  @Test
  void theStatusIsStillJudgedBeforeTheBody() {
    // The extraction must keep the existing status rules: a transient status is retried, a definitive one is not.
    assertThat(BootstrapElection.probeOutcomeOf(ASKED, 503, "", Set.of("graph")).result())
        .isEqualTo(BootstrapElection.ProbeResult.RETRYABLE);
    assertThat(BootstrapElection.probeOutcomeOf(ASKED, 404, "", Set.of("graph")).result())
        .isEqualTo(BootstrapElection.ProbeResult.FATAL);
  }

  @Test
  void aMalformedAnswerIsStillRetried() {
    assertThat(BootstrapElection.probeOutcomeOf(ASKED, 200, "not json", Set.of("graph")).result())
        .isEqualTo(BootstrapElection.ProbeResult.RETRYABLE);
  }
}
