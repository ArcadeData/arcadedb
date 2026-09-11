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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7331: the capability probe buffered its failures and applied them only after the second pass, so for
 * the rest of the round a peer that had just failed to answer was still believed capable - and could be sent a
 * schema delta it cannot decode.
 * <p>
 * That is the exact scenario the mechanism exists for. A rolling upgrade restarts a node onto an older build,
 * which answers 404 on the capability route; the leader's own javadoc states, in bold, that <i>every failure
 * forgets rather than keeps</i>, because a peer that stopped answering may have been replaced by an older
 * build. Buffering suspended that guarantee for the width of a probe round.
 * <p>
 * The deferral had a real motive, and it is kept: the second pass is worth running before anything is REPORTED,
 * so a peer it identifies at a shared address is not warned about for the refusal that sent us looking for it.
 * The split is that the belief moves at the failure and only the report waits -
 * {@link PeerCapabilityRegistry#suspend} against {@link PeerCapabilityRegistry#forget}.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7331">issue #7331</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7331CapabilityForgottenAtProbeFailureTest {

  /** Local node, one peer on its own address, and two that collapse onto localhost:2490 so pass 2 runs. */
  private static final String MIXED_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2490,localhost:2437:2490";
  private static final String PEER_A     = "localhost_2435";
  private static final String PEER_C     = "localhost_2436";

  /**
   * The repro. Round 1 records a capable advertisement for the peer with its own address. Round 2 has that peer
   * answer 404 - it has been restarted onto an older build - and samples what the registry says about it from
   * INSIDE the second pass, which is the window the buffering held open.
   */
  @Test
  void aPeerThatJustFailedIsNoLongerBelievedWhileTheRoundIsStillRunning() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
    final ScriptedProber prober = new ScriptedProber(raft);

    // Round 1: the peer on its own address advertises schema-delta.
    prober.guardedAnswer = advertisement(PEER_A);
    raft.setCapabilityProber(prober);
    raft.refreshPeerCapabilities();

    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A))
        .as("the round has to establish the belief, or the next one cannot show it being dropped")
        .isNotNull();

    // Round 2: the same peer answers 404. The shared-endpoint probe of pass 2 samples the registry.
    prober.guardedAnswer = null;
    prober.sampleDuringSharedPass = true;
    raft.refreshPeerCapabilities();

    assertThat(prober.seenDuringSharedPass.get())
        .as("the belief must be gone by the time the round is still probing: a schema change committed in this "
            + "window used to be shipped as a delta to a node that had already answered 404")
        .isNull();
    assertThat(raft.peersMissingCapability(PeerCapabilities.SCHEMA_DELTA))
        .as("and it is still missing after the round, which never regressed")
        .contains(PEER_A);
  }

  /** The reason an operator reads is set at the failure too, not only once the round has settled. */
  @Test
  void theReasonIsAvailableAsSoonAsTheProbeFails() {
    final RaftHAServer raft = Issue7256SharedAddressCapabilityProbeTest.newDetachedServer(MIXED_LIST, -1);
    final ScriptedProber prober = new ScriptedProber(raft);
    prober.guardedAnswer = null;
    prober.sampleReasonDuringSharedPass = true;
    raft.setCapabilityProber(prober);

    raft.refreshPeerCapabilities();

    assertThat(prober.reasonDuringSharedPass.get())
        .as("'unknown, and here is why' is the whole point of #7256's reason field; it cannot arrive a pass late")
        .contains("404");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The registry contract the split rests on, pinned where it lives.
  // ---------------------------------------------------------------------------------------------------------

  /**
   * {@code suspend} drops the belief and leaves the report shadow alone; {@code forget} settles it.
   * <p>
   * Both halves matter. Dropping the belief is the fix. NOT settling the shadow is what keeps the fix from
   * costing a warning and a re-advertisement every five seconds on a cluster whose peers all dial through one
   * collapsed address - which is the cost the buffering was there to avoid, and the reason it is the LOG that
   * waits rather than the state.
   */
  @Test
  void suspendMovesTheBeliefWithoutSettlingTheReport() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long generation = registry.generation();

    registry.record(generation, PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    assertThat(registry.freshAdvertisementOf(PEER_A)).isNotNull();

    registry.suspend(generation, PEER_A, "capability query returned HTTP 404");
    assertThat(registry.freshAdvertisementOf(PEER_A))
        .as("the belief is gone immediately")
        .isNull();
    assertThat(registry.unknownReasonOf(PEER_A)).isEqualTo("capability query returned HTTP 404");

    assertThat(registry.record(generation, PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1"))
        .as("a peer the second pass recovers said nothing new, so there is nothing to announce")
        .isFalse();
    assertThat(registry.freshAdvertisementOf(PEER_A)).as("and it is believed again").isNotNull();

    registry.suspend(generation, PEER_A, "capability query returned HTTP 404");
    assertThat(registry.forget(generation, PEER_A, "capability query returned HTTP 404"))
        .as("a peer the second pass does NOT recover is a transition, and is reported once")
        .isTrue();
    assertThat(registry.forget(generation, PEER_A, "capability query returned HTTP 404"))
        .as("and not again while it persists")
        .isFalse();
  }

  /** An answer from an ended leadership term must not resurrect - or drop - this term's state. */
  @Test
  void suspendIsDroppedWhenItsRoundBelongsToAnEndedTerm() {
    final PeerCapabilityRegistry registry = new PeerCapabilityRegistry();
    final long staleGeneration = registry.generation();
    registry.clear();

    registry.record(registry.generation(), PEER_A, Set.of(PeerCapabilities.SCHEMA_DELTA), "26.10.1");
    registry.suspend(staleGeneration, PEER_A, "a probe from the previous term");

    assertThat(registry.freshAdvertisementOf(PEER_A))
        .as("a failure observed under the previous leadership says nothing about this one")
        .isNotNull();
    assertThat(registry.unknownReasonOf(PEER_A)).isNull();
  }

  private static PeerCapabilityQuery.Advertisement advertisement(final String peerId) {
    return new PeerCapabilityQuery.Advertisement(peerId, "26.10.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
  }

  /**
   * Answers the guarded (pass 1) probe from a script and, when asked to, reads the registry from inside the
   * shared-endpoint (pass 2) probe - the only place a test can observe the state MID-ROUND, which is where the
   * defect lived.
   */
  private final class ScriptedProber implements RaftHAServer.CapabilityProber {
    private final RaftHAServer                                    raft;
    private final List<String>                                    calls                       = new ArrayList<>();
    private       PeerCapabilityQuery.Advertisement               guardedAnswer;
    private       boolean                                         sampleDuringSharedPass;
    private       boolean                                         sampleReasonDuringSharedPass;
    private final AtomicReference<PeerCapabilityRegistry.Advertisement> seenDuringSharedPass  = new AtomicReference<>();
    private final AtomicReference<String>                         reasonDuringSharedPass      = new AtomicReference<>();

    private ScriptedProber(final RaftHAServer raft) {
      this.raft = raft;
    }

    @Override
    public PeerCapabilityQuery.Advertisement probe(final String expectedPeerId, final String httpAddress,
        final String httpsAddress, final String clusterToken) throws IOException {
      calls.add(expectedPeerId + "@" + httpAddress);

      if (expectedPeerId == null) {
        // Pass 2. Sample what the leader currently believes about the peer whose pass-1 probe just failed.
        if (sampleDuringSharedPass)
          seenDuringSharedPass.set(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER_A));
        if (sampleReasonDuringSharedPass)
          reasonDuringSharedPass.set(raft.getPeerCapabilityRegistry().unknownReasonOf(PEER_A));
        return advertisement(PEER_C);
      }

      if (guardedAnswer == null)
        throw new IOException("capability query to " + httpAddress + " returned HTTP 404");
      return guardedAnswer;
    }
  }
}
