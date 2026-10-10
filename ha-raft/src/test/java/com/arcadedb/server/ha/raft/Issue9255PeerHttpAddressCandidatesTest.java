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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9255: the candidate HTTP addresses a node holds for its peers, and the two wire members that carry them - the
 * caller's self-description in a capability request and the relayed addresses in the reply.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9255PeerHttpAddressCandidatesTest {

  private static final String PEER = "hostB_2434";

  @Test
  void anOfferedAddressIsDueUntilItsProbeFails() {
    final AtomicLong now = new AtomicLong(1_000L);
    final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
    candidates.setClock(now::get);

    candidates.offer(PEER, "hostB:2490");
    candidates.offer(PEER, "hostB:2491");
    assertThat(candidates.due(PEER)).containsExactly("hostB:2490", "hostB:2491");

    candidates.failed(PEER, "hostB:2490");
    assertThat(candidates.due(PEER)).containsExactly("hostB:2491");

    // A relay re-offers the same stale address every round: that must not undo the back-off
    candidates.offer(PEER, "hostB:2490");
    assertThat(candidates.due(PEER)).containsExactly("hostB:2491");

    now.addAndGet(PeerHttpAddressCandidates.RETRY_AFTER_FAILURE_MS);
    assertThat(candidates.due(PEER)).containsExactly("hostB:2490", "hostB:2491");
  }

  @Test
  void aPeerHoldsABoundedNumberOfCandidatesAndTheOldestGoesFirst() {
    final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
    for (int i = 0; i < PeerHttpAddressCandidates.MAX_PER_PEER + 2; i++)
      candidates.offer(PEER, "hostB:" + (2490 + i));

    assertThat(candidates.all(PEER)).hasSize(PeerHttpAddressCandidates.MAX_PER_PEER).doesNotContain("hostB:2490", "hostB:2491")
        .contains("hostB:" + (2490 + PeerHttpAddressCandidates.MAX_PER_PEER + 1));
  }

  @Test
  void aConfirmedOrDepartedPeerTakesItsCandidatesWithIt() {
    final PeerHttpAddressCandidates candidates = new PeerHttpAddressCandidates();
    candidates.offer(PEER, "hostB:2490");
    candidates.offer("hostC_2434", "hostC:2490");

    candidates.confirmed(PEER);
    assertThat(candidates.due(PEER)).isEmpty();

    candidates.retainOnly(Set.of(PEER));
    assertThat(candidates.all("hostC_2434")).isEmpty();
  }

  @Test
  void theReplyRelaysAddressesSortedAndKeepsTheOldShapeWhenThereAreNone() throws IOException {
    final JSONObject withRelay = PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of(),
        Map.of("hostC_2434", "hostC:2490", "hostB_2434", "hostB:2490"));
    assertThat(withRelay.getJSONObject(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES).keySet()).containsExactly("hostB_2434",
        "hostC_2434");

    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("hostA_2434", withRelay.toString(), "test");
    assertThat(parsed.peerHttpAddresses()).containsExactlyInAnyOrderEntriesOf(
        Map.of("hostB_2434", "hostB:2490", "hostC_2434", "hostC:2490"));

    // Byte-identical to what a node wrote before the field, so the document of a node that relays nothing does not move
    assertThat(PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of(), Map.of()).toString())
        .isEqualTo(PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"), false, Set.of()).toString());
    assertThat(PeerCapabilityQuery.parse("hostA_2434",
        PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x")).toString(), "test").peerHttpAddresses()).isEmpty();
  }

  /** A malformed relayed entry is dropped, never the answer it came with: the relay is only ever a hint. */
  @Test
  void aMalformedRelayedAddressIsDroppedAndTheAnswerKept() throws IOException {
    final JSONObject reply = PostCapabilitiesHandler.advertisement("hostA_2434", Set.of("x"))
        .put(PostCapabilitiesHandler.PEER_HTTP_ADDRESSES, new JSONObject().put("hostB_2434", "not an address")
            .put("hostC_2434", "hostC:2490"));

    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("hostA_2434", reply.toString(), "test");
    assertThat(parsed.capabilities()).containsExactly("x");
    assertThat(parsed.peerHttpAddresses()).containsExactly(Map.entry("hostC_2434", "hostC:2490"));
    assertThat(List.of(PeerCapabilityQuery.isPeerAddress("hostB:2490"), PeerCapabilityQuery.isPeerAddress("[::1]:2490"),
        PeerCapabilityQuery.isPeerAddress("hostB"), PeerCapabilityQuery.isPeerAddress(""))).containsExactly(true, true, false,
        false);
  }
}
