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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7219: the advertisement document is a wire contract between two nodes that may be running different
 * builds, so its shape - and the identity check the reader makes on it - are pinned here rather than left to an
 * integration test that only ever exercises two identical nodes.
 */
class Issue7219CapabilityAdvertisementTest {

  private static final String URL = "http://localhost:2480/api/v1/cluster/capabilities";

  @Test
  void thisBuildAdvertisesThatItDecodesSchemaDeltas() {
    // The decoder has been unconditional since #6989, so this build must say so - otherwise a cluster of nodes
    // that can all read a delta would never agree to write one.
    assertThat(PeerCapabilities.LOCAL).contains(PeerCapabilities.SCHEMA_DELTA);
  }

  @Test
  void theAdvertisementNamesThePeerItsVersionAndItsCapabilities() {
    final JSONObject document = PostCapabilitiesHandler.advertisement("arcadedb0", Set.of(PeerCapabilities.SCHEMA_DELTA));

    assertThat(document.getString("peerId")).isEqualTo("arcadedb0");
    assertThat(document.getString("version", "")).isNotEmpty();
    assertThat(document.getJSONArray("capabilities").toList()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
  }

  @Test
  void capabilitiesAreSortedSoTwoPeersDocumentsCanBeDiffed() {
    final Set<String> unsorted = new LinkedHashSet<>();
    unsorted.add("zeta");
    unsorted.add("alpha");
    unsorted.add("mu");

    assertThat(PostCapabilitiesHandler.advertisement("arcadedb0", unsorted).getJSONArray("capabilities").toList())
        .containsExactly("alpha", "mu", "zeta");
  }

  @Test
  void anAdvertisementIsReadBackFromItsOwnDocument() throws IOException {
    final String body = PostCapabilitiesHandler.advertisement("arcadedb1", Set.of(PeerCapabilities.SCHEMA_DELTA))
        .toString();

    final PeerCapabilityQuery.Advertisement parsed = PeerCapabilityQuery.parse("arcadedb1", body, URL);

    assertThat(parsed.peerId()).isEqualTo("arcadedb1");
    assertThat(parsed.capabilities()).containsExactly(PeerCapabilities.SCHEMA_DELTA);
  }

  @Test
  void anAdvertisementFromAnotherPeerIsRejected() {
    // With no 'http' port declared in arcadedb.ha.serverList a peer's endpoint is DERIVED, and several peers can
    // collapse onto one address (#6202, #6267). PeerDialAddress withholds such an address, and this is the
    // independent second half of that guard: an answer that came back from the wrong node must not be recorded
    // against the node that was asked, or the leader would credit a capability to a peer that never claimed it.
    final String body = PostCapabilitiesHandler.advertisement("arcadedb0", Set.of(PeerCapabilities.SCHEMA_DELTA))
        .toString();

    assertThatThrownBy(() -> PeerCapabilityQuery.parse("arcadedb1", body, URL))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("arcadedb1")
        .hasMessageContaining("arcadedb0");
  }

  @Test
  void anAdvertisementWithNoCapabilityArrayIsEmptyRatherThanAFailure() throws IOException {
    // Forward compatibility in the other direction: a future build may answer with fields this one does not know
    // about, and it must read as "nothing advertised" rather than throwing, which is a strictly safer answer.
    final PeerCapabilityQuery.Advertisement parsed =
        PeerCapabilityQuery.parse("arcadedb1", "{\"peerId\":\"arcadedb1\",\"somethingNew\":42}", URL);

    assertThat(parsed.capabilities()).isEmpty();
    assertThat(parsed.version()).isEmpty();
  }

  @Test
  void theEndpointIsPreferredOverHttpsOnlyWhenSslIsOn() {
    assertThat(PeerCapabilityQuery.chooseUrl("h:1", "h:2", false)).isEqualTo("http://h:1/api/v1/cluster/capabilities");
    assertThat(PeerCapabilityQuery.chooseUrl("h:1", "h:2", true)).isEqualTo("https://h:2/api/v1/cluster/capabilities");
    // SSL on but no HTTPS address resolved: fall back to the listener that is always there, exactly as
    // LeaderDatabaseQuery does, rather than refusing to ask at all.
    assertThat(PeerCapabilityQuery.chooseUrl("h:1", null, true)).isEqualTo("http://h:1/api/v1/cluster/capabilities");
    assertThat(PeerCapabilityQuery.chooseUrl(null, null, false)).isNull();
  }
}
