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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7523: {@code POST /api/v1/cluster/peer} can name the joining peer's leader-election priority, which
 * before this it could not - so a witness added at runtime always got Ratis's default and was as electable as
 * every other node, while the same peer declared in {@code arcadedb.ha.serverList} was not.
 * <p>
 * The last test is the one that says why this is not cosmetic: it drives the peer the handler really builds
 * through {@link RaftHAServer#selectStepDownTargets}, which is where a priority-0 peer stops being a candidate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7523AddPeerPriorityTest {

  /** Omitting the field keeps what every existing caller already gets: Ratis's own default. */
  @Test
  void anAbsentPriorityIsZero() {
    assertThat(PostAddPeerHandler.readPriority(new JSONObject())).isZero();
    assertThat(PostAddPeerHandler.readPriority(new JSONObject().put("peerId", "n1"))).isZero();
  }

  /** A JSON null is "not stated", not a malformed value: it takes the default rather than being refused. */
  @Test
  void anExplicitNullPriorityIsZero() {
    assertThat(PostAddPeerHandler.readPriority(new JSONObject("{\"priority\":null}"))).isZero();
  }

  @Test
  void anExplicitPriorityIsCarriedThrough() {
    assertThat(PostAddPeerHandler.readPriority(new JSONObject().put("priority", 7))).isEqualTo(7);
    assertThat(PostAddPeerHandler.readPriority(new JSONObject().put("priority", 0))).isZero();
  }

  /**
   * Ratis rejects a negative priority, so naming the field here is the difference between a 400 that says which
   * field was wrong and a failed membership change that says nothing of the sort.
   */
  @Test
  void aNegativePriorityIsRefusedByName() {
    assertThatThrownBy(() -> PostAddPeerHandler.readPriority(new JSONObject().put("priority", -1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("priority")
        .hasMessageContaining("-1");
  }

  @Test
  void aNonNumericPriorityIsRefusedByName() {
    assertThatThrownBy(() -> PostAddPeerHandler.readPriority(new JSONObject().put("priority", "witness")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("priority");
  }

  /**
   * The narrowing {@code JSONObject.getInt} would have done silently, and why it is not a rounding nit: every one
   * of these truncates to {@code 0}, and {@code 0} is not a neutral default here - it is the value that declares
   * the peer a witness as soon as any other peer carries a positive priority. The operator would have been told
   * the peer was added, at the one priority with the opposite meaning to the one they asked for.
   */
  @Test
  void aFractionalPriorityIsRefusedRatherThanTruncated() {
    for (final Object fractional : List.of(0.5, 0.99, 2.5))
      assertThatThrownBy(() -> PostAddPeerHandler.readPriority(new JSONObject().put("priority", fractional)))
          .as("priority %s", fractional)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("whole number");
  }

  /** Same reason: 2^32 narrows to 0 through an int cast, which would have read as a witness. */
  @Test
  void aPriorityTooLargeForAnIntIsRefusedRatherThanWrapped() {
    assertThatThrownBy(() -> PostAddPeerHandler.readPriority(new JSONObject().put("priority", 4294967296L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("32-bit");

    assertThatThrownBy(() -> PostAddPeerHandler.readPriority(new JSONObject().put("priority", Long.MIN_VALUE)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** The boundary stays usable: a value that does fit is not caught by the guard above. */
  @Test
  void theLargestUsablePriorityIsStillAccepted() {
    assertThat(PostAddPeerHandler.readPriority(new JSONObject().put("priority", Integer.MAX_VALUE)))
        .isEqualTo(Integer.MAX_VALUE);
  }

  /** A whole number that merely arrived as a double is a whole number, and is accepted. */
  @Test
  void aWholeNumberWrittenAsADecimalIsAccepted() {
    assertThat(PostAddPeerHandler.readPriority(new JSONObject("{\"priority\":10.0}"))).isEqualTo(10);
  }

  /** The peer the handler hands to {@code addPeer} carries the id, the address and the priority together. */
  @Test
  void thePeerBuiltFromThePayloadCarriesThePriority() {
    final RaftPeer peer = PostAddPeerHandler.peerFromPayload("host_2434", "host:2434",
        new JSONObject().put("priority", 12));

    assertThat(peer.getId()).isEqualTo(RaftPeerId.valueOf("host_2434"));
    assertThat(peer.getAddress()).isEqualTo("host:2434");
    assertThat(peer.getPriority()).isEqualTo(12);
  }

  /**
   * The point of the field. A peer admitted through this route with {@code priority: 0}, while another peer
   * carries a positive one, is a witness: it is skipped as a step-down target and Ratis never elects it. Before
   * this it would have been an ordinary candidate, which is the one thing declaring it a witness was for.
   */
  @Test
  void aPeerAddedAsAWitnessIsExcludedFromLeadership() {
    final RaftPeer witness = PostAddPeerHandler.peerFromPayload("witness_2436", "witness:2436",
        new JSONObject().put("priority", 0));
    final RaftPeer voter = PostAddPeerHandler.peerFromPayload("voter_2435", "voter:2435",
        new JSONObject().put("priority", 10));
    final RaftPeer local = PostAddPeerHandler.peerFromPayload("local_2434", "local:2434", new JSONObject());

    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(List.of(local, voter, witness),
        local.getId(), null);

    assertThat(targets).extracting(p -> p.getId().toString()).containsExactly("voter_2435");
  }

  /**
   * And the regression it replaces: the same two peers added the way this route used to build them - id and
   * address only - are both candidates, because both end up on the default priority.
   */
  @Test
  void withoutThePriorityFieldTheWitnessWouldStillBeACandidate() {
    final RaftPeer witness = PostAddPeerHandler.peerFromPayload("witness_2436", "witness:2436", new JSONObject());
    final RaftPeer voter = PostAddPeerHandler.peerFromPayload("voter_2435", "voter:2435", new JSONObject());
    final RaftPeer local = PostAddPeerHandler.peerFromPayload("local_2434", "local:2434", new JSONObject());

    final List<RaftPeer> targets = RaftHAServer.selectStepDownTargets(List.of(local, voter, witness),
        local.getId(), null);

    assertThat(targets).extracting(p -> p.getId().toString())
        .containsExactlyInAnyOrder("voter_2435", "witness_2436");
  }
}
