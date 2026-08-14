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

import com.arcadedb.server.HAServerPlugin;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.HAServerPlugin.ROUTING_PROTOCOL.GRPC;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two peers cannot both be listening on one {@code host:port} for one protocol, so a routing view that maps two
 * of them to the same address has resolved at most one of them and cannot say which (issue #6183). These are the
 * cases {@link RaftHAServer#selectUnambiguousRouting} has to separate, exercised directly because the shape that
 * matters - a follower advertising itself as the leader - is what an in-process cluster produces by default and
 * what a heterogeneous one never can.
 * <p>
 * Index 0 is the writer throughout, matching the caller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6183AmbiguousRoutingTest {

  @Test
  void distinctAddressesAreAllAdvertised() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "db0:50051", "db1:50051", "db2:50051" },
        new boolean[] { false, false, false });

    assertThat(table).isNotNull();
    assertThat(table.protocol()).isEqualTo(GRPC);
    assertThat(table.writer()).isEqualTo("db0:50051");
    assertThat(table.readers()).containsExactly("db1:50051", "db2:50051");
  }

  /**
   * The reachable defect: every node on one host, nothing declared, so every peer derives to this node's own
   * port. Advertising that would hand a caller the address of the node that just refused it.
   */
  @Test
  void aDerivedViewThatCannotTellPeersApartAdvertisesNothing() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "localhost:50051", "localhost:50051", "localhost:50051" },
        new boolean[] { false, false, false });

    assertThat(table).isNull();
  }

  /** A declared address is a statement of fact; a derived one is a guess. The collision only indicts the guess. */
  @Test
  void aDeclaredWriterOutranksACollidingDerivedFollower() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "localhost:50051", "localhost:50051" },
        new boolean[] { true, false });

    assertThat(table).isNotNull();
    assertThat(table.writer()).isEqualTo("localhost:50051");
    assertThat(table.readers()).as("the follower's derived address is the one proven wrong").isEmpty();
  }

  /** Mirror image: the leader's address is the guess, so there is no writer left to advertise. */
  @Test
  void aDerivedWriterCollidingWithADeclaredFollowerSuppressesTheTable() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "localhost:50051", "localhost:50051" },
        new boolean[] { false, true });

    assertThat(table).isNull();
  }

  /** Two peers declared the same endpoint: a configuration error with no defensible winner. */
  @Test
  void twoDeclaredClaimsOnOneAddressCancelEachOtherOut() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "db0:50051", "db0:50051" },
        new boolean[] { true, true });

    assertThat(table).isNull();
  }

  /** Ambiguity among followers costs only those followers: the writer is still worth advertising. */
  @Test
  void anUnambiguousWriterSurvivesAmbiguousFollowers() {
    final HAServerPlugin.RoutingTable table = select(
        new String[] { "db0:50051", "db1:50051", "db1:50051", "db2:50051" },
        new boolean[] { true, false, false, true });

    assertThat(table).isNotNull();
    assertThat(table.writer()).isEqualTo("db0:50051");
    assertThat(table.readers()).containsExactly("db2:50051");
  }

  /** A single-node cluster has nothing to be ambiguous with. */
  @Test
  void aLoneWriterIsNeverAmbiguous() {
    final HAServerPlugin.RoutingTable table = select(new String[] { "db0:50051" }, new boolean[] { false });

    assertThat(table).isNotNull();
    assertThat(table.writer()).isEqualTo("db0:50051");
    assertThat(table.readers()).isEmpty();
  }

  private static HAServerPlugin.RoutingTable select(final String[] addresses, final boolean[] fromConfig) {
    return RaftHAServer.selectUnambiguousRouting(GRPC, addresses, fromConfig, addresses.length);
  }
}
