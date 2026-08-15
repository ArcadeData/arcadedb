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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A single-machine cluster is the commonest way to meet the address the self-forward guard exists to catch
 * (issue #6191), and it is also the one place where the same socket gets written two ways: one entry of
 * {@code HA_SERVER_LIST} says {@code localhost}, the next says {@code 127.0.0.1}. The guard compares text, so
 * before issue #6204 those two read as different nodes and the guard stayed silent on exactly the deployment
 * that needs it.
 * <p>
 * The equivalence stops at the spellings of <em>the</em> loopback address. A distinct loopback address
 * ({@code 127.0.0.2}), a wildcard bind ({@code 0.0.0.0}) and a declared host name are all still different
 * endpoints: those can genuinely name another node, and answering "that is me" for one of them would refuse a
 * write that had to be forwarded.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6204LoopbackEndpointTest {

  @Test
  void localhostAndTheIpv4LoopbackAreTheSameEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "127.0.0.1:2480")).isTrue();
    assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "localhost:2480")).isTrue();
  }

  @Test
  void theIpv6LoopbackIsTheSameEndpointToo() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "[::1]:2480")).isTrue();
    assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "[0:0:0:0:0:0:0:1]:2480")).isTrue();
    // Unbracketed, which is how a hand-written server list tends to spell it: the last colon is still the
    // port separator, so the host is read as "::1" and not as a truncated address.
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "::1:2480")).isTrue();
  }

  /** Host names are case-insensitive on both sides of the equivalence, not only when they match textually. */
  @Test
  void caseIsStillIgnored() {
    assertThat(RaftHAServer.isSameHttpEndpoint("LOCALHOST:2480", "127.0.0.1:2480")).isTrue();
  }

  /**
   * The port is what tells apart the nodes of a same-host cluster, and it is the reason the equivalence is
   * safe: two of them cannot be listening on one port of one host, so equal port plus loopback on both sides
   * is the same socket.
   */
  @Test
  void thePortStillDecides() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "127.0.0.1:2481")).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint("[::1]:2480", "localhost:2481")).isFalse();
  }

  /**
   * {@code 127.0.0.2} is a loopback address but not the same one: binding two nodes to different addresses of
   * the loopback range with a shared port is a legitimate way to run a cluster on one machine, and unifying
   * them would refuse the forwarding between them.
   */
  @Test
  void anotherLoopbackAddressIsADifferentSocket() {
    assertThat(RaftHAServer.isSameHttpEndpoint("127.0.0.1:2480", "127.0.0.2:2480")).isFalse();
  }

  /**
   * A wildcard bind accepts connections on every local interface, so it is tempting to read it as "any
   * address is mine". It is not: two nodes on two machines both bound to {@code 0.0.0.0:2480} would then
   * refuse to forward to each other, turning a working cluster into a read-only one.
   */
  @Test
  void theWildcardBindIsNotEveryPeer() {
    assertThat(RaftHAServer.isSameHttpEndpoint("0.0.0.0:2480", "db1.example.com:2480")).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint("0.0.0.0:2480", "127.0.0.1:2480")).isFalse();
  }

  /** A declared host name is a statement about which node owns which port, and it is not overruled here. */
  @Test
  void aDeclaredHostNameIsNotUnifiedWithAnIp() {
    assertThat(RaftHAServer.isSameHttpEndpoint("db0.example.com:2480", "127.0.0.1:2480")).isFalse();
  }

  /** Without a port there is no endpoint to compare, so the answer falls back to the text. */
  @Test
  void aBareHostIsNotAnEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost", "127.0.0.1")).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost", "localhost")).isTrue();
  }

  @Test
  void theLoopbackSpellingsAreRecognisedOnTheirOwn() {
    assertThat(LoopbackHosts.isLoopback("localhost")).isTrue();
    assertThat(LoopbackHosts.isLoopback("LocalHost")).isTrue();
    assertThat(LoopbackHosts.isLoopback("127.0.0.1")).isTrue();
    assertThat(LoopbackHosts.isLoopback("::1")).isTrue();
    assertThat(LoopbackHosts.isLoopback("[::1]")).isTrue();
    assertThat(LoopbackHosts.isLoopback("0:0:0:0:0:0:0:1")).isTrue();

    assertThat(LoopbackHosts.isLoopback("127.0.0.2")).isFalse();
    assertThat(LoopbackHosts.isLoopback("0.0.0.0")).isFalse();
    assertThat(LoopbackHosts.isLoopback("db0")).isFalse();
    assertThat(LoopbackHosts.isLoopback("[")).isFalse();
    assertThat(LoopbackHosts.isLoopback("")).isFalse();
    assertThat(LoopbackHosts.isLoopback(null)).isFalse();
  }
}
