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
 * The question every automatic redirect has to answer before it dials a resolved leader address: is this
 * address my own? One that is identifies nobody, and following it lands the request back on the node that
 * resolved it - which resolves the same address and redirects again (issue #6191).
 * <p>
 * Exercised directly because the address comparison is shared by the write-forwarding path and the snapshot
 * resync refusal ({@code ArcadeStateMachine.triggerSnapshotDownload}, issue #6111), and neither of those can
 * show what it does with a hostname that differs only in case.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6191SelfForwardGuardTest {

  @Test
  void theSameEndpointIsRecognised() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "localhost:2480")).isTrue();
  }

  /** Host names are case-insensitive, and "a different case" must not read as "a different node". */
  @Test
  void hostCaseDoesNotMakeItADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("DB0.example.com:2480", "db0.example.com:2480")).isTrue();
  }

  /** The whole point on a same-host cluster: the port is what tells the peers apart. */
  @Test
  void aDifferentPortIsADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "localhost:2481")).isFalse();
  }

  @Test
  void aDifferentHostIsADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("db0:2480", "db1:2480")).isFalse();
  }

  /**
   * A declared host name is a statement about which node owns which port, and no name resolution happens
   * here to overrule it. The spellings of the loopback address are the one exception, in
   * {@link Issue6204LoopbackEndpointTest}.
   */
  @Test
  void aHostNameIsNotUnifiedWithAnIp() {
    assertThat(RaftHAServer.isSameHttpEndpoint("db0.example.com:2480", "10.0.0.7:2480")).isFalse();
  }

  /** An address that could not be resolved is not evidence of anything, in either position. */
  @Test
  void anUnresolvedAddressIsNeverAMatch() {
    assertThat(RaftHAServer.isSameHttpEndpoint(null, "localhost:2480")).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", null)).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint(null, null)).isFalse();
  }
}
