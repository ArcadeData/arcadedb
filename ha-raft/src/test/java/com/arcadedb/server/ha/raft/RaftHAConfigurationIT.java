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

import com.arcadedb.server.ServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates {@link RaftPeerAddressResolver#parsePeerList} configuration error handling.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAConfigurationIT {

  @Test
  void invalidPeerAddressRejected() {
    // Mix non-localhost IPs with localhost - this should be rejected
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("192.168.0.1:2424,192.168.0.1:2425,localhost:2424", 2424))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Found a localhost");
  }

  @Test
  void twoPartFormatParsed() {
    final RaftPeerAddressResolver.ParsedPeerList result = RaftPeerAddressResolver.parsePeerList("host1:2424,host2:2425", 2424);
    assertThat(result.peers()).hasSize(2);
    assertThat(result.peers().get(0).getAddress()).isEqualTo("host1:2424");
    assertThat(result.peers().get(1).getAddress()).isEqualTo("host2:2425");
    assertThat(result.httpAddresses()).isEmpty();
  }

  @Test
  void threePartFormatParsed() {
    final RaftPeerAddressResolver.ParsedPeerList result = RaftPeerAddressResolver.parsePeerList("host1:2424:2480,host2:2425:2481", 2424);
    assertThat(result.peers()).hasSize(2);
    assertThat(result.peers().get(0).getAddress()).isEqualTo("host1:2424");
    assertThat(result.httpAddresses().get(result.peers().get(0).getId())).isEqualTo("host1:2480");
  }

  @Test
  void fourPartFormatWithPriorityParsed() {
    final RaftPeerAddressResolver.ParsedPeerList result = RaftPeerAddressResolver.parsePeerList("host1:2424:2480:10", 2424);
    assertThat(result.peers()).hasSize(1);
    assertThat(result.peers().get(0).getPriority()).isEqualTo(10);
  }

  @Test
  void allLocalhostAddressesAllowed() {
    // All-localhost is fine - only mixing is rejected
    assertThatCode(() -> RaftPeerAddressResolver.parsePeerList("localhost:2424,localhost:2425,127.0.0.1:2426", 2424))
        .doesNotThrowAnyException();
  }
}
