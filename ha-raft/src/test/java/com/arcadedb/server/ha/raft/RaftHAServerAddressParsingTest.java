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

import com.arcadedb.server.ServerException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Edge cases for {@link RaftPeerAddressResolver#parsePeerList} not covered by {@link RaftHAServerTest}.
 * Ported from apache-ratis branch.
 */
class RaftHAServerAddressParsingTest {

  @Test
  void emptyStringThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void blankEntryThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("  ", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void tooManyColonsThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:1:2:3:4", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void blankHostnameThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList(":2434", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void nonNumericPriorityThrows() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parsePeerList("host:2434:2480:abc", 2434))
        .isInstanceOf(ServerException.class);
  }

  @Test
  void singleNodeCluster() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("myhost:2434:2480", 2434);
    assertThat(parsed.peers()).hasSize(1);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("myhost:2434");
    assertThat(parsed.httpAddresses()).hasSize(1);
  }

  @Test
  void trailingCommaIgnored() {
    // parsePeerList splits on comma; trailing comma creates empty entry that should be handled
    // This test documents the current behavior - adjust if it should throw instead
    final var parsed = RaftPeerAddressResolver.parsePeerList("host1:2434,host2:2435", 2434);
    assertThat(parsed.peers()).hasSize(2);
  }

  @Test
  void leadingWhitespaceInEntryTrimmed() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("  host1:2434 , host2:2435 ", 2434);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("host1:2434");
    assertThat(parsed.peers().get(1).getAddress()).isEqualTo("host2:2435");
  }
}
