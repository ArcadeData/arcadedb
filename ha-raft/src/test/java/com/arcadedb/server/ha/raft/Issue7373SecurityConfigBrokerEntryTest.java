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

import com.arcadedb.network.binary.QuorumNotReachedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The submit half of issue #7373: {@link RaftTransactionBroker}'s two new security methods, covered the same way
 * {@code RaftTransactionBrokerTest} covers {@code replicateSecurityUsers} - a broker built on a null
 * {@code RaftClient}, so the entry is encoded and then fails on dispatch.
 * <p>
 * What this pins that the codec test cannot: that each method reaches the group committer at all. A method that
 * encoded its entry and then returned without submitting would pass every round-trip assertion in
 * {@code Issue7373SecurityConfigEntryCodecTest} and replicate nothing.
 */
class Issue7373SecurityConfigBrokerEntryTest {

  private RaftTransactionBroker broker;

  @BeforeEach
  void setUp() {
    broker = new RaftTransactionBroker(null, Quorum.MAJORITY, 5_000);
  }

  @AfterEach
  void tearDown() {
    if (broker != null)
      broker.stop();
  }

  @Test
  void replicateSecurityGroupsSubmitsAnEntry() {
    assertThatThrownBy(() ->
        broker.replicateSecurityGroups("{\"databases\":{\"*\":{\"groups\":{}}},\"version\":2}"))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateSecurityApiTokensSubmitsAnEntry() {
    assertThatThrownBy(() ->
        broker.replicateSecurityApiTokens("{\"version\":1,\"tokens\":[]}"))
        .isInstanceOf(QuorumNotReachedException.class);
  }
}
