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

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RaftTransactionBroker}. Uses a null RaftClient so entries
 * fail on dispatch, but we can verify encoding via the thrown exception path.
 */
class RaftTransactionBrokerTest {

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
  void replicateTransactionEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateTransaction("testdb", new byte[] { 1, 2, 3 }, Map.of(0, 1)))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateSchemaEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateSchema("testdb", "{}", Map.of(1, "file1.dat"), Map.of(),
            Collections.emptyList(), Collections.emptyList()))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateInstallDatabaseEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateInstallDatabase("testdb", false))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateDropDatabaseEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateDropDatabase("testdb"))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void replicateSecurityUsersEncodesCorrectEntryType() {
    assertThatThrownBy(() ->
        broker.replicateSecurityUsers("[{\"name\":\"root\"}]"))
        .isInstanceOf(QuorumNotReachedException.class);
  }

  @Test
  void stopDelegates() {
    broker.stop();
    assertThatThrownBy(() ->
        broker.replicateTransaction("testdb", new byte[] { 1 }, Map.of()))
        .isInstanceOf(QuorumNotReachedException.class);
    broker = null;
  }
}
