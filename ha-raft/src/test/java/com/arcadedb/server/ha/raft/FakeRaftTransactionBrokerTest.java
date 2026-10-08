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

import com.arcadedb.network.binary.QuorumNotReachedException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Issue #9464: the recording fakes record what they receive, answer what was set, and hold no thread. */
class FakeRaftTransactionBrokerTest {
  @Test
  void callsAreRecordedWithTheirArgumentsAndAnswerTheMockDefaults() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();

    assertThat(broker.replicateSecurityUsers("[]", "fp")).isFalse();
    assertThat(broker.replicateDropDatabase("db")).isZero();
    // The 3-argument overload delegates to the 4-argument form, and is recorded under its name
    broker.replicateTransaction("db", new byte[0], Map.of());

    assertThat(broker.calls("replicateSecurityUsers")).containsExactly(List.of("[]", "fp"));
    assertThat(broker.calls("replicateDropDatabase")).containsExactly(List.of("db"));
    assertThat(broker.calls("replicateTransaction")).hasSize(1);
    // -1: the "not prepared at any index" the real 3-argument overload passes to the 4-argument form
    assertThat(broker.calls("replicateTransaction").getFirst().get(3)).isEqualTo(-1L);
    assertThat(broker.calls("replicateSecurityGroups")).isEmpty();
  }

  @Test
  void answersCanBeSetToAValueAFailureOrAFunction() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker().returns("replicateDropDatabase", 42L)
        .fails("replicateSecurityGroups", new QuorumNotReachedException("q"))
        .on("replicateSecurityUsers", args -> "expected".equals(args[1]));

    assertThat(broker.replicateDropDatabase("db")).isEqualTo(42L);
    assertThatThrownBy(() -> broker.replicateSecurityGroups("{}", null)).isInstanceOf(QuorumNotReachedException.class);
    assertThat(broker.replicateSecurityUsers("[]", "expected")).isTrue();
    assertThat(broker.replicateSecurityUsers("[]", "other")).isFalse();
    assertThat(broker.calls("replicateSecurityGroups")).as("a failing call is still recorded").hasSize(1);
  }

  @Test
  void aMisspelledMethodIsRefusedWhereItIsWritten() {
    assertThatThrownBy(() -> new FakeRaftTransactionBroker().returns("replicateDropDatabse", 1L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("replicateDropDatabse");
    assertThatThrownBy(() -> FakeRaftHAServer.detached().returns("waitForAppliedIndx", null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aSharedLogKeepsTheOrderAcrossFakes() {
    final CallLog log = new CallLog();
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker(log);
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().recordingOn(log).transactionBroker(broker);

    raft.getTransactionBroker().replicateDropDatabase("db");
    raft.waitForAppliedIndex("db", 7L, true);

    assertThat(log.methods()).containsExactly("replicateDropDatabase", "waitForAppliedIndex");
    assertThat(raft.calls("waitForAppliedIndex")).containsExactly(List.of("db", 7L, true));
    assertThat(raft.peersMissingCapabilityNow("cap")).as("an unstubbed mock's answer: nobody missing").isEmpty();
  }

  @Test
  void aValueOfTheWrongTypeIsRefusedWhereItIsWritten() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    assertThatThrownBy(() -> broker.returns("replicateDropDatabase", 42)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Long");
    broker.on("replicateDropDatabase", args -> null);
    assertThatThrownBy(() -> broker.replicateDropDatabase("db")).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("replicateDropDatabase");
  }

  @Test
  void theFakeHoldsNoCommitterThread() {
    // Only the threads this construction started: another test's real broker may own a committer of its own
    final Set<Thread> before = Set.copyOf(Thread.getAllStackTraces().keySet());
    new FakeRaftTransactionBroker();
    await().atMost(Duration.ofSeconds(10)).until(() -> Thread.getAllStackTraces().keySet().stream()
        .filter(t -> !before.contains(t))
        .noneMatch(t -> t.isAlive() && t.getName().equals("arcadedb-raft-group-committer")));
  }
}
