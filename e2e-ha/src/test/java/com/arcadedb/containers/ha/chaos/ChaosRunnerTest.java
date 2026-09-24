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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosRunnerTest {
  @TempDir
  Path dir;

  private final List<Duration> sleeps = new ArrayList<>();

  private record Harness(ChaosRunner runner, FakeNodeControl control, FakeLoad load, LedgerNodeReader reader,
                         Ledger ledger) {
  }

  private static ChaosConfig config(final String... overrides) {
    final List<String> keyValues = new ArrayList<>(List.of("chaos.seed", "7", "chaos.maxSteps", "5", "chaos.duration",
        "PT1H", "chaos.writers", "1"));
    keyValues.addAll(Arrays.asList(overrides));
    return ChaosConfig.fromProperties(ChaosConfigTest.props(keyValues.toArray(new String[0])));
  }

  private Harness harness(final ChaosConfig config) throws IOException {
    final Ledger ledger = new Ledger(config.writers());
    final FakeNodeControl control = new FakeNodeControl();
    final FakeLoad load = new FakeLoad(ledger);
    final LedgerNodeReader reader = new LedgerNodeReader(ledger);
    final InvariantChecker checker = new InvariantChecker(ledger);
    final Checkpoint checkpoint = new Checkpoint(reader, ledger, checker, config.nodes(), Duration.ofSeconds(1),
        Duration.ofMillis(1));
    final ChaosReport report = new ChaosReport(dir.resolve(Long.toString(config.seed())), config);
    final ChaosRunner runner = new ChaosRunner(config, new ClusterState(config.nodes()), control,
        new FaultPicker(config.faultWeights(), config.electionTimeout()), load, ledger, checkpoint, checker, report, null,
        sleeps::add);
    return new Harness(runner, control, load, reader, ledger);
  }

  @Test
  void healthyClusterPassesAfterMaxSteps() throws IOException {
    final Harness harness = harness(config());
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.PASS);
    assertThat(result.steps()).isEqualTo(5);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).hasSize(5);
    assertThat(new JSONObject(Files.readString(dir.resolve("7/summary.json"))).getString("result")).isEqualTo("PASS");
    assertThat(harness.load().quiesced).isEqualTo(5);
    assertThat(harness.load().resumed).isEqualTo(5);
    assertThat(harness.load().closed).isTrue();
  }

  @Test
  void sameSeedReplaysTheSameFaultSequence() throws IOException {
    final Harness first = harness(config());
    first.runner().run();
    final Harness second = harness(config());
    second.runner().run();
    assertThat(first.control().calls).isEqualTo(second.control().calls);

    final Harness other = harness(config("chaos.seed", "8"));
    other.runner().run();
    assertThat(other.control().calls).isNotEqualTo(first.control().calls);
  }

  @Test
  void noAcksDuringHoldIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    for (int i = 0; i < ChaosRunner.WARMUP_ACKS; i++)
      harness.ledger().record(harness.ledger().reserve(0, false), Ledger.ACKED);
    harness.load().progress = false;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("kill");
    assertThat(result.steps()).isEqualTo(1);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString()
        .startsWith("step=1 fault=kill targets=").contains("FAILED kind=AVAILABILITY").contains("No write acknowledged");
  }

  @Test
  void nodeThatExitedOnItsOwnIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    harness.control().unexpectedExit = "node 1 exited unexpectedly: exitCode=137 OOMKilled=true";
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("node 1 exited unexpectedly: exitCode=137 OOMKilled=true");
    assertThat(result.steps()).isEqualTo(1);
    assertThat(harness.control().calls).doesNotContain("kill:0", "kill:1", "kill:2");
  }

  @Test
  void nodeThatExitedDuringTheStepIsReportedInsteadOfTheMissingLeader() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    harness.control().leaderAvailable = false;
    harness.control().unexpectedExit = "node 2 exited unexpectedly: exitCode=1 OOMKilled=false";
    harness.control().checksBeforeExit = 1;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("node 2 exited unexpectedly");
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString()
        .contains("FAILED kind=AVAILABILITY").contains("node 2 exited unexpectedly");
  }

  @Test
  void missingLeaderAfterHealIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    harness.control().leaderAvailable = false;
    harness.control().leaderView = "node 0: connect failed on 127.0.0.1:32918 (Connection refused)";
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("No leader").contains("node 0: connect failed on 127.0.0.1:32918");
  }

  @Test
  void lostAcknowledgedWriteStopsTheRunAtTheFirstCheckpoint() throws IOException {
    final Harness harness = harness(config());
    harness.reader().dropKey = Ledger.key(0, 0);
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.steps()).isEqualTo(1);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("I1");
    assertThat(Files.readString(dir.resolve("7/ledger-diff.txt"))).contains("w0-0");
  }

  @Test
  void nodeThatCrashedDuringInjectIsAnAvailabilityFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "rolling"));
    harness.control().failure = new IllegalStateException("Container is not running (304)");
    harness.control().unexpectedExit = "node 2 exited unexpectedly: exitCode=137 OOMKilled=true";
    harness.control().checksBeforeExit = 1;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("node 2 exited unexpectedly");
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString()
        .contains("fault=rolling").contains("FAILED kind=AVAILABILITY");
  }

  @Test
  void harnessErrorIsNotReportedAsAClusterBug() throws IOException {
    final Harness harness = harness(config());
    harness.control().failure = new IllegalStateException("docker daemon gone");
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.HARNESS);
    assertThat(result.message()).contains("docker daemon gone");
  }

  @Test
  void availabilityWindowIsNeverShorterThanAWriterTimeout() throws IOException {
    final Harness harness = harness(config("chaos.faults", "pause", "chaos.maxSteps", "1", "chaos.holdMin", "PT10S",
        "chaos.holdMax", "PT10S", "chaos.availabilityGrace", "PT5S"));
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    assertThat(sleeps).containsSubsequence(Duration.ofSeconds(5), ChaosRunner.MIN_AVAILABILITY_WINDOW);
  }
}
