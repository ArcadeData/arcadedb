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
    harness.control().leaderView = "node 2: unreachable on 127.0.0.1:32920 (SocketTimeoutException: Read timed out)";
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("kill").contains("node 2: unreachable on 127.0.0.1:32920");
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
  void nodeOutOfMemoryIsAnAvailabilityFailureNamingTheNode() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill"));
    harness.control().outOfMemory = "node 2 logged java.lang.OutOfMemoryError: Java heap space";
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("node 2 logged java.lang.OutOfMemoryError");
  }

  @Test
  void outOfMemoryExplainsAHoldWithoutAcknowledgedWrites() throws IOException {
    final Harness harness = harness(config("chaos.faults", "pause"));
    for (int i = 0; i < ChaosRunner.WARMUP_ACKS; i++)
      harness.ledger().record(harness.ledger().reserve(0, false), Ledger.ACKED);
    harness.load().progress = false;
    harness.control().outOfMemory = "node 2 logged java.lang.OutOfMemoryError: Java heap space";
    harness.control().checksBeforeOutOfMemory = 1;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(result.message()).contains("OutOfMemoryError");
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString().contains("OutOfMemoryError");
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

  @Test
  void longPauseHoldsTheFollowerFrozenLongerThanTheRatisCloseThreshold() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause", "chaos.maxSteps", "1", "chaos.holdMin", "PT10S",
        "chaos.holdMax", "PT10S", "chaos.availabilityGrace", "PT5S"));
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    final Duration rest = LongPauseFault.MIN_HOLD.minusSeconds(5);
    assertThat(sleeps).containsSubsequence(Duration.ofSeconds(5), rest);
    assertThat(Duration.ofSeconds(5).plus(rest)).isGreaterThan(Duration.ofSeconds(60));
    assertThat(harness.control().calls).filteredOn(call -> call.startsWith("pause:") || call.startsWith("unpause:"))
        .hasSize(2);
  }

  @Test
  void reformatAfterALongPauseIsASafetyFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    harness.control().reformatOnUnpause = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.steps()).isEqualTo(1);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("REFORMAT");
    assertThat(result.message()).contains("reformat").contains("longpause");
  }

  @Test
  void inPlaceRecoveryAfterALongPauseIsAccepted() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    harness.control().recoverOnUnpause = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.PASS);
    // one thawed follower per step, each recovered in place once
    assertThat(Arrays.stream(harness.control().recovered).sum()).isEqualTo(result.steps());
  }

  @Test
  void reformatIsReportedEvenWhenTheCheckpointAlsoFails() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    harness.control().reformatOnUnpause = true;
    harness.reader().dropKey = Ledger.key(0, 0);
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.steps()).isEqualTo(1);
    assertThat(result.violations()).extracting(Violation::invariant).contains("I1", "REFORMAT");
  }

  @Test
  void restartCountThatShrinksIsAHarnessFailureNotAPass() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    // the counts restart with the server process, so a process restart during the step could hide a reformat behind a
    // lower count (issue #9429)
    harness.control().reformatted[0] = 3;
    harness.control().reformatted[1] = 3;
    harness.control().reformatted[2] = 3;
    harness.control().processRestartsOnUnpause = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.HARNESS);
    assertThat(result.message()).contains("went down");
  }

  /** Issue #9429: counts that cannot be read never pass for zero. */
  @Test
  void unreadableRestartCountsAreAHarnessFailureNotAPass() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    harness.control().countsUnreadableAfterUnpause = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.HARNESS);
    assertThat(result.message()).contains("in-place restart counts");
  }

  /**
   * Issue #9429: the counts are read over HTTP, so a node the checkpoint found broken may not answer; the checkpoint's
   * violations are the finding and must not be replaced by a harness error about the counts.
   */
  @Test
  void unreadableRestartCountsDoNotHideACheckpointFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "longpause"));
    harness.control().countsUnreadableAfterUnpause = true;
    harness.reader().dropKey = Ledger.key(0, 0);
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.violations()).extracting(Violation::invariant).contains("I1");
  }

  /** Issue #10159: a plain pause is the long pause with a shorter hold, and a reformat after it is the same destructive recovery. */
  @Test
  void reformatAfterAPauseIsASafetyFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "pause"));
    harness.control().reformatOnUnpause = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("REFORMAT");
    assertThat(result.message()).contains("pause");
  }

  /** Issue #10159: a graceful stop and start leaves the other nodes healthy, so a node discarding its log on the way up is the finding. */
  @Test
  void reformatAfterAGracefulStopIsASafetyFailure() throws IOException {
    final Harness harness = harness(config("chaos.faults", "stop"));
    harness.control().reformatOnStart = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.steps()).isEqualTo(1);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("REFORMAT");
    assertThat(result.message()).contains("reformat").contains("stop");
  }

  @Test
  void reformatDuringARollingRestartIsASafetyFailureNamingTheNode() throws IOException {
    final Harness harness = harness(config("chaos.faults", "rolling"));
    harness.control().reformatOnStartOfNode = 1;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(result.violations().getFirst().invariant()).isEqualTo("REFORMAT");
    assertThat(result.message()).contains("rolling").contains("node 1 x1");
  }

  /**
   * The counts restart with the server process, so a node a graceful stop restarted reports fewer than it did before the step.
   * That is the restart the fault performed, not an unexplained one: the count since the restart is what the step did.
   */
  @Test
  void aProcessRestartTheFaultPerformedIsNotAHarnessFailure() throws IOException {
    for (final String fault : new String[] { "stop", "rolling" }) {
      final Harness harness = harness(config("chaos.faults", fault, "chaos.maxSteps", "2"));
      // reformats of earlier steps, still in the counters the restart is about to reset
      Arrays.fill(harness.control().reformatted, 3);
      final ChaosResult result = harness.runner().run();
      assertThat(result.kind()).as(fault).isEqualTo(ResultKind.PASS);
      assertThat(result.steps()).as(fault).isEqualTo(2);
    }
  }

  /** A node that did not restart keeps its count, so the reformats of earlier steps are not blamed on this one. */
  @Test
  void reformatsOfEarlierStepsAreNotBlamedOnAGracefulStopOfAnotherNode() throws IOException {
    final Harness harness = harness(config("chaos.faults", "stop", "chaos.maxSteps", "1"));
    for (int node = 0; node < 3; node++)
      harness.control().reformatted[node] = 2;
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
  }

  /**
   * Issue #10159: a fault that may reformat (a SIGKILL can leave a node genuinely diverged) does not fail the run, but the
   * step still says so: in steps.log and in the summary, so the decision about which faults may reformat is not also the
   * decision about whether anyone can see it.
   */
  @Test
  void aReformatAFaultMayCauseIsLoggedAndInTheSummaryButDoesNotFail() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill", "chaos.maxSteps", "2"));
    harness.control().reformatOnStart = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.PASS);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).hasSize(2).allSatisfy(line -> assertThat(line).contains("reformats=1"));
    assertThat(new JSONObject(Files.readString(dir.resolve("7/summary.json"))).getInt("reformats")).isEqualTo(2);
  }

  /**
   * A count that went down on a node the fault did not restart is a restart nobody explains. Where the fault may reformat that does not
   * fail the step, and it is not counted either: what the node has counted since holds the reformats of earlier steps too, so the step
   * says "?" instead of reporting them as its own.
   */
  @Test
  void anUnexplainedDropOfACountIsReportedAsUnknownWhereTheFaultMayReformat() throws IOException {
    final Harness harness = harness(config("chaos.faults", "isolate", "chaos.maxSteps", "1"));
    Arrays.fill(harness.control().reformatted, 3);
    harness.control().processRestartsOnReconnect = true;
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString().contains("reformats=?");
    final JSONObject summary = new JSONObject(Files.readString(dir.resolve("7/summary.json")));
    assertThat(summary.getInt("reformats")).isZero();
    assertThat(summary.getInt("stepsWithUnreadReformats")).isEqualTo(1);
  }

  @Test
  void aCleanStepReportsNoReformat() throws IOException {
    final Harness harness = harness(config("chaos.faults", "stop", "chaos.maxSteps", "1"));
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString().contains("reformats=0");
    final JSONObject summary = new JSONObject(Files.readString(dir.resolve("7/summary.json")));
    assertThat(summary.getInt("reformats")).isZero();
    assertThat(summary.getInt("stepsWithUnreadReformats")).isZero();
  }

  /**
   * Counts that cannot be read are an observation lost, not a failure, for a fault that is allowed to reformat. The step says so
   * ("?", and a count of such steps in the summary) rather than reading as a step in which no reformat happened.
   */
  @Test
  void unreadableRestartCountsDoNotFailAFaultThatMayReformat() throws IOException {
    final Harness harness = harness(config("chaos.faults", "kill", "chaos.maxSteps", "1"));
    harness.control().restartCountsUnreadable = true;
    assertThat(harness.runner().run().kind()).isEqualTo(ResultKind.PASS);
    assertThat(Files.readAllLines(dir.resolve("7/steps.log"))).singleElement().asString().contains("reformats=?");
    final JSONObject summary = new JSONObject(Files.readString(dir.resolve("7/summary.json")));
    assertThat(summary.getInt("reformats")).isZero();
    assertThat(summary.getInt("stepsWithUnreadReformats")).isEqualTo(1);
  }

  /** ... and still a harness failure for one that forbids it, which is what the counts are there to prove. */
  @Test
  void unreadableRestartCountsStillFailAFaultThatForbidsAReformat() throws IOException {
    final Harness harness = harness(config("chaos.faults", "stop", "chaos.maxSteps", "1"));
    harness.control().restartCountsUnreadable = true;
    final ChaosResult result = harness.runner().run();
    assertThat(result.kind()).isEqualTo(ResultKind.HARNESS);
    assertThat(result.message()).contains("in-place restart counts");
  }
}
