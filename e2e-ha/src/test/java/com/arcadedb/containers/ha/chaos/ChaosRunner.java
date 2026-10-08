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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * The step loop: pick a fault, inject, hold under load (checking that acknowledged writes keep flowing when a majority
 * stays connected), heal, wait for a leader, then checkpoint with the load quiesced. The first violation ends the run.
 * Every random decision comes from one {@code Random(seed)}, so a seed replays the same fault sequence.
 */
public final class ChaosRunner {
  static final long     WARMUP_ACKS             = 20;
  /** A writer stuck on a paused node blocks for its 15 s read timeout; the window must outlast it. */
  static final Duration MIN_AVAILABILITY_WINDOW = Duration.ofSeconds(25);

  @FunctionalInterface
  public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;
  }

  @FunctionalInterface
  public interface TrendSource {
    TrendRow sample(int step, double acksPerSecond, long checkpointMs);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ChaosRunner.class);

  private final ChaosConfig      config;
  private final ClusterState     state;
  private final NodeControl      control;
  private final FaultPicker      picker;
  private final LoadGenerator    load;
  private final Ledger           ledger;
  private final Checkpoint       checkpoint;
  private final InvariantChecker checker;
  private final ChaosReport      report;
  private final TrendSource      trends;
  private final Sleeper          sleeper;
  private       long             lastSampleAcked;
  private       long             lastSampleNanos;

  public ChaosRunner(final ChaosConfig config, final ClusterState state, final NodeControl control, final FaultPicker picker,
      final LoadGenerator load, final Ledger ledger, final Checkpoint checkpoint, final InvariantChecker checker,
      final ChaosReport report, final TrendSource trends, final Sleeper sleeper) {
    this.config = config;
    this.state = state;
    this.control = control;
    this.picker = picker;
    this.load = load;
    this.ledger = ledger;
    this.checkpoint = checkpoint;
    this.checker = checker;
    this.report = report;
    this.trends = trends;
    this.sleeper = sleeper;
  }

  public ChaosResult run() {
    final Random random = new Random(config.seed());
    final long deadline = System.nanoTime() + config.duration().toNanos();
    int step = 0;
    try {
      load.start();
      warmup();
      lastSampleAcked = load.acked();
      lastSampleNanos = System.nanoTime();
      while (System.nanoTime() < deadline && (config.maxSteps() == 0 || step < config.maxSteps())) {
        ++step;
        final ChaosResult failed = step(step, random);
        if (failed != null)
          return finish(failed);
        sleeper.sleep(between(random, config.calmMin(), config.calmMax()));
      }
      load.close();
      final Checkpoint.Result last = checkpoint.run();
      if (!last.violations().isEmpty())
        return finish(fromViolations(last.violations(), step));
      return finish(new ChaosResult(ResultKind.PASS, "Completed " + step + " steps", step, List.of()));
    } catch (final ChaosFailure e) {
      return finish(single(e.kind(), e.getMessage(), step));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return finish(single(ResultKind.HARNESS, "Interrupted at step " + step, step));
    } catch (final Exception e) {
      LOGGER.error("Chaos harness error at step {}", step, e);
      return finish(single(ResultKind.HARNESS, e.getClass().getSimpleName() + ": " + e.getMessage(), step));
    } finally {
      load.close();
    }
  }

  private void warmup() throws InterruptedException {
    final long until = System.nanoTime() + config.electionTimeout().toNanos();
    while (load.acked() < WARMUP_ACKS) {
      if (System.nanoTime() > until)
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "Warmup: fewer than " + WARMUP_ACKS + " writes acknowledged within " + config.electionTimeout());
      sleeper.sleep(Duration.ofMillis(500));
    }
  }

  private ChaosResult step(final int step, final Random random) throws Exception {
    failIfANodeIsUnhealthy(step);
    final Fault fault = picker.pick(state, random);
    if (fault == null)
      throw new ChaosFailure(ResultKind.HARNESS, "No applicable fault for cluster state " + state);
    final int leaderBefore = control.findLeader();
    LOGGER.info("CHAOS step {} fault={} leader={} state={}", step, fault.name(), leaderBefore, state);

    // Snapshot before inject: a reformat during the hold or the catch-up counts against the step
    final NodeControl.InPlaceRestarts[] restartsBefore = fault.forbidsReformat() ? inPlaceRestarts() : null;
    String targets = "(inject failed)";
    try {
      try {
        targets = fault.inject(state, control, random);
      } catch (final Exception e) {
        // A node that crashed while the fault was being applied (typically during a minutes-long rolling restart)
        // makes Docker refuse the next command: report the crash, not the Docker error
        failIfANodeIsUnhealthy(step);
        throw e;
      }
      return holdHealAndCheck(step, fault, targets, leaderBefore, random, restartsBefore);
    } catch (final ChaosFailure e) {
      report.stepFailed(step, fault.name(), targets, single(e.kind(), e.getMessage(), step));
      throw e;
    } catch (final InterruptedException e) {
      report.stepFailed(step, fault.name(), targets, single(ResultKind.HARNESS, "Interrupted at step " + step, step));
      throw e;
    } catch (final Exception e) {
      report.stepFailed(step, fault.name(), targets,
          single(ResultKind.HARNESS, e.getClass().getSimpleName() + ": " + e.getMessage(), step));
      throw e;
    }
  }

  private ChaosResult holdHealAndCheck(final int step, final Fault fault, final String targets, final int leaderBefore,
      final Random random, final NodeControl.InPlaceRestarts[] restartsBefore) throws Exception {
    final Duration drawn = between(random, config.holdMin(), config.holdMax());
    final Duration hold = drawn.compareTo(fault.minHold()) < 0 ? fault.minHold() : drawn;
    long ackedDuringHold = -1;
    if (fault.expectsWritesAvailable()) {
      sleeper.sleep(config.availabilityGrace());
      final long ackedAtGrace = load.acked();
      final Duration rest = hold.minus(config.availabilityGrace());
      sleeper.sleep(rest.compareTo(MIN_AVAILABILITY_WINDOW) < 0 ? MIN_AVAILABILITY_WINDOW : rest);
      ackedDuringHold = load.acked() - ackedAtGrace;
      if (ackedDuringHold <= 0) {
        failIfANodeIsUnhealthy(step);
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "No write acknowledged during fault '" + fault.name() + "' " + targets + " after the " + config.availabilityGrace()
                + " election grace (step " + step + "). Nodes report: " + control.leaderView());
      }
    } else
      sleeper.sleep(hold);

    try {
      fault.heal(state, control);
    } catch (final Exception e) {
      failIfANodeIsUnhealthy(step);
      throw e;
    }
    final long healedAt = System.nanoTime();
    if (!control.awaitLeader(config.electionTimeout())) {
      failIfANodeIsUnhealthy(step);
      throw new ChaosFailure(ResultKind.AVAILABILITY,
          "No leader known by every node within " + config.electionTimeout() + " after healing fault '" + fault.name() + "' (step " + step
              + "). Nodes report: " + control.leaderView());
    }
    final long timeToLeaderMs = (System.nanoTime() - healedAt) / 1_000_000;
    final int leaderAfter = control.findLeader();

    load.quiesce();
    final Checkpoint.Result result;
    try {
      result = checkpoint.run();
    } finally {
      load.resume();
    }

    report.step(new StepRecord(step, fault.name(), targets, leaderBefore, leaderAfter, timeToLeaderMs,
        result.convergenceMillis(), ackedDuringHold, ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE),
        ledger.count(Ledger.UNKNOWN), ledger.count(Ledger.FAILED)));
    if (trends != null)
      report.trend(trends.sample(step, acksPerSecond(), result.durationMillis()));
    // Checked whatever the checkpoint found: a node that reformatted and then failed to converge is the case where the
    // reformat is the most useful diagnostic
    final Violation reformat;
    try {
      reformat = restartsBefore != null ? reformatViolation(step, fault, targets, restartsBefore) : null;
    } catch (final ChaosFailure e) {
      // The counts come from each node's HTTP status since issue #9429, so a node the checkpoint already found broken
      // may not answer: its violations are the finding, and must not be replaced by a harness error about the counts
      if (result.violations().isEmpty())
        throw e;
      LOGGER.warn("CHAOS step {}: in-place restart counts unavailable, reporting the checkpoint violations: {}", step,
          e.getMessage());
      return fromViolations(result.violations(), step);
    }
    if (reformat == null)
      return result.violations().isEmpty() ? null : fromViolations(result.violations(), step);
    final List<Violation> violations = new ArrayList<>(result.violations());
    violations.add(reformat);
    return fromViolations(violations, step);
  }

  private NodeControl.InPlaceRestarts[] inPlaceRestarts() {
    final NodeControl.InPlaceRestarts[] restarts = new NodeControl.InPlaceRestarts[state.size()];
    for (int i = 0; i < restarts.length; i++)
      restarts[i] = control.inPlaceRestarts(i);
    return restarts;
  }

  /**
   * Logs which recovery path each node took during the step and returns a SAFETY violation when any node reformatted
   * its Raft storage, which the fault gave it no reason to do (issue #8954). Every node is checked, not only the
   * fault's targets, on purpose: with the rest of the cluster healthy no node has a legitimate reason to discard its log,
   * so a leader or a bystander that reformats is as much a finding as the frozen follower.
   */
  private Violation reformatViolation(final int step, final Fault fault, final String targets,
      final NodeControl.InPlaceRestarts[] before) {
    final NodeControl.InPlaceRestarts[] after = inPlaceRestarts();
    final StringBuilder reformatted = new StringBuilder();
    for (int i = 0; i < after.length; i++) {
      final int recovered = after[i].recovered() - before[i].recovered();
      final int reformats = after[i].reformatted() - before[i].reformatted();
      if (recovered < 0 || reformats < 0)
        throw new ChaosFailure(ResultKind.HARNESS,
            "In-place restart count of node " + i + " went down during step " + step + " (" + before[i] + " -> " + after[i]
                + "): the counts restart with the server process, so it restarted and a reformat cannot be ruled out");
      if (recovered > 0 || reformats > 0)
        LOGGER.info("CHAOS step {} fault={}: node {} restarted Ratis in place {} time(s) keeping its storage, {} time(s) reformatting it",
            step, fault.name(), i, recovered, reformats);
      if (reformats > 0)
        reformatted.append(reformatted.isEmpty() ? "" : ", ").append("node ").append(i).append(" x").append(reformats);
    }
    if (reformatted.isEmpty())
      return null;
    return new Violation(ResultKind.SAFETY, "REFORMAT",
        "Raft storage reformatted during fault '" + fault.name() + "' " + targets + " (step " + step + "): " + reformatted
            + "; the node should have caught up, or been recovered in place keeping its log", new long[0]);
  }

  /**
   * A node the runner believes running that exited on its own (crash, OOM kill), or that logged an OutOfMemoryError
   * (the JVM usually keeps running, unresponsive), is an availability failure.
   */
  private void failIfANodeIsUnhealthy(final int step) {
    final String exit = control.unexpectedExit(state);
    if (exit != null)
      throw new ChaosFailure(ResultKind.AVAILABILITY, exit + " (step " + step + ")");
    final String outOfMemory = control.outOfMemory();
    if (outOfMemory != null)
      throw new ChaosFailure(ResultKind.AVAILABILITY, outOfMemory + " (step " + step + ")");
  }

  private double acksPerSecond() {
    final long acked = load.acked();
    final long now = System.nanoTime();
    final double seconds = Math.max((now - lastSampleNanos) / 1e9, 1e-3);
    final double rate = (acked - lastSampleAcked) / seconds;
    lastSampleAcked = acked;
    lastSampleNanos = now;
    return rate;
  }

  private static Duration between(final Random random, final Duration min, final Duration max) {
    final long span = max.toMillis() - min.toMillis();
    return min.plusMillis(span == 0 ? 0 : (long) (random.nextDouble() * span));
  }

  private static ChaosResult single(final ResultKind kind, final String message, final int step) {
    return new ChaosResult(kind, message, step, List.of(new Violation(kind, "RUNNER", message, new long[0])));
  }

  private static ChaosResult fromViolations(final List<Violation> violations, final int step) {
    final Violation first = violations.stream().filter(v -> v.kind() == ResultKind.SAFETY).findFirst()
        .orElse(violations.getFirst());
    final String message = first.describe() + (violations.size() > 1 ? " (+" + (violations.size() - 1) + " more)" : "");
    return new ChaosResult(first.kind(), message, step, violations);
  }

  private ChaosResult finish(final ChaosResult result) {
    try {
      if (!result.violations().isEmpty())
        report.ledgerDiff(result.violations());
      report.summary(result, ledger, checker.lateCommits());
    } catch (final IOException e) {
      LOGGER.error("Could not write the chaos report to {}", report.dir(), e);
    }
    LOGGER.info("CHAOS result {} after {} steps: {} | replay: {}", result.kind(), result.steps(), result.message(),
        config.replayCommand());
    return result;
  }
}
