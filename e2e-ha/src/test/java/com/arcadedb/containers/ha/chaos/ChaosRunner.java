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
    failIfANodeExited(step);
    final Fault fault = picker.pick(state, random);
    if (fault == null)
      throw new ChaosFailure(ResultKind.HARNESS, "No applicable fault for cluster state " + state);
    final int leaderBefore = control.findLeader();
    LOGGER.info("CHAOS step {} fault={} leader={} state={}", step, fault.name(), leaderBefore, state);

    String targets = "(inject failed)";
    try {
      try {
        targets = fault.inject(state, control, random);
      } catch (final Exception e) {
        // A node that crashed while the fault was being applied (typically during a minutes-long rolling restart)
        // makes Docker refuse the next command: report the crash, not the Docker error
        failIfANodeExited(step);
        throw e;
      }
      return holdHealAndCheck(step, fault, targets, leaderBefore, random);
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
      final Random random) throws Exception {
    final Duration hold = between(random, config.holdMin(), config.holdMax());
    long ackedDuringHold = -1;
    if (fault.expectsWritesAvailable()) {
      sleeper.sleep(config.availabilityGrace());
      final long ackedAtGrace = load.acked();
      final Duration rest = hold.minus(config.availabilityGrace());
      sleeper.sleep(rest.compareTo(MIN_AVAILABILITY_WINDOW) < 0 ? MIN_AVAILABILITY_WINDOW : rest);
      ackedDuringHold = load.acked() - ackedAtGrace;
      if (ackedDuringHold <= 0)
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "No write acknowledged during fault '" + fault.name() + "' " + targets + " after the " + config.availabilityGrace()
                + " election grace (step " + step + ")");
    } else
      sleeper.sleep(hold);

    try {
      fault.heal(state, control);
    } catch (final Exception e) {
      failIfANodeExited(step);
      throw e;
    }
    final long healedAt = System.nanoTime();
    if (!control.awaitLeader(config.electionTimeout())) {
      failIfANodeExited(step);
      throw new ChaosFailure(ResultKind.AVAILABILITY,
          "No leader known by every node within " + config.electionTimeout() + " after healing fault '" + fault.name() + "' (step " + step + ")");
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
    return result.violations().isEmpty() ? null : fromViolations(result.violations(), step);
  }

  /** A node the runner believes running that exited on its own (crash, OOM kill) is an availability failure. */
  private void failIfANodeExited(final int step) {
    final String exit = control.unexpectedExit(state);
    if (exit != null)
      throw new ChaosFailure(ResultKind.AVAILABILITY, exit + " (step " + step + ")");
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
