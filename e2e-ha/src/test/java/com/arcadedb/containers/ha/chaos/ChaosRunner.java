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
  /** How often the Raft indexes are re-read while waiting for a follower to catch up (issue #11378). */
  static final Duration CATCH_UP_POLL = Duration.ofSeconds(1);
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
  private       long             previousConvergenceMs = -1;
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
      report.facetWarnings(last.warnings());
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

    // Snapshot before inject: a reformat during the hold or the catch-up counts against the step. Taken for every fault, so a
    // reformat is visible in the report even where the fault may cause one (issue #10159); only a fault that forbids it needs the
    // counts to be readable
    final NodeControl.InPlaceRestarts[] restartsBefore = inPlaceRestarts(fault.forbidsReformat());
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
    // Writers stuck on a frozen node for their whole read timeout would make the acknowledgement count below a measure of their
    // luck, not of the cluster (issue #11377): they route around it until the fault is healed. A write already blocked when the
    // fault landed ends within its read timeout, which the availability grace is expected to outlast
    final long frozen = frozenNodes();
    if (frozen != 0)
      load.routeAround(frozen);
    try {
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
                  + " election grace (step " + step + "). Nodes report: " + control.leaderView() + ". " + lagView()
                  + previousConvergence());
        }
      } else
        sleeper.sleep(hold);
    } finally {
      if (frozen != 0)
        load.routeAround(0);
    }

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
    awaitFollowersCaughtUp(step, fault);

    load.quiesce();
    final Checkpoint.Result result;
    try {
      result = checkpoint.run();
    } finally {
      load.resume();
    }
    report.facetWarnings(result.warnings());

    // Read whatever the checkpoint found: a node that reformatted and then failed to converge is the case where the reformat is the
    // most useful diagnostic. Before the step is written, so that its line carries the count
    ReformatCensus census = ReformatCensus.NONE;
    ChaosFailure countsFailure = null;
    try {
      census = reformatCensus(step, fault, restartsBefore);
    } catch (final ChaosFailure e) {
      countsFailure = e;
      census = ReformatCensus.UNKNOWN;
    }
    previousConvergenceMs = result.convergenceMillis();
    report.step(new StepRecord(step, fault.name(), targets, leaderBefore, leaderAfter, timeToLeaderMs,
        result.convergenceMillis(), ackedDuringHold, ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE),
        ledger.count(Ledger.UNKNOWN), ledger.count(Ledger.FAILED), census.reformats()));
    if (trends != null)
      report.trend(trends.sample(step, acksPerSecond(), result.durationMillis()));
    if (countsFailure != null) {
      // The counts come from each node's HTTP status since issue #9429, so a node the checkpoint already found broken
      // may not answer: its violations are the finding, and must not be replaced by a harness error about the counts
      if (result.violations().isEmpty())
        throw countsFailure;
      LOGGER.warn("CHAOS step {}: in-place restart counts unavailable, reporting the checkpoint violations: {}", step,
          countsFailure.getMessage());
      return fromViolations(result.violations(), step);
    }
    // A reformat is a violation only where the fault gives no node a reason for one; elsewhere it is reported and the run goes on
    if (census.reformats() <= 0 || !fault.forbidsReformat())
      return result.violations().isEmpty() ? null : fromViolations(result.violations(), step);
    final List<Violation> violations = new ArrayList<>(result.violations());
    violations.add(new Violation(ResultKind.SAFETY, "REFORMAT",
        "Raft storage reformatted during fault '" + fault.name() + "' " + targets + " (step " + step + "): " + census.nodes()
            + "; the node should have caught up, or been recovered in place keeping its log", new long[0]));
    return fromViolations(violations, step);
  }

  /**
   * Waits until every live follower has applied to within {@code catchUpLag} entries of the leader's commit index (issue #11378).
   * A leader known by every node and equal row counts do not say how far behind a follower still is in Raft terms: the next fault
   * would land on a cluster whose only other voter is still catching up. A follower whose indexes cannot be read is never taken
   * for caught up.
   */
  private void awaitFollowersCaughtUp(final int step, final Fault fault) throws InterruptedException {
    Duration waited = Duration.ZERO;
    while (true) {
      final long readStart = System.nanoTime();
      final NodeControl.RaftPosition[] positions = control.raftPositions();
      // The read itself counts: a node that is slow to answer must not stretch the bound far past the configured timeout
      waited = waited.plusNanos(System.nanoTime() - readStart);
      final String behind = followersBehind(positions, control.findLeader());
      if (behind.isEmpty())
        return;
      if (waited.compareTo(config.catchUpTimeout()) >= 0)
        throw new ChaosFailure(ResultKind.AVAILABILITY,
            "A follower did not catch up within " + config.catchUpTimeout() + " after healing fault '" + fault.name() + "' (step "
                + step + "), more than " + config.catchUpLag() + " entries behind the leader: " + behind + ". "
                + lagView(positions, control.findLeader()) + previousConvergence());
      sleeper.sleep(CATCH_UP_POLL);
      waited = waited.plus(CATCH_UP_POLL);
    }
  }

  /**
   * The commit index the followers are measured against: the leader's, or the highest one reported when no leader is known (a
   * leader change between the election check and this read), or {@code null} when no node reports any.
   */
  private static NodeControl.RaftPosition head(final NodeControl.RaftPosition[] positions, final int leader) {
    if (leader >= 0 && leader < positions.length && positions[leader] != null)
      return positions[leader];
    NodeControl.RaftPosition highest = null;
    for (final NodeControl.RaftPosition position : positions)
      if (position != null && (highest == null || position.commit() > highest.commit()))
        highest = position;
    return highest;
  }

  /** The live followers further behind the head than allowed, as {@code node N lag=L}; empty when none is. */
  private String followersBehind(final NodeControl.RaftPosition[] positions, final int leader) {
    final NodeControl.RaftPosition head = head(positions, leader);
    final StringBuilder behind = new StringBuilder();
    for (int node = 0; node < config.nodes(); node++) {
      if (node == leader || state.state(node) != ClusterState.NodeState.UP)
        continue;
      final NodeControl.RaftPosition position = positions[node];
      final String description;
      if (position == null || head == null)
        description = "node " + node + " indexes unreadable";
      else if (head.commit() - position.applied() > config.catchUpLag())
        description = "node " + node + " lag=" + (head.commit() - position.applied());
      else
        continue;
      if (!behind.isEmpty())
        behind.append(", ");
      behind.append(description);
    }
    return behind.toString();
  }

  /** Every node's applied and commit index and its lag behind the head, for the failure messages. */
  private String lagView(final NodeControl.RaftPosition[] positions, final int leader) {
    final NodeControl.RaftPosition head = head(positions, leader);
    final StringBuilder view = new StringBuilder("Raft lag:");
    for (int node = 0; node < config.nodes(); node++) {
      final NodeControl.RaftPosition position = positions[node];
      view.append(" node ").append(node);
      if (position == null)
        view.append(" unreadable");
      else {
        view.append(" applied=").append(position.applied()).append(" commit=").append(position.commit());
        if (head != null)
          view.append(" lag=").append(head.commit() - position.applied());
      }
      if (node == leader)
        view.append(" (leader)");
      view.append(';');
    }
    return view.toString();
  }

  /** {@link #lagView(NodeControl.RaftPosition[], int)} read now, for a failure that has no positions at hand. */
  private String lagView() {
    try {
      return lagView(control.raftPositions(), control.findLeader());
    } catch (final RuntimeException e) {
      return "Raft lag unavailable: " + e.getMessage();
    }
  }

  private String previousConvergence() {
    return previousConvergenceMs < 0 ? "" : " Previous step converged in " + previousConvergenceMs + "ms.";
  }

  /** A bit per node the fault froze: a connection to it is accepted and never answered, unlike a stopped node's refusal. */
  private long frozenNodes() {
    long mask = 0;
    for (int i = 0; i < state.size(); i++)
      if (state.state(i) == ClusterState.NodeState.PAUSED)
        mask |= 1L << i;
    return mask;
  }

  /**
   * What the nodes did to their Raft storage during a step: {@code reformats} in all, {@code nodes} names them. UNKNOWN is a step whose
   * counts could not be read, which is not the same as none: its line says so.
   */
  private record ReformatCensus(int reformats, String nodes) {
    static final ReformatCensus NONE    = new ReformatCensus(0, "");
    static final ReformatCensus UNKNOWN = new ReformatCensus(StepRecord.REFORMATS_UNKNOWN, "");
  }

  /**
   * The in-place restart counts of every node, or null when they cannot be read and the caller can do without: a fault that may
   * reformat is observed, not judged, so counts that do not come must not fail its step. A fault that forbids a reformat needs
   * them, and a count that cannot be read never passes for zero (issue #9429).
   */
  private NodeControl.InPlaceRestarts[] inPlaceRestarts(final boolean required) {
    final NodeControl.InPlaceRestarts[] restarts = new NodeControl.InPlaceRestarts[state.size()];
    try {
      for (int i = 0; i < restarts.length; i++)
        restarts[i] = control.inPlaceRestarts(i);
    } catch (final ChaosFailure e) {
      if (required)
        throw e;
      LOGGER.warn("CHAOS: in-place restart counts unavailable, a reformat in this step will not be reported: {}", e.getMessage());
      return null;
    }
    return restarts;
  }

  /**
   * Logs which recovery path each node took during the step and counts the reformats of the Raft storage, which a fault that
   * {@link Fault#forbidsReformat() forbids them} gave no node a reason for (issue #8954). Every node is checked, not only the
   * fault's targets, on purpose: with the rest of the cluster healthy no node has a legitimate reason to discard its log, so a
   * leader or a bystander that reformats is as much a finding as the frozen follower.
   * <p>
   * The counts are kept by the server process. A node the fault restarted counts from zero again, so for it the count after the
   * step is the whole of what the step did; for any other node a count that went down is a restart nobody explains, and a reformat
   * cannot be ruled out (a harness failure where the fault forbids one, and where it does not the step is reported as unknown, since
   * what the node counts since its restart may hold the reformats of earlier steps).
   */
  private ReformatCensus reformatCensus(final int step, final Fault fault, final NodeControl.InPlaceRestarts[] before) {
    if (before == null)
      return ReformatCensus.UNKNOWN;
    final boolean forbidden = fault.forbidsReformat();
    final NodeControl.InPlaceRestarts[] after = inPlaceRestarts(forbidden);
    if (after == null)
      return ReformatCensus.UNKNOWN;

    final boolean[] restarted = new boolean[after.length];
    for (final int node : fault.restartedNodes(state))
      if (node >= 0 && node < restarted.length)
        restarted[node] = true;

    int total = 0;
    boolean unexplained = false;
    final StringBuilder reformatted = new StringBuilder();
    for (int i = 0; i < after.length; i++) {
      int recovered = after[i].recovered();
      int reformats = after[i].reformatted();
      if (!restarted[i]) {
        recovered -= before[i].recovered();
        reformats -= before[i].reformatted();
        if (recovered < 0 || reformats < 0) {
          final String went = "In-place restart count of node " + i + " went down during step " + step + " (" + before[i] + " -> "
              + after[i] + "): the counts restart with the server process, so it restarted and a reformat cannot be ruled out";
          if (forbidden)
            throw new ChaosFailure(ResultKind.HARNESS, went);
          LOGGER.warn("CHAOS step {} fault={}: {}", step, fault.name(), went);
          // The count since that restart may hold reformats of earlier steps, so it is not this step's: the step is not counted at all
          unexplained = true;
          continue;
        }
      }
      if (recovered > 0 || reformats > 0)
        LOGGER.info("CHAOS step {} fault={}: node {} restarted Ratis in place {} time(s) keeping its storage, {} time(s) reformatting it",
            step, fault.name(), i, recovered, reformats);
      if (reformats > 0) {
        total += reformats;
        reformatted.append(reformatted.isEmpty() ? "" : ", ").append("node ").append(i).append(" x").append(reformats);
      }
    }
    if (unexplained)
      return ReformatCensus.UNKNOWN;
    if (total > 0 && !forbidden)
      LOGGER.warn("CHAOS step {} fault={}: {} reformat(s) of the Raft storage ({}), which this fault may cause, so the step stands", step,
          fault.name(), total, reformatted);
    return total == 0 ? ReformatCensus.NONE : new ReformatCensus(total, reformatted.toString());
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
