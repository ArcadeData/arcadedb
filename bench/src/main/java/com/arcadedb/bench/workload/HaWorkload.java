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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ClusterUnderTest;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.ha.ContentDigest;
import com.arcadedb.bench.workload.ha.LagMatcher;
import com.arcadedb.bench.workload.ha.WriteLedger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * A 3-node Raft cluster of the image under test ({@link ClusterUnderTest}, each member on its own slice of the server cpuset),
 * started by the workload itself once per case: write throughput through the leader with 1 and 8 closed-loop writers, and the
 * replication lag to the two followers of one writer at a scheduled 100 writes/s. All three nodes share one host and one Docker
 * network, so the numbers track the engine's replication cost, not a real network.
 * <p>
 * One write is one transaction (one {@code INSERT} command) of one {@code HaRecord {id, writer, payload}} on the leader, {@code id}
 * unique and indexed. A repetition runs for a fixed time (15 s at XS and S, 45 s at M and L). A write that throws makes its id
 * <em>unknown</em> (it may have landed), counted in {@code unknown_writes} and never a failure by itself; after 3 failures in a
 * row the writer asks the cluster for the leader again and goes on against it, every further 3 in a row asking again; a leader
 * other than the last one found is counted in {@code leader_changes} (unit {@code changes}, informational: the gate never judges
 * it), since the bench logs nowhere.
 * <p>
 * The lag of a write is when a follower poll first showed it minus when the leader acknowledged it. A poll is stamped with the
 * time it was sent, not the time it answered: the ids it shows were visible on the follower by the time the follower read them,
 * after the previous poll's read, so the send time is the closest bound the client has, and the response leg stays out. The
 * pollers pause 1 ms between polls so they do not hammer the follower they measure; the effective resolution is one poll cycle
 * (the 1 ms pause plus one round trip of the {@code max(id)} query), and a write shown before the leader's answer reached the
 * client counts as 0.
 * <p>
 * After each repetition, outside its timed window, the followers get up to 60 s to reach the leader's record count, then every
 * acknowledged id of the repetition is looked up on every node, and every type of the database is digested on every node in
 * {@code @rid} order ({@link ContentDigest}). An acknowledged id missing on a node, or a node whose digest differs from the
 * leader's, fails the case with the repetition's metrics; so does a member that died, named with the reason.
 */
public class HaWorkload implements Workload {
  static final String   THROUGHPUT_1W           = "write-throughput-1w";
  static final String   THROUGHPUT_8W           = "write-throughput-8w";
  static final String   LAG                     = "replication-lag";
  static final String   DATABASE                = "ha";
  static final String   TYPE                    = "HaRecord";
  static final int      NODES                   = 3;
  static final int      PAYLOAD_CHARS           = 200;
  static final int      FAILURES_BEFORE_REFRESH = 3;
  static final int      LAG_WRITES_PER_S        = 100;
  static final Duration MEMBER_WAIT             = Duration.ofSeconds(60);
  static final Duration CATCH_UP_WAIT           = Duration.ofSeconds(60);
  /** How long a repetition, or the checks after it, wait for the cluster to name a leader (an election in progress). */
  static final Duration LEADER_WAIT             = Duration.ofSeconds(60);
  /** After the lag writer stops, how long a poller keeps looking for the last acknowledged id on its follower. */
  static final Duration LAG_DRAIN               = Duration.ofSeconds(30);
  static final String   WRITES                  = "writes";
  /** The unit of {@code leader_changes}: informational, never judged by the gate. */
  static final String   CHANGES                 = "changes";

  private static final Logger LOGGER = LoggerFactory.getLogger(HaWorkload.class);

  private static final int    MISSING_SHOWN      = 10;
  private static final long   POLL_PAUSE_NANOS   = 1_000_000L;
  private static final long   COUNT_PAUSE_MS     = 250L;
  private static final double NANOS_PER_MS       = 1_000_000.0;
  private static final String PAYLOAD            = "p".repeat(PAYLOAD_CHARS);

  /** The cluster as the workload uses it; the seam that lets a test script a leader change, a lost write or a diverging node. */
  interface Cluster extends AutoCloseable {
    int nodes();

    /** The index of the leader as last known; never -1 once a leader was known. */
    int leader();

    /**
     * Asks the members who leads now; -1 when none answers as leader (an election in progress). A -1 never replaces the leader
     * last known, so {@link #leader()} stays a member's index.
     */
    int refreshLeader();

    /** Null while every member is alive, else {@code ha-<i>: <reason>}. */
    String deathReason();

    JSONObject config();

    JSONObject imageInfo();

    /** A connection of one writer to the current leader. */
    Writer writer();

    /** A connection reading the highest id on member {@code node}. */
    Poller poller(int node);

    long count(int node);

    /** The ids in {@code [from, to)} on member {@code node}, ascending. */
    long[] ids(int node, long from, long to);

    /** The types of the database. */
    List<String> types();

    /** {@link ContentDigest} of {@code type} on member {@code node}. */
    String digest(int node, String type);

    @Override
    void close();
  }

  /** One writer's connection to the leader. Used by one thread. */
  interface Writer extends AutoCloseable {
    /** One transaction inserting one record; throws when it was not acknowledged. */
    void insert(long id, int writer) throws Exception;

    /** Asks the cluster who leads now and moves this connection there; returns the leader's index, -1 for none. */
    int refreshLeader();

    @Override
    void close();
  }

  /** One follower's connection reading the highest id it shows. Used by one thread. */
  interface Poller extends AutoCloseable {
    /** The highest id the member shows, -1 for none. */
    long maxId() throws Exception;

    @Override
    void close();
  }

  @FunctionalInterface
  interface ClusterStarter {
    Cluster start(WorkloadContext ctx) throws Exception;
  }

  // null = the Raft cluster of the run's image
  private final ClusterStarter starter;
  // null = the spec's length for the scale
  private final Duration       repOverride;
  private final Duration       leaderWait;
  private final Duration       lagDrain;
  private final AtomicLong     nextId = new AtomicLong();
  private Cluster              cluster;
  private volatile JSONObject  config;
  private volatile JSONObject  imageInfo;

  public HaWorkload() {
    this(null, null);
  }

  /** A scripted cluster and shorter repetitions, for the tests and the IT; the spec's values come from the public constructor. */
  HaWorkload(final ClusterStarter starter, final Duration repLength) {
    this(starter, repLength, LEADER_WAIT);
  }

  /** As {@link #HaWorkload(ClusterStarter, Duration)}, with a shorter wait for a leader, so a test of a cluster that elects none ends. */
  HaWorkload(final ClusterStarter starter, final Duration repLength, final Duration leaderWait) {
    this(starter, repLength, leaderWait, LAG_DRAIN);
  }

  /** As {@link #HaWorkload(ClusterStarter, Duration, Duration)}, with a shorter drain, so a test of followers never seen ends. */
  HaWorkload(final ClusterStarter starter, final Duration repLength, final Duration leaderWait, final Duration lagDrain) {
    this.starter = starter;
    this.repOverride = repLength;
    this.leaderWait = leaderWait;
    this.lagDrain = lagDrain;
  }

  @Override
  public String name() {
    return "ha";
  }

  @Override
  public List<String> cases() {
    return List.of(THROUGHPUT_1W, THROUGHPUT_8W, LAG);
  }

  @Override
  public boolean ownsServers() {
    return true;
  }

  @Override
  public JSONObject serverConfig() {
    return config;
  }

  @Override
  public JSONObject imageInfo() {
    return imageInfo;
  }

  /** 15 s at XS and S, 45 s at M and L, unless the override says otherwise. */
  Duration repLength(final Scale scale) {
    if (repOverride != null)
      return repOverride;
    return switch (scale) {
      case XS, S -> Duration.ofSeconds(15);
      case M, L -> Duration.ofSeconds(45);
    };
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) throws Exception {
    if (!cases().contains(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    if (ctx.spec() == null)
      throw new IllegalStateException("the ha workload starts its own cluster and needs the run's server spec");
    closeCluster();
    nextId.set(0);
    cluster = starter != null ? starter.start(ctx) : RaftCluster.start(ctx.spec(), serverSettings());
    config = cluster.config();
    if (imageInfo == null)
      try {
        imageInfo = cluster.imageInfo();
      } catch (final RuntimeException e) {
        LOGGER.warn("Cannot read the image of the cluster: {}", e.toString());
      }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final Cluster c = cluster;
    if (c == null)
      throw new IllegalStateException("no cluster: prepare did not run");
    final Duration length = repLength(ctx.scale());
    // the leader as the members name it now: one that changed between repetitions is not written to as a follower, and a
    // repetition never starts against index -1
    final int leader = awaitLeader(c, leaderWait);
    if (leader < 0)
      throw new IllegalStateException("no Raft leader elected within " + leaderWait.toSeconds() + " s before the repetition");
    final long from = nextId.get();
    final LeaderChanges changes = new LeaderChanges(leader);
    final WriteLedger ledger;
    final List<Metric> metrics;
    final Measurement measurement;
    // empty when no follower showed an acknowledged write; null for a throughput case
    LatencyRecorder lags = null;
    if (LAG.equals(caseName)) {
      final LagRep rep = lagRep(c, leader, length, changes);
      ledger = rep.ledger();
      lags = rep.lags();
      metrics = lagMetrics(lags, ledger, changes);
      measurement = new Measurement(metrics);
    } else {
      final ThroughputRep rep = throughputRep(c, THROUGHPUT_8W.equals(caseName) ? 8 : 1, length, changes);
      ledger = rep.ledger();
      metrics = throughputMetrics(rep, ledger, changes);
      measurement = Measurement.of(metrics, rep.latencies());
    }
    final String failure = check(c, ledger, from, nextId.get(), leaderWait);
    if (failure != null)
      throw new CaseFailedWithMetrics(failure, metrics);
    // a rate, a latency and a lag of 0 read as better than any real repetition: a repetition that measured nothing fails
    if (ledger.ackedCount() == 0)
      throw new CaseFailedWithMetrics("no write was acknowledged in the repetition (" + ledger.unknownCount() + " unknown)", metrics);
    if (lags != null && lags.count() == 0)
      throw new CaseFailedWithMetrics("no follower showed an acknowledged write of the repetition (" + ledger.ackedCount()
          + " acknowledged): no lag was measured", metrics);
    return measurement;
  }

  @Override
  public void teardown(final WorkloadContext ctx) {
    closeCluster();
  }

  private void closeCluster() {
    if (cluster != null) {
      cluster.close();
      cluster = null;
    }
  }

  /**
   * The failure of a repetition, null for none: one line per node missing acknowledged ids (how many, the first 10), then one
   * per node and type whose digest is not the leader's, joined with {@code "; "}.
   *
   * @param idsByNode            the ids of the repetition on each node, ascending
   * @param digestsByTypeAndNode the digest of {@code types[t]} on node {@code n} at {@code [t][n]}
   */
  static String verify(final WriteLedger ledger, final long[][] idsByNode, final String[] types, final String[][] digestsByTypeAndNode,
      final int leader) {
    final List<String> failures = new ArrayList<>();
    for (int n = 0; n < idsByNode.length; n++) {
      final long[] missing = ledger.missingFrom(idsByNode[n]);
      if (missing.length > 0)
        failures.add("ha-" + n + " is missing " + missing.length + " acknowledged writes, first ids "
            + Arrays.toString(Arrays.copyOf(missing, Math.min(MISSING_SHOWN, missing.length))));
    }
    for (int t = 0; t < types.length; t++)
      failures.addAll(ContentDigest.differences(types[t], digestsByTypeAndNode[t], leader));
    return failures.isEmpty() ? null : String.join("; ", failures);
  }

  /**
   * The checks after a repetition, outside its timed window; the failure message or null. A read that throws is a failure
   * naming the read and the member, or the death of a member when one died: never an exception that loses the metrics.
   */
  private static String check(final Cluster c, final WriteLedger ledger, final long from, final long to, final Duration leaderWait) {
    // the leader first: an election in progress is waited for, never read as a death; a member that died explains a leader
    // that never came better than the wait does
    final int leader = awaitLeader(c, leaderWait);
    String death = c.deathReason();
    if (death != null)
      return "a cluster member died: " + death;
    if (leader < 0)
      return "no Raft leader elected within " + leaderWait.toSeconds() + " s after the repetition, nothing could be checked";
    final int nodes = c.nodes();
    String reading = "the record count on ha-" + leader;
    try {
      final long leaderCount = c.count(leader);
      for (int n = 0; n < nodes; n++)
        if (n != leader)
          awaitCount(c, n, leaderCount);
      final long[][] ids = new long[nodes][];
      for (int n = 0; n < nodes; n++) {
        reading = "the ids " + from + ".." + (to - 1) + " on ha-" + n;
        ids[n] = c.ids(n, from, to);
      }
      reading = "the types on ha-" + leader;
      final String[] types = c.types().toArray(new String[0]);
      final String[][] digests = new String[types.length][nodes];
      for (int t = 0; t < types.length; t++)
        for (int n = 0; n < nodes; n++) {
          reading = "the digest of " + types[t] + " on ha-" + n;
          digests[t][n] = c.digest(n, types[t]);
        }
      final String failure = verify(ledger, ids, types, digests, leader);
      // a member that died during the checks explains them better than what it no longer answered
      death = c.deathReason();
      return death != null ? "a cluster member died: " + death : failure;
    } catch (final RuntimeException e) {
      death = c.deathReason();
      if (death != null)
        return "a cluster member died: " + death;
      LOGGER.warn("The check could not read {}", reading, e);
      return "the check could not read " + reading + ": " + e;
    }
  }

  /**
   * The leader as the members name it, asking every 250 ms for up to {@code wait} while none does (an election in progress);
   * -1 when none was named in time.
   */
  static int awaitLeader(final Cluster c, final Duration wait) {
    final long deadline = System.nanoTime() + wait.toNanos();
    while (true) {
      final int leader = c.refreshLeader();
      if (leader >= 0)
        return leader;
      if (System.nanoTime() >= deadline)
        return -1;
      try {
        Thread.sleep(COUNT_PAUSE_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return -1;
      }
    }
  }

  /** Waits up to {@link #CATCH_UP_WAIT} for member {@code node} to hold {@code target} records; the checks then say what is missing. */
  private static void awaitCount(final Cluster c, final int node, final long target) {
    final long deadline = System.nanoTime() + CATCH_UP_WAIT.toNanos();
    long count = -1;
    while (true) {
      try {
        count = c.count(node);
      } catch (final RuntimeException e) {
        LOGGER.debug("ha-{} did not answer its count: {}", node, e.toString());
      }
      if (count >= target)
        return;
      if (System.nanoTime() >= deadline) {
        LOGGER.warn("ha-{} holds {} of the leader's {} records after {} s", node, count, target, CATCH_UP_WAIT.toSeconds());
        return;
      }
      try {
        Thread.sleep(COUNT_PAUSE_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private static List<Metric> throughputMetrics(final ThroughputRep rep, final WriteLedger ledger, final LeaderChanges changes) {
    final List<Metric> metrics = new ArrayList<>(6);
    metrics.add(Metrics.rate("records_per_s", "records/s", rep.ackedInWindow() / (rep.lengthNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(rep.latencies()));
    metrics.add(unknownWrites(ledger));
    metrics.add(leaderChanges(changes));
    return metrics;
  }

  private static List<Metric> lagMetrics(final LatencyRecorder lags, final WriteLedger ledger, final LeaderChanges changes) {
    return List.of(Metrics.of("lag_p50_ms", "ms", Metric.Better.LOWER, lags.percentile(50) / NANOS_PER_MS),
        Metrics.of("lag_p95_ms", "ms", Metric.Better.LOWER, lags.percentile(95) / NANOS_PER_MS),
        Metrics.of("lag_max_ms", "ms", Metric.Better.LOWER, lags.percentile(100) / NANOS_PER_MS), unknownWrites(ledger),
        leaderChanges(changes));
  }

  /**
   * How many times a writer's ask found a leader other than the one before, in unit {@value #CHANGES}, which the gate records and
   * never judges. The bench logs nowhere, so this is the only trace a failover leaves besides {@code unknown_writes}.
   */
  private static Metric leaderChanges(final LeaderChanges changes) {
    return Metrics.of("leader_changes", CHANGES, Metric.Better.LOWER, changes.count());
  }

  private static Metric unknownWrites(final WriteLedger ledger) {
    return Metrics.of("unknown_writes", WRITES, Metric.Better.LOWER, ledger.unknownCount());
  }

  /**
   * @param ackedInWindow the writes acknowledged by the end of the repetition: a write in flight at the end is still checked on
   *                      every node, but its answer can take up to the write timeout and is no throughput of the repetition
   */
  private record ThroughputRep(WriteLedger ledger, LatencyRecorder latencies, long ackedInWindow, long lengthNanos) {
  }

  private record LagRep(WriteLedger ledger, LatencyRecorder lags) {
  }

  /** {@code writers} closed-loop writers for {@code length}, each on its own connection, opened before the timed window. */
  private ThroughputRep throughputRep(final Cluster c, final int writers, final Duration length, final LeaderChanges changes)
      throws InterruptedException {
    final WriterLoop[] loops = new WriterLoop[writers];
    final Thread[] threads = new Thread[writers];
    try {
      for (int i = 0; i < writers; i++)
        loops[i] = new WriterLoop(c.writer(), i, 1024, changes);
      final long start = System.nanoTime();
      final long end = start + length.toNanos();
      for (int i = 0; i < writers; i++) {
        final WriterLoop loop = loops[i];
        threads[i] = Thread.ofPlatform().name("ha-writer-" + i).daemon(true).start(() -> {
          while (System.nanoTime() < end && !Thread.currentThread().isInterrupted()) {
            final long ack = loop.write(nextId.getAndIncrement());
            if (ack != LagMatcher.NOT_ACKED && ack - end <= 0)
              loop.ackedInWindow++;
          }
        });
      }
      joinAll(threads);
      final WriteLedger ledger = new WriteLedger(1024);
      final LatencyRecorder latencies = new LatencyRecorder(1024);
      long ackedInWindow = 0;
      for (final WriterLoop loop : loops) {
        ledger.addAll(loop.ledger);
        latencies.addAll(loop.latencies);
        ackedInWindow += loop.ackedInWindow;
      }
      return new ThroughputRep(ledger, latencies, ackedInWindow, end - start);
    } finally {
      for (final WriterLoop loop : loops)
        if (loop != null)
          loop.target.close();
    }
  }

  /**
   * One writer at {@value #LAG_WRITES_PER_S} writes/s on a fixed schedule for {@code length}, and one poller per follower reading
   * the highest id it shows. A poller goes on after the writer stops until its follower shows the last acknowledged id, for at
   * most {@link #LAG_DRAIN}: an id it never sees has no lag, and the ledger check reports it if it was acknowledged.
   */
  private LagRep lagRep(final Cluster c, final int leader, final Duration length, final LeaderChanges changes)
      throws InterruptedException {
    final long periodNanos = 1_000_000_000L / LAG_WRITES_PER_S;
    final int arrivals = (int) Math.max(1, (length.toNanos() + periodNanos - 1) / periodNanos);
    final long[] acks = new long[arrivals];
    Arrays.fill(acks, LagMatcher.NOT_ACKED);
    final int nodes = c.nodes();
    final LagMatcher[] matchers = new LagMatcher[nodes - 1];
    final Poller[] pollers = new Poller[nodes - 1];
    final Thread[] threads = new Thread[nodes];
    WriterLoop loop = null;
    try {
      for (int n = 0, f = 0; n < nodes; n++)
        if (n != leader) {
          pollers[f] = c.poller(n);
          matchers[f++] = new LagMatcher(acks);
        }
      loop = new WriterLoop(c.writer(), 0, arrivals, changes);
      // a single writer takes consecutive ids, so the id's position in the acknowledgement array is its offset from the first
      final long base = nextId.get();
      final AtomicBoolean writing = new AtomicBoolean(true);
      final AtomicLong lastAcked = new AtomicLong(-1);
      final WriterLoop writer = loop;
      final long start = System.nanoTime();
      threads[0] = Thread.ofPlatform().name("ha-lag-writer").daemon(true).start(() -> {
        try {
          for (int i = 0; i < arrivals && !Thread.currentThread().isInterrupted(); i++) {
            final long at = start + i * periodNanos;
            for (long wait = at - System.nanoTime(); wait > 0; wait = at - System.nanoTime())
              LockSupport.parkNanos(wait);
            final long ack = writer.write(nextId.getAndIncrement());
            if (ack != LagMatcher.NOT_ACKED) {
              acks[i] = ack;
              lastAcked.set(i);
            }
          }
        } finally {
          writing.set(false);
        }
      });
      for (int f = 0; f < pollers.length; f++) {
        final Poller poller = pollers[f];
        final LagMatcher matcher = matchers[f];
        threads[f + 1] = Thread.ofPlatform().name("ha-lag-poller-" + f).daemon(true)
            .start(() -> poll(poller, matcher, base, writing, lastAcked, lagDrain, System::nanoTime));
      }
      joinAll(threads);
      final LatencyRecorder lags = new LatencyRecorder(arrivals * matchers.length);
      for (final LagMatcher matcher : matchers)
        lags.addAll(matcher.lags());
      return new LagRep(loop.ledger, lags);
    } finally {
      if (loop != null)
        loop.target.close();
      for (final Poller poller : pollers)
        if (poller != null)
          poller.close();
    }
  }

  static void poll(final Poller poller, final LagMatcher matcher, final long base, final AtomicBoolean writing,
      final AtomicLong lastAcked, final Duration drain, final LongSupplier clock) {
    long drainDeadline = 0;
    boolean warned = false;
    while (!Thread.currentThread().isInterrupted()) {
      // read before the poll, so the poll that sees the writer done comes after its last write
      final boolean stillWriting = writing.get();
      // the ids the poll shows were visible when it was sent at the latest after the previous poll: stamping the send rather
      // than the answer keeps the response leg out of the lag
      final long sent = clock.getAsLong();
      long max = -1;
      try {
        max = poller.maxId();
      } catch (final Exception e) {
        if (!warned) {
          LOGGER.debug("Lag poll failed, polling on: {}", e.toString());
          warned = true;
        }
      }
      final long now = clock.getAsLong();
      if (max >= base)
        matcher.seen(max - base, sent);
      if (!stillWriting) {
        if (drainDeadline == 0)
          drainDeadline = now + drain.toNanos();
        if (max - base >= lastAcked.get() || now - drainDeadline >= 0)
          return;
      }
      LockSupport.parkNanos(POLL_PAUSE_NANOS);
    }
  }

  private static void joinAll(final Thread[] threads) throws InterruptedException {
    try {
      for (final Thread t : threads)
        if (t != null)
          t.join();
    } catch (final InterruptedException e) {
      for (final Thread t : threads)
        if (t != null)
          t.interrupt();
      throw e;
    }
  }

  /**
   * One writer: its connection, its ledger and the latencies of its acknowledged writes. A write that throws is unknown; after
   * every {@value #FAILURES_BEFORE_REFRESH} in a row the writer asks for the leader again and goes on, a leader other than the
   * last one found counted in the repetition's {@link LeaderChanges}.
   */
  static final class WriterLoop {
    final Writer          target;
    final WriteLedger     ledger;
    final LatencyRecorder latencies;
    private final int     index;
    private final LeaderChanges changes;
    private int           failures;
    // written by the writer's thread only, read after it is joined
    long                  ackedInWindow;

    WriterLoop(final Writer target, final int index, final int capacityHint, final LeaderChanges changes) {
      this.target = target;
      this.index = index;
      this.changes = changes;
      this.ledger = new WriteLedger(capacityHint);
      this.latencies = new LatencyRecorder(capacityHint);
    }

    /** Writes {@code id}; returns when it was acknowledged ({@link System#nanoTime()}), {@link LagMatcher#NOT_ACKED} when it threw. */
    long write(final long id) {
      final long start = System.nanoTime();
      try {
        target.insert(id, index);
      } catch (final Exception e) {
        ledger.unknown(id);
        if (++failures == FAILURES_BEFORE_REFRESH) {
          failures = 0;
          final int leader = target.refreshLeader();
          changes.found(leader);
          LOGGER.warn("Writer {}: {} writes in a row failed (last: {}), asked for the leader again: {}", index,
              FAILURES_BEFORE_REFRESH, e.toString(), leader < 0 ? "none elected" : "ha-" + leader);
        }
        return LagMatcher.NOT_ACKED;
      }
      final long ack = System.nanoTime();
      failures = 0;
      ledger.acked(id);
      latencies.record(ack - start);
      return ack;
    }
  }

  /**
   * The leader changes of one repetition: the leader it started against, and how many times an ask of any of its writers found
   * a different one. Asks are rare (one per {@value #FAILURES_BEFORE_REFRESH} failed writes), so a lock costs nothing; with 8
   * writers asking after the same failover, the first counts the change and the others find the leader already known.
   */
  static final class LeaderChanges {
    private int leader;
    private int count;

    LeaderChanges(final int leader) {
      this.leader = leader;
    }

    /** A leader an ask found; -1 (none elected) changes nothing. */
    synchronized void found(final int found) {
      if (found < 0 || found == leader)
        return;
      leader = found;
      count++;
    }

    synchronized int count() {
      return count;
    }
  }

  /** The Raft cluster of the run's image, read and written over HTTP. */
  static final class RaftCluster implements Cluster {
    private static final int    PAGE             = 20_000;
    private static final int    WRITE_TIMEOUT_MS = 10_000;
    private static final String INSERT           = "INSERT INTO " + TYPE + " SET id = ?, writer = ?, payload = ?";

    private final ClusterUnderTest      cluster;
    private final List<ServerUnderTest> members;
    // one per member, for the checks, which run on the measuring thread only
    private final RemoteDatabase[]      readers;
    // the leader last named; ClusterUnderTest forgets it on an ask that finds none, this one keeps it
    private volatile int                leader;

    private RaftCluster(final ClusterUnderTest cluster) {
      this.cluster = cluster;
      this.members = cluster.members();
      this.readers = new RemoteDatabase[members.size()];
      this.leader = cluster.leaderIndex();
    }

    /** Starts the cluster, creates the database on the leader, waits for it on every member, then creates the type. */
    static Cluster start(final WorkloadContext.ServerSpec spec, final Map<String, String> settings) throws InterruptedException {
      final ClusterUnderTest started = ClusterUnderTest.start(spec.image(), spec.serverCpus(), spec.memLimitGb(), NODES, settings);
      final RaftCluster raft = new RaftCluster(started);
      try {
        raft.createDatabase();
        return raft;
      } catch (final Throwable e) {
        raft.close();
        throw e;
      }
    }

    private void createDatabase() throws InterruptedException {
      RemoteClients.ensureDatabase(members.get(leader), DATABASE);
      final long deadline = System.nanoTime() + MEMBER_WAIT.toNanos();
      for (int n = 0; n < members.size(); n++) {
        while (!exists(n)) {
          if (System.nanoTime() >= deadline)
            throw new IllegalStateException("database " + DATABASE + " is not on ha-" + n + " after " + MEMBER_WAIT.toSeconds() + " s");
          Thread.sleep(COUNT_PAUSE_MS);
        }
        readers[n] = RemoteClients.open(members.get(n), DATABASE);
      }
      final RemoteDatabase db = readers[leader];
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS").close();
      db.command("sql", "CREATE PROPERTY " + TYPE + ".id IF NOT EXISTS LONG").close();
      db.command("sql", "CREATE PROPERTY " + TYPE + ".writer IF NOT EXISTS INTEGER").close();
      db.command("sql", "CREATE PROPERTY " + TYPE + ".payload IF NOT EXISTS STRING").close();
      db.command("sql", "CREATE INDEX IF NOT EXISTS ON " + TYPE + " (id) UNIQUE").close();
    }

    private boolean exists(final int node) {
      try {
        return RemoteClients.databaseExists(members.get(node), DATABASE);
      } catch (final RuntimeException e) {
        return false;
      }
    }

    @Override
    public int nodes() {
      return members.size();
    }

    @Override
    public int leader() {
      return leader;
    }

    @Override
    public int refreshLeader() {
      final int found;
      try {
        found = cluster.refreshLeader();
      } catch (final RuntimeException e) {
        return -1;
      }
      if (found >= 0)
        leader = found;
      return found;
    }

    @Override
    public String deathReason() {
      return cluster.deathReason();
    }

    @Override
    public JSONObject config() {
      return cluster.config();
    }

    @Override
    public JSONObject imageInfo() {
      return members.getFirst().imageInfo();
    }

    @Override
    public Writer writer() {
      return new Writer() {
        private int            node = leader;
        private RemoteDatabase db   = openWriter(node);

        @Override
        public void insert(final long id, final int writer) {
          db.command("sql", INSERT, id, writer, PAYLOAD).close();
        }

        @Override
        public int refreshLeader() {
          // no leader named (an election in progress): the connection stays where it is until one is
          final int found = RaftCluster.this.refreshLeader();
          if (found >= 0 && found != node) {
            db.close();
            node = found;
            db = openWriter(found);
          }
          return found;
        }

        @Override
        public void close() {
          db.close();
        }
      };
    }

    private RemoteDatabase openWriter(final int node) {
      final RemoteDatabase db = RemoteClients.open(members.get(node), DATABASE);
      // a write the leader does not answer is unknown after this, rather than holding the writer past the end of the repetition
      db.setTimeout(WRITE_TIMEOUT_MS);
      return db;
    }

    @Override
    public Poller poller(final int node) {
      final RemoteDatabase db = RemoteClients.open(members.get(node), DATABASE);
      return new Poller() {
        @Override
        public long maxId() {
          try (final ResultSet rs = db.query("sql", "SELECT max(id) AS n FROM " + TYPE)) {
            if (!rs.hasNext())
              return -1;
            final Object n = rs.next().getProperty("n");
            return n instanceof Number number ? number.longValue() : -1;
          }
        }

        @Override
        public void close() {
          db.close();
        }
      };
    }

    @Override
    public long count(final int node) {
      return RemoteClients.count(readers[node], "sql", "SELECT count(*) AS n FROM " + TYPE);
    }

    @Override
    public long[] ids(final int node, final long from, final long to) {
      long[] ids = new long[1024];
      int count = 0;
      long cursor = from;
      while (true) {
        int page = 0;
        try (final ResultSet rs = readers[node].query("sql",
            "SELECT id FROM " + TYPE + " WHERE id >= :from AND id < :to ORDER BY id LIMIT " + PAGE,
            Map.<String, Object>of("from", cursor, "to", to))) {
          while (rs.hasNext()) {
            final long id = ((Number) rs.next().getProperty("id")).longValue();
            if (count == ids.length)
              ids = Arrays.copyOf(ids, count * 2);
            ids[count++] = id;
            cursor = id + 1;
            page++;
          }
        }
        if (page < PAGE)
          return Arrays.copyOf(ids, count);
      }
    }

    @Override
    public List<String> types() {
      final List<String> types = new ArrayList<>();
      try (final ResultSet rs = readers[leader].query("sql", "SELECT name FROM schema:types ORDER BY name")) {
        while (rs.hasNext())
          types.add(rs.next().getProperty("name"));
      }
      return types;
    }

    @Override
    public String digest(final int node, final String type) {
      return digest(node, type, PAGE);
    }

    /** As {@link #digest(int, String)} read {@code pageSize} records at a time; the IT pages a few records to reach the keyset query. */
    String digest(final int node, final String type, final int pageSize) {
      final RemoteDatabase db = readers[node];
      return ContentDigest.of(new RidPages(type, pageSize, after -> page(db, type, after, pageSize)));
    }

    /** One page of {@code type} on member {@code node} as the digest reads it, for the IT. */
    List<JSONObject> page(final int node, final String type, final String after, final int pageSize) {
      return page(readers[node], type, after, pageSize);
    }

    /** Up to {@code pageSize} records of {@code type} after rid {@code after} (null: from the first), each with its {@code @rid}. */
    private static List<JSONObject> page(final RemoteDatabase db, final String type, final String after, final int pageSize) {
      // the rid goes into the statement as a literal: only the shape a rid prints as
      if (after != null && !after.matches("#\\d+:\\d+"))
        throw new IllegalStateException("not a rid to page from: " + after);
      final String query = "SELECT FROM `" + type + "`" + (after == null ? "" : " WHERE @rid > " + after) + " ORDER BY @rid LIMIT " + pageSize;
      final List<JSONObject> page = new ArrayList<>(pageSize);
      try (final ResultSet rs = db.query("sql", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final JSONObject json = row.toJSON();
          row.getIdentity().ifPresent(rid -> json.put("@rid", rid.toString()));
          page.add(json);
        }
      }
      return page;
    }

    @Override
    public void close() {
      for (final RemoteDatabase db : readers)
        if (db != null)
          db.close();
      cluster.close();
    }
  }

  /** Reads one page of a type: the records after rid {@code after} (null: from the first), in rid order, each with its {@code @rid}. */
  @FunctionalInterface
  interface PageReader {
    List<JSONObject> read(String after);
  }

  /**
   * The records of a type in {@code @rid} order, read {@code pageSize} at a time after the last rid seen, so neither the node nor
   * the client holds the whole type at once. A full page whose last record carries no {@code @rid} leaves nothing to go on from:
   * that throws, since stopping there would digest the first page alone, identically on every node, and check nothing.
   */
  static final class RidPages implements Iterator<JSONObject> {
    private final String     type;
    private final int        pageSize;
    private final PageReader reader;
    private List<JSONObject> page = List.of();
    private int              next;
    private String           lastRid;
    private boolean          done;

    RidPages(final String type, final int pageSize, final PageReader reader) {
      this.type = type;
      this.pageSize = pageSize;
      this.reader = reader;
    }

    @Override
    public boolean hasNext() {
      if (next < page.size())
        return true;
      if (done)
        return false;
      page = reader.read(lastRid);
      next = 0;
      if (page.size() < pageSize)
        done = true;
      else {
        final String rid = page.getLast().getString("@rid", null);
        if (rid == null)
          throw new IllegalStateException(
              "a full page of " + type + " ends with a record without @rid: the digest cannot go on from it and would cover the first page only");
        lastRid = rid;
      }
      return !page.isEmpty();
    }

    @Override
    public JSONObject next() {
      if (!hasNext())
        throw new NoSuchElementException();
      return page.get(next++);
    }
  }
}
