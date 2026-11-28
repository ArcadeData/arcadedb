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
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.workload.ha.LagMatcher;
import com.arcadedb.bench.workload.ha.WriteLedger;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class HaWorkloadTest {
  private static final Duration REP = Duration.ofMillis(40);

  /**
   * Three members in memory. The leader starts as ha-0; with {@code leaderGone} every write to it throws until a writer asks for
   * the leader again, which then is ha-1.
   */
  private static final class FakeCluster implements HaWorkload.Cluster {
    final ConcurrentSkipListSet<Long> stored     = new ConcurrentSkipListSet<>();
    final AtomicInteger               refreshes  = new AtomicInteger();
    volatile int                      leader;
    volatile boolean                  leaderGone;
    // an id ha-1 lacks although it was acknowledged; -1 for none
    volatile long                     lostOnHa1  = -1;
    volatile boolean                  ha2Differs;
    volatile String                   death;
    // cluster-level asks that find no leader (mid-election) before one answers
    volatile int                      electionPolls;
    // writer-level asks that find no leader before one answers
    volatile int                      writerMisses;
    // every write takes twice the repetition, so it is acknowledged after the repetition's end
    volatile boolean                  slowWrites;
    // the member whose digest read throws; -1 for none
    volatile int                      digestFailsOn = -1;
    // every write throws
    volatile boolean                  writesFail;
    // every poll of a follower throws
    volatile boolean                  pollsFail;
    // the first write starts an election: until the cluster is asked who leads, no leader is named and the members' /ready
    // answers 503, which a death check that probed it would read as a death
    volatile boolean                  electionAtFirstWrite;
    volatile boolean                  electing;
    // the death a failed digest read reveals; null for none
    volatile String                   deathOnFailedRead;
    boolean                           closed;

    @Override
    public int nodes() {
      return 3;
    }

    @Override
    public int leader() {
      return leader;
    }

    @Override
    public int refreshLeader() {
      if (electing) {
        electing = false;
        return -1;
      }
      if (electionPolls > 0) {
        electionPolls--;
        return -1;
      }
      if (leader < 0)
        leader = 0;
      return leader;
    }

    @Override
    public String deathReason() {
      return electing ? "ha-0: not answering /api/v1/ready" : death;
    }

    @Override
    public JSONObject config() {
      return new JSONObject().put("nodes", 3);
    }

    @Override
    public JSONObject imageInfo() {
      return new JSONObject().put("revision", "0123456789abcdef");
    }

    @Override
    public HaWorkload.Writer writer() {
      return new HaWorkload.Writer() {
        @Override
        public void insert(final long id, final int writer) throws InterruptedException {
          if (leader < 0)
            throw new AssertionError("a write was sent while no leader was known");
          if (writesFail)
            throw new IllegalStateException("write refused");
          if (electionAtFirstWrite) {
            electionAtFirstWrite = false;
            electing = true;
          }
          if (leaderGone && leader == 0)
            throw new IllegalStateException("ha-0 is no longer the leader");
          if (slowWrites)
            Thread.sleep(REP.toMillis() * 2);
          stored.add(id);
          if (lostOnHa1 == -2)
            lostOnHa1 = id;
        }

        @Override
        public int refreshLeader() {
          refreshes.incrementAndGet();
          if (writerMisses > 0) {
            writerMisses--;
            return -1;
          }
          if (leaderGone)
            leader = 1;
          return leader;
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public HaWorkload.Poller poller(final int node) {
      return new HaWorkload.Poller() {
        @Override
        public long maxId() {
          if (pollsFail)
            throw new IllegalStateException("follower unreachable");
          return stored.isEmpty() ? -1 : stored.last();
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public long count(final int node) {
      return stored.size();
    }

    @Override
    public long[] ids(final int node, final long from, final long to) {
      final List<Long> ids = new ArrayList<>(stored.subSet(from, to));
      if (node == 1)
        ids.remove((Long) lostOnHa1);
      final long[] array = new long[ids.size()];
      for (int i = 0; i < array.length; i++)
        array[i] = ids.get(i);
      return array;
    }

    @Override
    public List<String> types() {
      return List.of(HaWorkload.TYPE);
    }

    @Override
    public String digest(final int node, final String type) {
      if (node == digestFailsOn) {
        death = deathOnFailedRead;
        throw new IllegalStateException("connection reset");
      }
      return node == 2 && ha2Differs ? "bad" : "good";
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static WorkloadContext ctx(final Scale scale) {
    return new WorkloadContext(null, scale, 42L, new JSONObject(), new WorkloadContext.ServerSpec("img:1", "0-11", 16, "8g"));
  }

  private static HaWorkload workload(final FakeCluster cluster) {
    return new HaWorkload(ctx -> cluster, REP);
  }

  private static Metric metric(final List<Metric> metrics, final String name) {
    for (final Metric m : metrics)
      if (m.name().equals(name))
        return m;
    throw new AssertionError("no metric " + name + " in " + metrics);
  }

  @Test
  void casesAndMetricsAreTheSpecs() throws Exception {
    final HaWorkload spec = new HaWorkload();
    assertThat(spec.name()).isEqualTo("ha");
    assertThat(spec.cases()).containsExactly("write-throughput-1w", "write-throughput-8w", "replication-lag");
    for (final Scale scale : Scale.values())
      assertThat(spec.cases(scale)).isEqualTo(spec.cases());
    assertThat(spec.repLength(Scale.XS)).isEqualTo(Duration.ofSeconds(15));
    assertThat(spec.repLength(Scale.S)).isEqualTo(Duration.ofSeconds(15));
    assertThat(spec.repLength(Scale.M)).isEqualTo(Duration.ofSeconds(45));
    assertThat(spec.repLength(Scale.L)).isEqualTo(Duration.ofSeconds(45));
    assertThat(WorkloadRegistry.all().getLast()).isInstanceOf(HaWorkload.class);

    for (final String caseName : List.of("write-throughput-1w", "write-throughput-8w")) {
      final FakeCluster cluster = new FakeCluster();
      final HaWorkload w = workload(cluster);
      w.prepare(ctx(Scale.XS), caseName);
      final Measurement m = w.measure(ctx(Scale.XS), caseName);
      w.teardown(ctx(Scale.XS));
      assertThat(cluster.closed).isTrue();
      assertThat(m.perRep()).extracting(Metric::name)
          .containsExactly("records_per_s", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "unknown_writes", "leader_changes");
      assertThat(metric(m.perRep(), "records_per_s").unit()).isEqualTo("records/s");
      assertThat(metric(m.perRep(), "records_per_s").better()).isEqualTo(Metric.Better.HIGHER);
      assertThat(metric(m.perRep(), "records_per_s").median()).isPositive();
      assertThat(metric(m.perRep(), "latency_p99_ms").unit()).isEqualTo("ms");
      assertThat(metric(m.perRep(), "unknown_writes").unit()).isEqualTo("writes");
      assertThat(metric(m.perRep(), "unknown_writes").better()).isEqualTo(Metric.Better.LOWER);
      assertThat(metric(m.perRep(), "unknown_writes").median()).isZero();
      // informational: a count the gate never judges
      assertThat(metric(m.perRep(), "leader_changes").unit()).isEqualTo("changes");
      assertThat(metric(m.perRep(), "leader_changes").median()).isZero();
      assertThat(m.histogram()).isNotNull();
    }

    final FakeCluster cluster = new FakeCluster();
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "replication-lag");
    final Measurement m = w.measure(ctx(Scale.XS), "replication-lag");
    w.teardown(ctx(Scale.XS));
    assertThat(m.perRep()).extracting(Metric::name).containsExactly("lag_p50_ms", "lag_p95_ms", "lag_max_ms", "unknown_writes",
        "leader_changes");
    for (final Metric metric : m.perRep())
      if (metric.name().startsWith("lag_")) {
        assertThat(metric.unit()).isEqualTo("ms");
        assertThat(metric.better()).isEqualTo(Metric.Better.LOWER);
      }
    // the scheduled writer wrote at least the first of its 100/s arrivals
    assertThat(cluster.stored).isNotEmpty();
    // the run file records the cluster, not a single server
    assertThat(w.serverConfig().getInt("nodes")).isEqualTo(3);
    assertThat(w.imageInfo().getString("revision")).isEqualTo("0123456789abcdef");
  }

  @Test
  void ownsItsServers() {
    assertThat(new HaWorkload().ownsServers()).isTrue();
    assertThat(new HaWorkload().serverConfig()).isNull();
    assertThat(new RampWorkload().ownsServers()).isFalse();
  }

  @Test
  void prepareNeedsTheServerSpec() {
    final HaWorkload w = workload(new FakeCluster());
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
        () -> w.prepare(new WorkloadContext(null, Scale.XS, 42L, new JSONObject()), "replication-lag"));
    assertThat(e).hasMessageContaining("server spec");
  }

  @Test
  void aMissingAckedWriteFailsTheCaseWithMetrics() throws Exception {
    final WriteLedger ledger = new WriteLedger(4);
    ledger.acked(1);
    ledger.acked(2);
    ledger.unknown(3);
    final String[] types = { HaWorkload.TYPE };
    final String[][] same = { { "d", "d", "d" } };
    assertThat(HaWorkload.verify(ledger, new long[][] { { 1, 2 }, { 1, 2, 3 }, { 1, 2 } }, types, same, 0)).isNull();
    assertThat(HaWorkload.verify(ledger, new long[][] { { 1, 2 }, { 1 }, { 1, 2 } }, types, same, 0))
        .isEqualTo("ha-1 is missing 1 acknowledged writes, first ids [2]");

    // through a whole rep: ha-1 lacks the first id the writer had acknowledged
    final FakeCluster cluster = new FakeCluster();
    cluster.lostOnHa1 = -2;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> w.measure(ctx(Scale.XS), "write-throughput-1w"));
    w.teardown(ctx(Scale.XS));
    assertThat(e).hasMessageContaining("ha-1 is missing 1 acknowledged writes");
    assertThat(e.metrics()).extracting(Metric::name).contains("records_per_s", "unknown_writes");
  }

  @Test
  void aDigestMismatchFailsTheCaseWithMetrics() throws Exception {
    final WriteLedger ledger = new WriteLedger(4);
    ledger.acked(1);
    final long[][] ids = { { 1 }, { 1 }, { 1 } };
    assertThat(HaWorkload.verify(ledger, ids, new String[] { HaWorkload.TYPE }, new String[][] { { "a", "a", "b" } }, 0))
        .isEqualTo("ha-2 differs from the leader on HaRecord");

    final FakeCluster cluster = new FakeCluster();
    cluster.ha2Differs = true;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "replication-lag");
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> w.measure(ctx(Scale.XS), "replication-lag"));
    w.teardown(ctx(Scale.XS));
    assertThat(e).hasMessage("ha-2 differs from the leader on HaRecord");
    assertThat(e.metrics()).extracting(Metric::name).contains("lag_p50_ms", "unknown_writes");
  }

  @Test
  void aDeadNodeFailsTheCaseNamingIt() throws Exception {
    final FakeCluster cluster = new FakeCluster();
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-8w");
    cluster.death = "ha-2: container exited with code 137";
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> w.measure(ctx(Scale.XS), "write-throughput-8w"));
    w.teardown(ctx(Scale.XS));
    assertThat(e).hasMessageContaining("ha-2: container exited with code 137");
    assertThat(e.metrics()).extracting(Metric::name).contains("records_per_s");
  }

  @Test
  void aLeaderChangeIsRecordedNotFatal() throws Exception {
    final FakeCluster cluster = new FakeCluster();
    cluster.leaderGone = true;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    final Measurement m = w.measure(ctx(Scale.XS), "write-throughput-1w");
    w.teardown(ctx(Scale.XS));
    // three writes failed against the old leader, then the writer asked once and went on against ha-1
    assertThat(cluster.refreshes.get()).isEqualTo(1);
    assertThat(cluster.leader).isEqualTo(1);
    assertThat(metric(m.perRep(), "unknown_writes").median()).isEqualTo(3);
    assertThat(metric(m.perRep(), "records_per_s").median()).isPositive();
    // the bench logs nowhere: the change is in the metrics or nowhere
    assertThat(metric(m.perRep(), "leader_changes").median()).isEqualTo(1);
  }

  @Test
  void aMomentWithoutALeaderIsWaitedForNotFatal() throws Exception {
    // a writer's ask finds no leader once (mid-election), the next finds ha-1: the writer keeps going, 6 writes unknown
    final FakeCluster cluster = new FakeCluster();
    cluster.leaderGone = true;
    cluster.writerMisses = 1;
    HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    Measurement m = w.measure(ctx(Scale.XS), "write-throughput-1w");
    w.teardown(ctx(Scale.XS));
    assertThat(cluster.refreshes.get()).isEqualTo(2);
    assertThat(metric(m.perRep(), "unknown_writes").median()).isEqualTo(6);
    // an ask that found no leader is no change; the one that found ha-1 is
    assertThat(metric(m.perRep(), "leader_changes").median()).isEqualTo(1);

    // the cluster knows no leader when a repetition starts and for its checks: both wait for one instead of using index -1
    for (final String caseName : List.of("replication-lag", "write-throughput-8w")) {
      final FakeCluster electing = new FakeCluster();
      electing.leader = -1;
      electing.electionPolls = 2;
      w = workload(electing);
      w.prepare(ctx(Scale.XS), caseName);
      m = w.measure(ctx(Scale.XS), caseName);
      w.teardown(ctx(Scale.XS));
      assertThat(electing.leader).isZero();
      assertThat(electing.stored).isNotEmpty();
      assertThat(metric(m.perRep(), "unknown_writes").median()).isZero();
    }
  }

  @Test
  void noLeaderFailsTheRepetitionClearly() throws Exception {
    final FakeCluster cluster = new FakeCluster();
    cluster.leader = -1;
    cluster.electionPolls = Integer.MAX_VALUE;
    final HaWorkload w = new HaWorkload(ctx -> cluster, REP, Duration.ofMillis(50));
    w.prepare(ctx(Scale.XS), "replication-lag");
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class,
        () -> w.measure(ctx(Scale.XS), "replication-lag"));
    w.teardown(ctx(Scale.XS));
    assertThat(e).hasMessageContaining("no Raft leader");
    assertThat(cluster.stored).isEmpty();
  }

  @Test
  void aCheckReadThatFailsFailsTheCaseWithMetrics() throws Exception {
    final FakeCluster cluster = new FakeCluster();
    cluster.digestFailsOn = 2;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> w.measure(ctx(Scale.XS), "write-throughput-1w"));
    w.teardown(ctx(Scale.XS));
    assertThat(e).hasMessageContaining("digest of HaRecord on ha-2").hasMessageContaining("connection reset");
    assertThat(e.metrics()).extracting(Metric::name).contains("records_per_s", "unknown_writes");

    // a member that died explains the failed read better than the read does
    final FakeCluster dying = new FakeCluster();
    dying.digestFailsOn = 1;
    dying.deathOnFailedRead = "ha-1: container exited with code 137";
    final HaWorkload w2 = workload(dying);
    w2.prepare(ctx(Scale.XS), "write-throughput-1w");
    final CaseFailedWithMetrics died = catchThrowableOfType(CaseFailedWithMetrics.class,
        () -> w2.measure(ctx(Scale.XS), "write-throughput-1w"));
    w2.teardown(ctx(Scale.XS));
    assertThat(died).hasMessageContaining("ha-1: container exited with code 137");
    assertThat(died.metrics()).extracting(Metric::name).contains("records_per_s");
  }

  @Test
  void aWriteAcknowledgedAfterTheRepetitionEndIsNotThroughput() throws Exception {
    // the one write of the repetition is answered after its end: it is checked on every node, but it is no throughput, and the
    // rate is over the repetition's length rather than over however long the last answer took
    final FakeCluster cluster = new FakeCluster();
    cluster.slowWrites = true;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    final Measurement m = w.measure(ctx(Scale.XS), "write-throughput-1w");
    w.teardown(ctx(Scale.XS));
    assertThat(metric(m.perRep(), "records_per_s").median()).isZero();
    assertThat(metric(m.perRep(), "unknown_writes").median()).isZero();
  }

  @Test
  void aRepetitionWithNoAcknowledgedWriteFails() throws Exception {
    // every write throws: a rate, latency and lag of 0 would read as better than any real repetition
    for (final String caseName : List.of("write-throughput-1w", "write-throughput-8w", "replication-lag")) {
      final FakeCluster cluster = new FakeCluster();
      cluster.writesFail = true;
      final HaWorkload w = workload(cluster);
      w.prepare(ctx(Scale.XS), caseName);
      final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class, () -> w.measure(ctx(Scale.XS), caseName));
      w.teardown(ctx(Scale.XS));
      assertThat(e).as(caseName).isNotNull();
      assertThat(e).hasMessageStartingWith("no write was acknowledged in the repetition (").hasMessageEndingWith(" unknown)");
      assertThat(metric(e.metrics(), "unknown_writes").median()).isPositive();
    }
  }

  @Test
  void aLagRepetitionNoFollowerSawFails() throws Exception {
    // the writes are acknowledged, but no poll of a follower ever answers: no lag was measured, and 0 must not stand for it
    final FakeCluster cluster = new FakeCluster();
    cluster.pollsFail = true;
    final HaWorkload w = new HaWorkload(ctx -> cluster, REP, Duration.ofMillis(50), Duration.ofMillis(50));
    w.prepare(ctx(Scale.XS), "replication-lag");
    final CaseFailedWithMetrics e = catchThrowableOfType(CaseFailedWithMetrics.class, () -> w.measure(ctx(Scale.XS), "replication-lag"));
    w.teardown(ctx(Scale.XS));
    assertThat(cluster.stored).isNotEmpty();
    assertThat(e).isNotNull();
    assertThat(e).hasMessageContaining("no follower showed an acknowledged write");
    assertThat(e.metrics()).extracting(Metric::name).contains("lag_p50_ms", "unknown_writes");
  }

  @Test
  void anElectionAtCheckTimeIsWaitedForNotADeath() throws Exception {
    // during an election /api/v1/ready answers 503: the checks wait for the leader before they ask whether a member died
    final FakeCluster cluster = new FakeCluster();
    cluster.electionAtFirstWrite = true;
    final HaWorkload w = workload(cluster);
    w.prepare(ctx(Scale.XS), "write-throughput-1w");
    final Measurement m = w.measure(ctx(Scale.XS), "write-throughput-1w");
    w.teardown(ctx(Scale.XS));
    assertThat(cluster.electing).isFalse();
    assertThat(metric(m.perRep(), "records_per_s").median()).isPositive();
  }

  @Test
  void aFollowerIsSeenWhenItsPollWasSentNotWhenItReturned() {
    // write 0 acknowledged at t=50; the poll that shows it is sent at t=100 and returns at t=200: the follower showed the write
    // by the time the poll was sent at the latest after the previous one, so the response leg is no lag
    final long[] acks = { 50 };
    final LagMatcher matcher = new LagMatcher(acks);
    final long[] clock = { 100 };
    HaWorkload.poll(new HaWorkload.Poller() {
      @Override
      public long maxId() {
        // the answer takes 100
        clock[0] = 200;
        return 0;
      }

      @Override
      public void close() {
      }
    }, matcher, 0, new AtomicBoolean(false), new AtomicLong(0), Duration.ofSeconds(30), () -> clock[0]);
    assertThat(matcher.lags().count()).isEqualTo(1);
    assertThat(matcher.lags().percentile(100)).isEqualTo(50);
  }

  @Test
  void aDigestPageWithoutRidsIsLoudNotHalfChecked() {
    // pages of 2: the records of a type, read after the last rid of the previous page
    final List<JSONObject> records = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      records.add(new JSONObject().put("@rid", "#1:" + i).put("id", i));
    final List<String> asked = new ArrayList<>();
    final HaWorkload.RidPages pages = new HaWorkload.RidPages(HaWorkload.TYPE, 2, after -> {
      asked.add(after);
      final int from = after == null ? 0 : Integer.parseInt(after.substring(3)) + 1;
      return new ArrayList<>(records.subList(Math.min(from, 5), Math.min(from + 2, 5)));
    });
    int n = 0;
    while (pages.hasNext()) {
      assertThat(pages.next().getInt("id")).isEqualTo(n);
      n++;
    }
    assertThat(n).isEqualTo(5);
    assertThat(asked).containsExactly(null, "#1:1", "#1:3");

    // a full page whose last record carries no rid gives nothing to go on from: the digest must not stop there quietly
    final HaWorkload.RidPages noRids = new HaWorkload.RidPages(HaWorkload.TYPE, 2,
        after -> new ArrayList<>(List.of(new JSONObject().put("id", 1), new JSONObject().put("id", 2))));
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, () -> {
      while (noRids.hasNext())
        noRids.next();
    });
    assertThat(e).hasMessageContaining("HaRecord").hasMessageContaining("@rid");
  }
}
