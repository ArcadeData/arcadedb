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
package com.arcadedb.bench.workload.mixed;

import com.arcadedb.bench.runner.LatencyRecorder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class MixedPhaseTest {
  private static final Duration JOIN_BOUND = Duration.ofMillis(200);

  /** A database whose counts and types the test sets: the seam that hands the check a wrong answer. */
  private static final class FakeDatabase implements MixedPhase.Probe {
    final AtomicLong    docs   = new AtomicLong();
    final AtomicLong    points = new AtomicLong();
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    volatile List<String> types = List.of("MixDoc", "MixSensor");
    /** Document counts read before {@link #docs}, one per read: a server still committing a batch. */
    final Deque<Long>   movingDocs = new ConcurrentLinkedDeque<>();
    final List<String>  dropped    = new CopyOnWriteArrayList<>();
    /** Thrown by every drop when set: a server that refuses to drop a type. */
    volatile RuntimeException dropFailure;

    FakeDatabase(final long docs, final long points) {
      this.docs.set(docs);
      this.points.set(points);
    }

    @Override
    public Reader open() {
      opened.incrementAndGet();
      return new Reader() {
        @Override
        public long count(final String type) {
          return switch (type) {
            case MixedPhase.DOC_TYPE -> {
              final Long moving = movingDocs.poll();
              yield moving != null ? moving : docs.get();
            }
            case MixedPhase.SENSOR_TYPE -> points.get();
            default -> throw new IllegalArgumentException(type);
          };
        }

        @Override
        public List<String> types() {
          return types;
        }

        @Override
        public void dropType(final String type) {
          if (dropFailure != null)
            throw dropFailure;
          dropped.add(type);
          final List<String> left = new ArrayList<>(types);
          left.remove(type);
          types = left;
        }

        @Override
        public void close() {
          closed.incrementAndGet();
        }
      };
    }
  }

  private static MixedPhase phase(final long baseDocs, final MixedPhase.Loops loops, final MixedPhase.Probe probe) {
    return new MixedPhase(baseDocs, loops, probe, JOIN_BOUND, Duration.ZERO);
  }

  /** An operation that takes 1 ms; a search one answers a row. */
  private static OperationLoop.Operation quick(final String loop, final AtomicLong answered) {
    return () -> {
      if ("search".equals(loop))
        answered.incrementAndGet();
      pause(1);
    };
  }

  private static void pause(final long millis) throws InterruptedException {
    Thread.sleep(millis);
  }

  @Test
  void aThreadThatOutlivesTheWindowFailsTheRepetition() throws Exception {
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> "ingest".equals(loop) && thread == 0 ?
        () -> pause(30_000) :
        quick(loop, answered), new FakeDatabase(10, 0));
    final MixedPhase.Result result = phase.run(Duration.ofMillis(100));
    assertThat(result.failures()).hasSize(1);
    assertThat(result.failures().get(0)).startsWith("ingest:").contains("still running");
    assertThat(result.latencies().keySet()).containsExactly("ingest", "search", "compaction", "ddl", "timeseries", "vector_churn");
  }

  @Test
  void theCheckFailsOnAMissingDocument() {
    final FakeDatabase db = new FakeDatabase(9, 0);
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> quick(loop, answered), db);
    assertThat(phase.expectedDocs()).isEqualTo(10);
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("MixDoc").contains("9").contains("10");
  }

  @Test
  void theCheckFailsOnALeftoverScratchType() {
    final FakeDatabase db = new FakeDatabase(10, 0);
    db.types = List.of("MixDoc", "MixScratch7", "MixSensor");
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> quick(loop, answered), db);
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("MixScratch7");
  }

  @Test
  void theCheckFailsOnAMissingPoint() throws Exception {
    final FakeDatabase db = new FakeDatabase(0, 0);
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> quick(loop, answered), db);
    final MixedPhase.Result result = phase.run(Duration.ofMillis(100));
    assertThat(result.failures()).isEmpty();
    assertThat(phase.expectedPoints()).isPositive();
    db.docs.set(phase.expectedDocs());
    db.points.set(phase.expectedPoints() - 1);
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("MixSensor");
  }

  @Test
  void theCheckReadsWithOneClient() throws Exception {
    final FakeDatabase db = new FakeDatabase(10, 0);
    final MixedPhase phase = new MixedPhase(10, (loop, thread, resources, answered) -> quick(loop, answered), db, JOIN_BOUND,
        Duration.ofMillis(600));
    db.docs.set(9);
    catchThrowableOfType(IllegalStateException.class, phase::check);
    // the check polled for 600 ms (several reads), all through one reader
    assertThat(db.opened.get()).isEqualTo(1);
    assertThat(db.closed.get()).isEqualTo(1);
  }

  @Test
  void theExpectationsGrowByWhatTheLoopsAcknowledged() throws Exception {
    final FakeDatabase db = new FakeDatabase(0, 0);
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> quick(loop, answered), db);
    final MixedPhase.Result first = phase.run(Duration.ofMillis(100));
    assertThat(first.failures()).isEmpty();
    final long ingested = first.latencies().get("ingest").count();
    final long batches = first.latencies().get("timeseries").count();
    assertThat(ingested).isPositive();
    assertThat(phase.expectedDocs()).isEqualTo(10 + ingested * MixedPhase.INGEST_BATCH);
    assertThat(phase.expectedPoints()).isEqualTo(batches * MixedPhase.POINTS_PER_BATCH);
    long operations = 0;
    for (final Map.Entry<String, LatencyRecorder> e : first.latencies().entrySet())
      operations += e.getValue().count();
    assertThat(first.operations()).isEqualTo(operations);
    // window start to end of the drain: never shorter than the window (a lower bound)
    assertThat(first.seconds()).isGreaterThanOrEqualTo(0.1);
    assertThat(first.longestCommitGapNanos()).isPositive();

    // cumulative over repetitions
    final MixedPhase.Result second = phase.run(Duration.ofMillis(100));
    assertThat(phase.expectedDocs()).isEqualTo(10 + (ingested + second.latencies().get("ingest").count()) * MixedPhase.INGEST_BATCH);
    db.docs.set(phase.expectedDocs());
    db.points.set(phase.expectedPoints());
    phase.check();
  }

  @Test
  void aFailedWindowLosesItsExpectationsAndTheNextOneReadsThemBack() throws Exception {
    final AtomicBoolean stuck = new AtomicBoolean(true);
    final FakeDatabase db = new FakeDatabase(0, 0);
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> "ingest".equals(loop) && thread == 0 ?
        () -> pause(stuck.get() ? 30_000 : 1) :
        quick(loop, answered), db);
    final MixedPhase.Result failed = phase.run(Duration.ofMillis(100));
    assertThat(failed.failures()).anyMatch(f -> f.startsWith("ingest:"));
    // the interrupted batch may or may not have landed: the failed window's own check says so instead of comparing counts
    final IllegalStateException lost = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(lost).isNotNull();
    assertThat(lost.getMessage()).contains("lost").contains("ingest");

    // what the database holds after the failed window, as the next window reads it back before its clock starts
    stuck.set(false);
    db.docs.set(12_345);
    db.points.set(777);
    final MixedPhase.Result clean = phase.run(Duration.ofMillis(100));
    assertThat(clean.failures()).isEmpty();
    assertThat(phase.expectedDocs()).isEqualTo(12_345 + clean.latencies().get("ingest").count() * MixedPhase.INGEST_BATCH);
    assertThat(phase.expectedPoints()).isEqualTo(777 + clean.latencies().get("timeseries").count() * MixedPhase.POINTS_PER_BATCH);
    db.docs.set(phase.expectedDocs());
    db.points.set(phase.expectedPoints());
    phase.check();
  }

  @Test
  void theReadBackWaitsForTheCountsToSettleAndDropsLeftoverScratchTypes() throws Exception {
    final AtomicBoolean stuck = new AtomicBoolean(true);
    final FakeDatabase db = new FakeDatabase(0, 0);
    final MixedPhase phase = new MixedPhase(10, (loop, thread, resources, answered) -> "ingest".equals(loop) && thread == 0 ?
        () -> pause(stuck.get() ? 30_000 : 1) :
        quick(loop, answered), db, JOIN_BOUND, Duration.ofSeconds(10));
    assertThat(phase.run(Duration.ofMillis(100)).failures()).isNotEmpty();

    // the server is still committing the interrupted batch, and the failed window left a scratch type behind
    stuck.set(false);
    db.movingDocs.addAll(List.of(1_000L, 1_050L, 1_100L));
    db.docs.set(1_100);
    db.types = List.of("MixDoc", "MixScratch3", "MixSensor");
    final MixedPhase.Result clean = phase.run(Duration.ofMillis(100));
    assertThat(clean.failures()).isEmpty();
    assertThat(db.dropped).containsExactly("MixScratch3");
    // the expectations start from the settled count, not from the first read
    assertThat(phase.expectedDocs()).isEqualTo(1_100 + clean.latencies().get("ingest").count() * MixedPhase.INGEST_BATCH);
    db.docs.set(phase.expectedDocs());
    db.points.set(phase.expectedPoints());
    phase.check();
  }

  @Test
  void aDropThatFailsKeepsTheExpectationsLostWithoutEscapingTheWindow() throws Exception {
    final AtomicBoolean stuck = new AtomicBoolean(true);
    final FakeDatabase db = new FakeDatabase(0, 0);
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> "ingest".equals(loop) && thread == 0 ?
        () -> pause(stuck.get() ? 30_000 : 1) :
        quick(loop, answered), db);
    assertThat(phase.run(Duration.ofMillis(100)).failures()).isNotEmpty();

    stuck.set(false);
    db.types = List.of("MixDoc", "MixScratch9", "MixSensor");
    db.dropFailure = new IllegalStateException("Error on dropping bucket 'MixScratch9_0'");
    // the failed drop does not escape run(): the window runs, and its expectations stay lost
    final MixedPhase.Result next = phase.run(Duration.ofMillis(100));
    assertThat(next.failures()).isEmpty();
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("lost").contains("MixScratch9").contains("Error on dropping bucket");
  }

  @Test
  void countsThatNeverSettleKeepTheExpectationsLost() throws Exception {
    final AtomicBoolean stuck = new AtomicBoolean(true);
    final AtomicLong reads = new AtomicLong();
    final MixedPhase.Probe moving = () -> new MixedPhase.Probe.Reader() {
      @Override
      public long count(final String type) {
        return reads.incrementAndGet();
      }

      @Override
      public List<String> types() {
        return List.of();
      }

      @Override
      public void dropType(final String type) {
      }

      @Override
      public void close() {
      }
    };
    final MixedPhase phase = new MixedPhase(10, (loop, thread, resources, answered) -> "ingest".equals(loop) && thread == 0 ?
        () -> pause(stuck.get() ? 30_000 : 1) :
        quick(loop, answered), moving, JOIN_BOUND, Duration.ofMillis(600));
    assertThat(phase.run(Duration.ofMillis(100)).failures()).isNotEmpty();
    stuck.set(false);
    phase.run(Duration.ofMillis(100));
    final IllegalStateException e = catchThrowableOfType(IllegalStateException.class, phase::check);
    assertThat(e).isNotNull();
    assertThat(e.getMessage()).contains("lost").contains("still moving");
  }

  @Test
  void anInterruptDuringTheWindowStopsAndJoinsTheLoopsAndKeepsTheFlag() throws Exception {
    final List<Thread> loopThreads = new CopyOnWriteArrayList<>();
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> {
      final OperationLoop.Operation quick = quick(loop, answered);
      return () -> {
        loopThreads.add(Thread.currentThread());
        quick.run();
      };
    }, new FakeDatabase(10, 0));
    final AtomicReference<MixedPhase.Result> result = new AtomicReference<>();
    final AtomicBoolean flagKept = new AtomicBoolean();
    final Thread worker = new Thread(() -> {
      try {
        result.set(phase.run(Duration.ofSeconds(60)));
      } catch (final Exception e) {
        throw new IllegalStateException(e);
      }
      flagKept.set(Thread.currentThread().isInterrupted());
    });
    worker.start();
    while (loopThreads.isEmpty())
      pause(5);
    worker.interrupt();
    // generous: a run that ignored the interrupt would sleep out its 60 s window
    worker.join(30_000);
    assertThat(worker.isAlive()).isFalse();
    assertThat(flagKept.get()).isTrue();
    assertThat(result.get().failures()).anyMatch(f -> f.startsWith("window: interrupted"));
    for (final Thread t : loopThreads)
      assertThat(t.isAlive()).as(t.getName()).isFalse();
  }

  @Test
  void aSearchLoopWhoseEveryAnswerWasEmptyFails() throws Exception {
    // no operation counts an answered search
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> () -> pause(1), new FakeDatabase(10, 0));
    final MixedPhase.Result result = phase.run(Duration.ofMillis(100));
    assertThat(result.failures()).singleElement().asString().startsWith("search:").contains("empty");
  }

  @Test
  void oneNonEmptySearchAnswerIsEnough() throws Exception {
    final AtomicBoolean once = new AtomicBoolean();
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> () -> {
      if ("search".equals(loop) && once.compareAndSet(false, true))
        answered.incrementAndGet();
      pause(1);
    }, new FakeDatabase(10, 0));
    assertThat(phase.run(Duration.ofMillis(100)).failures()).isEmpty();
  }

  @Test
  void aFailedOperationIsNamedWithItsLoop() throws Exception {
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> "ddl".equals(loop) ?
        () -> {
          throw new IllegalStateException("boom");
        } :
        quick(loop, answered), null);
    final MixedPhase.Result result = phase.run(Duration.ofMillis(100));
    assertThat(result.failures()).containsExactly("ddl: java.lang.IllegalStateException: boom");
  }

  @Test
  void theClientsAreClosedAfterTheWindow() throws Exception {
    final List<AutoCloseable> opened = new CopyOnWriteArrayList<>();
    final boolean[] closed = new boolean[10];
    final int[] next = { 0 };
    final MixedPhase phase = phase(10, (loop, thread, resources, answered) -> {
      final int slot = next[0]++;
      final AutoCloseable client = () -> closed[slot] = true;
      resources.add(client);
      opened.add(client);
      return quick(loop, answered);
    }, null);
    phase.run(Duration.ofMillis(50));
    // ingest and search run two threads each, the other four loops one: eight clients
    assertThat(opened).hasSize(8);
    for (int i = 0; i < 8; i++)
      assertThat(closed[i]).as("client " + i).isTrue();
  }
}
