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

import com.arcadedb.bench.data.DatasetGenerator;
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Progress;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.LineProtocolClient;
import com.arcadedb.bench.workload.RemoteClients;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Six kinds of work at once against one server, for one window: {@code ingest} (2 threads, 100 inserts per transaction),
 * {@code search} (2 threads, a BM25 top-10 and a 10 nearest-neighbors search in turn), {@code compaction} (the three indexes of
 * {@value #DOC_TYPE} in turn, every 2 s), {@code ddl} (a scratch type created, indexed and dropped, every 1 s), {@code timeseries}
 * (line-protocol batches of 500 points) and {@code vector_churn} (50 embedding updates per transaction). Each thread has its own
 * client, opened before the clock starts, and runs its operation in an {@link OperationLoop}.
 * <p>
 * What the loops acknowledged adds up across the repetitions of one instance, and {@link #check()}, run outside the window,
 * holds the database to it: every acknowledged document and point is there, nothing more, and no scratch type is left. The
 * database is prepared by {@code MixedWorkload.prepareDatabase}, with the data helpers of this class.
 */
public final class MixedPhase {
  public static final String       DATABASE         = "mixed";
  public static final String       DOC_TYPE         = "MixDoc";
  public static final String       SENSOR_TYPE      = "MixSensor";
  public static final String       SCRATCH_PREFIX   = "MixScratch";
  public static final String       TEXT_INDEX       = DOC_TYPE + "[body]";
  public static final String       ID_INDEX         = DOC_TYPE + "[id]";
  public static final String       VECTOR_INDEX     = DOC_TYPE + "[embedding]";
  public static final int          DIM              = 128;
  public static final int          WORDS_PER_DOC    = 40;
  /** The loops, in the order of the latency map. */
  public static final List<String> LOOPS            = List.of("ingest", "search", "compaction", "ddl", "timeseries", "vector_churn");
  /** How long the check waits for the counts to reach what was acknowledged. */
  public static final Duration     CHECK_WAIT       = Duration.ofSeconds(60);

  static final int INGEST_BATCH     = 100;
  static final int POINTS_PER_BATCH = 500;
  static final int SENSORS          = 50;
  static final int CHURN_BATCH      = 50;
  static final int K                = 10;

  private static final Logger LOGGER = LoggerFactory.getLogger(MixedPhase.class);

  private static final int[]     THREADS  = { 2, 2, 1, 1, 1, 1 };
  private static final long[]    PAUSES   = { 0L, 0L, 2_000L, 1_000L, 0L, 0L };
  private static final boolean[] COMMITS  = { true, false, false, true, true, true };
  private static final int       INGEST   = 0;
  private static final int       SEARCH   = 1;
  private static final int       TIMESERIES = 4;

  private static final int    CAPACITY        = 4_096;
  private static final int    POOL            = 1_000;
  private static final long   DOC_SEED_OFFSET = 1_000L;
  private static final long   NEW_DOC_OFFSET  = 50_000_000L;
  private static final long   SPACE_SEED      = 2L;
  private static final long   QUERY_SEED      = 3L;
  private static final long   POOL_SEED       = 4L;
  /** The first point's timestamp, ms: 2023-11-14, so the points fall in the past and far from any retention edge. */
  private static final long   TS_BASE         = 1_700_000_000_000L;
  private static final long   CHECK_POLL_MS   = 250L;
  private static final long   JOIN_SLICE_MS   = 2_000L;
  private static final String BM25_SQL        =
      "SELECT id, $score AS score FROM " + DOC_TYPE + " WHERE SEARCH_INDEX('" + TEXT_INDEX + "', :q) = true ORDER BY score DESC LIMIT "
          + K;
  private static final String KNN_SQL         = "SELECT id FROM (SELECT expand(vectorNeighbors('" + VECTOR_INDEX + "', :q, " + K + ")))";
  private static final String[] COMPACTED     = { TEXT_INDEX, ID_INDEX, VECTOR_INDEX };

  /** The work of each loop thread; the seam that lets a test run the phase without a server. */
  @FunctionalInterface
  interface Loops {
    /**
     * The operation thread {@code thread} of loop {@code loop} runs. A client it opens goes in {@code resources}, which are closed
     * after the window. A search operation adds one to {@code answered} for each answer with at least one row. Called before the
     * clock starts.
     */
    OperationLoop.Operation open(String loop, int thread, List<AutoCloseable> resources, AtomicLong answered) throws Exception;
  }

  /** What the check reads from the database; the seam that lets a test hand it a wrong answer. */
  @FunctionalInterface
  interface Probe {
    /** A reader of the database, used for one check or one re-baseline. */
    Reader open();

    interface Reader extends AutoCloseable {
      /** The unfiltered record count of {@code type}. */
      long count(String type);

      /** The names of every type of the database. */
      List<String> types();

      /** Drops {@code type} when it exists. */
      void dropType(String type);

      @Override
      void close();
    }
  }

  /**
   * One window: the latencies of each loop (both threads of a two-thread loop merged), the longest stretch in which nothing
   * committed, the operations completed by all loops, the seconds from the window start to the end of the drain and one entry per loop that failed.
   */
  public record Result(Map<String, LatencyRecorder> latencies, long longestCommitGapNanos, long operations, double seconds,
                       List<String> failures) {
  }

  private final Loops    loops;
  private final Probe    probe;
  private final Duration joinBound;
  private final Duration checkWait;
  private final List<Thread> stragglers = new ArrayList<>();
  private long           expectedDocs;
  private long           expectedPoints;
  /** Null while the expectations hold; else why the last window left them unknown. */
  private String         lostReason;

  public MixedPhase(final ServerUnderTest server, final long seed, final Scale scale) {
    this(baseDocs(scale), new RemoteLoops(server, seed, baseDocs(scale)), new RemoteProbe(server), joinBound(scale), CHECK_WAIT);
  }

  MixedPhase(final long baseDocs, final Loops loops, final Probe probe, final Duration joinBound, final Duration checkWait) {
    this.expectedDocs = baseDocs;
    this.loops = loops;
    this.probe = probe;
    this.joinBound = joinBound;
    this.checkWait = checkWait;
  }

  /**
   * How long, after the window, a loop thread has to end before it is interrupted and fails the repetition. A hang detector, not
   * a latency bound: a loop can legitimately sit in a compaction that drains for tens of seconds (46 s at S on a laptop), and the
   * drain grows with the documents, so the bound grows with the scale.
   */
  public static Duration joinBound(final Scale scale) {
    return switch (scale) {
      case XS -> Duration.ofSeconds(60);
      case S -> Duration.ofMinutes(3);
      case M, L -> Duration.ofMinutes(5);
    };
  }

  /** The documents of {@value #DOC_TYPE} loaded before the first window. */
  public static int baseDocs(final Scale scale) {
    return switch (scale) {
      case XS -> 5_000;
      case S -> 50_000;
      case M -> 200_000;
      case L -> 1_000_000;
    };
  }

  /** The body of base document {@code id}. */
  public static String docText(final long seed, final long id) {
    return new DatasetGenerator(seed + DOC_SEED_OFFSET + id).text(WORDS_PER_DOC);
  }

  /** The space every embedding of the phase is drawn from: base documents, new documents, updates and queries. */
  public static DatasetGenerator.EmbeddingSpace space(final long seed) {
    return new DatasetGenerator(seed + SPACE_SEED).embeddingSpace(DIM);
  }

  /** {@code v} as an SQL array literal. */
  public static String literal(final float[] v) {
    final StringBuilder sb = new StringBuilder(v.length * 12);
    appendLiteral(sb, v);
    return sb.toString();
  }

  private static void appendLiteral(final StringBuilder sb, final float[] v) {
    sb.append('[');
    for (int j = 0; j < v.length; j++) {
      if (j > 0)
        sb.append(',');
      sb.append(v[j]);
    }
    sb.append(']');
  }

  /** Every document acknowledged so far, the base included. */
  public long expectedDocs() {
    return expectedDocs;
  }

  /** Every time-series point acknowledged so far. */
  public long expectedPoints() {
    return expectedPoints;
  }

  /**
   * Runs the six loops for {@code window}. The clients are opened before the clock starts and closed after the threads end. A
   * thread still running the join bound after the window is interrupted and fails the repetition; so does an interrupt of
   * the calling thread during the window, which is restored when this returns.
   * <p>
   * A repetition with a failure leaves writes nobody can account for (a batch interrupted after the server took it, a timeout
   * with an unknown outcome, a thread that outlived both bounds): its expectations are marked lost, and the next repetition
   * starts by joining what is left of it and reading the counts back from the database.
   */
  public Result run(final Duration window) throws Exception {
    if (lostReason != null)
      rebaseline();
    int total = 0;
    for (final int t : THREADS)
      total += t;
    final OperationLoop.Operation[] ops = new OperationLoop.Operation[total];
    final int[] loopOf = new int[total];
    final int[] firstOf = new int[THREADS.length];
    final OperationLoop[] running = new OperationLoop[total];
    final Thread[] threads = new Thread[total];
    final List<AutoCloseable> resources = new ArrayList<>();
    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicLong answered = new AtomicLong();
    final List<String> failures = new ArrayList<>();
    final Map<String, LatencyRecorder> latencies = new LinkedHashMap<>();
    for (final String loop : LOOPS)
      latencies.put(loop, new LatencyRecorder(CAPACITY));
    long operations = 0;
    long searches = 0;
    long docs = 0;
    long points = 0;
    boolean interrupted = false;
    final long start;
    final long end;
    long drained = 0;
    try {
      int k = 0;
      for (int i = 0; i < THREADS.length; i++) {
        firstOf[i] = k;
        for (int t = 0; t < THREADS[i]; t++) {
          loopOf[k] = i;
          ops[k++] = loops.open(LOOPS.get(i), t, resources, answered);
        }
      }

      start = System.nanoTime();
      final CommitGaps gaps = new CommitGaps(start);
      for (k = 0; k < total; k++) {
        final int i = loopOf[k];
        running[k] = new OperationLoop(LOOPS.get(i), ops[k], PAUSES[i], stop, gaps, COMMITS[i], CAPACITY);
        threads[k] = new Thread(running[k], "bench-mixed-" + LOOPS.get(i) + "-" + (k - firstOf[i]));
        threads[k].setDaemon(true);
        threads[k].start();
      }
      try {
        Thread.sleep(window.toMillis());
      } catch (final InterruptedException e) {
        // the loops are still stopped and joined below; the flag is restored once they are
        interrupted = true;
        failures.add("window: interrupted after " + (System.nanoTime() - start) / 1_000_000L + " ms");
      } finally {
        stop.set(true);
      }
      end = System.nanoTime();

      // every thread gets one shared bound to end by itself, then every thread still alive is interrupted and all of them get
      // a second shared bound to unwind
      final long deadline = end + joinBound.toNanos();
      for (final Thread thread : threads)
        interrupted |= joinUntil(thread, deadline);
      boolean anyAlive = false;
      for (final Thread thread : threads)
        if (thread.isAlive()) {
          thread.interrupt();
          anyAlive = true;
        }
      if (anyAlive) {
        final long second = System.nanoTime() + joinBound.toNanos();
        for (k = 0; k < total; k++) {
          if (!threads[k].isAlive())
            continue;
          interrupted |= joinUntil(threads[k], second);
          failures.add(running[k].name() + ": still running " + joinBound.toMillis() + " ms after the window ended, interrupted"
              + (threads[k].isAlive() ? " and still alive, its samples are dropped" : ""));
        }
      }

      drained = System.nanoTime();
      for (k = 0; k < total; k++) {
        if (threads[k].isAlive()) {
          stragglers.add(threads[k]);
          continue;
        }
        final OperationLoop loop = running[k];
        if (loop.failure() != null) {
          LOGGER.warn("Mixed loop {} failed", loop.name(), loop.failure());
          failures.add(loop.name() + ": " + loop.failure());
        }
        latencies.get(loop.name()).addAll(loop.latencies());
        operations += loop.completed();
        if (loopOf[k] == INGEST)
          docs += loop.completed() * INGEST_BATCH;
        else if (loopOf[k] == TIMESERIES)
          points += loop.completed() * POINTS_PER_BATCH;
        else if (loopOf[k] == SEARCH)
          searches += loop.completed();
      }
      if (searches > 0 && answered.get() == 0)
        failures.add(LOOPS.get(SEARCH) + ": every one of the " + searches + " answers of the window was empty");

      if (failures.isEmpty()) {
        expectedDocs += docs;
        expectedPoints += points;
      } else
        lostReason = String.join("; ", failures);
      return new Result(latencies, gaps.longestGapNanos(end), operations, (drained - start) / 1e9, failures);
    } finally {
      stop.set(true);
      for (final AutoCloseable resource : resources) {
        try {
          resource.close();
        } catch (final Exception e) {
          LOGGER.warn("Closing a mixed client failed", e);
        }
      }
      if (interrupted)
        Thread.currentThread().interrupt();
    }
  }

  /**
   * Joins {@code thread} until {@code deadline}, in slices, each followed by a {@link Progress} tick: a legitimate drain after
   * the window is not a stall. True when the caller was interrupted meanwhile (the flag is then cleared).
   */
  private static boolean joinUntil(final Thread thread, final long deadline) {
    try {
      while (thread.isAlive()) {
        final long left = (deadline - System.nanoTime()) / 1_000_000L;
        if (left <= 0)
          return false;
        thread.join(Math.min(left, JOIN_SLICE_MS));
        Progress.advance();
      }
      return false;
    } catch (final InterruptedException e) {
      return true;
    }
  }

  /**
   * After a failed repetition: the threads it left alive get one more interrupt and the join bound, then the expectations
   * become what the database holds. The scratch types the failed window left are dropped first, and the counts are read until
   * two reads in a row agree, for at most {@link #CHECK_WAIT}: the server may still be committing a batch whose client was
   * interrupted. A thread alive even then (nothing is dropped then), a drop that fails or counts still moving keep the expectations
   * lost: the window that follows fails too.
   */
  private void rebaseline() throws InterruptedException {
    for (final Thread thread : stragglers)
      thread.interrupt();
    final long deadline = System.nanoTime() + joinBound.toNanos();
    for (final Thread thread : stragglers)
      if (joinUntil(thread, deadline))
        throw new InterruptedException("interrupted while joining the threads of an earlier window");
    stragglers.removeIf(t -> !t.isAlive());
    if (!stragglers.isEmpty()) {
      // a live ddl thread may still be using a scratch type: nothing is dropped, and the expectations stay lost
      lostReason = stragglers.size() + " loop thread(s) of an earlier window still alive";
      return;
    }
    boolean settled = false;
    try (final Probe.Reader reader = probe.open()) {
      for (final String type : reader.types())
        if (type.startsWith(SCRATCH_PREFIX))
          try {
            reader.dropType(type);
          } catch (final RuntimeException e) {
            LOGGER.warn("Dropping the leftover scratch type {} failed", type, e);
            lostReason = "dropping the leftover scratch type " + type + " failed: " + e;
            return;
          }
      final long settleBy = System.nanoTime() + checkWait.toNanos();
      long docs = reader.count(DOC_TYPE);
      long points = reader.count(SENSOR_TYPE);
      do {
        Thread.sleep(CHECK_POLL_MS);
        final long nextDocs = reader.count(DOC_TYPE);
        final long nextPoints = reader.count(SENSOR_TYPE);
        settled = nextDocs == docs && nextPoints == points;
        docs = nextDocs;
        points = nextPoints;
      } while (!settled && System.nanoTime() < settleBy);
      expectedDocs = docs;
      expectedPoints = points;
    }
    if (!settled)
      lostReason = "the counts were still moving " + checkWait.toSeconds() + " s after an earlier failed window";
    else {
      LOGGER.info("Mixed expectations read back after a failed window: {} documents, {} points", expectedDocs, expectedPoints);
      lostReason = null;
    }
  }

  /**
   * Untimed: {@value #DOC_TYPE} holds exactly {@link #expectedDocs()} documents, {@value #SENSOR_TYPE} exactly
   * {@link #expectedPoints()} points, and no {@value #SCRATCH_PREFIX} type is left. The counts get {@link #CHECK_WAIT} to settle,
   * read with one client. Throws naming each mismatch; after a failed repetition, throws that its expectations were lost
   * instead of comparing counts that cannot match.
   */
  public void check() throws InterruptedException {
    if (lostReason != null)
      throw new IllegalStateException("The expected counts were lost, the last window ended with failures: " + lostReason);
    final List<String> problems = new ArrayList<>();
    try (final Probe.Reader reader = probe.open()) {
      final long deadline = System.nanoTime() + checkWait.toNanos();
      long docs = reader.count(DOC_TYPE);
      long points = reader.count(SENSOR_TYPE);
      while ((docs != expectedDocs || points != expectedPoints) && System.nanoTime() < deadline) {
        Thread.sleep(CHECK_POLL_MS);
        docs = reader.count(DOC_TYPE);
        points = reader.count(SENSOR_TYPE);
      }
      if (docs != expectedDocs)
        problems.add(DOC_TYPE + " holds " + docs + " documents, expected " + expectedDocs);
      if (points != expectedPoints)
        problems.add(SENSOR_TYPE + " holds " + points + " points, expected " + expectedPoints);
      for (final String type : reader.types())
        if (type.startsWith(SCRATCH_PREFIX))
          problems.add("the scratch type " + type + " is still in the schema");
    }
    if (!problems.isEmpty())
      throw new IllegalStateException(String.join("; ", problems));
  }

  /** The loops against the server under test: one client per thread. */
  private static final class RemoteLoops implements Loops {
    private final ServerUnderTest server;
    private final long            seed;
    private final int             baseDocs;
    private final AtomicLong      nextId;
    private final AtomicLong      nextScratch = new AtomicLong();
    private final AtomicLong      nextPoint   = new AtomicLong();
    // built once, outside every window: the operations only pick from them
    private final String[]        queryTerms  = new String[POOL];
    private final float[][]       queryVectors;
    private final String[]        bodies      = new String[POOL];
    private final String[]        embeddings  = new String[POOL];

    private RemoteLoops(final ServerUnderTest server, final long seed, final int baseDocs) {
      this.server = server;
      this.seed = seed;
      this.baseDocs = baseDocs;
      this.nextId = new AtomicLong(baseDocs);
      final DatasetGenerator.EmbeddingSpace space = space(seed);
      this.queryVectors = new DatasetGenerator(seed + QUERY_SEED).embeddings(POOL, space);
      final SplittableRandom random = new SplittableRandom(seed + QUERY_SEED);
      for (int i = 0; i < POOL; i++)
        queryTerms[i] = term(docText(seed, random.nextInt(baseDocs)), random);
      final float[][] vectors = new DatasetGenerator(seed + POOL_SEED).embeddings(POOL, space);
      for (int i = 0; i < POOL; i++) {
        bodies[i] = new DatasetGenerator(seed + NEW_DOC_OFFSET + i).text(WORDS_PER_DOC);
        embeddings[i] = literal(vectors[i]);
      }
    }

    // a word of the document long enough to be a term, and not a query operator
    private static String term(final String text, final SplittableRandom random) {
      final String[] words = text.split(" ");
      while (true) {
        final String word = words[random.nextInt(words.length)];
        if (word.length() >= 3 && !"and".equals(word) && !"not".equals(word))
          return word;
      }
    }

    @Override
    public OperationLoop.Operation open(final String loop, final int thread, final List<AutoCloseable> resources,
        final AtomicLong answered) {
      final SplittableRandom random = new SplittableRandom(seed * 31 + LOOPS.indexOf(loop) * 7L + thread);
      if ("timeseries".equals(loop)) {
        final LineProtocolClient client = new LineProtocolClient(server.wrapper(), DATABASE);
        resources.add(client);
        return timeseries(client, random);
      }
      final RemoteDatabase db = RemoteClients.open(server, DATABASE);
      resources.add(db);
      return switch (loop) {
        case "ingest" -> ingest(db, random);
        case "search" -> search(db, random, thread, answered);
        case "compaction" -> compaction(db);
        case "ddl" -> ddl(db);
        case "vector_churn" -> churn(db, random);
        default -> throw new IllegalArgumentException("Unknown loop: " + loop);
      };
    }

    private OperationLoop.Operation ingest(final RemoteDatabase db, final SplittableRandom random) {
      final StringBuilder script = new StringBuilder(INGEST_BATCH * (WORDS_PER_DOC * 10 + DIM * 12));
      return () -> {
        script.setLength(0);
        script.append("BEGIN;\n");
        final long first = nextId.getAndAdd(INGEST_BATCH);
        for (int i = 0; i < INGEST_BATCH; i++) {
          final int p = random.nextInt(POOL);
          script.append("INSERT INTO ").append(DOC_TYPE).append(" SET id = ").append(first + i).append(", body = '").append(bodies[p])
              .append("', embedding = ").append(embeddings[p]).append(";\n");
        }
        script.append("COMMIT;");
        db.command("sqlscript", script.toString()).close();
      };
    }

    private OperationLoop.Operation search(final RemoteDatabase db, final SplittableRandom random, final int thread,
        final AtomicLong answered) {
      // the two threads start on different kinds, so both kinds run at every moment
      final int[] turn = { thread };
      return () -> {
        final int q = random.nextInt(POOL);
        final boolean text = (turn[0]++ & 1) == 0;
        try (final ResultSet rs = text ?
            db.query("sql", BM25_SQL, Map.<String, Object>of("q", queryTerms[q])) :
            db.query("sql", KNN_SQL, Map.<String, Object>of("q", queryVectors[q]))) {
          if (rs.hasNext())
            answered.incrementAndGet();
          while (rs.hasNext())
            rs.next();
        }
      };
    }

    private static OperationLoop.Operation compaction(final RemoteDatabase db) {
      final int[] turn = { 0 };
      return () -> {
        db.command("sql", "COMPACT INDEX `" + COMPACTED[turn[0] % COMPACTED.length] + "`").close();
        turn[0]++;
      };
    }

    /**
     * A retried attempt (after a conflict) reuses the scratch type of the attempt it repeats, and every statement tolerates the
     * part of it an earlier attempt already did, so a conflict never leaves a type behind.
     */
    private OperationLoop.Operation ddl(final RemoteDatabase db) {
      final long[] current = { -1L };
      return () -> {
        if (current[0] < 0)
          current[0] = nextScratch.getAndIncrement();
        final String type = SCRATCH_PREFIX + current[0];
        db.command("sql", "CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS").close();
        db.command("sql", "CREATE PROPERTY " + type + ".v IF NOT EXISTS LONG").close();
        db.command("sql", "CREATE INDEX IF NOT EXISTS ON " + type + " (v) NOTUNIQUE").close();
        db.command("sql", "DROP TYPE " + type + " IF EXISTS UNSAFE").close();
        current[0] = -1L;
      };
    }

    private OperationLoop.Operation timeseries(final LineProtocolClient client, final SplittableRandom random) {
      final StringBuilder body = new StringBuilder(POINTS_PER_BATCH * 48);
      return () -> {
        body.setLength(0);
        // unique timestamps across every batch, so each acknowledged point is one more point of the type
        final long first = nextPoint.getAndAdd(POINTS_PER_BATCH);
        for (int i = 0; i < POINTS_PER_BATCH; i++)
          body.append(SENSOR_TYPE).append(",sensor_id=m").append(i % SENSORS).append(" value=").append(random.nextInt(10_000) / 100.0)
              .append(' ').append(TS_BASE + first + i).append('\n');
        client.write(body.toString());
      };
    }

    private OperationLoop.Operation churn(final RemoteDatabase db, final SplittableRandom random) {
      final StringBuilder script = new StringBuilder(CHURN_BATCH * (DIM * 12 + 64));
      return () -> {
        script.setLength(0);
        script.append("BEGIN;\n");
        for (int i = 0; i < CHURN_BATCH; i++)
          script.append("UPDATE ").append(DOC_TYPE).append(" SET embedding = ").append(embeddings[random.nextInt(POOL)])
              .append(" WHERE id = ").append(random.nextInt(baseDocs)).append(";\n");
        script.append("COMMIT;");
        db.command("sqlscript", script.toString()).close();
      };
    }
  }

  /** The counts and types of the database on the server under test, read with one client per check. */
  private record RemoteProbe(ServerUnderTest server) implements Probe {
    @Override
    public Reader open() {
      final RemoteDatabase db = RemoteClients.open(server, DATABASE);
      return new Reader() {
        @Override
        public long count(final String type) {
          return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + type);
        }

        @Override
        public List<String> types() {
          final List<String> names = new ArrayList<>();
          try (final ResultSet rs = db.query("sql", "SELECT name FROM schema:types")) {
            while (rs.hasNext())
              names.add(rs.next().getProperty("name"));
          }
          return names;
        }

        @Override
        public void dropType(final String type) {
          db.command("sql", "DROP TYPE " + type + " IF EXISTS UNSAFE").close();
        }

        @Override
        public void close() {
          db.close();
        }
      };
    }
  }
}
