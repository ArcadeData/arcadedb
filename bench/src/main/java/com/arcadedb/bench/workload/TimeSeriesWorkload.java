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
import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.test.support.TimeSeriesDatabaseWrapper;
import com.arcadedb.test.support.TimeSeriesDatabaseWrapper.AggResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;

/**
 * Time series over the {@code sensor} type of the load tests ({@link TimeSeriesDatabaseWrapper}: tags sensor_id and region, fields
 * temperature and humidity).
 *
 * <p>The two ingest cases write {@link #pointsPerRep} points per repetition into sensors no earlier repetition used. The read cases
 * query a dataset of {@link #sensors} sensors with {@link #pointsPerSensor} points each (one point per second), loaded once in
 * {@link #prepare} and never inside a timed region. Every {@link #prepare} ends with the correctness check: the aggregate of the
 * verification series must match its known count and extremes, which fails the case otherwise.
 */
public class TimeSeriesWorkload implements Workload {
  static final String INGEST_LP    = "ingest-line-protocol";
  static final String INGEST_SQL   = "ingest-sql";
  static final String RANGE        = "range-query";
  static final String AGGREGATE    = "aggregate-query";
  static final String AGGREGATE_CA = "aggregate-continuous";

  static final int BATCH             = 500;
  static final int QUERIES_PER_REP   = 1_000;
  static final int VERIFY_POINTS     = 1_000;
  /** One point per second, so a window of one hour holds this many points of a sensor. */
  static final int POINTS_PER_WINDOW = 3_600;

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesWorkload.class);

  private static final long   STEP_MS               = 1_000L;
  private static final long   HOUR_MS               = 3_600_000L;
  /** Aligned to an hour, so the verification series (points 0..999 s after it) falls in one bucket. */
  private static final long   BASE_TS               = HOUR_MS * 472_222L;
  private static final long   VERIFY_BASE_TS        = 0L;
  private static final int    INGEST_SENSORS        = 20;
  private static final long   VERIFY_TIMEOUT_MS     = 60_000L;
  private static final long   DATASET_TIMEOUT_MS    = 180_000L;
  private static final long   VERIFY_POLL_MS        = 1_000L;
  private static final double VERIFY_MIN            = 15.0;
  private static final double VERIFY_MAX            = 34.0;
  private static final double TOLERANCE             = 1e-9;
  private static final String CONTINUOUS_AGGREGATE  = "sensor_hourly";
  private static final String REGION                = "eu-west";

  /** Distinct per repetition of this run, so ingested sensors never repeat. */
  private int reps;

  static int pointsPerRep(final Scale scale) {
    return switch (scale) {
      case XS -> 20 * BATCH;
      case S -> 1_000 * BATCH;
      case M -> 2_000 * BATCH;
      case L -> 4_000 * BATCH;
    };
  }

  static int sensors(final Scale scale) {
    return switch (scale) {
      case XS -> 10;
      case S -> 50;
      case M -> 100;
      case L -> 200;
    };
  }

  static int pointsPerSensor(final Scale scale) {
    return switch (scale) {
      case XS -> 2 * POINTS_PER_WINDOW;
      case S -> 4 * POINTS_PER_WINDOW;
      case M -> 12 * POINTS_PER_WINDOW;
      case L -> 24 * POINTS_PER_WINDOW;
    };
  }

  @Override
  public String name() {
    return "timeseries";
  }

  @Override
  public List<String> cases() {
    return List.of(INGEST_LP, INGEST_SQL, RANGE, AGGREGATE, AGGREGATE_CA);
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) throws InterruptedException {
    if (!cases().contains(caseName))
      throw new IllegalArgumentException("Unknown case: " + caseName);
    // every case draws the same query windows and sensor names, alone or in the nightly sequence
    reps = 0;
    try (final TimeSeriesDatabaseWrapper admin = new TimeSeriesDatabaseWrapper(ctx.server().wrapper(),
        TimeSeriesDatabaseWrapper.Protocol.LINE_PROTOCOL)) {
      final boolean reads = !INGEST_LP.equals(caseName) && !INGEST_SQL.equals(caseName);
      if (!reads || !datasetReady(admin, ctx.scale()))
        createDatabase(ctx, admin, reads);
      awaitVerification(admin);
      if (reads)
        awaitDatasetSealed(ctx);
      if (AGGREGATE_CA.equals(caseName))
        ensureContinuousAggregate(ctx);
    }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws InterruptedException {
    final int rep = reps++;
    return switch (caseName) {
      case INGEST_LP -> ingest(ctx, rep, "lp", false);
      case INGEST_SQL -> ingest(ctx, rep, "sql", true);
      case RANGE -> query(ctx, rep, RANGE, false);
      case AGGREGATE -> query(ctx, rep, AGGREGATE, false);
      case AGGREGATE_CA -> query(ctx, rep, AGGREGATE_CA, true);
      default -> throw new IllegalArgumentException("Unknown case: " + caseName);
    };
  }

  /** One sample per batch of {@link #BATCH} points; the clock covers the request only, the batch text is built before it. */
  private Measurement ingest(final WorkloadContext ctx, final int rep, final String prefix, final boolean sql)
      throws InterruptedException {
    final int points = pointsPerRep(ctx.scale());
    final LatencyRecorder latencies = new LatencyRecorder(points / BATCH);
    final long before = countPoints(ctx);
    ctx.server().resetPeak();
    ingestBurst(ctx, rep, prefix, points, sql, false, latencies);
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("points_per_s", "points/s", points / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    metrics.add(Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes()));
    awaitPoints(ctx, before + points);
    return Measurement.of(metrics, latencies);
  }

  /**
   * One burst of {@code points} points in batches of {@link #BATCH}. By default every batch goes to a sensor no earlier repetition
   * used; {@code inDataset} writes into the sensors of the loaded dataset instead, at timestamps inside its hours but between its
   * one-second points, so the continuous aggregate is maintained without gaining rows.
   */
  private void ingestBurst(final WorkloadContext ctx, final int rep, final String prefix, final int points, final boolean sql,
      final boolean inDataset, final LatencyRecorder latencies) {
    final int datasetSensors = sensors(ctx.scale());
    final int perSensor = pointsPerSensor(ctx.scale());
    try (final LineProtocolClient lineProtocol = sql ? null : new LineProtocolClient(ctx.server().wrapper(), DATABASE);
        final RemoteDatabase db = sql ? RemoteClients.open(ctx.server(), DATABASE) : null) {
      final StringBuilder body = new StringBuilder(BATCH * 96);
      for (int from = 0; from < points; from += BATCH) {
        final int batch = from / BATCH;
        final String sensor = inDataset ? "s-" + (batch % datasetSensors) : "ingest-" + prefix + "-" + rep + "-" + (batch % INGEST_SENSORS);
        body.setLength(0);
        for (int i = from; i < Math.min(from + BATCH, points); i++) {
          final long ts = inDataset ? BASE_TS + (i % perSensor) * STEP_MS + 1 + rep % (STEP_MS - 1) : BASE_TS + i * STEP_MS;
          if (sql)
            appendInsert(body, sensor, ts, i);
          else
            appendLine(body, sensor, ts, i);
        }
        final String text = body.toString();
        final long t0 = System.nanoTime();
        if (sql)
          db.command("sqlscript", text).close();
        else
          lineProtocol.write(text);
        latencies.record(System.nanoTime() - t0);
      }
    }
  }

  /** The unfiltered point count of the type: it sees mutable and sealed data, unlike a tag filtered query. */
  private static long countPoints(final WorkloadContext ctx) {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      return RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + TimeSeriesDatabaseWrapper.TYPE_NAME);
    }
  }

  /** Untimed: the repetition's points must all have arrived, or its times measured a write that lost data. */
  private static void awaitPoints(final WorkloadContext ctx, final long expected) throws InterruptedException {
    final long deadline = System.nanoTime() + VERIFY_TIMEOUT_MS * 1_000_000L;
    long seen = countPoints(ctx);
    while (seen != expected && System.nanoTime() < deadline) {
      Thread.sleep(VERIFY_POLL_MS);
      seen = countPoints(ctx);
    }
    if (seen != expected)
      throw new IllegalStateException("The ingest repetition left " + seen + " points in the database, expected " + expected
          + " after " + VERIFY_TIMEOUT_MS / 1000 + " s");
  }

  /** The window queries; {@code continuous} reads the continuous aggregate and then ingests while it is maintained. */
  private Measurement query(final WorkloadContext ctx, final int rep, final String caseName, final boolean continuous) {
    final Random random = new Random(ctx.seed() + rep);
    final int sensors = sensors(ctx.scale());
    final int span = pointsPerSensor(ctx.scale()) - POINTS_PER_WINDOW;
    final LatencyRecorder latencies = new LatencyRecorder(QUERIES_PER_REP);
    int emptyAnswers = 0;
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      ctx.server().resetPeak();
      for (int i = 0; i < QUERIES_PER_REP; i++) {
        final String sensor = "s-" + random.nextInt(sensors);
        final long from = BASE_TS + random.nextInt(span + 1) * STEP_MS;
        final long to = from + HOUR_MS;
        final long t0 = System.nanoTime();
        final int rows = drain(run(db, caseName, sensor, from, to));
        latencies.record(System.nanoTime() - t0);
        if (rows == 0)
          emptyAnswers++;
      }
    }
    checkAnswered(caseName, emptyAnswers, QUERIES_PER_REP);
    final List<Metric> metrics = new ArrayList<>();
    metrics.add(Metrics.rate("queries_per_s", "queries/s", QUERIES_PER_REP / (latencies.totalNanos() / 1e9)));
    metrics.addAll(Metrics.latencies(latencies));
    metrics.add(Metrics.bytes("peak_rss_bytes", ctx.server().peakRssBytes()));
    if (continuous) {
      final int points = pointsPerRep(ctx.scale());
      final LatencyRecorder batches = new LatencyRecorder(points / BATCH);
      // the burst lands inside the dataset's sensors and hours, so the aggregate keeps its row count, only its values move
      ingestBurst(ctx, rep, "ca", points, false, true, batches);
      metrics.add(Metrics.rate("ingest_points_per_s_maintained", "points/s", points / (batches.totalNanos() / 1e9)));
    }
    return Measurement.of(metrics, latencies);
  }

  private static ResultSet run(final RemoteDatabase db, final String caseName, final String sensor, final long from, final long to) {
    return switch (caseName) {
      case RANGE -> db.query("sql", "SELECT ts, temperature, humidity FROM " + TimeSeriesDatabaseWrapper.TYPE_NAME
          + " WHERE sensor_id = ? AND ts BETWEEN ? AND ?", sensor, from, to);
      case AGGREGATE -> db.query("sql", "SELECT ts.timeBucket('1h', ts) AS tbucket, avg(temperature) AS avgT, min(temperature) AS minT,"
          + " max(temperature) AS maxT FROM " + TimeSeriesDatabaseWrapper.TYPE_NAME
          + " WHERE sensor_id = ? AND ts BETWEEN ? AND ? GROUP BY tbucket", sensor, from, to);
      default -> db.query("sql", "SELECT hr, avgT, minT, maxT FROM " + CONTINUOUS_AGGREGATE
          + " WHERE sensor_id = ? AND hr BETWEEN ? AND ?", sensor, from, to);
    };
  }

  private static int drain(final ResultSet rs) {
    int rows = 0;
    try (rs) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** Every window lies inside the loaded data, so an empty answer means the query timed nothing (data not sealed, wrong filter). */
  static void checkAnswered(final String caseName, final int emptyAnswers, final int queries) {
    if (emptyAnswers > 0)
      throw new IllegalStateException(caseName + ": " + emptyAnswers + " of " + queries
          + " queries returned no rows, the measured times are not valid");
  }

  /** True when the database holds exactly this scale's dataset and the verification series. */
  private boolean datasetReady(final TimeSeriesDatabaseWrapper admin, final Scale scale) {
    try {
      return admin.countPoints() == (long) sensors(scale) * pointsPerSensor(scale) + VERIFY_POINTS;
    } catch (final RuntimeException e) {
      // a missing database or type means "build"; the first build of a run always lands here
      final String message = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
      if (message.contains("not found") || message.contains("does not exist") || message.contains("not exist"))
        return false;
      throw e;
    }
  }

  /** Untimed: a fresh database and schema, the verification series and, for a read case, the dataset. */
  private void createDatabase(final WorkloadContext ctx, final TimeSeriesDatabaseWrapper admin, final boolean withDataset) {
    LOGGER.info("Building the time series database{}", withDataset ? " with its dataset" : "");
    admin.createDatabase();
    admin.createSchema();
    try (final LineProtocolClient lineProtocol = new LineProtocolClient(ctx.server().wrapper(), DATABASE)) {
      lineProtocol.write(verificationBody());
      if (!withDataset)
        return;
      final int sensors = sensors(ctx.scale());
      final int perSensor = pointsPerSensor(ctx.scale());
      final StringBuilder body = new StringBuilder(BATCH * 96);
      for (int s = 0; s < sensors; s++) {
        final String sensor = "s-" + s;
        for (int from = 0; from < perSensor; from += BATCH) {
          body.setLength(0);
          for (int i = from; i < Math.min(from + BATCH, perSensor); i++)
            appendLine(body, sensor, BASE_TS + i * STEP_MS, i);
          lineProtocol.write(body.toString());
        }
      }
    }
    LOGGER.info("Time series dataset loaded: {} points", (long) sensors(ctx.scale()) * pointsPerSensor(ctx.scale()));
  }

  /**
   * A tag filtered query sees only sealed data, and the verification series is written before the dataset, so it says nothing
   * about the dataset: wait until the first and the last loaded sensor answer their full count.
   */
  private static void awaitDatasetSealed(final WorkloadContext ctx) throws InterruptedException {
    final int sensors = sensors(ctx.scale());
    final long expected = pointsPerSensor(ctx.scale());
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      for (final int s : new int[] { 0, sensors - 1 }) {
        final String query = "SELECT count(*) AS n FROM " + TimeSeriesDatabaseWrapper.TYPE_NAME + " WHERE sensor_id = 's-" + s + "'";
        final long deadline = System.nanoTime() + DATASET_TIMEOUT_MS * 1_000_000L;
        long seen = RemoteClients.count(db, "sql", query);
        while (seen != expected && System.nanoTime() < deadline) {
          Thread.sleep(VERIFY_POLL_MS);
          seen = RemoteClients.count(db, "sql", query);
        }
        if (seen != expected)
          throw new IllegalStateException("Sensor s-" + s + " answers " + seen + " points of " + expected + " after "
              + DATASET_TIMEOUT_MS / 1000 + " s, the dataset is not queryable yet");
      }
    }
  }

  private void ensureContinuousAggregate(final WorkloadContext ctx) throws InterruptedException {
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), DATABASE)) {
      if (!db.getSchema().existsType(CONTINUOUS_AGGREGATE))
        db.command("sql", "CREATE CONTINUOUS AGGREGATE " + CONTINUOUS_AGGREGATE + " AS SELECT ts.timeBucket('1h', ts) AS hr, sensor_id,"
            + " avg(temperature) AS avgT, min(temperature) AS minT, max(temperature) AS maxT FROM "
            + TimeSeriesDatabaseWrapper.TYPE_NAME + " GROUP BY hr, sensor_id").close();
      // one row per sensor and hour of the dataset, plus the verification hour
      final long expectedRows = (long) sensors(ctx.scale()) * (pointsPerSensor(ctx.scale()) / POINTS_PER_WINDOW) + 1;
      final long deadline = System.nanoTime() + DATASET_TIMEOUT_MS * 1_000_000L;
      long rows = RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + CONTINUOUS_AGGREGATE);
      while (rows < expectedRows && System.nanoTime() < deadline) {
        Thread.sleep(VERIFY_POLL_MS);
        rows = RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM " + CONTINUOUS_AGGREGATE);
      }
      if (rows < expectedRows)
        throw new IllegalStateException("The continuous aggregate " + CONTINUOUS_AGGREGATE + " has " + rows + " rows of at least "
            + expectedRows + " after " + DATASET_TIMEOUT_MS / 1000 + " s");
    }
  }

  /** Polls the verification aggregate until it is right: sealing makes the aggregate lag the ingest. The last answer is in the failure. */
  private static void awaitVerification(final TimeSeriesDatabaseWrapper admin) throws InterruptedException {
    final long deadline = System.nanoTime() + VERIFY_TIMEOUT_MS * 1_000_000L;
    while (true) {
      try {
        checkVerification(admin.aggregate(TimeSeriesDatabaseWrapper.VERIFY_SENSOR), VERIFY_POINTS, VERIFY_MIN, VERIFY_MAX);
        return;
      } catch (final IllegalStateException e) {
        if (System.nanoTime() >= deadline)
          throw new IllegalStateException("Verification series not right after " + VERIFY_TIMEOUT_MS / 1000 + " s: " + e.getMessage(), e);
      }
      Thread.sleep(VERIFY_POLL_MS);
    }
  }

  /** Fails when the aggregate of the verification series is not the known count and extremes. */
  static void checkVerification(final AggResult got, final long expectedCount, final double expectedMin, final double expectedMax) {
    if (got == null)
      throw new IllegalStateException("The verification aggregate returned no rows, expected count " + expectedCount + ", min "
          + expectedMin + ", max " + expectedMax);
    if (got.count() != expectedCount || Math.abs(got.min() - expectedMin) > TOLERANCE || Math.abs(got.max() - expectedMax) > TOLERANCE)
      throw new IllegalStateException("The verification aggregate is wrong: expected count " + expectedCount + ", min " + expectedMin
          + ", max " + expectedMax + "; got count " + got.count() + ", min " + got.min() + ", max " + got.max());
  }

  /** The verification series: {@link #VERIFY_POINTS} points, one second apart from time 0 (one hour bucket), temperature 15.0 to 34.0. */
  static String verificationBody() {
    final StringBuilder body = new StringBuilder(VERIFY_POINTS * 96);
    for (int i = 0; i < VERIFY_POINTS; i++)
      appendLine(body, TimeSeriesDatabaseWrapper.VERIFY_SENSOR, VERIFY_BASE_TS + i * STEP_MS, i);
    return body.toString();
  }

  private static void appendLine(final StringBuilder body, final String sensor, final long ts, final int i) {
    body.append(TimeSeriesDatabaseWrapper.TYPE_NAME).append(",sensor_id=").append(sensor).append(",region=").append(REGION)
        .append(" temperature=").append(15.0 + (i % 20)).append(",humidity=").append(40.0 + (i % 30)).append(' ').append(ts)
        .append('\n');
  }

  private static void appendInsert(final StringBuilder body, final String sensor, final long ts, final int i) {
    body.append("INSERT INTO ").append(TimeSeriesDatabaseWrapper.TYPE_NAME).append(" SET ts = ").append(ts).append(", sensor_id = '")
        .append(sensor).append("', region = '").append(REGION).append("', temperature = ").append(15.0 + (i % 20))
        .append(", humidity = ").append(40.0 + (i % 30)).append(";\n");
  }
}
