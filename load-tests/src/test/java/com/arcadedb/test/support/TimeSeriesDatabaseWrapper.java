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
package com.arcadedb.test.support;

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;
import static com.arcadedb.test.support.ContainersTestTemplate.PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Drives and verifies a time series workload against one ArcadeDB server over one ingestion protocol.
 * Ingestion goes through the InfluxDB Line Protocol HTTP endpoint or SQL INSERT (HTTP/gRPC);
 * verification queries use SQL and the latest-value HTTP endpoint.
 */
public class TimeSeriesDatabaseWrapper implements AutoCloseable {

  public enum Protocol {LINE_PROTOCOL, SQL_HTTP, SQL_GRPC}

  /**
   * Aggregation result over a single time bucket.
   */
  public record AggResult(double avg, double min, double max, long count) {
  }

  public static final  String TYPE_NAME     = "sensor";
  public static final  String VERIFY_SENSOR = "verify";
  private static final Logger logger        = LoggerFactory.getLogger(TimeSeriesDatabaseWrapper.class);

  private final ServerWrapper  server;
  private final Protocol       protocol;
  private final RemoteDatabase db;
  private final Timer          pointsTimer;

  public TimeSeriesDatabaseWrapper(final ServerWrapper server, final Protocol protocol) {
    this.server = server;
    this.protocol = protocol;
    this.db = connect(protocol);
    this.pointsTimer = Metrics.timer("arcadedb.test.ts.inserted.points");
  }

  private RemoteDatabase connect(final Protocol protocol) {
    return switch (protocol) {
      case SQL_GRPC -> {
        final RemoteGrpcServer grpcServer = new RemoteGrpcServer(server.host(), server.grpcPort(), "root", PASSWORD, true,
            List.of());
        yield new RemoteGrpcDatabase(grpcServer, server.host(), server.grpcPort(), server.httpPort(), DATABASE, "root", PASSWORD);
      }
      case LINE_PROTOCOL, SQL_HTTP -> {
        final RemoteDatabase database = new RemoteDatabase(server.host(), server.httpPort(), DATABASE, "root", PASSWORD);
        database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        database.setTimeout(30000);
        yield database;
      }
    };
  }

  @Override
  public void close() {
    db.close();
  }

  /**
   * Creates (dropping any pre-existing) the test database. Retries while the Raft leader address
   * is not yet propagated or the server is temporarily unavailable during election.
   */
  public void createDatabase() {
    // RemoteServer owns its own HttpClient (RemoteHttpComponent), so close it to avoid leaking the
    // client and its NIO thread - this is a separate connection from the long-lived query `db`.
    final RemoteServer httpServer = new RemoteServer(server.host(), server.httpPort(), "root", PASSWORD);
    try {
      httpServer.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);

      if (httpServer.exists(DATABASE)) {
        logger.info("Dropping existing database {}", DATABASE);
        httpServer.drop(DATABASE);
      }
      logger.info("Creating database {}", DATABASE);

      final int maxAttempts = 30;
      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          httpServer.create(DATABASE);
          return;
        } catch (final ServerIsNotTheLeaderException e) {
          if (e.getLeaderAddress() != null || attempt == maxAttempts)
            throw e;
          logger.info("Leader address not yet known (attempt {}/{}), retrying in 2s...", attempt, maxAttempts);
        } catch (final Exception e) {
          if (attempt == maxAttempts)
            throw e;
          logger.info("Database creation attempt {}/{} failed ({}), retrying in 2s...", attempt, maxAttempts, e.getMessage());
        }
        sleep(2_000);
      }
    } finally {
      httpServer.close();
    }
  }

  /**
   * Creates the {@code sensor} time series type. Retries while the database is still being
   * replicated/opened after Raft creation.
   */
  public void createSchema() {
    final int maxAttempts = 30;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        db.command("sql",
            "CREATE TIMESERIES TYPE " + TYPE_NAME
                + " TIMESTAMP ts TAGS (sensor_id STRING, region STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");
        return;
      } catch (final Exception e) {
        if (attempt == maxAttempts)
          throw e;
        if (e.getMessage() == null || !e.getMessage().contains("not available")) {
          logger.warn("Schema creation failed with unexpected error on attempt {}: {}", attempt, e.getMessage());
          throw e;
        }
        logger.info("Database not yet available for schema creation (attempt {}/{}): {}", attempt, maxAttempts, e.getMessage());
        sleep(2_000);
      }
    }
  }

  /**
   * Waits until the {@code sensor} time series type is queryable on this node. On a Raft cluster the
   * type is created on the leader and applied on followers asynchronously, and a connection opened
   * before creation caches an empty schema, so this polls a server-side query (which always hits the
   * node and bypasses any client-side schema cache) rather than asserting a cached {@code existsType}.
   */
  public void checkSchema() {
    final long deadline = System.currentTimeMillis() + 30_000;
    Exception last = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME).next();
        return;
      } catch (final Exception e) {
        last = e;
        sleep(1_000);
      }
    }
    throw new AssertionError("Time series type '" + TYPE_NAME + "' not available after 30s", last);
  }

  /**
   * Ingests {@code count} bulk points for one sensor with strictly increasing, unique timestamps
   * starting at {@code fromTs}. Field values are deterministic but irrelevant to assertions.
   * <p>
   * Note: the protocols are intentionally not throughput-comparable. {@code LINE_PROTOCOL} batches
   * up to 500 points per HTTP call, while the SQL protocols send one {@code INSERT} per round-trip
   * (one transaction per point) - the per-row SQL path is kept deliberately so it exercises the
   * single-row append path under concurrency. Treat per-protocol timings as independent.
   */
  public void ingestSeries(final String sensorId, final String region, final long fromTs, final int count) {
    if (protocol == Protocol.LINE_PROTOCOL) {
      final int batchSize = 500;
      final StringBuilder sb = new StringBuilder();
      int inBatch = 0;
      for (int i = 0; i < count; i++) {
        sb.append(lineFor(sensorId, region, fromTs + i, 15.0 + (i % 20), 40.0 + (i % 30))).append('\n');
        if (++inBatch == batchSize) {
          final String body = sb.toString();
          pointsTimer.record(() -> postLineProtocol(body));
          sb.setLength(0);
          inBatch = 0;
        }
      }
      if (inBatch > 0) {
        final String body = sb.toString();
        pointsTimer.record(() -> postLineProtocol(body));
      }
    } else {
      for (int i = 0; i < count; i++) {
        final int idx = i;
        final long ts = fromTs + i;
        pointsTimer.record(() -> insertSql(sensorId, region, ts, 15.0 + (idx % 20), 40.0 + (idx % 30)));
      }
    }
  }

  /**
   * Ingests the deterministic 4-point verification series.
   */
  public void ingestVerificationSeries() {
    ingestPoint(VERIFY_SENSOR, "eu-west", 0L, 10.0, 40.0);
    ingestPoint(VERIFY_SENSOR, "eu-west", 1000L, 20.0, 50.0);
    ingestPoint(VERIFY_SENSOR, "eu-west", 2000L, 30.0, 60.0);
    ingestPoint(VERIFY_SENSOR, "eu-west", 3000L, 40.0, 70.0);
  }

  private void ingestPoint(final String sensorId, final String region, final long ts, final double temperature,
      final double humidity) {
    if (protocol == Protocol.LINE_PROTOCOL)
      postLineProtocol(lineFor(sensorId, region, ts, temperature, humidity));
    else
      insertSql(sensorId, region, ts, temperature, humidity);
  }

  private static String lineFor(final String sensorId, final String region, final long ts, final double temperature,
      final double humidity) {
    return TYPE_NAME + ",sensor_id=" + sensorId + ",region=" + region
        + " temperature=" + temperature + ",humidity=" + humidity + " " + ts;
  }

  private void insertSql(final String sensorId, final String region, final long ts, final double temperature,
      final double humidity) {
    try {
      db.command("sql",
          "INSERT INTO " + TYPE_NAME + " SET ts = ?, sensor_id = ?, region = ?, temperature = ?, humidity = ?",
          ts, sensorId, region, temperature, humidity);
    } catch (final Exception e) {
      Metrics.counter("arcadedb.test.ts.inserted.points.error").increment();
      logger.error("SQL ingest error at ts {}: {}", ts, e.getMessage());
    }
  }

  private void postLineProtocol(final String body) {
    HttpURLConnection conn = null;
    try {
      final URL url = URI.create(
          "http://" + server.host() + ":" + server.httpPort() + "/api/v1/ts/" + DATABASE + "/write?precision=ms").toURL();
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization", basicAuth());
      conn.setRequestProperty("Content-Type", "text/plain");
      conn.setConnectTimeout(5_000);
      conn.setReadTimeout(30_000);
      conn.setDoOutput(true);
      try (final OutputStream os = conn.getOutputStream()) {
        os.write(body.getBytes(StandardCharsets.UTF_8));
      }
      final int status = conn.getResponseCode();
      if (status != 204) {
        String err = "";
        final InputStream errorStream = conn.getErrorStream();
        if (errorStream != null) {
          try (errorStream) {
            err = new String(errorStream.readAllBytes(), StandardCharsets.UTF_8);
          }
        }
        throw new IOException("Line protocol write failed: HTTP " + status + " " + err);
      }
    } catch (final Exception e) {
      Metrics.counter("arcadedb.test.ts.inserted.points.error").increment();
      logger.error("Line protocol ingest error: {}", e.getMessage());
    } finally {
      if (conn != null)
        conn.disconnect();
    }
  }

  public long countPoints() {
    final ResultSet rs = db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME);
    return ((Number) rs.next().getProperty("cnt")).longValue();
  }

  /**
   * Runs a single-bucket time-bucket aggregation over the given sensor and returns the first row,
   * or {@code null} if no rows are visible yet. The time-series aggregation reads sealed data only,
   * so callers should target a fully-flushed sensor (a bulk series) and poll (see
   * {@link #assertAggregateExtremes}) to absorb seal/replication lag. This is a single query with no
   * internal retry, so the caller owns the single polling loop.
   */
  public AggResult aggregate(final String sensorId) {
    // 'bucket' is a reserved word in ArcadeDB SQL, so the time-bucket alias is 'tbucket'.
    final String sql = "SELECT ts.timeBucket('1h', ts) AS tbucket, avg(temperature) AS avgT, min(temperature) AS minT, "
        + "max(temperature) AS maxT, count(*) AS cnt FROM " + TYPE_NAME
        + " WHERE sensor_id = ? GROUP BY tbucket ORDER BY tbucket";
    final ResultSet rs = db.query("sql", sql, sensorId);
    if (!rs.hasNext())
      return null;
    final Result r = rs.next();
    return new AggResult(toDouble(r.getProperty("avgT")), toDouble(r.getProperty("minT")), toDouble(r.getProperty("maxT")),
        ((Number) r.getProperty("cnt")).longValue());
  }

  /**
   * Asserts the time-bucket aggregation reports the expected min/max temperature extremes for the
   * given (bulk) sensor, polling (single level, up to 60s) to absorb asynchronous sealing. The
   * aggregation reflects only the sealed subset of points, which grows over time, so the exact
   * aggregated count and average are not deterministic; the extremes are: the bulk temperature
   * cycles 15.0..34.0 every 20 points, so as soon as one full cycle is sealed the min/max settle at
   * their final values. The exact total is verified separately via {@link #countPoints()} (the
   * unfiltered count, which sees all data, mutable and sealed - unlike tag-filtered queries, which
   * see only the sealed subset).
   */
  public void assertAggregateExtremes(final String sensorId, final double expectedMin, final double expectedMax) {
    final double tolerance = 1e-9;
    final long deadline = System.currentTimeMillis() + 60_000;
    AggResult agg = null;
    while (System.currentTimeMillis() < deadline) {
      agg = aggregate(sensorId);
      if (agg != null && Math.abs(agg.min() - expectedMin) < tolerance && Math.abs(agg.max() - expectedMax) < tolerance)
        return;
      sleep(2_000);
    }
    assertThat(agg).as("time-bucket aggregation returned rows for sensor_id=%s", sensorId).isNotNull();
    assertThat(agg.min()).isCloseTo(expectedMin, within(tolerance));
    assertThat(agg.max()).isCloseTo(expectedMax, within(tolerance));
  }

  /**
   * Returns the latest timestamp via the HTTP latest endpoint. {@code tag} may be null or "name:value".
   */
  public long latestTimestamp(final String tag) throws IOException {
    final StringBuilder url = new StringBuilder(
        "http://" + server.host() + ":" + server.httpPort() + "/api/v1/ts/" + DATABASE + "/latest?type=" + TYPE_NAME);
    if (tag != null)
      url.append("&tag=").append(URLEncoder.encode(tag, StandardCharsets.UTF_8));

    final HttpURLConnection conn = (HttpURLConnection) URI.create(url.toString()).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization", basicAuth());
    conn.setConnectTimeout(5_000);
    conn.setReadTimeout(5_000);
    try {
      if (conn.getResponseCode() != 200)
        throw new IOException("Latest query failed: HTTP " + conn.getResponseCode());
      final JSONObject json;
      try (final InputStream is = conn.getInputStream()) {
        json = new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
      }
      final JSONArray latest = json.getJSONArray("latest");
      return latest.getLong(0);
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Asserts the total point count equals {@code expected}, retrying up to 30s to tolerate replication lag.
   */
  public void assertThatPointCountIs(final long expected) {
    final long deadline = System.currentTimeMillis() + 30_000;
    long actual = -1;
    Exception lastException = null;
    do {
      try {
        actual = countPoints();
        if (actual == expected)
          return;
        lastException = null;
      } catch (final Exception e) {
        lastException = e;
      }
      if (System.currentTimeMillis() < deadline)
        sleep(2_000);
    } while (System.currentTimeMillis() < deadline);
    if (lastException != null)
      throw new AssertionError(
          "Expected point count " + expected + " but the database was not available after 30s: " + lastException.getMessage(),
          lastException);
    assertThat(actual).isEqualTo(expected);
  }

  private static double toDouble(final Object o) {
    return ((Number) o).doubleValue();
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + PASSWORD).getBytes(StandardCharsets.UTF_8));
  }

  private static void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
