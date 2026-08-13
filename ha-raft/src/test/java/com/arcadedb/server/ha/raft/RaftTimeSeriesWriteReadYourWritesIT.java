/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.ReadConsistency;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;

import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5866: {@code PostTimeSeriesWriteHandler} (InfluxDB line protocol ingestion)
 * and {@code PostPrometheusWriteHandler} (Prometheus remote-write) both extend
 * {@code AbstractServerHttpHandler} directly rather than {@code DatabaseAbstractHandler}, and commit
 * their own transaction outside the generic per-request wrapper - the same root cause issue #5862 fixed
 * for {@code PostBatchHandler}. Before the fix, neither endpoint's response ever carried the
 * {@code X-ArcadeDB-Commit-Index} bookmark, so a {@code READ_YOUR_WRITES} client ingesting metrics through
 * either protocol had no bookmark to carry into its next read.
 * <p>
 * Each test writes through the raw HTTP endpoint (neither protocol has a {@code RemoteDatabase}-level
 * client helper), captures the response header directly, and then proves the captured value is a usable
 * bookmark by seeding a follower {@link RemoteDatabase} connection with it and reading the just-ingested
 * data back under {@link ReadConsistency#READ_YOUR_WRITES}.
 */
class RaftTimeSeriesWriteReadYourWritesIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void lineProtocolWriteEmitsCommitIndexBookmarkUsableOnFollower() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String tsType = "RywWeather";

    try (final RemoteDatabase setup = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      setup.command("sql",
          "CREATE TIMESERIES TYPE " + tsType + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    }

    final String lineProtocol = tsType + ",location=us-east temperature=22.5 1000\n";
    final HttpURLConnection connection = openLineProtocolConnection(leaderPort, password);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(lineProtocol.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
    assertThat(connection.getResponseCode()).isEqualTo(204);

    final String bookmarkHeader = connection.getHeaderField("X-ArcadeDB-Commit-Index");
    assertThat(bookmarkHeader)
        .as("PostTimeSeriesWriteHandler must emit the X-ArcadeDB-Commit-Index bookmark on its response (issue #5866)")
        .isNotNull();
    final long writerIndex = Long.parseLong(bookmarkHeader);
    assertThat(writerIndex).isGreaterThanOrEqualTo(0);

    try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
      reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      // Seed the reader with the writer's commit index via reflection, since updateLastCommitIndex is
      // package-private in com.arcadedb.remote (same technique as RaftRemoteReadYourWritesIT).
      final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
      updateMethod.setAccessible(true);
      updateMethod.invoke(reader, writerIndex);

      final ResultSet rs = reader.query("sql", "SELECT FROM " + tsType);
      assertThat(rs.stream().count())
          .as("Follower must see the line-protocol-ingested sample when using READ_YOUR_WRITES with the "
              + "handler's bookmark")
          .isEqualTo(1);
    }
  }

  /**
   * Regression test for the fourth review round's coverage gap: the PR body's own stated motivation is that
   * a partial-write 400 still needs a usable bookmark, since {@code TimeSeriesEngine.appendBatch} commits
   * each measurement's shard transaction independently - a later measurement being dropped as unknown
   * doesn't undo an earlier one that already landed. This drives that exact scenario end-to-end: one valid
   * measurement plus one unknown measurement in the same request (matching the non-HA
   * {@code PostTimeSeriesWriteHandlerIT#partialWriteReportsDroppedMeasurements} semantics), asserts the 400
   * response still carries the bookmark, and that the bookmark is usable to read the surviving sample back
   * from a follower.
   */
  @Test
  void lineProtocolPartialWrite400EmitsUsableCommitIndexBookmark() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String tsType = "RywPartialWriteWeather";

    try (final RemoteDatabase setup = new RemoteDatabase("127.0.0.1", leaderPort, dbName, "root", password)) {
      setup.command("sql",
          "CREATE TIMESERIES TYPE " + tsType + " TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");
    }

    // One valid measurement (tsType) plus one unknown measurement: the handler inserts the valid sample,
    // then reports 400 naming the dropped one (issue #5036 semantics) - a partial write, not a full rollback.
    final String lineProtocol = tsType + ",location=us-east temperature=22.5 1000\n"
        + "ryw_unknown_measurement,location=us value=1.0 2000\n";
    final HttpURLConnection connection = openLineProtocolConnection(leaderPort, password);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(lineProtocol.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }
    assertThat(connection.getResponseCode())
        .as("The unknown measurement must still produce a partial-write 400 (issue #5036)")
        .isEqualTo(400);

    final String bookmarkHeader = connection.getHeaderField("X-ArcadeDB-Commit-Index");
    assertThat(bookmarkHeader)
        .as("A partial-write 400 must still carry the bookmark for the measurement that did commit (issue #5866)")
        .isNotNull();
    final long writerIndex = Long.parseLong(bookmarkHeader);
    assertThat(writerIndex).isGreaterThanOrEqualTo(0);

    try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
      reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
      updateMethod.setAccessible(true);
      updateMethod.invoke(reader, writerIndex);

      final ResultSet rs = reader.query("sql", "SELECT FROM " + tsType);
      assertThat(rs.stream().count())
          .as("Follower must see the sample that DID commit despite the request's overall 400, using "
              + "READ_YOUR_WRITES with the partial-write response's bookmark")
          .isEqualTo(1);
    }
  }

  @Test
  void prometheusRemoteWriteEmitsCommitIndexBookmarkUsableOnFollower() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final int followerIndex = leaderIndex == 0 ? 1 : 0;
    final int leaderPort = 2480 + leaderIndex;
    final int followerPort = 2480 + followerIndex;
    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String metricName = "ryw_cpu_usage";

    final WriteRequest writeRequest = new WriteRequest(List.of(
        new TimeSeries(
            List.of(new Label("__name__", metricName), new Label("host", "server1")),
            List.of(new Sample(75.5, 1000))
        )
    ));
    final byte[] compressed = Snappy.compress(writeRequest.encode());

    final HttpURLConnection connection = openPrometheusWriteConnection(leaderPort, password);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressed);
      os.flush();
    }
    assertThat(connection.getResponseCode()).isEqualTo(204);

    final String bookmarkHeader = connection.getHeaderField("X-ArcadeDB-Commit-Index");
    assertThat(bookmarkHeader)
        .as("PostPrometheusWriteHandler must emit the X-ArcadeDB-Commit-Index bookmark on its response (issue #5866)")
        .isNotNull();
    final long writerIndex = Long.parseLong(bookmarkHeader);
    assertThat(writerIndex).isGreaterThanOrEqualTo(0);

    try (final RemoteDatabase reader = new RemoteDatabase("127.0.0.1", followerPort, dbName, "root", password)) {
      reader.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      final var updateMethod = RemoteDatabase.class.getDeclaredMethod("updateLastCommitIndex", long.class);
      updateMethod.setAccessible(true);
      updateMethod.invoke(reader, writerIndex);

      final ResultSet rs = reader.query("sql", "SELECT FROM " + metricName);
      assertThat(rs.stream().count())
          .as("Follower must see the Prometheus remote-write sample when using READ_YOUR_WRITES with the "
              + "handler's bookmark")
          .isEqualTo(1);
    }
  }

  /**
   * Regression test for the second and third review rounds on this issue. Two earlier revisions of the fix
   * each moved {@code database} resolution further ahead - first past the request-body/Snappy checks, then
   * past those but still ahead of the {@code writeRequest.getTimeSeries().isEmpty()} short-circuit - trying
   * to give that 204 a bookmark too. Both times, {@code httpServer.getServer().getDatabase(..., allowLoad=
   * false)} throwing {@code DatabaseOperationException} for an absent/closed database turned some
   * nonexistent-database request into a 500 instead of its pre-#5866 status, since
   * {@code AbstractServerHttpHandler.handleRequest}'s catch chain has no arm for that exception. The final
   * fix backs off the "isEmpty() 204 also gets a bookmark" attempt entirely and resolves {@code database}
   * only once there is an actual write to make - matching the ordering PostPrometheusWriteHandler had before
   * this PR - so a nonexistent database never reaches {@code getDatabase()} on either the truly-empty-body
   * path or the well-formed-but-zero-series path.
   */
  @Test
  void prometheusNonexistentDatabaseNeverSurfacesA500() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int leaderPort = 2480 + leaderIndex;
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
    final String bogusDb = "this_database_does_not_exist";

    // A nonexistent database combined with an empty body must stay 400 - not the 500 a premature
    // getDatabase() resolution ahead of the rawBytes-empty check would produce.
    final HttpURLConnection badDbEmptyBodyConnection = openPrometheusWriteConnectionForDatabase(leaderPort, password, bogusDb);
    try (final OutputStream os = badDbEmptyBodyConnection.getOutputStream()) {
      os.flush();
    }
    assertThat(badDbEmptyBodyConnection.getResponseCode())
        .as("A nonexistent database plus an empty body must stay 400, not fall through to a 500")
        .isEqualTo(400);

    // The exact case the third review round flagged: a well-formed, Snappy-compressed, protobuf-decodable
    // WriteRequest with zero time series, against a nonexistent database. This must NOT surface as 500 -
    // not the 500 a getDatabase() resolution ahead of the isEmpty() check would produce.
    final byte[] emptyCompressed = Snappy.compress(new WriteRequest(List.of()).encode());
    final HttpURLConnection badDbEmptyWriteRequestConnection =
        openPrometheusWriteConnectionForDatabase(leaderPort, password, bogusDb);
    try (final OutputStream os = badDbEmptyWriteRequestConnection.getOutputStream()) {
      os.write(emptyCompressed);
      os.flush();
    }
    assertThat(badDbEmptyWriteRequestConnection.getResponseCode())
        .as("A nonexistent database plus a well-formed but zero-series WriteRequest must not surface as 500")
        .isNotEqualTo(500);
  }

  private HttpURLConnection openLineProtocolConnection(final int port, final String password) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + port + "/api/v1/ts/" + getDatabaseName() + "/write?precision=ms")
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + password).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);
    return connection;
  }

  private HttpURLConnection openPrometheusWriteConnection(final int port, final String password) throws Exception {
    return openPrometheusWriteConnectionForDatabase(port, password, getDatabaseName());
  }

  private HttpURLConnection openPrometheusWriteConnectionForDatabase(final int port, final String password,
      final String databaseName) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + port + "/api/v1/ts/" + databaseName + "/prom/write")
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + password).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    connection.setDoOutput(true);
    return connection;
  }
}
