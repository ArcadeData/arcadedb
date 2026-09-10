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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.ReadConsistency;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7351: a query answered with the NDJSON streaming encoding of issue #7306 has to carry the
 * {@code X-ArcadeDB-Commit-Index} read-your-writes bookmark, exactly as the buffered encoding of the same
 * query does.
 * <p>
 * The bookmark used to be emitted only by {@link com.arcadedb.server.http.handler.DatabaseAbstractHandler}
 * <b>after</b> the handler body returned. On a streamed response the body has been written and the stream
 * closed by then, so the response headers were serialized long before: the {@code put} landed on a header map
 * nothing would read again, and the header simply never reached the client. A client that switched from
 * {@code query()} to {@code queryStream()} silently stopped advancing its bookmark, and its next
 * {@code READ_YOUR_WRITES} read could be served by a follower that had not applied the write it was reading
 * after.
 * <p>
 * Both halves are asserted here, because either one alone would leave the client where it started: the server
 * has to emit the header before the first byte, and the driver has to capture it off the streamed response.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class RaftStreamedQueryBookmarkIT extends BaseRaftHATest {

  private static final String   TYPE_NAME    = "StreamBookmark";
  private static final String   NDJSON       = "application/x-ndjson";
  private static final String   BOOKMARK     = "X-ArcadeDB-Commit-Index";
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The POST {@code /query} endpoint under {@code Accept: application/x-ndjson}. The comparison against the
   * buffered answer to the same query is the point: a bookmark that is present but does not mean what the
   * buffered one means would be worse than an absent one, because a client cannot tell.
   */
  @Test
  void aStreamedQueryCarriesTheSameBookmarkTheBufferedOneDoes() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    httpCommand(leaderIndex, "CREATE DOCUMENT TYPE " + TYPE_NAME + " IF NOT EXISTS");
    httpCommand(leaderIndex, "INSERT INTO " + TYPE_NAME + " SET id = 'v1'");
    waitForAllServers();

    final long buffered = bookmarkOf(sendBuffered(leaderIndex, "SELECT FROM " + TYPE_NAME));
    assertThat(buffered)
        .as("the buffered encoding must carry the bookmark, otherwise this test proves nothing about the "
            + "streamed one")
        .isGreaterThanOrEqualTo(0);

    final HttpResponse<InputStream> streamed = sendStreamedPost(leaderIndex, "SELECT FROM " + TYPE_NAME);
    assertThat(streamed.statusCode()).isEqualTo(200);
    assertThat(streamed.headers().firstValue("content-type").orElse(""))
        .as("the request has to actually have been streamed for the assertion below to mean anything")
        .contains(NDJSON);

    final long streamedBookmark = bookmarkOf(streamed);
    assertThat(streamedBookmark)
        .as("a streamed query response must carry the '%s' read-your-writes bookmark, exactly as the buffered "
            + "response to the same query does", BOOKMARK)
        .isGreaterThanOrEqualTo(buffered);

    drain(streamed);
  }

  /**
   * The GET {@code /query/{db}/{language}/{command}} endpoint streams through the same helper, and is the one a
   * browser or curl reaches for, so it is asserted separately rather than assumed.
   */
  @Test
  void aStreamedGetQueryCarriesTheBookmarkToo() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    httpCommand(leaderIndex, "CREATE DOCUMENT TYPE " + TYPE_NAME + "Get IF NOT EXISTS");
    httpCommand(leaderIndex, "INSERT INTO " + TYPE_NAME + "Get SET id = 'g1'");
    waitForAllServers();

    final HttpResponse<InputStream> streamed = sendStreamedGet(leaderIndex, "SELECT FROM " + TYPE_NAME + "Get");
    assertThat(streamed.statusCode()).isEqualTo(200);
    assertThat(streamed.headers().firstValue("content-type").orElse("")).contains(NDJSON);

    assertThat(bookmarkOf(streamed))
        .as("the GET query endpoint streams through the same helper and must emit the bookmark as well")
        .isGreaterThanOrEqualTo(0);

    drain(streamed);
  }

  /**
   * The end the issue is actually about: {@code RemoteDatabase.getLastCommitIndex()} must advance after a
   * {@code queryStream} exactly as it does after a {@code query}, so a client that switches encodings keeps its
   * read-your-writes barrier.
   */
  @Test
  void queryStreamAdvancesTheDriverBookmarkLikeQueryDoes() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final String dbName = getDatabaseName();
    final String password = BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;

    try (final RemoteDatabase client = new RemoteDatabase("127.0.0.1", httpPort(leaderIndex), dbName, "root",
        password)) {
      client.setReadConsistency(ReadConsistency.READ_YOUR_WRITES);

      client.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + "Driver IF NOT EXISTS");
      client.command("sql", "INSERT INTO " + TYPE_NAME + "Driver SET id = 'd1'");

      final long afterWrite = client.getLastCommitIndex();
      assertThat(afterWrite).as("the write must have produced a bookmark").isGreaterThanOrEqualTo(0);

      // A second write, so the index the streamed read can report is strictly ahead of the one the driver
      // already holds: an assertion against a bookmark that could not have moved would pass on a driver that
      // ignores the header entirely.
      client.command("sql", "INSERT INTO " + TYPE_NAME + "Driver SET id = 'd2'");
      final long afterSecondWrite = client.getLastCommitIndex();
      assertThat(afterSecondWrite).isGreaterThanOrEqualTo(afterWrite);

      // Rewind the driver's own bookmark, which is the only way to tell "the streamed response advanced it"
      // from "it was already there". updateLastCommitIndex() cannot do it: it folds with Math::max, precisely
      // so a bookmark never goes backwards in normal use.
      final var bookmarkField = RemoteDatabase.class.getDeclaredField("lastCommitIndex");
      bookmarkField.setAccessible(true);
      ((AtomicLong) bookmarkField.get(client)).set(-1L);
      assertThat(client.getLastCommitIndex()).isEqualTo(-1);

      try (final ResultSet rs = client.queryStream("sql", "SELECT FROM " + TYPE_NAME + "Driver")) {
        assertThat(rs.stream().count()).isEqualTo(2);
      }

      assertThat(client.getLastCommitIndex())
          .as("getLastCommitIndex() must advance after queryStream exactly as it does after query, otherwise a "
              + "client that switches to the streaming encoding silently loses read-your-writes")
          .isGreaterThanOrEqualTo(afterSecondWrite);
    }
  }

  private long bookmarkOf(final HttpResponse<?> response) {
    return response.headers().firstValue(BOOKMARK).map(Long::parseLong).orElse(-1L);
  }

  private void drain(final HttpResponse<InputStream> response) throws Exception {
    try (final InputStream in = response.body()) {
      in.readAllBytes();
    }
  }

  private HttpResponse<String> sendBuffered(final int serverIndex, final String command) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(serverIndex) + "/query/" + getDatabaseName()))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    final HttpResponse<String> response = newClient().send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);
    return response;
  }

  private HttpResponse<InputStream> sendStreamedPost(final int serverIndex, final String command) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(serverIndex) + "/query/" + getDatabaseName()))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .header("Accept", NDJSON)
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    return newClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private HttpResponse<InputStream> sendStreamedGet(final int serverIndex, final String command) throws Exception {
    // URLEncoder targets application/x-www-form-urlencoded, where a space is '+'. This is a path segment, where
    // '+' is a literal plus and a space has to be %20, or the server parses a different query and answers 400.
    final String url = baseUrl(serverIndex) + "/query/" + getDatabaseName() + "/sql/"
        + URLEncoder.encode(command, StandardCharsets.UTF_8).replace("+", "%20");
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Accept", NDJSON)
        .GET()
        .build();
    return newClient().send(request, HttpResponse.BodyHandlers.ofInputStream());
  }

  private String httpCommand(final int serverIndex, final String sql) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl(serverIndex) + "/command/" + getDatabaseName()))
        .timeout(HTTP_TIMEOUT)
        .header("Authorization", basicAuth())
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    final HttpResponse<String> response = newClient().send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("command '%s' failed: %s", sql, response.body()).isEqualTo(200);
    return response.body();
  }

  private String baseUrl(final int serverIndex) {
    return "http://127.0.0.1:" + httpPort(serverIndex) + "/api/v1";
  }

  /**
   * The port a node actually bound, never the 248n it was asked for: anything already listening there would
   * otherwise take these requests and answer them as a different build.
   */
  private int httpPort(final int serverIndex) {
    return getServer(serverIndex).getHttpServer().getPort();
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private static HttpClient newClient() {
    return HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();
  }
}
