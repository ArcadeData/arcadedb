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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ForwardedRequestIdContext;
import com.arcadedb.server.http.IdempotencyCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8347, the receiving side: a follower forwards a client's SQL write to the leader with a body it rebuilt from
 * the statement, so the leader keyed the forward differently from the retry the client may send it directly - with its
 * own body, carrying 'serializer', 'limit' or simply other whitespace - and ran that retry as a second write. The
 * forward now names the key the client's own request has, and the leader claims it beside the forward's own key.
 * <p>
 * The server here plays the leader. A request that carries the cluster token plays the follower's forward; one that
 * carries Basic credentials plays the client. The type has no unique index, so a second execution is a second record.
 */
class Issue8347DirectRetryClientKeyTest extends BaseGraphServerTest {

  private static final String     CLUSTER_TOKEN = "issue8347-cluster-token";
  private static final String     TYPE          = "Issue8347Doc";
  private static final HttpClient HTTP          = HttpClient.newHttpClient();

  @BeforeEach
  void setClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
    getServerDatabase(0, getDatabaseName()).command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  /** The entry point of the issue: the answer to a follower-forwarded write was lost, and the client asks the leader. */
  @Test
  void aRetrySentStraightToTheLeaderIsReplayedFromTheForward() throws Exception {
    final String requestId = "issue8347-direct-after-forward";
    final String clientBody = clientBody("direct-after-forward");

    final HttpResponse<String> forward = send(forwardBody("direct-after-forward"), requestId, true,
        clientKey(requestId, clientBody));
    assertThat(forward.statusCode()).as("forward, body: %s", forward.body()).isEqualTo(200);

    final HttpResponse<String> retry = send(clientBody, requestId, false, null);
    assertThat(retry.statusCode()).as("retry, body: %s", retry.body()).isEqualTo(200);

    assertThat(countTagged("direct-after-forward")).as("the retry must be replayed, not executed a second time")
        .isEqualTo(1L);
    assertThat(retry.body()).as("the replay is the forward's answer").isEqualTo(forward.body());
  }

  /**
   * The other order: the client's first attempt went straight to the leader and its answer was lost, and the retry
   * reached a follower, which forwards it with the rebuilt body.
   */
  @Test
  void aForwardOfARetryIsReplayedFromTheDirectAttempt() throws Exception {
    final String requestId = "issue8347-forward-after-direct";
    final String clientBody = clientBody("forward-after-direct");

    final HttpResponse<String> direct = send(clientBody, requestId, false, null);
    assertThat(direct.statusCode()).as("direct, body: %s", direct.body()).isEqualTo(200);

    final HttpResponse<String> forward = send(forwardBody("forward-after-direct"), requestId, true,
        clientKey(requestId, clientBody));
    assertThat(forward.statusCode()).as("forward, body: %s", forward.body()).isEqualTo(200);

    assertThat(countTagged("forward-after-direct")).isEqualTo(1L);
    assertThat(forward.body()).isEqualTo(direct.body());
  }

  /** The control: without the key the two bodies really are two keys, which is the defect the issue reports. */
  @Test
  void withoutTheClientKeyTheTwoBodiesAreTwoWrites() throws Exception {
    final String requestId = "issue8347-control";

    assertThat(send(forwardBody("control"), requestId, true, null).statusCode()).isEqualTo(200);
    assertThat(send(clientBody("control"), requestId, false, null).statusCode()).isEqualTo(200);

    assertThat(countTagged("control")).isEqualTo(2L);
  }

  /**
   * From a client the header means nothing: honored, it would let a request settle - and be answered from - the key of
   * a different request with the same id.
   */
  @Test
  void aClientKeySentWithoutTheClusterTokenIsIgnored() throws Exception {
    final String requestId = "issue8347-client-sent";
    final String firstBody = clientBody("client-sent-1");

    assertThat(send(firstBody, requestId, false, null).statusCode()).isEqualTo(200);
    assertThat(send(clientBody("client-sent-2"), requestId, false, clientKey(requestId, firstBody)).statusCode())
        .isEqualTo(200);

    assertThat(countTagged("client-sent-2")).as("a different request, whatever key it names").isEqualTo(1L);
  }

  /** A value that is not a key a peer could have computed is dropped, and the forward keys as it did before. */
  @Test
  void aMalformedClientKeyFromAPeerIsIgnored() throws Exception {
    final String requestId = "issue8347-malformed";

    assertThat(send(forwardBody("malformed"), requestId, true, "not-a-key").statusCode()).isEqualTo(200);
    assertThat(send(clientBody("malformed"), requestId, false, null).statusCode()).isEqualTo(200);

    assertThat(countTagged("malformed")).isEqualTo(2L);
  }

  // ---------------------------------------------------------------------------------------------

  private static String insert(final String tag) {
    return "INSERT INTO " + TYPE + " SET tag = '" + tag + "'";
  }

  /** What the follower's forward carries: the statement alone, rebuilt by RaftReplicatedDatabase. */
  private static String forwardBody(final String tag) {
    return new JSONObject().put("language", "sql").put("command", insert(tag)).toString();
  }

  /** What a client sends: its own fields, in its own order and spacing. */
  private static String clientBody(final String tag) {
    return "{ \"serializer\": \"record\", \"limit\": 20, \"language\": \"sql\", \"command\": \"" + insert(tag) + "\" }";
  }

  /** The key the follower computed for the client's request, exactly as this server computes it for a direct one. */
  private String clientKey(final String requestId, final String clientBody) {
    return AbstractServerHttpHandler.buildIdempotencyKey(requestId, "POST", "/command/" + getDatabaseName(),
        getDatabaseName(), clientBody, null, 0);
  }

  private long countTagged(final String tag) {
    final Database db = getServerDatabase(0, getDatabaseName());
    return ((Number) db.query("sql", "SELECT count(*) AS cnt FROM " + TYPE + " WHERE tag = ?", tag).next()
        .getProperty("cnt")).longValue();
  }

  private HttpResponse<String> send(final String body, final String requestId, final boolean asPeer,
      final String clientKey) throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Content-Type", "application/json")
        .header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
    if (asPeer)
      builder.header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN).header("X-ArcadeDB-Forwarded-User", "root");
    else
      builder.header("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (clientKey != null)
      builder.header(ForwardedRequestIdContext.CLIENT_KEY_HEADER, clientKey);
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
  }
}
