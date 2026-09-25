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
 * Issue #8323, the receiving side: the leader folds the ordinal of a follower's second, third, ... SQL write forward
 * within one client request into its idempotency key - and only when the request carries a valid cluster token. A
 * client can send any {@code X-Request-Id} it likes, so the ordinal must not be something a client can reproduce: an
 * ordinal encoded in the id itself ({@code order#2}) would let a request with that id share a key with another
 * request's second forward of the same statement, and one of the two writes would be answered from the other's cache.
 */
class Issue8323ForwardOrdinalKeyTest extends BaseGraphServerTest {

  private static final String      CLUSTER_TOKEN = "issue8323-cluster-token";
  private static final String      TYPE          = "Issue8323OrdinalDoc";
  private static final HttpClient  HTTP          = HttpClient.newHttpClient();

  @BeforeEach
  void setClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, CLUSTER_TOKEN);
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
  }

  @AfterEach
  void clearClusterToken() {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.HA_CLUSTER_TOKEN, "");
  }

  @Test
  void theOrdinalIsPartOfTheKeyAndNoClientIdCanReproduceIt() {
    final String first = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
        "{\"command\":\"x\"}", null, 0);
    final String second = AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
        "{\"command\":\"x\"}", null, 2);
    final String clientSuffixed = AbstractServerHttpHandler.buildIdempotencyKey("order#2", "POST", "/api/v1/command/db",
        "db", "{\"command\":\"x\"}", null, 0);

    assertThat(second).isNotEqualTo(first);
    assertThat(second).isNotEqualTo(clientSuffixed);
    // Ordinal 0 leaves the key exactly as it was before the ordinal existed.
    assertThat(first).isEqualTo(AbstractServerHttpHandler.buildIdempotencyKey("order", "POST", "/api/v1/command/db", "db",
        "{\"command\":\"x\"}", null));
  }

  @Test
  void aSecondForwardUnderTheTokenExecutesAndIsItselfDeduplicated() throws Exception {
    final String body = insert("forward");

    assertThat(send(body, "issue8323-fwd", true, null).statusCode()).isEqualTo(200);
    assertThat(send(body, "issue8323-fwd", true, "2").statusCode()).isEqualTo(200);
    assertThat(countTagged("forward")).as("the second forward is a separate write").isEqualTo(2L);

    // A retry of that second forward is answered from the cache.
    assertThat(send(body, "issue8323-fwd", true, "2").statusCode()).isEqualTo(200);
    assertThat(countTagged("forward")).isEqualTo(2L);
  }

  /** From a client the header means nothing: the request keys exactly as it would without it. */
  @Test
  void anOrdinalWithoutTheClusterTokenIsIgnored() throws Exception {
    final String body = insert("client");

    assertThat(send(body, "issue8323-client", false, null).statusCode()).isEqualTo(200);
    assertThat(send(body, "issue8323-client", false, "2").statusCode()).isEqualTo(200);

    assertThat(countTagged("client")).as("the second request is a retry of the first, whatever header it adds").isEqualTo(1L);
  }

  /** The collision an ordinal folded into the id would have had: a client id that spells the suffix is its own key. */
  @Test
  void aClientIdThatSpellsAnOrdinalDoesNotReplayASecondForward() throws Exception {
    final String body = insert("suffix");

    assertThat(send(body, "issue8323-order", true, null).statusCode()).isEqualTo(200);
    assertThat(send(body, "issue8323-order", true, "2").statusCode()).isEqualTo(200);
    assertThat(send(body, "issue8323-order#2", false, null).statusCode()).isEqualTo(200);

    assertThat(countTagged("suffix")).isEqualTo(3L);
  }

  // ---------------------------------------------------------------------------------------------

  private static String insert(final String tag) {
    return new JSONObject().put("language", "sql").put("command", "INSERT INTO " + TYPE + " SET tag = '" + tag + "'")
        .toString();
  }

  private long countTagged(final String tag) {
    return ((Number) getServerDatabase(0, getDatabaseName())
        .query("sql", "SELECT count(*) AS cnt FROM " + TYPE + " WHERE tag = ?", tag).next().getProperty("cnt")).longValue();
  }

  private HttpResponse<String> send(final String body, final String requestId, final boolean asPeer, final String ordinal)
      throws Exception {
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Content-Type", "application/json")
        .header(IdempotencyCache.HEADER_REQUEST_ID, requestId);
    if (asPeer)
      builder.header("X-ArcadeDB-Cluster-Token", CLUSTER_TOKEN).header("X-ArcadeDB-Forwarded-User", "root");
    else
      builder.header("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    if (ordinal != null)
      builder.header(ForwardedRequestIdContext.FORWARD_ORDINAL_HEADER, ordinal);
    return HTTP.send(builder.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
  }
}
