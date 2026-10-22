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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10342: {@code DELETE FROM <timeseries type>} and {@code TRUNCATE TYPE} remove samples outside the transaction they run in,
 * as an {@code INSERT} adds them outside it (issue #7410). Over HTTP every command is wrapped in a transaction and a client can hold a
 * session open, so this is the path on which a caller meets the rule: the answer says {@code transactional: false}, and a
 * {@code ROLLBACK} of the session brings back nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10342TimeSeriesDeleteHttpIT extends BaseGraphServerTest {
  private static final String TYPE = "Ts10342";

  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  @Test
  void aDeleteOrATruncateInAnHttpSessionIsNotUndoneByTheRollback() throws Exception {
    sql(null, "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
    for (int i = 0; i < 10; i++)
      sql(null, "INSERT INTO " + TYPE + " SET ts = " + (1_000L + i * 1_000L) + ", host = 'h" + (i % 2) + "', v = " + i);
    assertThat(count()).isEqualTo(10L);

    // a DELETE in a session: the answer says it is not transactional, the samples are gone, and the rollback restores none of them
    final String session = begin();
    final JSONObject deleted = sql(session, "DELETE FROM " + TYPE + " WHERE host = 'h0' AND ts < 6000").getJSONArray("result")
        .getJSONObject(0);
    assertThat(deleted.getLong("count")).isEqualTo(3L);
    assertThat(deleted.getBoolean("transactional")).isFalse();
    rollback(session);
    assertThat(count()).as("the rollback of the session did not bring the samples back").isEqualTo(7L);

    // a TRUNCATE in a session: the same
    final String second = begin();
    final JSONObject truncated = sql(second, "TRUNCATE TYPE " + TYPE).getJSONArray("result").getJSONObject(0);
    assertThat(truncated.getLong("count")).isEqualTo(7L);
    assertThat(truncated.getBoolean("transactional")).isFalse();
    rollback(second);
    assertThat(count()).as("the rollback of the session did not bring the samples back").isZero();

    // the type takes samples again
    sql(null, "INSERT INTO " + TYPE + " SET ts = 50000, host = 'h1', v = 1");
    assertThat(count()).isEqualTo(1L);
  }

  private long count() throws Exception {
    return sql(null, "SELECT count(*) AS c FROM " + TYPE).getJSONArray("result").getJSONObject(0).getLong("c");
  }

  private String begin() throws Exception {
    final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
    assertThat(begun.statusCode()).isEqualTo(204);
    final String session = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
    assertThat(session).as("POST /begin must hand back a session id").isNotBlank();
    return session;
  }

  private void rollback(final String session) throws Exception {
    assertThat(send("/rollback/" + getDatabaseName(), session, "").statusCode()).isEqualTo(204);
  }

  private JSONObject sql(final String session, final String command) throws Exception {
    final HttpResponse<String> response = send("/command/" + getDatabaseName(), session,
        new JSONObject().put("language", "sql").put("command", command).toString());
    assertThat(response.statusCode()).as("command failed: %s", response.body()).isEqualTo(200);
    return new JSONObject(response.body());
  }

  private HttpResponse<String> send(final String path, final String session, final String body) throws Exception {
    final HttpRequest.Builder request = HttpRequest.newBuilder(
            URI.create("http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1" + path))
        .timeout(Duration.ofSeconds(30))
        .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json");
    if (session != null)
      request.header(HttpSessionManager.ARCADEDB_SESSION_ID, session);
    return http.send(request.POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
  }
}
