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
package com.arcadedb.server.http;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end regression for issue #8324. A retry whose {@code X-Request-Id} (and method, path, database and body)
 * matches a request that is STILL executing used to wait 5 s for it and then "fall through and execute uncached":
 * the same write ran a second time, side by side with the first. Behind a follower's 504 for a long
 * {@code restore database} that is the retry most likely to arrive, and it ran the restore twice.
 * <p>
 * The first request here is a script that sleeps well past that 5 s wait and then inserts one marker record; the
 * retry arrives while it sleeps. The retry must be refused with {@code 409} + {@code Retry-After}, the marker must be
 * inserted exactly once, and a retry after the first execution settled must replay its answer.
 */
@Tag("slow")
class Issue8324InFlightRetryIsNotExecutedTwiceTest extends BaseGraphServerTest {

  private static final String DATABASE_NAME = "graph";
  private static final String REQUEST_ID    = "restore-8324";
  private static final String SCRIPT        = "SLEEP 9000; INSERT INTO Person SET marker = 'idem8324';";

  @Test
  void aRetryOfARequestStillExecutingIsRefusedAndNotExecutedASecondTime() throws Exception {
    final IdempotencyCache cache = getServer(0).getHttpServer().getIdempotencyCache();
    final int entriesBefore = cache.size();

    final CompletableFuture<Answer> first = CompletableFuture.supplyAsync(this::post);

    // The first request has reserved the key once its pending marker is in the cache.
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (cache.size() == entriesBefore && System.nanoTime() < deadline)
      Thread.sleep(20);
    assertThat(cache.size()).as("the first request never reserved its X-Request-Id").isGreaterThan(entriesBefore);

    final Answer retry = post();
    assertThat(first.isDone())
        .as("the retry must have been answered while the first execution was still running")
        .isFalse();
    assertThat(retry.status).as("body=%s", retry.body).isEqualTo(409);
    assertThat(retry.retryAfter).isEqualTo("5");
    assertThat(new JSONObject(retry.body).getString("error")).contains("still executing");
    // Issue #8343: the body names the refusal and its back-off, so a follower that forwarded this request to the
    // leader can rebuild it and answer its own client 409 + Retry-After instead of a generic 500.
    assertThat(new JSONObject(retry.body).getString("exception")).isEqualTo(RequestStillInFlightException.class.getName());
    assertThat(new JSONObject(retry.body).getString("exceptionArgs")).isEqualTo("5");

    final Answer original = first.get(60, TimeUnit.SECONDS);
    assertThat(original.status).as("body=%s", original.body).isEqualTo(200);

    // The same retry once the first execution settled replays its answer instead of executing.
    final Answer afterwards = post();
    assertThat(afterwards.status).isEqualTo(200);
    assertThat(afterwards.body).isEqualTo(original.body);

    try (final ResultSet rs = getServerDatabase(0, DATABASE_NAME).query("sql",
        "SELECT count(*) AS n FROM Person WHERE marker = 'idem8324'")) {
      assertThat(rs.next().<Long>getProperty("n"))
          .as("the script ran more than once: the refused retry (or the replayed one) executed it again")
          .isEqualTo(1L);
    }
  }

  private record Answer(int status, String body, String retryAfter) {
  }

  private Answer post() {
    try {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          getServerHttpUrl(0, "/api/v1/command/" + DATABASE_NAME)).openConnection();
      connection.setRequestMethod("POST");
      connection.setReadTimeout(60_000);
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty(IdempotencyCache.HEADER_REQUEST_ID, REQUEST_ID);

      final JSONObject payload = new JSONObject();
      payload.put("language", "sqlscript");
      payload.put("command", SCRIPT);
      formatPayload(connection, payload);
      connection.connect();
      try {
        final int status = connection.getResponseCode();
        final String body = status < 400 ? readResponse(connection) : readError(connection);
        return new Answer(status, body, connection.getHeaderField("Retry-After"));
      } finally {
        connection.disconnect();
      }
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
