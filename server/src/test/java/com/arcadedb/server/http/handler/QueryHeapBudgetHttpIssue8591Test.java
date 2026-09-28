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
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8591 over HTTP: a query refused its share of the heap budget all the running queries share is answered 503,
 * the status a client's retry policy keys on, and a served query gives back every reservation its buffers took by the
 * time the response is sent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryHeapBudgetHttpIssue8591Test extends BaseGraphServerTest {
  private static final int        ROWS = 20_000;
  private static final long       MB   = 1024 * 1024;
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static final String SQL_SORT    = "SELECT count(*) AS c FROM (SELECT FROM Doc ORDER BY name)";
  private static final String CYPHER_SORT = "MATCH (d:Doc) WITH d ORDER BY d.name RETURN count(*) AS c";

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aRefusedQueryIsAnswered503AndAServedOneGivesItsHeapBack() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createVertexType("Doc");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("Doc").set("id", i, "name", "name-%06d-%s".formatted(i, "x".repeat(80))).save();
    });

    final long baseline = QueryHeapBudget.getReservedBytes();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(baseline / MB + 16);

    // Other queries hold all the budget but 512KB
    final QueryHeapTracker others = new QueryHeapTracker();
    others.charge(QueryHeapTracker.UNRESERVED_BYTES + QueryHeapBudget.getLimitBytes() - QueryHeapBudget.getReservedBytes() - MB / 2,
        "other queries");
    try {
      for (final String[] query : new String[][] { { "sql", SQL_SORT }, { "opencypher", CYPHER_SORT } }) {
        final HttpResponse<String> response = post(query[0], query[1]);
        assertThat(response.statusCode()).as(query[1] + ": " + response.body()).isEqualTo(503);
        assertThat(new JSONObject(response.body()).getString("exception"))
            .isEqualTo(QueryHeapBudgetExceededException.class.getName());
      }
    } finally {
      others.close();
    }

    for (final String[] query : new String[][] { { "sql", SQL_SORT }, { "opencypher", CYPHER_SORT } }) {
      final HttpResponse<String> response = post(query[0], query[1]);
      assertThat(response.statusCode()).as(query[1] + ": " + response.body()).isEqualTo(200);
      assertThat(new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("c")).isEqualTo(ROWS);
      assertThat(QueryHeapBudget.getReservedBytes()).as("the served query gave its heap back: " + query[1]).isEqualTo(baseline);
    }
  }

  private HttpResponse<String> post(final String language, final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())))
        .header("Content-Type", "application/json")
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("language", language).put("command", command).toString()))
        .build();
    return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
