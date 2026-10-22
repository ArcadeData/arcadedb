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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.QueryTracer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10234, the server half: a database on disk that the server did not open - because
 * {@code arcadedb.server.databaseLoadAtStartup} was off - answered "not available" over HTTP until somebody opened it by
 * hand, while the binary protocols opened it on its first request. HTTP now opens it on its first request too, so a server
 * holding thousands of databases can leave them closed at start. A database closed with {@code close database} stays
 * closed until {@code open database}. A name that is not a database on disk is still refused, and never becomes one.
 * <p>
 * And issue #10218: once the server is online, the query stack is warmed in the background, so the first client request
 * does not pay for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10234DatabaseOpenedOnRequestIT extends BaseGraphServerTest {

  @Test
  void aDatabaseTheServerDidNotOpenIsOpenedByItsFirstRequest() throws Exception {
    // AS WITH databaseLoadAtStartup=false: ON DISK, NOT OPEN, NOT CLOSED BY ANYBODY
    getServer(0).getDatabase(getDatabaseName()).getEmbedded().close();
    getServer(0).removeDatabase(getDatabaseName());
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();

    final HttpResponse<String> response = postQuery(getDatabaseName(), "select count(*) as c from V1");
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).contains("\"c\"");
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
  }

  @Test
  void concurrentFirstRequestsOpenTheDatabaseOnce() throws Exception {
    getServer(0).getDatabase(getDatabaseName()).getEmbedded().close();
    getServer(0).removeDatabase(getDatabaseName());

    final int requests = 8;
    final CountDownLatch go = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(requests);
    try {
      final List<Future<Integer>> answers = new ArrayList<>();
      for (int i = 0; i < requests; i++)
        answers.add(pool.submit(() -> {
          go.await();
          return postQuery(getDatabaseName(), "select count(*) as c from V1").statusCode();
        }));
      go.countDown();

      for (final Future<Integer> answer : answers)
        assertThat(answer.get(60, TimeUnit.SECONDS)).isEqualTo(200);
    } finally {
      pool.shutdownNow();
    }

    // ONE OPEN INSTANCE SERVES THEM ALL: A SECOND OPEN OF THE SAME DIRECTORY WOULD HAVE BEEN REFUSED AS ALREADY IN USE
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    assertThat(getServer(0).getDatabase(getDatabaseName()).isOpen()).isTrue();
  }

  @Test
  void aDatabaseClosedByCommandStaysClosedUntilOpened() throws Exception {
    // CLOSED ON PURPOSE (A RESTORE OF ITS FILES, A COLD BACKUP): A REQUEST DOES NOT REOPEN IT UNDER THE OPERATOR
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    try {
      assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isNotEqualTo(200);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();
    } finally {
      assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
    }

    assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isEqualTo(200);
  }

  @Test
  void requestsRacingTheCloseCommandNeverLeaveTheDatabaseOpen() throws Exception {
    final AtomicBoolean running = new AtomicBoolean(true);
    final Thread requests = new Thread(() -> {
      while (running.get())
        try {
          postQuery(getDatabaseName(), "select 1");
        } catch (final Exception e) {
          // THE SERVER ANSWERS OR REFUSES: THE ASSERTION IS ON WHAT THE CLOSE LEFT BEHIND
        }
    }, "issue10234-requests");
    requests.start();
    try {
      for (int i = 0; i < 5; i++) {
        assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
        // THE REQUESTS KEEP COMING: NONE OF THEM REOPENS A DATABASE THE COMMAND CLOSED
        Thread.sleep(20);
        assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();
        assertThat(postServerCommand("open database " + getDatabaseName()).statusCode()).isEqualTo(200);
      }
    } finally {
      running.set(false);
      requests.join();
    }
  }

  /** What a close that failed leaves: the mark is taken back, and the database is served again. */
  @Test
  void aMarkTakenBackServesTheDatabaseAgain() throws Exception {
    getServer(0).getDatabase(getDatabaseName()).getEmbedded().close();
    getServer(0).removeDatabase(getDatabaseName());
    getServer(0).markClosedByCommand(getDatabaseName());
    assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isNotEqualTo(200);

    getServer(0).unmarkClosedByCommand(getDatabaseName());
    assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isEqualTo(200);
  }

  @Test
  void aDeregistrationEndsTheCloseACommandAskedFor() throws Exception {
    // A DROP, A RESTORE, AN IMPORT OR A SNAPSHOT INSTALL REGISTERS OR REMOVES THE DATABASE: THE CLOSE NO LONGER APPLIES
    assertThat(postServerCommand("close database " + getDatabaseName()).statusCode()).isEqualTo(200);
    assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isNotEqualTo(200);

    getServer(0).removeDatabase(getDatabaseName());

    assertThat(postQuery(getDatabaseName(), "select 1").statusCode()).isEqualTo(200);
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
  }

  @Test
  void aNameThatIsNotADatabaseOnDiskIsStillRefused() throws Exception {
    final String name = getDatabaseName() + "_10234_missing";
    final File directory = new File(
        getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), name);

    final HttpResponse<String> missing = postQuery(name, "select 1");
    assertThat(missing.statusCode()).isNotEqualTo(200);
    assertThat(directory).doesNotExist();
    assertThat(getServer(0).existsDatabase(name)).isFalse();

    // A DIRECTORY THE OPERATING SYSTEM OR THE STORAGE PUT THERE (#8805): IT DOES NOT BECOME A DATABASE BECAUSE A REQUEST NAMED IT
    assertThat(directory.mkdirs()).isTrue();
    try {
      final HttpResponse<String> notADatabase = postQuery(name, "select 1");
      assertThat(notADatabase.statusCode()).isNotEqualTo(200);
      assertThat(directory.list()).isEmpty();
      assertThat(getServer(0).existsDatabase(name)).isFalse();
    } finally {
      directory.delete();
    }
  }

  @Test
  void aNullOrBlankNameGetsTheAnswerItAlwaysDid() {
    assertThatThrownBy(() -> getServer(0).getDatabaseForRequest(null)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid database name");
    assertThatThrownBy(() -> getServer(0).getDatabaseForRequest("  ")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> getServer(0).getDatabaseForRequest("")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aNameLeavingTheDatabaseDirectoryIsRefused() throws Exception {
    final HttpResponse<String> response = postQuery("..", "select 1");
    assertThat(response.statusCode()).isNotEqualTo(200);

    // ANSWERED AS A NAME THAT IS NOT A DATABASE ALWAYS WAS, NOT WITH A NEW KIND OF ERROR
    final HttpResponse<String> unknown = postQuery(getDatabaseName() + "_never_existed", "select 1");
    assertThat(response.statusCode()).isEqualTo(unknown.statusCode());
    assertThat(postQuery("a..b", "select 1").statusCode()).isEqualTo(unknown.statusCode());
  }

  /** Encoded and separator variants of a parent-directory reference: none is opened, none creates a directory. */
  @Test
  void encodedNamesThatLeaveTheDatabaseDirectoryAreRefused() throws Exception {
    final File databaseDirectory = new File(
        getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    final String[] before = databaseDirectory.getParentFile().list();

    for (final String name : new String[] { "%2e%2e", "%2E%2E%2Fescape", "..%2F..", "%5c..", "a%2f..%2fb", "%00", "..%5Cescape",
        ".%2e" }) {
      final HttpResponse<String> response = postQuery(name, "select 1");
      assertThat(response.statusCode()).as("status for '%s'", name).isNotEqualTo(200);
    }

    assertThat(databaseDirectory.getParentFile().list()).containsExactlyInAnyOrder(before);
    assertThat(getServer(0).existsDatabase("..")).isFalse();
  }

  @Test
  void theQueryStackIsWarmedOnceTheServerIsOnline() throws Exception {
    final QueryWarmUp warmUp = getServer(0).getQueryWarmUp();
    assertThat(warmUp).isNotNull();
    assertThat(warmUp.awaitCompletion(60, TimeUnit.SECONDS)).isTrue();
    // SQL AND OPENCYPHER, ON THE DATABASE THE FIXTURE OPENED
    assertThat(warmUp.getExecutedStatements()).isEqualTo(QueryWarmUp.EXECUTED_STATEMENTS.size());
  }

  /**
   * The statements of the warm-up are nobody's request: they must show in no tracing span (and so in no
   * {@code arcadedb.query.duration} metric), where a span tagged {@code internal} made the tracing and the Redis metrics
   * integration tests see a query they had not run.
   */
  @Test
  void theWarmUpIsSeenByNoTracerAndNoMetric() throws Exception {
    final QueryWarmUp warmUp = getServer(0).getQueryWarmUp();
    assertThat(warmUp.awaitCompletion(60, TimeUnit.SECONDS)).isTrue();

    final QueryTracer previous = QueryTracer.Holder.get();
    final AtomicInteger spans = new AtomicInteger();
    QueryTracer.Holder.register((protocol, database, language, type, query) -> {
      spans.incrementAndGet();
      return QueryTracer.Span.NO_OP;
    });
    try {
      final QueryWarmUp second = new QueryWarmUp(getServer(0));
      final Object queriesBefore = getServer(0).getDatabase(getDatabaseName()).getEmbedded().getStats().get("queries");
      second.run();
      assertThat(second.getExecutedStatements()).isEqualTo(QueryWarmUp.EXECUTED_STATEMENTS.size());
      assertThat(spans.get()).as("spans opened by the warm-up").isZero();
      // NOT COUNTED AMONG THE QUERIES OF THE DATABASE EITHER
      assertThat(getServer(0).getDatabase(getDatabaseName()).getEmbedded().getStats().get("queries")).isEqualTo(queriesBefore);
    } finally {
      QueryTracer.Holder.register(previous);
    }
  }

  private HttpResponse<String> postQuery(final String database, final String query) throws Exception {
    return post("/api/v1/query/" + database, new JSONObject().put("language", "sql").put("command", query));
  }

  private HttpResponse<String> postServerCommand(final String command) throws Exception {
    return post("/api/v1/server", new JSONObject().put("command", command));
  }

  private HttpResponse<String> post(final String path, final JSONObject payload) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(getServerHttpUrl(0, path)))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
