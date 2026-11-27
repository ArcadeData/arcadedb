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
package com.arcadedb.server.ai;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The tools the AI Assistant runs in a database are work like any other request: a call that runs a statement waits for a
 * slot of the query admission gate, and every call that works in a database is listed by "list queries" and obeys a terminate,
 * for as long as it runs, and gives both back at its end.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ToolDispatcherRunningQueryTest extends BaseGraphServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation: it ends early only when it is terminated. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @AfterEach
  void resetGate() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
  }

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  private ToolDispatcher dispatcher() {
    return new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());
  }

  private static JSONObject query(final String language, final String command) {
    return new JSONObject().put("language", language).put("command", command);
  }

  /** Runs {@code work} on a thread of its own: the slot a thread holds is shared by the queries it starts. */
  private static <T> T onAnotherThread(final Supplier<T> work) throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      return CompletableFuture.supplyAsync(work, executor).get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private RunningQuery awaitListed(final String protocol) {
    final AtomicReference<RunningQuery> found = new AtomicReference<>();
    await("a statement of the AI Assistant is listed").atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
        .until(() -> {
          for (final RunningQuery entry : getServer(0).getRunningQueries().getRunning())
            if (protocol.equals(entry.getProtocol())) {
              found.set(entry);
              return true;
            }
          return false;
        });
    return found.get();
  }

  @Test
  void aQueryWaitsForASlotOfTheGateAndEveryCallGivesItsSlotBack() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    final ToolDispatcher dispatcher = dispatcher();

    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      final JSONObject refused = new JSONObject(onAnotherThread(() -> dispatcher.execute("query_database", query("sql", "SELECT 1 AS one"))));
      assertThat(refused.getString("error")).contains("Query not started");

      // what only reads the schema stays answerable on a busy server
      assertThat(new JSONObject(onAnotherThread(() -> dispatcher.execute("get_schema", new JSONObject()))).has("error")).isFalse();
      assertThat(new JSONObject(onAnotherThread(() -> dispatcher.execute("get_type", new JSONObject().put("name", VERTEX1_TYPE_NAME))))
          .has("error")).isFalse();
    }

    // WITH ONE SLOT AND NO WAITING, A SLOT NOT GIVEN BACK WOULD REFUSE THE SECOND CALL
    for (int i = 0; i < 2; i++)
      assertThat(new JSONObject(dispatcher.execute("query_database", query("sql", "SELECT 1 AS one"))).has("error")).isFalse();
    // ... AND ONE THAT FAILS GIVES IT BACK TOO
    assertThat(new JSONObject(dispatcher.execute("query_database", query("sql", "SELECT FROM NoSuchType"))).has("error")).isTrue();
    assertThat(new JSONObject(dispatcher.execute("query_database", query("sql", "SELECT 1 AS one"))).has("error")).isFalse();
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void aRunningQueryIsListedHoldsASlotAndStopsWhenTerminated() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final CompletableFuture<String> running = CompletableFuture.supplyAsync(
          () -> dispatcher.execute("query_database", query("cypher", LONG_CYPHER)), executor);

      final RunningQuery entry = awaitListed(AiRunningQuery.PROTOCOL);
      assertThat(entry.getUser()).isEqualTo("root");
      assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
      assertThat(entry.getLanguage()).isEqualTo("cypher");
      assertThat(entry.getText()).isEqualTo(LONG_CYPHER);
      assertThat(entry.isTerminated()).isFalse();

      // the one slot is held for as long as the statement runs
      assertThat(gate.getRunning()).isEqualTo(1);
      final JSONObject refused = new JSONObject(onAnotherThread(() -> dispatcher.execute("query_database", query("sql", "SELECT 1"))));
      assertThat(refused.getString("error")).contains("Query not started");

      assertThat(entry.terminate("root")).isTrue();

      final JSONObject answer = new JSONObject(running.get(60, TimeUnit.SECONDS));
      assertThat(answer.getString("error")).contains("terminated");
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(getServer(0).getRunningQueries().getRunning()).noneMatch(e -> AiRunningQuery.PROTOCOL.equals(e.getProtocol()));
      assertThat(gate.getRunning()).isZero();
      assertThat(new JSONObject(dispatcher.execute("query_database", query("sql", "SELECT 1 AS one"))).has("error")).isFalse();
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * The entry a schema walk checks has to be the one the dispatcher opened for the call. The call is made on a thread that
   * already carries a TERMINATED entry: a dispatcher that publishes its own entry hides it and the read succeeds, one that
   * registers nothing leaves the walk checking the terminated one, and the read fails.
   */
  @Test
  void theSchemaToolRunsUnderItsOwnEntry() {
    try (final RunningQuery outer = getServer(0).getRunningQueries()
        .register(getDatabaseName(), "root", "test", null, null, RunningQuery.Client.NONE)) {
      outer.terminate("root");

      final JSONObject result = new JSONObject(dispatcher().execute("get_schema", new JSONObject()));

      assertThat(result.has("error")).as(result.toString()).isFalse();
      assertThat(result.getJSONArray("types").length()).isGreaterThan(0);
    }
  }

  /** The same for the summary the chat sends with every question: it walks the schema, so a terminate stops it. */
  @Test
  void theSchemaSummaryStopsWhenItsEntryIsTerminated() {
    AiSchemaDigest.clearCache();
    try (final RunningQuery entry = getServer(0).getRunningQueries()
        .register(getDatabaseName(), "root", "test", null, null, RunningQuery.Client.NONE)) {
      entry.terminate("root");

      assertThatThrownBy(() -> AiSchemaDigest.forUser(getServer(0), rootUser(), getDatabaseName()))
          .isInstanceOf(QueryTerminatedException.class);
    } finally {
      AiSchemaDigest.clearCache();
    }
  }

  @Test
  void theListedWorkIsPublishedOnTheThreadAndGivenBackWhateverItsOutcome() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
    final RunningQuery.Client client = new RunningQuery.Client("conn-1", "10.1.2.3:4567");
    final ServerSecurityUser root = rootUser();
    final AtomicReference<RunningQuery> seen = new AtomicReference<>();

    final String answer = AiRunningQuery.run(getServer(0), root, client, getDatabaseName(), true, "sql", "SELECT 1", () -> {
      seen.set(RunningQuery.current());
      assertThat(gate.getRunning()).isEqualTo(1);
      return "done";
    });

    assertThat(answer).isEqualTo("done");
    final RunningQuery entry = seen.get();
    assertThat(entry).isNotNull();
    assertThat(entry.getProtocol()).isEqualTo(AiRunningQuery.PROTOCOL);
    assertThat(entry.getUser()).isEqualTo("root");
    assertThat(entry.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(entry.getLanguage()).isEqualTo("sql");
    assertThat(entry.getText()).isEqualTo("SELECT 1");
    assertThat(entry.getConnectionId()).isEqualTo("conn-1");
    assertThat(entry.getClientAddress()).isEqualTo("10.1.2.3:4567");
    assertThat(entry.isEnded()).isTrue();
    assertThat(RunningQuery.current()).isNull();
    assertThat(gate.getRunning()).isZero();

    // a failure gives both back too, and is the caller's
    assertThatThrownBy(() -> AiRunningQuery.run(getServer(0), root, client, getDatabaseName(), true, "sql", "SELECT 1", () -> {
      throw new IllegalStateException("failed");
    })).isInstanceOf(IllegalStateException.class);
    assertThat(RunningQuery.current()).isNull();
    assertThat(gate.getRunning()).isZero();
    assertThat(getServer(0).getRunningQueries().getRunning()).noneMatch(e -> AiRunningQuery.PROTOCOL.equals(e.getProtocol()));

    // work that is not admitted is listed all the same, and never takes a slot
    final AtomicInteger slotsTaken = new AtomicInteger(-1);
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      AiRunningQuery.run(getServer(0), root, client, getDatabaseName(), false, "ai", "get_schema", () -> {
        slotsTaken.set(gate.getRunning());
        return RunningQuery.current();
      });
    }
    assertThat(slotsTaken.get()).isEqualTo(1);

    // work that is admitted is refused, and listed nowhere, when the gate has no slot for it
    try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
      assertThatThrownBy(() -> onAnotherThread(() -> AiRunningQuery.run(getServer(0), root, client, getDatabaseName(), true, "sql", "SELECT 1",
          () -> "never"))).hasRootCauseInstanceOf(QueryAdmissionException.class);
    }
    assertThat(getServer(0).getRunningQueries().getRunning()).noneMatch(e -> AiRunningQuery.PROTOCOL.equals(e.getProtocol()));
  }
}
