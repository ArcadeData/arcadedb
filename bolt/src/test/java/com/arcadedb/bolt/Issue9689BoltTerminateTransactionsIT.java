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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ClientException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9689 over BOLT: a Cypher statement run by a Neo4j driver is listed by {@code SHOW TRANSACTIONS} and stopped by
 * {@code TERMINATE TRANSACTIONS}, Neo4j's own syntax, and the client is told with Neo4j's code for a transaction a user
 * killed - which no driver retries. A statement stopped inside an explicit transaction takes the transaction with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9689BoltTerminateTransactionsIT extends BaseBoltServerTest {
  /** About 16 s on one core when left alone, all of it inside one aggregation. */
  private static final String LONG_CYPHER =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";
  private static final String OTHER    = "other9689";
  private static final String PASSWORD = "pwd9689-secret";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(OTHER) != null)
      security.dropUser(OTHER);
    super.endTest();
  }

  @Test
  void showAndTerminateTransactionsStopABoltStatement() throws Exception {
    try (final Driver driver = driver("root", DEFAULT_PASSWORD_FOR_TESTS);
        final Session runner = driver.session(SessionConfig.forDatabase(getDatabaseName()));
        final Session admin = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final CompletableFuture<Void> running = CompletableFuture.runAsync(() -> runner.run(LONG_CYPHER).consume());

      final String id = awaitListed(admin);
      final List<Record> listed = admin.run(
          "SHOW TRANSACTIONS YIELD transactionId, currentQuery, username, protocol, connectionId WHERE transactionId = $id",
          Map.of("id", id)).list();
      assertThat(listed).hasSize(1);
      assertThat(listed.getFirst().get("currentQuery").asString()).isEqualTo(LONG_CYPHER);
      assertThat(listed.getFirst().get("username").asString()).isEqualTo("root");
      assertThat(listed.getFirst().get("protocol").asString()).isEqualTo("bolt");
      assertThat(listed.getFirst().get("connectionId").asString()).startsWith("bolt-");

      // The same statement, in the server's own listing
      final RunningQuery entry = getServer(0).getRunningQueries().get(id);
      assertThat(entry).isNotNull();
      assertThat(entry.getProtocol()).isEqualTo("bolt");

      final Record terminated = admin.run("TERMINATE TRANSACTIONS $id", Map.of("id", id)).single();
      assertThat(terminated.get("transactionId").asString()).isEqualTo(id);
      assertThat(terminated.get("message").asString()).isEqualTo("Transaction terminated.");

      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(e -> {
        assertThat(e.getCause()).isInstanceOf(ClientException.class);
        assertThat(((ClientException) e.getCause()).code()).isEqualTo("Neo.ClientError.Transaction.Terminated");
      });
      assertThat(entry.awaitEnd(30_000)).isTrue();
      assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);

      // The connection serves the next statement
      assertThat(runner.run("RETURN 42 AS answer").single().get("answer").asInt()).isEqualTo(42);
      assertThat(admin.run("TERMINATE TRANSACTIONS $id", Map.of("id", id)).single().get("message").asString())
          .isEqualTo("Transaction not found.");
    }
  }

  @Test
  void aStatementStoppedInAnExplicitTransactionRollsItBack() throws Exception {
    try (final Driver driver = driver("root", DEFAULT_PASSWORD_FOR_TESTS);
        final Session runner = driver.session(SessionConfig.forDatabase(getDatabaseName()));
        final Session admin = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final CompletableFuture<Void> running = CompletableFuture.runAsync(() -> {
        try (final Transaction tx = runner.beginTransaction()) {
          tx.run("CREATE (:Written9689 {x: 1})").consume();
          tx.run(LONG_CYPHER).consume();
          tx.commit();
        }
      });

      final String id = awaitListed(admin);
      assertThat(admin.run("TERMINATE TRANSACTIONS $id", Map.of("id", id)).single().get("message").asString())
          .isEqualTo("Transaction terminated.");
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).satisfies(
          e -> assertThat(((ClientException) e.getCause()).code()).isEqualTo("Neo.ClientError.Transaction.Terminated"));

      // What the transaction wrote before the stopped statement did not survive it
      assertThat(admin.run("MATCH (n:Written9689) RETURN count(n) AS c").single().get("c").asLong()).isZero();
    }
  }

  @Test
  void aUserSeesAndStopsOnlyTheirOwnTransactions() throws Exception {
    createUser(OTHER);
    try (final Driver rootDriver = driver("root", DEFAULT_PASSWORD_FOR_TESTS);
        final Driver otherDriver = driver(OTHER, PASSWORD);
        final Session runner = rootDriver.session(SessionConfig.forDatabase(getDatabaseName()));
        final Session admin = rootDriver.session(SessionConfig.forDatabase(getDatabaseName()));
        final Session other = otherDriver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final CompletableFuture<Void> running = CompletableFuture.runAsync(() -> runner.run(LONG_CYPHER).consume());
      final String id = awaitListed(admin);

      // The other user lists only their own SHOW, and is told root's statement does not exist
      final List<Record> visible = other.run("SHOW TRANSACTIONS").list();
      assertThat(visible).hasSize(1);
      assertThat(visible.getFirst().get("username").asString()).isEqualTo(OTHER);
      assertThat(other.run("TERMINATE TRANSACTIONS $id", Map.of("id", id)).single().get("message").asString())
          .isEqualTo("Transaction not found.");
      assertThat(getServer(0).getRunningQueries().get(id).isTerminated()).isFalse();

      admin.run("TERMINATE TRANSACTIONS $id", Map.of("id", id)).consume();
      assertThatThrownBy(() -> running.get(30, TimeUnit.SECONDS)).isNotNull();
    }
  }

  /** The id of the long statement once SHOW TRANSACTIONS lists it. */
  private String awaitListed(final Session admin) {
    final String[] id = new String[1];
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50)).until(() -> {
      for (final Record row : admin.run("SHOW TRANSACTIONS").list())
        if (LONG_CYPHER.equals(row.get("currentQuery").asString())) {
          id[0] = row.get("transactionId").asString();
          return true;
        }
      return false;
    });
    return id[0];
  }

  private void createUser(final String name) {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(name) == null)
      security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private Driver driver(final String user, final String password) {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(user, password),
        Config.builder().withoutEncryption().withMaxConnectionPoolSize(4).build());
  }
}
