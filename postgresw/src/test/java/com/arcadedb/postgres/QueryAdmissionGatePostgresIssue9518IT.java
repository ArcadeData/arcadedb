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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9518 over the Postgres protocol: a statement waits for the query admission gate like an HTTP request, on both
 * the simple and the extended protocol; one the gate does not start is refused with SQLSTATE 40001, the code drivers
 * retry on; and every statement gives its slot back.
 * <p>
 * The test takes the only slot itself, through the JVM-wide gate the protocol uses, so what a statement does while it
 * is taken does not depend on timing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryAdmissionGatePostgresIssue9518IT extends PostgresWireProtocolTestBase {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    super.endTest();
  }

  @Test
  void aStatementTheGateDoesNotStartIsRefusedWithARetryableStateOnBothProtocols() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    for (final String mode : new String[] { "simple", "extended" }) {
      try (final Connection conn = getConnection(mode); final Statement st = conn.createStatement()) {
        try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
          assertThatThrownBy(() -> st.executeQuery("SELECT 1 AS one")).as(mode).isInstanceOf(SQLException.class)
              .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("40001"));
        }

        // EVERY STATEMENT GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE SECOND
        for (int i = 0; i < 2; i++)
          try (final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("one")).isEqualTo(1);
          }
      }
      assertThat(gate.getRunning()).as(mode).isZero();
    }
  }

  @Test
  void aStatementWaitsForAFreeSlot() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(60_000L);

    for (final String mode : new String[] { "simple", "extended" }) {
      try (final Connection conn = getConnection(mode); final Statement st = conn.createStatement()) {
        final int queuedBefore = gate.getQueued();
        final CompletableFuture<Integer> answer;
        final QueryAdmissionGate.Ticket held = gate.admit();
        try {
          answer = CompletableFuture.supplyAsync(() -> {
            try (final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
              return rs.next() ? rs.getInt("one") : -1;
            } catch (final SQLException e) {
              throw new RuntimeException(e);
            }
          });
          await().atMost(Duration.ofSeconds(30)).until(() -> gate.getQueued() == queuedBefore + 1 || answer.isDone());
          assertThat(answer).as(mode + ": the statement waits while the only slot is taken").isNotDone();
        } finally {
          held.close();
        }
        assertThat(answer.get(30, TimeUnit.SECONDS)).as(mode).isEqualTo(1);
      }
    }
  }

  private Connection getConnection(final String mode) throws SQLException, ClassNotFoundException {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    // simple: every statement is a 'Q' message; extended: Parse/Bind/Describe/Execute
    props.setProperty("preferQueryMode", mode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
