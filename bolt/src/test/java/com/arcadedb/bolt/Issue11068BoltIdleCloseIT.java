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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseIdleCloser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11068 over Bolt: a database the server closed for idleness is reopened by the next statement of a connection that
 * stayed open across the close, and an explicit transaction open on the connection keeps the database open until it ends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11068BoltIdleCloseIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    // Driven by the test: the scheduled check waits an hour
    config.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL, TimeUnit.HOURS.toMillis(1));
  }

  @AfterEach
  void reopen() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      server.getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
      server.getDatabaseForOperator(getDatabaseName());
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void aConnectionHeldAcrossAnIdleCloseReopensTheDatabase() throws Exception {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:IdleBolt {i: 1})").consume();

      assertThat(closeIdle()).isEqualTo(1);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isFalse();

      assertThat(session.run("MATCH (n:IdleBolt) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    }
  }

  @Test
  void anExplicitTransactionKeepsTheDatabaseOpen() throws Exception {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction transaction = session.beginTransaction()) {
        transaction.run("CREATE (:IdleBoltTx {i: 1})").consume();

        assertThat(closeIdle()).as("a quiet transaction is still a transaction").isZero();

        transaction.run("CREATE (:IdleBoltTx {i: 2})").consume();
        transaction.commit();
      }
      assertThat(session.run("MATCH (n:IdleBoltTx) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(2);
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  /**
   * Checks a little apart, until one closes something: the first samples the database, a later one finds it unused past a 1 ms
   * timeout. Several, so that background work landing between two of them delays the close instead of failing the test.
   */
  private int closeIdle() throws InterruptedException {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 1L);
    final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
    int closed = closer.check();
    for (int attempt = 0; attempt < 10 && closed == 0; attempt++) {
      Thread.sleep(20);
      closed += closer.check();
    }
    return closed;
  }
}
