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
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4141 (ISO/IEC 39075 GQL, section 2): end-to-end Bolt test for Session Management. A value set with
 * {@code SESSION SET $x = ...} must be visible as {@code $x} to a later command on the same Bolt connection,
 * {@code SESSION RESET} clears it, and {@code SESSION CLOSE} clears the session parameters.
 * <p>
 * Session parameters are connection-scoped, so the driver is pinned to a single pooled connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue4141BoltSessionManagementIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder()
            .withoutEncryption()
            // Session parameters are connection-scoped, so pin the driver to one server-side connection
            // for deterministic cross-command visibility in these tests.
            .withMaxConnectionPoolSize(1)
            .build());
  }

  @Test
  void sessionCloseDoesNotRollBackTheBoltTransaction() {
    // Asymmetry vs HTTP: over Bolt the connection/transaction lifecycle is the protocol's, so SESSION CLOSE
    // only clears session parameters - it must NOT roll back the connection's open transaction.
    try (final Driver driver = getDriver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction()) {
        tx.run("CREATE (n:BoltCloseAsym {id: 1})").consume();
        tx.run("SESSION CLOSE").consume();
        // The CREATE is still visible inside the transaction: SESSION CLOSE did not roll it back.
        final Result r = tx.run("MATCH (n:BoltCloseAsym {id: 1}) RETURN count(n) AS c");
        assertThat(r.next().get("c").asInt()).isEqualTo(1);
        tx.commit();
      }
    }
  }

  @Test
  void sessionParametersFlowAcrossCommandsOverBolt() {
    try (final Driver driver = getDriver();
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {

      // SESSION SET binds a parameter on the connection's session and echoes the operation/name/value.
      final Record setRec = session.run("SESSION SET $threshold = 21").single();
      assertThat(setRec.get("operation").asString()).isEqualTo("set");
      assertThat(setRec.get("name").asString()).isEqualTo("threshold");
      assertThat(setRec.get("value").asInt()).isEqualTo(21);

      // A later command on the same connection sees it as $threshold.
      Result result = session.run("RETURN $threshold AS t");
      assertThat(result.next().get("t").asInt()).isEqualTo(21);

      // SESSION RESET clears it: $threshold is now unbound (null).
      session.run("SESSION RESET").consume();
      result = session.run("RETURN $threshold AS t");
      assertThat(result.next().get("t").isNull()).isTrue();

      // SESSION SET again, then SESSION CLOSE clears the session parameters.
      session.run("SESSION SET $threshold = 99").consume();
      session.run("SESSION CLOSE").consume();
      result = session.run("RETURN $threshold AS t");
      assertThat(result.next().get("t").isNull()).isTrue();
    }
  }
}
