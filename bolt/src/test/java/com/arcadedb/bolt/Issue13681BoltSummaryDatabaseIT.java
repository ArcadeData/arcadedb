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
import com.arcadedb.bolt.message.BoltMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13681: the last SUCCESS of a result stream carried no {@code db}, so {@code ResultSummary.database().name()} was
 * {@code null} in a driver. Neo4j sends the name of the database the statement ran on.
 */
public class Issue13681BoltSummaryDatabaseIT extends BaseBoltServerTest {

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

  /** The driver's view, in an auto-commit query, a transaction function and an explicit transaction. */
  @Test
  void theSummaryNamesTheDatabase() {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("RETURN 1").consume().database().name()).isEqualTo(getDatabaseName());
      assertThat(session.run("CREATE (:N13681)").consume().database().name()).isEqualTo(getDatabaseName());
      assertThat(session.executeRead(tx -> tx.run("RETURN 1").consume()).database().name()).isEqualTo(getDatabaseName());
      assertThat(session.executeWrite(tx -> tx.run("RETURN 1").consume()).database().name()).isEqualTo(getDatabaseName());
    }
  }

  /** On the wire: the SUCCESS that ends a stream carries it, the one of a PULL that leaves rows behind does not. */
  @Test
  void onlyTheLastSuccessOfAStreamCarriesIt() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.pull(2, -1);
      final BoltWireConnection.Summary partial = bolt.readSummary();
      assertThat(partial.records()).hasSize(2);
      assertThat(partial.metadata()).containsEntry("has_more", true).doesNotContainKey("db");

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary last = bolt.readSummary();
      assertThat(last.records()).hasSize(1);
      assertThat(last.metadata()).containsEntry("db", getDatabaseName());

      // A DISCARD that ends a stream answers it as well
      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.discard(-1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("db", getDatabaseName());
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }
}
