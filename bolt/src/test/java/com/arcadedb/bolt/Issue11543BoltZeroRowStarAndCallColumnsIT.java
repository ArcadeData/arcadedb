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
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11543, follow-up of #11386: a zero-row result announces its columns in the RUN SUCCESS {@code fields} also when they
 * are not written as RETURN items: the variables in scope of a {@code RETURN *}, and the yielded fields of a standalone
 * {@code CALL} of a procedure the engine serves (not one {@code BoltSystemProcedures} answers with fixed fields).
 */
public class Issue11543BoltZeroRowStarAndCallColumnsIT extends BaseBoltServerTest {

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

  /** The reported wire exchange for shape 1: {@code MATCH (n:Missing) RETURN *} announces {@code [n]}. */
  @Test
  void aZeroRowReturnStarAnnouncesTheVariablesInScopeOnTheWire() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("MATCH (n:Missing) RETURN *");
      final BoltWireConnection.Summary run = bolt.readSummary();
      assertThat(run.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(run.metadata().get("fields")).isEqualTo(List.of("n"));

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pulled.records()).isEmpty();
    }
  }

  /** Shape 1 through the driver: the keys of a zero-row {@code RETURN *} are the keys of the same statement with rows. */
  @Test
  void aZeroRowReturnStarHasTheKeysOfTheSameResultWithRows() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:S {i: 1})-[:T]->(:S {i: 2})").consume();

      final Result empty = session.run("MATCH (a:S)-[r:T]->(b:S) WHERE a.i > 100 RETURN *");
      assertThat(empty.keys()).containsExactly("a", "r", "b");
      assertThat(empty.list()).isEmpty();
      assertThat(session.run("MATCH (a:S)-[r:T]->(b:S) RETURN *").keys()).containsExactly("a", "r", "b");

      assertThat(session.run("MATCH (a:S) WHERE a.i > 100 WITH a, a.i AS i RETURN *").keys()).containsExactly("a", "i");
      assertThat(session.run("UNWIND [] AS x RETURN *").keys()).containsExactly("x");
    }
  }

  /** Shape 2: a standalone CALL of an engine-served procedure that yields no row announces what it yields. */
  @Test
  void aZeroRowCallAnnouncesItsYieldedFields() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("CALL algo.wcc() YIELD componentId AS c WHERE c IS NULL");
      final BoltWireConnection.Summary run = bolt.readSummary();
      assertThat(run.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(run.metadata().get("fields")).isEqualTo(List.of("c"));

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pulled.records()).isEmpty();
    }

    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Result yielded = session.run("CALL algo.wcc() YIELD node, componentId WHERE componentId IS NULL");
      assertThat(yielded.keys()).containsExactly("node", "componentId");
      assertThat(yielded.list()).isEmpty();
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
