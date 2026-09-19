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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7946, over the wire the issue was reported on.
 * <p>
 * {@code SHOW DATABASES WHERE name = $dbName} is what the Neo4j driver's multi-database bootstrap runs against the
 * {@code system} database to decide whether the target database has to be created. ArcadeDB answers
 * {@code SHOW DATABASES} from the server rather than from a query plan, and the {@code WHERE} was parsed and then
 * dropped, so the query reported every database on the server whatever was bound to {@code $dbName} - and could
 * never answer "this one does not exist".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7946ShowDatabasesWhereIT extends BaseGraphServerTest {
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
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void theWhereClauseSelectsTheNamedDatabase() {
    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase("system"))) {

      final List<Record> all = session.run("SHOW DATABASES").list();
      assertThat(all).isNotEmpty();
      final String existing = all.getFirst().get("name").asString();

      final List<Record> matching = session.run("SHOW DATABASES WHERE name = $dbName", Map.of("dbName", existing))
          .list();
      assertThat(matching).hasSize(1);
      assertThat(matching.getFirst().get("name").asString()).isEqualTo(existing);
    }
  }

  /**
   * The point of the query: a name that has never existed must come back empty, so the bootstrap can tell the two
   * answers apart.
   */
  @Test
  void aDatabaseThatDoesNotExistAnswersNoRows() {
    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase("system"))) {

      assertThat(session.run("SHOW DATABASES WHERE name = $dbName",
          Map.of("dbName", "totally_bogus_never_created_xyz")).list()).isEmpty();
    }
  }

  @Test
  void aYieldProjectsTheColumnsItNames() {
    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase("system"))) {

      final List<Record> rows = session.run("SHOW DATABASES YIELD name, currentStatus WHERE name = $dbName",
          Map.of("dbName", "system")).list();

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().keys()).containsExactly("name", "currentStatus");
      assertThat(rows.getFirst().get("currentStatus").asString()).isEqualTo("online");
    }
  }

  /**
   * A command with no tail still answers exactly what it always did.
   */
  @Test
  void theBareCommandIsUnchanged() {
    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase("system"))) {

      final List<Record> rows = session.run("SHOW DATABASES").list();
      assertThat(rows).isNotEmpty();
      assertThat(rows.getFirst().keys()).contains("name", "type", "access", "currentStatus");
    }
  }
}
