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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code YIELD}/{@code WHERE} tail of a {@code SHOW} command served over Bolt is the clause the {@code SHOW} grammar defines,
 * and the rows it produces are bounded like the rows any other operation buffers. A command that starts with {@code CALL} keeps
 * the clauses a query may follow it with, under the same bounds.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class ShowCommandTailBoundsIT extends BaseBoltServerTest {
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
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void aShowTailMadeOfOtherClausesIsRefused() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      for (final String query : List.of(
          "SHOW DATABASES YIELD * UNWIND range(1, 3) AS x RETURN x",
          "SHOW DATABASES YIELD name, type UNWIND range(1, 3) AS a UNWIND range(1, 3) AS b RETURN name, a, b",
          "SHOW DATABASES YIELD name WITH name RETURN name",
          "SHOW PROCEDURES YIELD name UNWIND range(1, 3) AS x RETURN x",
          "SHOW FUNCTIONS YIELD name UNWIND range(1, 3) AS x RETURN x",
          "SHOW CURRENT USER YIELD user UNWIND range(1, 3) AS x RETURN x"))
        assertThatThrownBy(() -> session.run(query).list()).as(query).isInstanceOf(Neo4jException.class);

      // The connection answers the next statement
      assertThat(session.run("SHOW DATABASES YIELD name").list()).isNotEmpty();
    }
  }

  @Test
  void aShowTailKeepsTheClausesOfItsGrammar() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final List<Record> rows = session.run("SHOW DATABASES YIELD name, type WHERE type = 'system' RETURN name ORDER BY name LIMIT 5")
          .list();

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().get("name").asString()).isEqualTo("system");
    }
  }

  @Test
  void aCallTailKeepsTheClausesAFollowingQueryMayHold() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final List<Record> rows = session.run(
          "CALL dbms.components() YIELD name, versions, edition UNWIND versions AS version RETURN name, version, edition").list();

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().get("version").asString()).isEqualTo("5.26.0");
    }
  }

  @Test
  void theRowsOfATailAreBoundedByTheElementLimit() {
    getServerDatabase(0, getDatabaseName()).getConfiguration()
        .setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100L);

    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("CALL dbms.components() YIELD name UNWIND range(1, 50) AS x RETURN x").list()).hasSize(50);

      assertThatThrownBy(() -> session.run("CALL dbms.components() YIELD name UNWIND range(1, 1000) AS x RETURN x").list())
          .isInstanceOf(Neo4jException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

      // Nested ranges multiply: each one is short, their product is what is buffered
      assertThatThrownBy(() -> session.run(
          "CALL dbms.components() YIELD name UNWIND range(1, 40) AS a UNWIND range(1, 40) AS b RETURN a, b").list())
          .isInstanceOf(Neo4jException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());

      // The connection answers the next statement
      assertThat(session.run("CALL dbms.components() YIELD name UNWIND range(1, 10) AS x RETURN x").list()).hasSize(10);
    }
  }
}
