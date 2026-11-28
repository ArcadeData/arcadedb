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
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13541: over Bolt {@code SHOW PROCEDURES} and {@code SHOW FUNCTIONS} always answered no row, whatever the {@code YIELD} and
 * the {@code WHERE} said, so a client that probed a database for a procedure was told nothing was there. They are answered from the
 * engine's registries now, with the {@code dbms.*} calls this layer serves itself added. Through the Neo4j Java driver, as a client
 * sees it; the names, columns and values are what Neo4j 5.26 answers for the same statements.
 */
public class Issue13541BoltShowRoutinesIT extends BaseBoltServerTest {

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

  @Test
  void aProcedureThatExistsIsListed() {
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name = 'db.labels' RETURN name")).containsExactly("db.labels");
    assertThat(names("SHOW PROCEDURES YIELD name RETURN name")).contains("db.labels", "db.propertyKeys", "db.relationshipTypes");
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name = 'no.such.procedure' RETURN name")).isEmpty();
  }

  @Test
  void theCallsOfTheBoltLayerAreListedToo() {
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'dbms.' OR name = 'db.ping' RETURN name ORDER BY name"))
        .contains("db.ping", "dbms.components", "dbms.info");
    assertThat(names("SHOW PROCEDURES YIELD name WHERE name = 'dbms.components' RETURN name")).containsExactly("dbms.components");
  }

  @Test
  void aFunctionIsListedUnderNeo4jSpelling() {
    assertThat(names("SHOW FUNCTIONS YIELD name WHERE name IN ['toString', 'abs'] RETURN name ORDER BY name"))
        .containsExactly("abs", "toString");
    assertThat(names("SHOW BUILT IN FUNCTIONS YIELD name WHERE name = 'abs' RETURN name")).containsExactly("abs");
    assertThat(names("SHOW FUNCTIONS YIELD name RETURN name")).hasSizeGreaterThan(50);
  }

  @Test
  void theColumnsAreNeo4jsAndAreReportedWithoutARow() {
    assertThat(keys("SHOW PROCEDURES")).containsExactly("name", "description", "mode", "worksOnSystem");
    assertThat(keys("SHOW PROCEDURES YIELD *")).containsExactly("name", "description", "mode", "worksOnSystem", "signature",
        "argumentDescription", "returnDescription", "admin", "rolesExecution", "rolesBoostedExecution", "isDeprecated", "deprecatedBy",
        "option");
    assertThat(keys("SHOW FUNCTIONS")).containsExactly("name", "category", "description");
    assertThat(keys("SHOW FUNCTIONS YIELD *")).containsExactly("name", "category", "description", "signature", "isBuiltIn",
        "argumentDescription", "returnDescription", "aggregating", "rolesExecution", "rolesBoostedExecution", "isDeprecated",
        "deprecatedBy");
    assertThat(keys("SHOW PROCEDURES YIELD name WHERE name = 'no.such.procedure'")).containsExactly("name");
  }

  private List<String> names(final String statement) {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      return session.run(statement).list(record -> record.get("name").asString());
    }
  }

  private List<String> keys(final String statement) {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final Result result = session.run(statement);
      final List<String> keys = result.keys();
      result.consume();
      return keys;
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }
}
