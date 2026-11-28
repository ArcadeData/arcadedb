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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.Neo4jException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #11973: a Bolt FAILURE for a statement that failed while it ran said "Error executing Cypher command: &lt;statement&gt;",
 * the message of the query engine's wrapper, and nothing about why. It carries the failure its code is classified from, as the
 * HTTP API's {@code detail} does. Issue #11785: a semantic error an analyzer wrapped in a parsing failure keeps its code and its
 * reason. Through the Neo4j Java driver, as a client sees it.
 */
public class Issue11973BoltFailureMessageIT extends BaseBoltServerTest {
  private static final String WRAPPER = "Error executing Cypher";

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

  @BeforeEach
  void createSchema() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT c11973 FOR (n:U11973) REQUIRE n.x IS UNIQUE").consume();
      session.run("CREATE INDEX i11973 FOR (n:A11973) ON (n.x)").consume();
      session.run("CREATE (:U11973 {x: 1}), (:A11973 {x: 1}), (:D11973 {x: 1}), (:D11973 {x: 1})").consume();
    }
  }

  @Test
  void aUniqueViolationNamesTheDuplicatedKey() {
    assertFailure("CREATE (:U11973 {x: 1})", "Neo.ClientError.Schema.ConstraintValidationFailed", "Duplicated key", "[1]");
  }

  @Test
  void aConstraintThatExistsSaysSo() {
    assertFailure("CREATE CONSTRAINT c11973 FOR (n:U11973) REQUIRE n.x IS UNIQUE",
        "Neo.ClientError.Schema.ConstraintWithNameAlreadyExists", "'c11973'", "already exists");
  }

  @Test
  void anEquivalentIndexNamesTheIndexThatHasTheProperties() {
    assertFailure("CREATE INDEX i2 FOR (n:A11973) ON (n.x)", "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists", "'i11973'");
  }

  @Test
  void aUniqueConstraintOverDuplicatesNamesTheDuplicatedKey() {
    assertFailure("CREATE CONSTRAINT d11973 FOR (n:D11973) REQUIRE n.x IS UNIQUE",
        "Neo.ClientError.Schema.ConstraintValidationFailed", "Duplicated key");
  }

  @Test
  void droppingAnIndexOrConstraintThatDoesNotExistNamesIt() {
    // Neo4j's codes and words since issue #13686
    assertFailure("DROP INDEX nope", "Neo.DatabaseError.Schema.IndexDropFailed", "`nope`", "There is no such index.");
    assertFailure("DROP CONSTRAINT nope", "Neo.DatabaseError.Schema.ConstraintDropFailed", "`nope`", "No such constraint nope.");
  }

  @Test
  void anArgumentErrorSaysWhatTheArgumentIs() {
    assertFailure("MATCH (n:A11973) SET n = 5", "Neo.ClientError.Statement.ArgumentError", "must be a map, a node or a relationship");
  }

  /** Issue #11785: apoc.do.when analyzes the inner statement first, and the analyzer wrapped its semantic error. */
  @Test
  void aSemanticErrorTheAnalyzerWrapsKeepsItsCodeAndReason() {
    assertFailure("CALL apoc.do.when(true, 'MATCH (n) RETURN m', '') YIELD value RETURN value",
        "Neo.ClientError.Statement.SemanticError", "Variable 'm' not defined");
  }

  private void assertFailure(final String statement, final String code, final String... reason) {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Neo4jException error = catchThrowableOfType(Neo4jException.class, () -> session.run(statement).consume());
      assertThat(error).as(statement).isNotNull();
      assertThat(error.code()).as(statement).isEqualTo(code);
      assertThat(error.getMessage()).as(statement).doesNotStartWith(WRAPPER).doesNotStartWith("Error analyzing").contains(reason);
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
