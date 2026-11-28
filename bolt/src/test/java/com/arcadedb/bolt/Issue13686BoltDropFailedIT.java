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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13686: a {@code DROP INDEX} or {@code DROP CONSTRAINT} of a name nothing holds answered
 * {@code Neo.ClientError.Statement.SemanticError}, where Neo4j answers {@code Neo.DatabaseError.Schema.IndexDropFailed} /
 * {@code ConstraintDropFailed}: a {@code DatabaseError}, which is what langchain-neo4j's {@code Neo4jVector} catches around the
 * {@code DROP INDEX} of {@code pre_delete_collection=True}. Through the Neo4j Java driver, as a client sees it.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue13686BoltDropFailedIT extends BaseBoltServerTest {
  private static final String INDEX_DROP_FAILED      = "Neo.DatabaseError.Schema.IndexDropFailed";
  private static final String CONSTRAINT_DROP_FAILED = "Neo.DatabaseError.Schema.ConstraintDropFailed";
  private static final String NO_SUCH_INDEX          = "Unable to drop index called `vector`. There is no such index.";
  private static final String NO_SUCH_CONSTRAINT     = "Unable to drop constraint `nothing`: No such constraint nothing.";

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
  void dropOfAMissingIndexIsIndexDropFailed() {
    assertFailure("DROP INDEX vector", Map.of(), INDEX_DROP_FAILED, NO_SUCH_INDEX);
  }

  @Test
  void dropOfAMissingIndexByParameterNamesTheValue() {
    assertFailure("DROP INDEX $n", Map.of("n", "vector"), INDEX_DROP_FAILED, NO_SUCH_INDEX);
  }

  @Test
  void dropOfAMissingConstraintIsConstraintDropFailed() {
    assertFailure("DROP CONSTRAINT nothing", Map.of(), CONSTRAINT_DROP_FAILED, NO_SUCH_CONSTRAINT);
    assertFailure("DROP CONSTRAINT $c", Map.of("c", "nothing"), CONSTRAINT_DROP_FAILED, NO_SUCH_CONSTRAINT);
  }

  @Test
  void createDropAndDropAgain() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE INDEX vector FOR (n:A13686) ON (n.p)").consume();
      assertThat(session.run("DROP INDEX vector").consume().counters().indexesRemoved()).isEqualTo(1);
    }
    assertFailure("DROP INDEX vector", Map.of(), INDEX_DROP_FAILED, NO_SUCH_INDEX);
  }

  @Test
  void anIndexNamedByAParameterIsTheOneDropped() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE INDEX $n FOR (n:A13686) ON (n.q)", Map.of("n", "byparam")).consume();
      assertThat(session.run("DROP INDEX $n", Map.of("n", "byparam")).consume().counters().indexesRemoved()).isEqualTo(1);
    }
    assertFailure("DROP INDEX $n", Map.of("n", "byparam"), INDEX_DROP_FAILED, "`byparam`");
  }

  @Test
  void ifExistsStillSucceeds() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(session.run("DROP INDEX vector IF EXISTS").consume().counters().indexesRemoved()).isZero();
      assertThat(session.run("DROP CONSTRAINT nothing IF EXISTS").consume().counters().constraintsRemoved()).isZero();
    }
  }

  /** Every other schema error stays the SemanticError of a statement that means nothing against the schema. */
  @Test
  void anotherSchemaErrorStaysASemanticError() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT t13686 FOR (n:T13686) REQUIRE n.x IS :: INTEGER").consume();
    }
    assertFailure("CREATE CONSTRAINT s13686 FOR (n:T13686) REQUIRE n.x IS :: STRING", Map.of(),
        "Neo.ClientError.Statement.SemanticError", "'t13686'", "already requires another type");
  }

  private void assertFailure(final String statement, final Map<String, Object> parameters, final String code,
      final String... reason) {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Neo4jException error = catchThrowableOfType(Neo4jException.class, () -> session.run(statement, parameters).consume());
      assertThat(error).as(statement).isNotNull();
      assertThat(error.code()).as(statement).isEqualTo(code);
      if (code.startsWith("Neo.DatabaseError"))
        assertThat(error).as(statement).isInstanceOf(DatabaseException.class);
      assertThat(error.getMessage()).as(statement).contains(reason);
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
