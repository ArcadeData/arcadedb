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
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.QueryType;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #11972: the summary's query type was only ever {@code r} or {@code w}. A write that returns columns is {@code rw} and an
 * index or constraint statement is {@code s}, as Neo4j reports them.
 */
public class Issue11972BoltQueryTypeIT extends BaseBoltServerTest {

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

  /** A write that returns nothing is a write only: the type the server answered before, which must not change. */
  @Test
  void aWriteWithoutReturnIsWriteOnly() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(type(session, "CREATE (n:A11972 {x: 1})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "CREATE (:B11972)")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "MATCH (n:A11972) SET n.y = 1")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "MERGE (:C11972 {x: 1})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "MATCH (n:C11972) DELETE n")).isEqualTo(QueryType.WRITE_ONLY);
    }
  }

  /** The reported matrix: a write that returns columns is read-write. */
  @Test
  void aWriteReturningColumnsIsReadWrite() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:B11972)").consume();

      assertThat(type(session, "CREATE (n:A11972 {x: 1}) RETURN n.x AS x")).isEqualTo(QueryType.READ_WRITE);
      assertThat(type(session, "MATCH (a:A11972), (b:B11972) MERGE (a)-[r:R11972]->(b) RETURN type(r) AS t"))
          .isEqualTo(QueryType.READ_WRITE);
      assertThat(type(session, "UNWIND [1, 2] AS i CALL { WITH i CREATE (:A11972 {i: i}) } RETURN count(*) AS c"))
          .isEqualTo(QueryType.READ_WRITE);
      assertThat(session.run("CREATE (n:A11972 $props) RETURN n", Map.of("props", Values.value(Map.of("x", 2)))).consume()
          .queryType()).isEqualTo(QueryType.READ_WRITE);
    }
  }

  /** A write whose MATCH finds nothing answers no row, and is still read-write: the columns are the statement's, not the rows'. */
  @Test
  void aWriteThatFindsNothingStillReturnsItsColumns() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(type(session, "MATCH (n:Nope11972) SET n.x = 1 RETURN n")).isEqualTo(QueryType.READ_WRITE);
      // the reported spelling: the OPTIONAL MATCH answers one row of nulls
      assertThat(type(session, "OPTIONAL MATCH (n:Nope11972) SET n.x = 1 RETURN n")).isEqualTo(QueryType.READ_WRITE);
      // control: no RETURN, no columns, a write only
      assertThat(type(session, "MATCH (n:Nope11972) SET n.x = 1")).isEqualTo(QueryType.WRITE_ONLY);
    }
  }

  /** Index and constraint statements are schema writes, whether the schema changes or the statement is a no-op. */
  @Test
  void aSchemaStatementIsSchemaWrite() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(type(session, "CREATE INDEX idx_a11972_x FOR (n:A11972) ON (n.x)")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "CREATE INDEX idx_a11972_x IF NOT EXISTS FOR (n:A11972) ON (n.x)")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "DROP INDEX idx_a11972_x")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "DROP INDEX idx_a11972_x IF EXISTS")).isEqualTo(QueryType.SCHEMA_WRITE);

      assertThat(type(session, "CREATE CONSTRAINT c_b11972_x FOR (n:B11972) REQUIRE n.x IS UNIQUE")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "CREATE CONSTRAINT c_b11972_x IF NOT EXISTS FOR (n:B11972) REQUIRE n.x IS UNIQUE"))
          .isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "DROP CONSTRAINT c_b11972_x")).isEqualTo(QueryType.SCHEMA_WRITE);
    }
  }

  /** What writes nothing is read-only, whether it finds rows or not. */
  @Test
  void aReadIsReadOnly() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:A11972 {x: 1})").consume();

      assertThat(type(session, "MATCH (n:A11972) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
      assertThat(type(session, "MATCH (n:Nope11972) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
      assertThat(type(session, "RETURN 1 AS one")).isEqualTo(QueryType.READ_ONLY);
      assertThat(type(session, "SHOW INDEXES")).isEqualTo(QueryType.READ_ONLY);
    }
  }

  /** The type is the same inside an explicit transaction, where RUN and PULL are the same messages. */
  @Test
  void theTypeIsTheSameInAnExplicitTransaction() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.executeWrite(tx -> {
        assertThat(tx.run("CREATE (n:A11972 {x: 1})").consume().queryType()).isEqualTo(QueryType.WRITE_ONLY);
        assertThat(tx.run("CREATE (n:A11972 {x: 2}) RETURN n.x AS x").consume().queryType()).isEqualTo(QueryType.READ_WRITE);
        assertThat(tx.run("MATCH (n:A11972) RETURN n.x AS x").consume().queryType()).isEqualTo(QueryType.READ_ONLY);
        return null;
      });
      final QueryType read = session.executeRead(tx -> tx.run("MATCH (n:A11972) RETURN n.x AS x").consume().queryType());
      assertThat(read).isEqualTo(QueryType.READ_ONLY);
    }
  }

  /** EXPLAIN and PROFILE report the type of the statement they wrap, as Neo4j does. */
  @Test
  void explainAndProfileReportTheTypeOfTheStatementTheyWrap() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:A11972 {x: 1})").consume();

      assertThat(type(session, "EXPLAIN MATCH (n:A11972) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
      assertThat(type(session, "PROFILE MATCH (n:A11972) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
      assertThat(type(session, "EXPLAIN CREATE (n:A11972 {x: 3})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "PROFILE CREATE (n:A11972 {x: 3})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "EXPLAIN CREATE (n:A11972 {x: 3}) RETURN n.x AS x")).isEqualTo(QueryType.READ_WRITE);
      assertThat(type(session, "PROFILE CREATE (n:A11972 {x: 3}) RETURN n.x AS x")).isEqualTo(QueryType.READ_WRITE);
    }
  }

  /** Reading the wrapped statement's type must not change what a READ access mode lets through: a PROFILE of a write is refused. */
  @Test
  void explainAndProfileKeepTheirAccessModeRules() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final QueryType explainedWrite = session.executeRead(tx -> tx.run("EXPLAIN CREATE (:A11972 {x: 1})").consume().queryType());
      assertThat(explainedWrite).isEqualTo(QueryType.WRITE_ONLY);
      final QueryType profiledRead = session.executeRead(tx -> tx.run("PROFILE MATCH (n:A11972) RETURN n.x AS x").consume().queryType());
      assertThat(profiledRead).isEqualTo(QueryType.READ_ONLY);

      final ClientException refused = catchThrowableOfType(ClientException.class,
          () -> session.executeRead(tx -> tx.run("PROFILE CREATE (:A11972 {x: 2})").consume()));
      assertThat(refused).isNotNull();
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);
      assertThat(session.run("MATCH (n:A11972) RETURN count(n) AS c").single().get("c").asLong()).isZero();
    }
  }

  private static QueryType type(final Session session, final String statement) {
    return session.run(statement).consume().queryType();
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
