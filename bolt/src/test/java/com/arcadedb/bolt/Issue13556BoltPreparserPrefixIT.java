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
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.summary.QueryType;
import org.neo4j.driver.summary.ResultSummary;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13556: Neo4j's {@code CYPHER <version> [option=value ...]} preparser prefix, which {@code @neo4j/graphql} puts before every
 * statement, is dropped before the Bolt server classifies the statement: a prefixed read is a read (it runs in a READ access
 * mode), a prefixed write is a write, and EXPLAIN / PROFILE behind the prefix are still recognised.
 */
public class Issue13556BoltPreparserPrefixIT extends BaseBoltServerTest {

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
  void aPrefixedReadIsARead() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:A13556 {x: 1})").consume();

      assertThat(type(session, "CYPHER 5\nMATCH (n:A13556) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
      // a READ access mode lets a read through, the prefix included
      final long x = session.executeRead(tx -> tx.run("CYPHER 5 runtime = slotted MATCH (n:A13556) RETURN n.x AS x").single()
          .get("x").asLong());
      assertThat(x).isEqualTo(1L);
    }
  }

  @Test
  void aPrefixedWriteIsAWrite() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(type(session, "CYPHER 5\nCREATE (:B13556 {x: 1})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "CYPHER 5 CREATE (n:B13556 {x: 2}) RETURN n.x AS x")).isEqualTo(QueryType.READ_WRITE);

      // the prefix does not hide a write from the READ access mode
      final ClientException refused = catchThrowableOfType(ClientException.class,
          () -> session.executeRead(tx -> tx.run("CYPHER 5\nCREATE (:B13556 {x: 3})").consume()));
      assertThat(refused).isNotNull();
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);
      assertThat(session.run("MATCH (n:B13556) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(2L);
    }
  }

  @Test
  void explainAndProfileBehindThePrefixAreRecognised() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:A13556 {x: 1})").consume();

      // a PROFILE of a read is a read, not a write
      final ResultSummary profiled = session.executeRead(tx -> tx.run("CYPHER 5 PROFILE MATCH (n:A13556) RETURN n.x AS x").consume());
      assertThat(profiled.queryType()).isEqualTo(QueryType.READ_ONLY);
      assertThat(profiled.hasProfile()).isTrue();

      assertThat(session.run("CYPHER 5 EXPLAIN MATCH (n:A13556) RETURN n.x AS x").consume().hasPlan()).isTrue();
      assertThat(type(session, "CYPHER 5 EXPLAIN CREATE (:A13556 {x: 9})")).isEqualTo(QueryType.WRITE_ONLY);

      // a comment between the prefix and EXPLAIN / PROFILE changes nothing
      final ResultSummary commented = session.run("CYPHER 5 /* c */ EXPLAIN MATCH (n:A13556) RETURN n.x AS x").consume();
      assertThat(commented.hasPlan()).isTrue();
      assertThat(commented.queryType()).isEqualTo(QueryType.READ_ONLY);
      assertThat(session.run("CYPHER 5 // c\nPROFILE MATCH (n:A13556) RETURN n.x AS x").consume().hasProfile()).isTrue();
      assertThat(session.run("MATCH (n:A13556) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1L);
    }
  }

  @Test
  void aPrefixWithNoStatementIsASyntaxError() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final String text : new String[] { "CYPHER 5", "CYPHER 5 /* c */", "CYPHER runtime = slotted" }) {
        final ClientException error = catchThrowableOfType(ClientException.class, () -> session.run(text).consume());
        assertThat(error).as(text).isNotNull();
        assertThat(error.code()).as(text).isEqualTo("Neo.ClientError.Statement.SyntaxError");
      }
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
