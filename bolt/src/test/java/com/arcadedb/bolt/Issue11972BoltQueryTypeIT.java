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
import org.neo4j.driver.summary.QueryType;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  void eachStatementReportsTheQueryTypeNeo4jReports() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThat(type(session, "CREATE (n:A11972 {x: 1})")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "CREATE (n:A11972 {x: 1}) RETURN n.x AS x")).isEqualTo(QueryType.READ_WRITE);
      assertThat(type(session, "CREATE (:B11972)")).isEqualTo(QueryType.WRITE_ONLY);
      assertThat(type(session, "MATCH (a:A11972), (b:B11972) MERGE (a)-[r:R11972]->(b) RETURN type(r) AS t"))
          .isEqualTo(QueryType.READ_WRITE);
      assertThat(type(session, "UNWIND [1, 2] AS i CALL { WITH i CREATE (:A11972 {i: i}) } RETURN count(*) AS c"))
          .isEqualTo(QueryType.READ_WRITE);
      // a write whose MATCH finds nothing still returns its columns
      assertThat(type(session, "MATCH (n:Nope11972) SET n.x = 1 RETURN n")).isEqualTo(QueryType.READ_WRITE);
      assertThat(session.run("CREATE (n:A11972 $props) RETURN n", Map.of("props", Values.value(Map.of("x", 2)))).consume()
          .queryType()).isEqualTo(QueryType.READ_WRITE);

      assertThat(type(session, "CREATE INDEX idx_a11972_x FOR (n:A11972) ON (n.x)")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "DROP INDEX idx_a11972_x")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "CREATE CONSTRAINT c_b11972_x FOR (n:B11972) REQUIRE n.x IS UNIQUE")).isEqualTo(QueryType.SCHEMA_WRITE);
      assertThat(type(session, "DROP CONSTRAINT c_b11972_x")).isEqualTo(QueryType.SCHEMA_WRITE);

      assertThat(type(session, "MATCH (n:A11972) RETURN n.x AS x")).isEqualTo(QueryType.READ_ONLY);
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
