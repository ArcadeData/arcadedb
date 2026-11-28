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
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13583: the statements of the report, through the Neo4j driver. A comment between {@code CALL} and the name of a
 * procedure only the Bolt interception serves ({@code dbms.*}, {@code db.ping}) made the server answer ProcedureNotFound,
 * where Neo4j skips the comment. Neo4j-OGM tags every read-only statement with such a hint, its capability probe included.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue13583CommentAfterCallIT extends BaseBoltServerTest {

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

  /** The probe Neo4j-OGM sends, verbatim. */
  @Test
  void theOgmHintedProbeAnswersTheComponent() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Result result = session.run("CALL /*+ OGM READ_ONLY */ dbms.components() yield name");
      assertThat(result.keys()).containsExactly("name");
      final List<Record> records = result.list();
      assertThat(records).hasSize(1);
      assertThat(records.getFirst().get("name").asString()).isEqualTo("Neo4j Kernel");
    }
  }

  @Test
  void aLineCommentBetweenCallAndTheNameIsSkipped() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final List<Record> records = session.run("CALL // line comment\n dbms.components() YIELD name RETURN name").list();
      assertThat(records).hasSize(1);
      assertThat(records.getFirst().get("name").asString()).isEqualTo("Neo4j Kernel");
    }
  }

  /** The comment answers what the same statement answers without it, on every placement the report lists. */
  @Test
  void theCommentChangesNothingWhereverItSits() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final List<String> plain = names(session, "CALL dbms.components() YIELD name RETURN name");
      assertThat(plain).containsExactly("Neo4j Kernel");
      for (final String query : new String[] { "/* hint */ CALL dbms.components() YIELD name RETURN name",
          "CALL /* hint */ dbms.components() YIELD name RETURN name", "CALL/* hint */dbms.components() YIELD name RETURN name",
          "CALL dbms.components() /* hint */ YIELD name RETURN name",
          "CALL /* a */ // b\n /* c */ dbms.components() YIELD name RETURN name" })
        assertThat(names(session, query)).as(query).isEqualTo(plain);

      assertThat(session.run("CALL /*+ OGM READ_ONLY */ db.ping()").single().get("success").asBoolean()).isTrue();
    }
  }

  /** A procedure the engine serves was never affected, and still answers through whichever path takes it. */
  @Test
  void aHintedSchemaProcedureAnswersTheSchema() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:Issue13583Label {i: 1})").consume();

      final List<Record> served = session.run("CALL /*+ OGM READ_ONLY */ db.labels()").list();
      assertThat(served.stream().map(r -> r.get("label").asString())).contains("Issue13583Label");
      assertThat(session.run("CALL /*+ OGM READ_ONLY */ db.labels() YIELD label RETURN count(label) >= 0 AS ok").single().get("ok")
          .asBoolean()).isTrue();
    }
  }

  /** The interception must not swallow a write: a hinted call that goes on writing is the engine's, which refuses it. */
  @Test
  void aWriteAfterAHintedCallIsNotSwallowed() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      assertThatThrownBy(
          () -> session.run("CALL /*+ OGM READ_ONLY */ dbms.components() YIELD name CREATE (:Issue13583Written {n: name})").consume())
          .isInstanceOf(ClientException.class);
      assertThat(session.run("MATCH (n:Issue13583Written) RETURN count(n) AS c").single().get("c").asLong()).isZero();
    }
  }

  private static List<String> names(final Session session, final String query) {
    return session.run(query).list(r -> r.get("name").asString());
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
