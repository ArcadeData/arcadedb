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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13583: a {@code CALL} with an {@code OGM READ_ONLY} hint comment before {@code dbms.components()} answered ProcedureNotFound, because the built-in
 * {@code dbms.*} calls exist only in the Bolt interception and that anchors on a normalized text that starts with
 * {@code call <name>}: a comment between the two sent the call to the engine, where the procedure is unknown. Neo4j-OGM tags every
 * statement it takes for read-only that way. Through the Neo4j Java driver, as a client sees it, and at the level of the
 * normalization.
 */
public class Issue13583BoltCommentBeforeProcedureIT extends BaseBoltServerTest {

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
  void aCommentBetweenCallAndTheNameDoesNotHideABuiltInProcedure() {
    for (final String statement : new String[] { "CALL dbms.components() YIELD name RETURN name",
        "CALL /*+ OGM READ_ONLY */ dbms.components() yield name", "CALL // line comment\n dbms.components() YIELD name RETURN name",
        "/* hint */ CALL dbms.components() YIELD name RETURN name", "CALL dbms.components() /* hint */ YIELD name RETURN name",
        "CALL/*a*/ /*b*/dbms.components() YIELD name" })
      assertThat(firstName(statement)).as(statement).isEqualTo("Neo4j Kernel");
  }

  @Test
  void theCommentIsSkippedOnlyWhereItIsOnlyBlank() {
    assertThat(BoltSystemProcedures.normalize("CALL /*+ OGM READ_ONLY */ dbms.components() yield name"))
        .isEqualTo("call dbms.components() yield name");
    assertThat(BoltSystemProcedures.normalize("call // c\n  db.ping()")).isEqualTo("call db.ping()");
    assertThat(BoltSystemProcedures.normalize("/* a */ CALL /* b */ db.labels()")).isEqualTo("call db.labels()");
    // an unterminated comment leaves nothing to serve, as it does in front of the statement
    assertThat(BoltSystemProcedures.normalize("CALL /* never closed dbms.components()")).isEqualTo("call ");
    // a comment that is not between CALL and its name is not touched, nor is a name that merely starts with it
    assertThat(BoltSystemProcedures.normalize("CALL db.labels() /* x */ YIELD label")).isEqualTo("call db.labels() /* x */ yield label");
    assertThat(BoltSystemProcedures.normalize("CALLX /* x */ y")).isEqualTo("callx /* x */ y");
    assertThat(BoltSystemProcedures.isSystemCall(BoltSystemProcedures.normalize("CALL /* x */ dbms.components() YIELD name"),
        "dbms.components")).isTrue();
  }

  private String firstName(final String statement) {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build()); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      return session.run(statement).list().getFirst().get("name").asString();
    }
  }
}
