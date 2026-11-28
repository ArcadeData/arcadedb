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
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #13564: in READ access mode (BEGIN or RUN with mode "r", what {@code session.executeRead} sends) a statement calling a
 * namespaced function or a procedure that nothing answers was refused with {@code Neo.ClientError.Statement.AccessMode}, "Writing
 * in read access mode not allowed". A dotted name no built-in answers counts as a possible write, because it can be a
 * {@code DEFINE FUNCTION} (issue #6418), and the refusal came before the name was resolved. Neo4j answers
 * {@code Neo.ClientError.Statement.SyntaxError} for the function and {@code Neo.ClientError.Procedure.ProcedureNotFound} for the
 * procedure, whatever the access mode, and so did ArcadeDB outside READ mode.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class Issue13564BoltReadModeUnknownNameIT extends BaseBoltServerTest {

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

  /** The reported statements, in a READ transaction. */
  @Test
  void anUnknownNamespacedFunctionInAReadTransactionIsASyntaxError() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final String query : new String[] { "RETURN no.such.fn(1) AS x", "MATCH (n) RETURN no.such.fn(n) AS x",
          "RETURN apoc.no.such.fn(1) AS x", "RETURN nosuchfn(1) AS x" }) {
        final ClientException refused = catchThrowableOfType(ClientException.class,
            () -> session.executeRead(tx -> tx.run(query).consume()));

        assertThat(refused).as(query).isNotNull();
        assertThat(refused.code()).as(query).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
        assertThat(refused.getMessage()).as(query).contains("nown function");
      }
      // the session goes on reading
      final long read = session.executeRead(tx -> tx.run("RETURN 42 AS v").single().get("v").asLong());
      assertThat(read).isEqualTo(42L);
    }
  }

  /** The statements of the follow-up comment: an unknown procedure, also one that follows another clause. */
  @Test
  void anUnknownProcedureInAReadTransactionIsProcedureNotFound() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final String query : new String[] { "CALL nosuch.proc() YIELD x RETURN x",
          "CALL dbms.procedures() YIELD name RETURN count(*) AS c", "UNWIND [1] AS x CALL nosuch.proc() YIELD y RETURN y",
          "CALL { CALL nosuch.proc() YIELD x RETURN x } RETURN x", "PROFILE CALL nosuch.proc() YIELD x RETURN x" }) {
        final ClientException refused = catchThrowableOfType(ClientException.class,
            () -> session.executeRead(tx -> tx.run(query).consume()));

        assertThat(refused).as(query).isNotNull();
        assertThat(refused.code()).as(query).isEqualTo(BoltErrorCodes.PROCEDURE_NOT_FOUND_ERROR);
      }
      // a procedure that is there, after another clause, is read as before
      final long labels = session.executeRead(
          tx -> tx.run("UNWIND [1] AS x CALL db.labels() YIELD label RETURN count(label) AS c").single().get("c").asLong());
      assertThat(labels).isGreaterThanOrEqualTo(0L);
    }
  }

  /** The same on an auto-commit RUN in READ mode, which takes the access mode from the RUN message. */
  @Test
  void anUnknownNameInAReadAutoCommitQueryIsReportedAsWhatItIs() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("RETURN no.such.fn(1) AS x", Map.of("mode", "r"));
      final BoltWireConnection.Summary function = bolt.readSummary();
      assertThat(function.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(function.code()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.run("CALL nosuch.proc() YIELD x RETURN x", Map.of("mode", "r"));
      final BoltWireConnection.Summary procedure = bolt.readSummary();
      assertThat(procedure.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(procedure.code()).isEqualTo(BoltErrorCodes.PROCEDURE_NOT_FOUND_ERROR);
    }
  }

  /**
   * The refusal this must not loosen: a name that resolves to a {@code DEFINE FUNCTION} may write, so it stays a write in READ
   * mode, called as a function or as a procedure, and its body does not run.
   */
  @Test
  void aUserDefinedFunctionThatMayWriteIsStillRefusedInReadMode() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE Issue13564Doc");
    database.command("sql", "DEFINE FUNCTION issue13564.write \"INSERT INTO Issue13564Doc SET a = 1\" LANGUAGE sql");

    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final String query : new String[] { "RETURN issue13564.write() AS x", "CALL issue13564.write()",
          "UNWIND [1] AS i CALL issue13564.write() RETURN i" }) {
        final ClientException refused = catchThrowableOfType(ClientException.class,
            () -> session.executeRead(tx -> tx.run(query).consume()));

        assertThat(refused).as(query).isNotNull();
        assertThat(refused.code()).as(query).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);
      }
    }
    assertThat(database.countType("Issue13564Doc", false)).isZero();
  }

  /** A write next to an unknown name writes nothing in READ mode, whichever of the two errors names the statement. */
  @Test
  void aWriteNextToAnUnknownNameWritesNothingInReadMode() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (final String query : new String[] { "CREATE (n:Issue13564 {x: 1}) RETURN no.such.fn(n) AS x",
          "CREATE (n:Issue13564 {x: 2}) WITH n CALL nosuch.proc() YIELD y RETURN y" }) {
        final ClientException refused = catchThrowableOfType(ClientException.class,
            () -> session.executeRead(tx -> tx.run(query).consume()));

        assertThat(refused).as(query).isNotNull();
        assertThat(refused.code()).as(query).isIn(BoltErrorCodes.SYNTAX_ERROR, BoltErrorCodes.PROCEDURE_NOT_FOUND_ERROR);
      }
      assertThat(session.run("MATCH (n:Issue13564) RETURN count(n) AS c").single().get("c").asLong()).isZero();
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
