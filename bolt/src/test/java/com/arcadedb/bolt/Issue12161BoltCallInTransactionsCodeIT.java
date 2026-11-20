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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ImplicitTransactionRequiredException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #12161: {@code CALL { ... } IN TRANSACTIONS} refused inside an explicit or managed transaction answered
 * {@code Neo.DatabaseError.General.UnknownError}. Neo4j answers {@code Neo.DatabaseError.Transaction.TransactionStartFailed}, so
 * a driver that branches on the code behaves the same against both servers. The auto-commit form keeps running in batches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue12161BoltCallInTransactionsCodeIT extends BaseBoltServerTest {
  private static final String STATEMENT = "UNWIND range(1, 5) AS i CALL { WITH i CREATE (:C12161 {i: i}) } IN TRANSACTIONS OF 2 ROWS";

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
  void refusedInAnExplicitTransactionWithNeo4jsCode() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Neo4jException refused;
      try (final Transaction tx = session.beginTransaction()) {
        refused = catchThrowableOfType(Neo4jException.class, () -> tx.run(STATEMENT).consume());
      }
      assertThat(refused).isNotNull();
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.TRANSACTION_START_FAILED_ERROR);
      assertThat(refused.getMessage()).contains("can only be executed in an implicit transaction");
      assertThat(count(session)).isZero();
    }
  }

  @Test
  void refusedInAManagedTransactionWithNeo4jsCode() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final Neo4jException refused = catchThrowableOfType(Neo4jException.class,
          () -> session.executeWrite(tx -> tx.run(STATEMENT).consume()));
      assertThat(refused).isNotNull();
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.TRANSACTION_START_FAILED_ERROR);
      assertThat(count(session)).isZero();
    }
  }

  @Test
  void runsInAnAutoCommitQuery() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run(STATEMENT).consume();
      assertThat(count(session)).isEqualTo(5L);
    }
  }

  /** The code comes from the type, not from the message: a plain CommandExecutionException stays a database error. */
  @Test
  void classifiedByTheExceptionType() {
    assertThat(BoltNetworkExecutor.classifyExecutionError(new ImplicitTransactionRequiredException("refused"),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.TRANSACTION_START_FAILED_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(
        new CommandExecutionException("wrapped", new ImplicitTransactionRequiredException("refused")),
        BoltErrorCodes.DATABASE_ERROR)).isEqualTo(BoltErrorCodes.TRANSACTION_START_FAILED_ERROR);
    assertThat(BoltNetworkExecutor.classifyExecutionError(new CommandExecutionException("other"), BoltErrorCodes.DATABASE_ERROR))
        .isEqualTo(BoltErrorCodes.DATABASE_ERROR);
  }

  private long count(final Session session) {
    return session.run("MATCH (n:C12161) RETURN count(n) AS c").single().get("c").asLong();
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
