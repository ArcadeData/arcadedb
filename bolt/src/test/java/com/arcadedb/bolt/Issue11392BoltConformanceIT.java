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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #11392: Bolt conformance gaps against the Bolt specification and what Neo4j answers.
 * <ul>
 *   <li>DISCARD n discards the next n records and keeps the stream open while records remain, as PULL n does;</li>
 *   <li>a write in READ access mode (BEGIN or RUN with mode "r", what {@code session.executeRead} sends) is refused with
 *   {@code Neo.ClientError.Statement.AccessMode};</li>
 *   <li>{@code tx_timeout} bounds the transaction: a statement past it is stopped, and a COMMIT past it is refused with
 *   {@code Neo.ClientError.Transaction.TransactionTimedOut}, the transaction rolled back;</li>
 *   <li>an index or a constraint equivalent to one under another name is
 *   {@code Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists}, not an argument error.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11392BoltConformanceIT extends BaseBoltServerTest {

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

  /** The reported exchange: DISCARD n=2 on four rows leaves two, which the following PULL reads. */
  @Test
  void discardNDiscardsTheNextNRecordsAndKeepsTheStreamOpen() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("UNWIND [1, 2, 3, 4] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.discard(2, -1);
      final BoltWireConnection.Summary discarded = bolt.readSummary();
      assertThat(discarded.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(discarded.records()).isEmpty();
      assertThat(discarded.metadata()).containsEntry("has_more", true);

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pulled.records()).containsExactly(List.of(3L), List.of(4L));
      assertThat(pulled.metadata()).containsEntry("has_more", false);
    }
  }

  /** DISCARD past the end closes the stream: the connection is READY for the next query. */
  @Test
  void discardingTheRestClosesTheStream() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("UNWIND [1, 2, 3] AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.discard(1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);
      bolt.discard(5, -1);
      final BoltWireConnection.Summary rest = bolt.readSummary();
      assertThat(rest.metadata()).containsEntry("has_more", false).containsEntry("type", "r");

      bolt.run("RETURN 1 AS one");
      assertThat(bolt.readSummary().signature()).as("the stream is closed, a new RUN is accepted").isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).containsExactly(List.of(1L));
    }
  }

  /** The reported case: session.executeRead could write. */
  @Test
  void aWriteInAReadTransactionIsRefused() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException refused = catchThrowableOfType(ClientException.class,
          () -> session.executeRead(tx -> tx.run("CREATE (:RO {x: 1})").consume()));

      assertThat(refused).isNotNull();
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);
      assertThat(session.run("MATCH (n:RO) RETURN count(n) AS c").single().get("c").asLong()).isZero();

      // reads are what a READ transaction is for
      final long read = session.executeRead(tx -> tx.run("RETURN 42 AS v").single().get("v").asLong());
      assertThat(read).isEqualTo(42L);
    }
  }

  @Test
  void aWriteInAReadAutoCommitQueryIsRefused() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("CREATE (:RO2 {x: 1})", Map.of("mode", "r"));
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.run("MATCH (n:RO2) RETURN count(n) AS c", Map.of("mode", "r"));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).containsExactly(List.of(0L));
    }
  }

  /** A write nested in a CALL subquery, a MERGE or a FOREACH is a write too: refused in READ mode, and nothing is written. */
  @Test
  void aNestedWriteInReadModeIsRefused() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      for (final String write : new String[] { "CALL { CREATE (:RO3 {x: 1}) } RETURN 1 AS one", "MERGE (:RO3 {x: 2})",
          "FOREACH (i IN [1, 2] | CREATE (:RO3 {x: i}))", "UNWIND [1] AS i CALL { WITH i CREATE (:RO3 {x: i}) } RETURN i" }) {
        bolt.run(write, Map.of("mode", "r"));
        final BoltWireConnection.Summary refused = bolt.readSummary();
        assertThat(refused.signature()).as(write).isEqualTo(BoltMessage.FAILURE);
        assertThat(refused.code()).as(write).isEqualTo(BoltErrorCodes.ACCESS_MODE_ERROR);
        bolt.sendNoFields(BoltMessage.RESET);
        assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      }
      bolt.run("MATCH (n:RO3) RETURN count(n) AS c");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).containsExactly(List.of(0L));
    }
  }

  /** A statement that does not parse is reported as such in READ access mode, not as a write refusal. */
  @Test
  void aMalformedQueryInReadModeIsASyntaxErrorNotAnAccessModeRefusal() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("MATC (n) RETRN n", Map.of("mode", "r"));
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
    }
  }

  /** A transaction that outlives its timeout is rolled back at COMMIT, whatever its statements did in time. */
  @Test
  void aCommitPastTheTransactionTimeoutIsRefusedAndRolledBack() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.sendMap(BoltMessage.BEGIN, Map.of("db", getDatabaseName(), "tx_timeout", 200L));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.run("CREATE (:TxTimeout {x: 1})");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      Thread.sleep(400);

      bolt.sendNoFields(BoltMessage.COMMIT);
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.run("MATCH (n:TxTimeout) RETURN count(n) AS c");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).as("the timed-out transaction was rolled back").containsExactly(List.of(0L));
    }
  }

  /** An absurd tx_timeout is no timeout at all in practice: it must not overflow into a deadline already past. */
  @Test
  void aHugeTransactionTimeoutDoesNotExpireAtOnce() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.sendMap(BoltMessage.BEGIN, Map.of("db", getDatabaseName(), "tx_timeout", Long.MAX_VALUE));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.run("RETURN 1 AS one");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).containsExactly(List.of(1L));
      bolt.sendNoFields(BoltMessage.COMMIT);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  /** The reported case: TransactionConfig.withTimeout did nothing, a statement ran far past it. */
  @Test
  void aStatementPastTheTransactionTimeoutIsStopped() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException stopped = catchThrowableOfType(ClientException.class,
          () -> session.executeRead(tx -> tx.run(LONG_QUERY).consume(), TransactionConfig.builder().withTimeout(Duration.ofMillis(50))
              .build()));

      assertThat(stopped).isNotNull();
      assertThat(stopped.code()).isEqualTo(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR);
    }
  }

  @Test
  void anAutoCommitQueryPastItsTimeoutIsStopped() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      final ClientException stopped = catchThrowableOfType(ClientException.class,
          () -> session.run(LONG_QUERY, TransactionConfig.builder().withTimeout(Duration.ofMillis(50)).build()).consume());

      assertThat(stopped).isNotNull();
      assertThat(stopped.code()).isEqualTo(BoltErrorCodes.TRANSACTION_TIMED_OUT_ERROR);

      // the control: the same query with no timeout runs to its end
      assertThat(session.run("UNWIND range(1, 1000) AS x WITH x WHERE x % 2 = 0 RETURN count(x) AS c").single().get("c").asLong())
          .isEqualTo(500L);
    }
  }

  /** The reported exchange: the same index under another name is an equivalent schema rule. */
  @Test
  void anEquivalentIndexUnderAnotherNameIsAnEquivalentSchemaRule() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE INDEX cyidxA FOR (n:CyZ) ON (n.a)").consume();

      final ClientException sameName = catchThrowableOfType(ClientException.class,
          () -> session.run("CREATE INDEX cyidxA FOR (n:CyZ) ON (n.a)").consume());
      assertThat(sameName.code()).as("unchanged, issue #10153").isEqualTo(BoltErrorCodes.INDEX_ALREADY_EXISTS_ERROR);

      final ClientException otherName = catchThrowableOfType(ClientException.class,
          () -> session.run("CREATE INDEX cyidxB FOR (n:CyZ) ON (n.a)").consume());
      assertThat(otherName.code()).isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);

      // IF NOT EXISTS still makes it a no-op
      session.run("CREATE INDEX cyidxC IF NOT EXISTS FOR (n:CyZ) ON (n.a)").consume();
    }
  }

  @Test
  void anEquivalentConstraintUnderAnotherNameIsAnEquivalentSchemaRule() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE CONSTRAINT cyuA FOR (n:CyU) REQUIRE n.a IS UNIQUE").consume();

      final ClientException otherName = catchThrowableOfType(ClientException.class,
          () -> session.run("CREATE CONSTRAINT cyuB FOR (n:CyU) REQUIRE n.a IS UNIQUE").consume());
      assertThat(otherName.code()).isEqualTo(BoltErrorCodes.SCHEMA_RULE_ALREADY_EXISTS_ERROR);
    }
  }

  /**
   * A statement the engine's command timeout is checked in, long enough to run for seconds without one: 25M rows, from two ranges
   * each well under arcadedb.queryMaxRangeSize, which refuses a single range of that size before it runs.
   */
  private static final String LONG_QUERY =
      "UNWIND range(1, 5000) AS a UNWIND range(1, 5000) AS b WITH a, b WHERE (a + b) % 7 = 0 RETURN count(*) AS c";

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
