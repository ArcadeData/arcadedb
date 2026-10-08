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
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.TransientException;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Issue #9518 over BOLT: a query the admission gate does not start is refused with a transient error, the class drivers
 * retry; the slot belongs to the result stream, so a stream the client has not finished reading keeps it; a second
 * stream of the same transaction shares it instead of waiting for it; and the slot goes back once the stream is done.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryAdmissionGateBoltIssue9518IT extends BaseBoltServerTest {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aQueryTheGateDoesNotStartIsATransientError() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    try (final Driver driver = driver(1000); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        assertThatThrownBy(() -> session.run("RETURN 1 AS one").consume()).isInstanceOf(TransientException.class);
      }

      // EVERY STREAM GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE SECOND QUERY
      for (int i = 0; i < 2; i++)
        assertThat(session.run("RETURN 1 AS one").single().get("one").asInt()).isEqualTo(1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  @Test
  void anUnfinishedStreamKeepsItsSlotAndASecondStreamOfItsTransactionSharesIt() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    // A FETCH SIZE OF 1 LEAVES THE STREAM OPEN ON THE SERVER AFTER THE FIRST ROW
    try (final Driver reader = driver(1); final Driver other = driver(1000)) {
      try (final Session session = reader.session(SessionConfig.forDatabase(getDatabaseName()));
          final Transaction tx = session.beginTransaction()) {
        final Result first = tx.run("UNWIND range(1, 10) AS x RETURN x");
        assertThat(first.next().get("x").asInt()).isEqualTo(1);
        assertThat(gate.getRunning()).isEqualTo(1);

        // ANOTHER CLIENT: THE ONLY SLOT IS HELD BY THE OPEN STREAM
        try (final Session otherSession = other.session(SessionConfig.forDatabase(getDatabaseName()))) {
          assertThatThrownBy(() -> otherSession.run("RETURN 1 AS one").consume()).isInstanceOf(TransientException.class);
        }

        // THE SAME TRANSACTION: SHARES THE SLOT ITS FIRST STREAM HOLDS, INSTEAD OF WAITING FOR IT
        assertThat(tx.run("RETURN 2 AS two").single().get("two").asInt()).isEqualTo(2);

        assertThat(first.list()).hasSize(9);
        tx.commit();
      }

      assertThat(gate.getRunning()).isZero();
      try (final Session otherSession = other.session(SessionConfig.forDatabase(getDatabaseName()))) {
        assertThat(otherSession.run("RETURN 1 AS one").single().get("one").asInt()).isEqualTo(1);
      }
    }
  }

  /**
   * A stream the client leaves open when it ends its transaction goes with the transaction, and so does its slot: the
   * next query of the same connection is admitted on its own rather than sharing a stale slot.
   */
  @Test
  void aStreamLeftOpenWhenItsTransactionEndsGivesItsSlotBack() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    // ONE CONNECTION, SO THE NEXT QUERY RUNS ON THE SAME SERVER THREAD AS THE ABANDONED STREAM
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withFetchSize(1).withMaxConnectionPoolSize(1).build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final Transaction tx = session.beginTransaction();
      final Result abandoned = tx.run("UNWIND range(1, 10) AS x RETURN x");
      assertThat(abandoned.next().get("x").asInt()).isEqualTo(1);
      assertThat(gate.getRunning()).isEqualTo(1);

      tx.rollback();
      assertThat(gate.getRunning()).as("the stream went with its transaction").isZero();

      final long admittedBefore = gate.getAdmitted();
      assertThat(session.run("RETURN 1 AS one").single().get("one").asInt()).isEqualTo(1);
      assertThat(gate.getAdmitted()).as("admitted on its own, not on the abandoned stream's slot").isEqualTo(admittedBefore + 1);
    }
    assertThat(gate.getRunning()).isZero();
  }

  /**
   * A client that drops its connection in the middle of a stream - no GOODBYE, no ROLLBACK - does not leak the stream's
   * slot: the connection thread closes every open stream on its way out.
   */
  @Test
  void aConnectionDroppedMidStreamGivesItsSlotBack() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final BoltWireConnection wire = new BoltWireConnection(getServerBoltPort(), getDatabaseName());
    wire.begin(getDatabaseName());
    wire.run("UNWIND range(1, 10) AS x RETURN x");
    assertThat(wire.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    wire.pull(1, -1);
    assertThat(wire.readSummary().records()).hasSize(1);
    assertThat(gate.getRunning()).as("the open stream holds the slot").isEqualTo(1);

    wire.close();
    await().atMost(Duration.ofSeconds(30)).until(() -> gate.getRunning() == 0);

    // THE SLOT IS FREE FOR THE NEXT CLIENT
    try (final Driver driver = driver(1000); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("RETURN 1 AS one").single().get("one").asInt()).isEqualTo(1);
    }
  }

  private Driver driver(final int fetchSize) {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withFetchSize(fetchSize).build());
  }
}
