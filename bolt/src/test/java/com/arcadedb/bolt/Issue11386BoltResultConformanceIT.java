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
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11386: Bolt result and streaming responses that deviated from what Neo4j answers.
 * <ul>
 *   <li>PROFILE of a write with no RETURN answers no records, as the write does without PROFILE;</li>
 *   <li>a result with no rows announces the columns its RETURN declares in the RUN SUCCESS {@code fields};</li>
 *   <li>DISCARD to the end carries {@code type} and {@code t_last}, and a failure met while discarding is classified as the
 *   same failure met while pulling (already fixed by #11392, pinned here).</li>
 * </ul>
 */
public class Issue11386BoltResultConformanceIT extends BaseBoltServerTest {

  /** Fails at the 250th of 300 rows, past the first batch RUN reads, so the failure is met by PULL or DISCARD. */
  private static final String FAILS_MID_STREAM = "UNWIND range(1, 300) AS x RETURN 1 / (x - 250) AS v";

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

  /** The reported matrix: every write clause under PROFILE, with no RETURN, answers no record. */
  @Test
  void profileOfAReturnlessWriteAnswersNoRecords() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      for (int i = 0; i < 5; i++)
        session.run("CREATE (:PF {i: " + i + "})").consume();

      assertNoRecords(session, "PROFILE CREATE (:PF {i: 10})");
      assertNoRecords(session, "PROFILE MATCH (n:PF) SET n.x = 1");
      assertNoRecords(session, "PROFILE MATCH (n:PF) REMOVE n.x");
      assertNoRecords(session, "PROFILE MATCH (n:PF {i: 10}) DELETE n");
      assertNoRecords(session, "PROFILE MERGE (:PF {i: 11})");

      // the writes ran, and their profile is still reported
      assertThat(session.run("MATCH (n:PF) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(6L);
      final Result profiled = session.run("PROFILE MATCH (n:PF) SET n.y = 2");
      assertThat(profiled.list()).isEmpty();
      assertThat(profiled.consume().hasProfile()).isTrue();

      // control: a profiled write with a RETURN keeps its rows
      assertThat(session.run("PROFILE MATCH (n:PF) SET n.z = 3 RETURN n.i AS i").list()).hasSize(6);
    }
  }

  /** The reported wire exchange: a zero-row RETURN of two aliases announces both. */
  @Test
  void aZeroRowResultAnnouncesItsReturnColumnsOnTheWire() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("MATCH (n:PT) WHERE n.name = 'zzz' RETURN n.name AS nm, n.x AS x");
      final BoltWireConnection.Summary run = bolt.readSummary();
      assertThat(run.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(run.metadata().get("fields")).isEqualTo(List.of("nm", "x"));

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pulled.records()).isEmpty();
    }
  }

  /** The reported driver case: result.keys() of a zero-row result, and the same keys as when rows come back. */
  @Test
  void aZeroRowResultHasTheKeysOfTheSameResultWithRows() {
    try (final Driver driver = driver(); final Session session = session(driver)) {
      session.run("CREATE (:D {i: 1})").consume();

      final Result empty = session.run("MATCH (n:D) WHERE n.i > 100 RETURN n.i AS i, n AS node");
      assertThat(empty.keys()).containsExactly("i", "node");
      assertThat(empty.list()).isEmpty();

      final Result rows = session.run("MATCH (n:D) WHERE n.i > 0 RETURN n.i AS i, n AS node");
      assertThat(rows.keys()).containsExactly("i", "node");

      // an unaliased item is named the same way with and without rows
      final List<String> withRows = session.run("MATCH (n:D) WHERE n.i = 1 RETURN n.i + 1").keys();
      final List<String> withoutRows = session.run("MATCH (n:D) WHERE n.i = 99 RETURN n.i + 1").keys();
      assertThat(withoutRows).isEqualTo(withRows);

      // EXPLAIN answers no row, but still names the columns of what it explains
      assertThat(session.run("EXPLAIN MATCH (n:D) RETURN n.i AS i").keys()).containsExactly("i");

      // a write with no RETURN has no columns
      assertThat(session.run("CREATE (:D {i: 2})").keys()).isEmpty();
    }
  }

  /** Part 3, fixed by #11392: a DISCARD to the end of a partly read result reports the query type and t_last. */
  @Test
  void discardingTheRestReportsTypeAndTLast() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("UNWIND range(1, 10) AS x RETURN x");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(1, -1);
      assertThat(bolt.readSummary().records()).hasSize(1);

      bolt.discard(-1, -1);
      final BoltWireConnection.Summary discarded = bolt.readSummary();
      assertThat(discarded.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(discarded.metadata()).containsEntry("type", "r").containsKey("t_last").containsEntry("has_more", false);
    }
  }

  /** Part 3, fixed by #11392: a failure met while discarding carries the code the same failure carries under PULL. */
  @Test
  void aFailureWhileDiscardingIsClassifiedAsUnderPull() throws Exception {
    final String pullCode;
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run(FAILS_MID_STREAM);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.FAILURE);
      pullCode = pulled.code();
    }

    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run(FAILS_MID_STREAM);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.discard(-1, -1);
      final BoltWireConnection.Summary discarded = bolt.readSummary();
      assertThat(discarded.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(discarded.code()).isEqualTo(pullCode).isEqualTo(BoltErrorCodes.ARITHMETIC_ERROR);
    }
  }

  private static void assertNoRecords(final Session session, final String query) {
    assertThat(session.run(query).list()).as("records of %s", query).isEmpty();
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().withMaxTransactionRetryTime(0, TimeUnit.MILLISECONDS).build());
  }

  private Session session(final Driver driver) {
    return driver.session(SessionConfig.forDatabase(getDatabaseName()));
  }
}
