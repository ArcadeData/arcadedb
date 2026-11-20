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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.utility.FailPoints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.TransientException;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12662: a write statement refused by a concurrent transaction while it runs, in an explicit transaction that had written
 * nothing yet, runs again in a fresh transaction on the server instead of failing the transaction back to the driver. The refusal
 * is injected at the creation of the first record the statement writes, the way a concurrent commit refuses it there.
 */
public class Bolt12662FirstWriteRetryIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    FailPoints.disarmAll();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  /** Refuses the first {@code times} record creations the way a concurrent commit does. */
  private static AtomicInteger refuseRecordCreations(final int times) {
    final AtomicInteger refused = new AtomicInteger();
    FailPoints.arm(FailPoints.BUCKET_CREATE_RECORD, point -> {
      if (refused.get() < times) {
        refused.incrementAndGet();
        throw new ConcurrentModificationException("injected refusal");
      }
    });
    return refused;
  }

  @Test
  void theFirstWriteOfATransactionIsRunAgainInAFreshOne() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction()) {
        // A statement that writes nothing, as Graphiti's bulk transaction sends before its writes
        tx.run("UNWIND [] AS row CREATE (n:Row {id: row})").consume();
        final AtomicInteger refused = refuseRecordCreations(1);
        tx.run("UNWIND [1, 2, 3] AS row CREATE (n:Row {id: row})").consume();
        assertThat(refused.get()).isEqualTo(1);
        tx.commit();
      }
      assertThat(session.run("MATCH (n:Row) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(3L);
    }
  }

  @Test
  void aWriteAfterTheFirstOneIsNotRunAgain() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction()) {
        tx.run("CREATE (n:Row {id: 0})").consume();
        final AtomicInteger refused = refuseRecordCreations(1);
        assertThatThrownBy(() -> tx.run("UNWIND [1, 2, 3] AS row CREATE (n:Row {id: row})").consume())
            .isInstanceOf(TransientException.class);
        assertThat(refused.get()).isEqualTo(1);
      }
      FailPoints.disarmAll();
      // The transaction the refusal failed wrote nothing that stays
      assertThat(session.run("MATCH (n:Row) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(0L);
    }
  }

  @Test
  void aStatementRefusedAtEveryAttemptGoesBackToTheDriver() {
    try (final Driver driver = getDriver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction()) {
        final AtomicInteger refused = refuseRecordCreations(Integer.MAX_VALUE);
        assertThatThrownBy(() -> tx.run("CREATE (n:Row {id: 1})").consume()).isInstanceOf(TransientException.class);
        assertThat(refused.get()).isEqualTo(GlobalConfiguration.TX_RETRIES.getValueAsInteger());
      }
    }
  }
}
