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
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.AccessMode;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13682: {@code CALL tx.getMetaData} answers the {@code tx_metadata} the transaction was begun with (BEGIN, or the RUN of an
 * auto-commit statement), in any access mode. It was an unknown procedure, and in a READ transaction a refused write.
 */
public class Issue13682BoltTxGetMetaDataIT extends BaseBoltServerTest {
  private static final Map<String, Object> METADATA = Map.of("foo", 1.5, "bar", "baz");

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

  private static TransactionConfig withMetadata() {
    return TransactionConfig.builder().withMetadata(METADATA).build();
  }

  /** A transaction function, in READ and in WRITE access mode: the read mode refused the call as a write before. */
  @Test
  void aTransactionFunctionReadsItsMetadata() {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.executeRead(tx -> tx.run("CALL tx.getMetaData").single().get(0).asMap(), withMetadata())).isEqualTo(METADATA);
      assertThat(session.executeWrite(tx -> tx.run("CALL tx.getMetaData").single().get(0).asMap(), withMetadata())).isEqualTo(METADATA);
    }
  }

  /** An explicit transaction, with the call spelled with parentheses and a YIELD, and its metadata gone once it is over. */
  @Test
  void anExplicitTransactionReadsItsMetadata() {
    try (final Driver driver = driver(); final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction(withMetadata())) {
        assertThat(tx.run("CALL tx.getMetaData()").single().get("metadata").asMap()).isEqualTo(METADATA);
        assertThat(tx.run("CALL tx.getMetaData() YIELD metadata RETURN metadata.foo AS f").single().get("f").asDouble()).isEqualTo(1.5);
        tx.commit();
      }
      // The next transaction sets none: it sees none
      assertThat(session.run("CALL tx.getMetaData").single().get(0).asMap()).isEmpty();
    }
  }

  /** An auto-commit statement carries its own, in any access mode. */
  @Test
  void anAutoCommitStatementReadsItsMetadata() {
    try (final Driver driver = driver();
        final Session read = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).withDefaultAccessMode(AccessMode.READ).build())) {
      assertThat(read.run(new Query("CALL tx.getMetaData"), withMetadata()).single().get(0).asMap()).isEqualTo(METADATA);
      assertThat(read.run("CALL tx.getMetaData").single().get(0).asMap()).isEmpty();
    }
  }

  private Driver driver() {
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }
}
