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
import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7058: with an {@code LSM_VECTOR} index on the written property, sustained
 * {@code MERGE ... SET n = $props} writes over Bolt failed with {@code Neo.ClientError.Transaction.TransactionNotFound:
 * Transaction not begun}, while the identical workload passed once the vector index was dropped. The failure was
 * scale-dependent: a handful of writes per database completed, hundreds did not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7058BoltVectorSustainedWritesIT extends BaseGraphServerTest {

  private static final int DIMENSIONS = 384;
  private static final int WRITES     = 400;

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

  private Driver getDriver() {
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  private void createSchema(final String typeName) {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE " + typeName);
    db.command("sql", "CREATE PROPERTY " + typeName + ".uuid STRING");
    db.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
    db.command("sql", "CREATE PROPERTY " + typeName + ".name_embedding ARRAY_OF_FLOATS");
    db.command("sql", "CREATE INDEX ON " + typeName + " (uuid) NOTUNIQUE");
    db.command("sql", "CREATE INDEX ON " + typeName + " (name_embedding) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
        + ", similarity: 'COSINE'}");
  }

  private static Map<String, Object> props(final Random random, final int i) {
    final List<Double> embedding = new ArrayList<>(DIMENSIONS);
    for (int d = 0; d < DIMENSIONS; d++)
      embedding.add(random.nextDouble());
    final Map<String, Object> props = new HashMap<>();
    props.put("uuid", "uuid-" + i);
    props.put("name", "entity " + i);
    props.put("name_embedding", embedding);
    return props;
  }

  @Test
  void sustainedAutocommitMergeWritesSucceedWithAVectorIndex() {
    final String typeName = "Entity7058A";
    createSchema(typeName);
    final Random random = new Random(7058);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      for (int i = 0; i < WRITES; i++) {
        final Map<String, Object> props = props(random, i);
        session.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n:" + typeName + " SET n = $props",
            Map.of("uuid", props.get("uuid"), "props", props)).consume();
      }

      assertThat(session.run("MATCH (n:" + typeName + ") RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(WRITES);
    }
  }

  @Test
  void sustainedManagedTransactionMergeWritesSucceedWithAVectorIndex() {
    final String typeName = "Entity7058B";
    createSchema(typeName);
    final Random random = new Random(7058);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      for (int i = 0; i < WRITES; i++) {
        final Map<String, Object> props = props(random, i);
        session.executeWrite(tx -> tx.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n:" + typeName + " SET n = $props",
            Map.of("uuid", props.get("uuid"), "props", props)).consume());
      }

      assertThat(session.run("MATCH (n:" + typeName + ") RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(WRITES);
    }
  }

  /**
   * The reporter's workload carries a {@code SET n:<labels>} clause: with more than one label per node the vertex is
   * moved to a composite type that is created on the fly the first time its label combination shows up, and every
   * index of the parent type, the vector one included, is propagated to it.
   */
  @Test
  void sustainedMultiLabelMergeWritesSucceedWithAVectorIndex() {
    final String typeName = "Entity7058C";
    createSchema(typeName);
    final String[] secondaryLabels = { "Person7058C", "Organization7058C", "Location7058C", "Event7058C", "Topic7058C" };
    final Database db = getServerDatabase(0, getDatabaseName());
    for (final String label : secondaryLabels)
      db.command("sql", "CREATE VERTEX TYPE " + label);
    final Random random = new Random(7058);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      for (int i = 0; i < WRITES; i++) {
        final Map<String, Object> props = props(random, i);
        final String labels = typeName + ":" + secondaryLabels[i % secondaryLabels.length];
        session.executeWrite(tx -> tx.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n:" + labels + " SET n = $props",
            Map.of("uuid", props.get("uuid"), "props", props)).consume());
      }

      assertThat(session.run("MATCH (n:" + typeName + ") RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(WRITES);
    }
  }

  /** All the writes inside ONE explicit transaction, the way a driver's {@code begin_transaction()} loop does it. */
  @Test
  void manyMergeWritesInOneExplicitTransactionSucceedWithAVectorIndex() {
    final String typeName = "Entity7058D";
    createSchema(typeName);
    final Random random = new Random(7058);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      try (final Transaction tx = session.beginTransaction()) {
        for (int i = 0; i < WRITES; i++) {
          final Map<String, Object> props = props(random, i);
          tx.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n:" + typeName + " SET n = $props",
              Map.of("uuid", props.get("uuid"), "props", props)).consume();
        }
        tx.commit();
      }

      assertThat(session.run("MATCH (n:" + typeName + ") RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(WRITES);
    }
  }

  /**
   * A similarity search inside the same explicit transaction as the writes: the first search after a batch of writes
   * rebuilds the graph synchronously on the calling thread, and that rebuild must not touch the caller's transaction.
   */
  @Test
  void vectorSearchInsideAnExplicitTransactionKeepsTheTransactionOpen() {
    final String typeName = "Entity7058E";
    createSchema(typeName);
    final Random random = new Random(7058);

    try (final Driver driver = getDriver(); final Session session = driver.session(
        SessionConfig.forDatabase(getDatabaseName()))) {
      for (int i = 0; i < 50; i++) {
        final Map<String, Object> props = props(random, i);
        session.executeWrite(tx -> tx.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n = $props",
            Map.of("uuid", props.get("uuid"), "props", props)).consume());
      }

      final List<Double> query = new ArrayList<>(DIMENSIONS);
      for (int d = 0; d < DIMENSIONS; d++)
        query.add(random.nextDouble());

      try (final Transaction tx = session.beginTransaction()) {
        final Map<String, Object> props = props(random, 1000);
        tx.run("MERGE (n:" + typeName + " {uuid: $uuid}) SET n = $props", Map.of("uuid", props.get("uuid"), "props", props)).consume();
        final long found = tx.run("CALL db.index.vector.queryNodes('" + typeName + "[name_embedding]', 5, $q) YIELD node RETURN count(node) AS c",
            Map.of("q", query)).single().get("c").asLong();
        assertThat(found).isGreaterThan(0);
        tx.commit();
      }

      assertThat(session.run("MATCH (n:" + typeName + ") RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(51);
    }
  }
}
