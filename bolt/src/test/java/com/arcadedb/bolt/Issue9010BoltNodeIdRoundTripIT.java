/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The numeric id in a Bolt node structure is the value {@code id(n)} returns, so {@code MATCH (m) WHERE id(m) = $id} with it
 * finds the same node (#9010).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("deprecation")
public class Issue9010BoltNodeIdRoundTripIT extends BaseBoltServerTest {

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
  void nodeStructureIdIsTheIdFunctionValue() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:N9010 {name: 'a'}), (:N9010 {name: 'b'})").consume();

      final List<Record> rows = session.run("MATCH (n:N9010) RETURN n, id(n) AS id ORDER BY n.name").list();
      assertThat(rows).hasSize(2);
      for (final Record row : rows) {
        final Node node = row.get("n").asNode();
        assertThat(node.id()).isEqualTo(row.get("id").asLong());
        final List<String> found = session.run("MATCH (m) WHERE id(m) = $id RETURN m.name AS name", Map.of("id", node.id()))
            .list(r -> r.get("name").asString());
        assertThat(found).containsExactly(node.get("name").asString());
      }
    }
  }

  @Test
  void relationshipStructureIdsAreTheIdFunctionValues() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:M9010 {name: 'a'})-[:REL9010]->(:M9010 {name: 'b'})").consume();

      final Record row = session.run("MATCH (a:M9010)-[r:REL9010]->(b:M9010) RETURN r, id(r) AS rid, id(a) AS aid, id(b) AS bid").single();
      final Relationship rel = row.get("r").asRelationship();
      assertThat(rel.id()).isEqualTo(row.get("rid").asLong());
      assertThat(rel.startNodeId()).isEqualTo(row.get("aid").asLong());
      assertThat(rel.endNodeId()).isEqualTo(row.get("bid").asLong());
    }
  }

  @Test
  void deleteOfNodeWithSeveralRelationshipsWithReturn() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (c:C8997 {id: 1})-[:R8997]->(:C8997 {id: 2}), (c)-[:R8997]->(:C8997 {id: 3})").consume();

      final List<Record> rows = session.run("MATCH (n:C8997 {id: 1})-[r:R8997]-() DELETE r, n RETURN 1 AS one").list();
      assertThat(rows).hasSize(2);
      assertThat(session.run("MATCH (n:C8997) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(2L);
    }
  }
}
