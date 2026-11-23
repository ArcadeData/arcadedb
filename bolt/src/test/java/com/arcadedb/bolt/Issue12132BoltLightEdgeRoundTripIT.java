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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Relationship;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12132: a driver reads the element id of a lightweight relationship and sends it back in
 * {@code WHERE elementId(r) = $id}. Every lightweight edge of a type used to share one element id, so that deleted every edge of
 * the type, and a lightweight relationship could not be returned over Bolt at all.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@SuppressWarnings("deprecation")
public class Issue12132BoltLightEdgeRoundTripIT extends BaseBoltServerTest {

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
  void theElementIdOfOneLightweightRelationshipSentBackDeletesThatRelationshipOnly() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE A12132");
    db.command("sql", "CREATE VERTEX TYPE B12132");
    db.command("sql", "CREATE EDGE TYPE L12132 LIGHTWEIGHT");

    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (a:A12132 {k:'a'}), (b1:B12132 {k:'b1'}), (b2:B12132 {k:'b2'}), (b3:B12132 {k:'b3'}) "
          + "CREATE (a)-[:L12132]->(b1), (a)-[:L12132]->(b2), (a)-[:L12132]->(b3)").consume();

      final List<Record> rows = session.run("MATCH (:A12132)-[r:L12132]->(b:B12132) RETURN r, b.k AS k ORDER BY k").list();
      assertThat(rows).hasSize(3);
      final List<Relationship> relationships = rows.stream().map(row -> row.get("r").asRelationship()).toList();
      assertThat(relationships).extracting(Relationship::elementId).doesNotHaveDuplicates();
      // no record id is negative, so a synthetic id of a lightweight edge cannot be mistaken for one
      assertThat(relationships).extracting(Relationship::id).doesNotHaveDuplicates().allMatch(id -> id < 0);

      // the id read from the b1 edge names that edge only
      final int deleted = session.run("MATCH ()-[r:L12132]->() WHERE elementId(r) = $id DELETE r",
          Map.of("id", relationships.getFirst().elementId())).consume().counters().relationshipsDeleted();
      assertThat(deleted).isEqualTo(1);

      final List<String> left = session.run("MATCH (:A12132)-[:L12132]->(b:B12132) RETURN b.k AS k ORDER BY k")
          .list(row -> row.get("k").asString());
      assertThat(left).containsExactly("b2", "b3");
    }
  }
}
