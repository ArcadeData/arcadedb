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
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10614 over the wire: a row produced before a later row's label removal held, in its path, the node and the
 * relationship that removal rewrote and deleted, and the Bolt PULL failed serializing it with
 * {@code Neo.ClientError.Statement.EntityNotFound: Record #4:0 not found}. The engine side is pinned by
 * {@link com.arcadedb.query.opencypher.CypherLabelWriteRedirectsEmittedRowsIssue10614Test}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10614BoltPathAfterLabelRemovalIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @Override
  protected void populateDatabase() {
    // The Cypher setup below creates every type it needs
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void theReportersQueryAnswersBothRowsWithTheirPaths() {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        s.run("CREATE (a {id: 13})-[:rt9]->(b:l9 {id: 27}), (b)-[:rt8]->(a)").consume();

        final List<Record> rows = s.run("""
            MATCH p0 = (n0) <-[:rt7|rt8|rt9*1]-()
            WITH n0 AS n1, p0 AS p1, n0.k4 AS alias2
            WHERE n1.id IN [13, 27]
            REMOVE n1.k10, n1:l9:l7
            RETURN DISTINCT alias2, n1, p1""").list();

        assertThat(rows).hasSize(2);
        final List<Long> ids = new ArrayList<>();
        for (final Record row : rows) {
          assertThat(row.get("alias2").isNull()).isTrue();
          final Node n1 = row.get("n1").asNode();
          final long id = n1.get("id").asLong();
          ids.add(id);
          assertThat(n1.labels()).doesNotContain("l9");

          final Path p1 = row.get("p1").asPath();
          assertThat(p1.length()).isEqualTo(1);
          assertThat(p1.start().get("id").asLong()).isEqualTo(id);
          assertThat(p1.end().get("id").asLong()).isEqualTo(id == 13 ? 27 : 13);
          assertThat(p1.end().labels()).doesNotContain("l9");
          final Relationship r = p1.relationships().iterator().next();
          assertThat(r.type()).isEqualTo(id == 13 ? "rt8" : "rt9");
          // The relationship points from the far node into n1, as the pattern matched it
          assertThat(r.startNodeElementId()).isEqualTo(p1.end().elementId());
          assertThat(r.endNodeElementId()).isEqualTo(p1.start().elementId());
        }
        assertThat(ids).containsExactlyInAnyOrder(13L, 27L);

        assertThat(s.run("MATCH ()-[r]->() RETURN count(r) AS c").single().get("c").asLong()).isEqualTo(2L);
        assertThat(s.run("MATCH (n:l9) RETURN count(n) AS c").single().get("c").asLong()).isZero();
      }
    }
  }
}
