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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that write commands populate QueryStatistics with Neo4j-compatible CRUD counts,
 * and that read queries carry no statistics.
 */
class CypherQueryStatisticsTest extends TestHelper {

  private QueryStatistics statsOf(final Database db, final String cypher) {
    final ResultSet rs = db.command("opencypher", cypher);
    while (rs.hasNext())
      rs.next();
    final Optional<QueryStatistics> s = rs.getStatistics();
    assertThat(s).isPresent();
    return s.get();
  }

  @Test
  void createNodesRelationshipAndProperties() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE (:Beer {name:'IPA'})-[:BREWED_BY {since:1990}]->(:Brewery {name:'Acme'})");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getRelationshipsCreated()).isEqualTo(1);
      assertThat(s.getPropertiesSet()).isEqualTo(3); // name, since, name
      assertThat(s.getLabelsAdded()).isEqualTo(2);   // Beer, Brewery
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void setPropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'a'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Person {name:'a'}) SET n.age = 30, n:Employee");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsAdded()).isEqualTo(1);
    });
  }

  @Test
  void removePropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag:Old {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Tag {name:'x'}) REMOVE n.name, n:Old");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsRemoved()).isEqualTo(1);
    });
  }

  @Test
  void deleteNode() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Doomed {id:1})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Doomed {id:1}) DELETE n");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
    });
  }

  @Test
  void mergeCreatesOnlyOnce() {
    database.transaction(() -> {
      final QueryStatistics first = statsOf(database, "MERGE (:City {name:'Rome'})");
      assertThat(first.getNodesCreated()).isEqualTo(1);
      final ResultSet rs = database.command("opencypher", "MERGE (:City {name:'Rome'})");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics().get().getNodesCreated()).isZero();
    });
  }

  @Test
  void readQueryHasNoStatistics() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:R {n:1})");
      final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics()).isEmpty();
    });
  }
}
