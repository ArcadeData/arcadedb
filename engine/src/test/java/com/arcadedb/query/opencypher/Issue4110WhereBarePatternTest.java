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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4110.
 * <p>
 * A bare uncorrelated pattern predicate in {@code WHERE} (e.g., {@code WHERE ()-[]->()})
 * is an existence test over the entire graph: it must evaluate to {@code true} for every
 * outer row when at least one matching relationship exists in the graph, and {@code false}
 * otherwise. {@code NOT} must invert correctly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4110WhereBarePatternTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4110-where-bare-pattern").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'Alice'}), (:Person {name:'Bob'})");
      database.command("opencypher",
          "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void barePatternPredicateMatchesAllWhenRelationshipExists() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE ()-[]->() RETURN p.name AS name ORDER BY name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Bob");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void notBarePatternPredicateRejectsAllWhenRelationshipExists() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE NOT ()-[]->() RETURN p.name AS name ORDER BY name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void barePatternPredicateRejectsAllWhenNoRelationshipExists() {
    database.transaction(() -> database.command("opencypher", "MATCH ()-[r:KNOWS]->() DELETE r"));
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE ()-[]->() RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void barePatternPredicateMatchesByRelationshipType() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE ()-[:KNOWS]->() RETURN p.name AS name ORDER BY name");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Bob");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void barePatternPredicateRejectsByMissingRelationshipType() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE ()-[:NONEXISTENT]->() RETURN p.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }
}
