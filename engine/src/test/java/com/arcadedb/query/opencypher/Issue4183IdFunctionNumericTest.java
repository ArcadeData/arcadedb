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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4183.
 * <p>
 * The OpenCypher {@code id(n)} function must return a numeric (Long) value to match Neo4j semantics.
 * Previously it returned the RID as a string (e.g. {@code "#1:0"}), which silently failed numeric
 * predicates such as {@code WHERE id(n) >= 0} (returning 0 rows instead of all rows).
 * <p>
 * The Neo4j-compatible way to obtain a string identifier is {@code elementId(n)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4183IdFunctionNumericTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4183-id-numeric").create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'Alice'})");
      database.command("opencypher", "CREATE (:Person {name:'Bob'})");
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
  void idFunctionReturnsNumericValue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) RETURN id(n) AS ident ORDER BY n.name");
    assertThat(rs.hasNext()).isTrue();
    final Result first = rs.next();
    final Object firstId = first.getProperty("ident");
    assertThat(firstId).isInstanceOf(Number.class);

    assertThat(rs.hasNext()).isTrue();
    final Result second = rs.next();
    final Object secondId = second.getProperty("ident");
    assertThat(secondId).isInstanceOf(Number.class);

    assertThat(((Number) firstId).longValue()).isNotEqualTo(((Number) secondId).longValue());
  }

  @Test
  void idNumericPredicateGreaterOrEqualZeroReturnsAllRows() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE id(n) >= 0 RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    assertThat(names).containsExactly("Alice", "Bob");
  }

  @Test
  void idIsNotNullReturnsAllRows() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE id(n) IS NOT NULL RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    assertThat(names).containsExactly("Alice", "Bob");
  }

  @Test
  void idCombinedWithStringPredicate() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person) WHERE n.name > 'Alice' AND id(n) >= 0 RETURN n.name AS name ORDER BY name");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    assertThat(names).containsExactly("Bob");
  }

  @Test
  void idForRelationshipReturnsNumeric() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)"));

    final ResultSet rs = database.query("opencypher",
        "MATCH ()-[r:KNOWS]->() RETURN id(r) AS ident");
    assertThat(rs.hasNext()).isTrue();
    final Object value = rs.next().getProperty("ident");
    assertThat(value).isInstanceOf(Number.class);
  }

  @Test
  void idForRelationshipNumericPredicate() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)"));

    final ResultSet rs = database.query("opencypher",
        "MATCH ()-[r:KNOWS]->() WHERE id(r) >= 0 RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("cnt")).longValue()).isEqualTo(1L);
  }

  @Test
  void idCanBeUsedToLookupVertexAsParameter() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person {name:'Alice'}) RETURN id(n) AS ident");
    final Object aliceId = rs.next().getProperty("ident");
    assertThat(aliceId).isInstanceOf(Number.class);

    final ResultSet rs2 = database.query("opencypher",
        "MATCH (n) WHERE id(n) = $id RETURN n.name AS name",
        Map.of("id", aliceId));
    assertThat(rs2.hasNext()).isTrue();
    assertThat(rs2.next().<String>getProperty("name")).isEqualTo("Alice");
    assertThat(rs2.hasNext()).isFalse();
  }

  @Test
  void elementIdStillReturnsStringRid() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:Person {name:'Alice'}) RETURN elementId(n) AS ident");
    assertThat(rs.hasNext()).isTrue();
    final Object value = rs.next().getProperty("ident");
    assertThat(value).isInstanceOf(String.class);
    assertThat((String) value).startsWith("#");
  }
}
