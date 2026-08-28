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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6843: a {@code REMOVE n:Subtype} on a vertex whose type was declared with
 * {@code EXTENDS} left the node with no label at all.
 * <p>
 * The reduced type was rebuilt from the vertex's OWN labels, which for {@code Cust_Agent EXTENDS Entity} is
 * {@code [Cust_Agent]} alone. Take {@code Cust_Agent} away and that set is empty, so the vertex was moved to the
 * unlabelled sentinel type - dropping {@code Entity}, a label the clause never named and one the node answered to
 * a moment earlier. The user's whole point in writing {@code SET n:Entity REMOVE n:Cust_Agent} was to keep the base
 * label, and the query silently did the opposite.
 * <p>
 * The invariant under test is Neo4j's, the same one issue #6363 pinned for the other direction:
 * {@code labels(n)} after {@code REMOVE n:A} is {@code labels(n)} before it, minus {@code A} - never minus more.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubtypeLabelTransitionIssue6843Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-subtype-label-transition-6843");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Entity");
      database.command("sql", "CREATE VERTEX TYPE Cust_Agent EXTENDS Entity");
      database.command("sql", "CREATE VERTEX TYPE EntityType");
      database.command("sql", "CREATE EDGE TYPE INSTANCE");
      database.command("sql", "INSERT INTO Cust_Agent SET project_id = 'p1', name = 'a1', class_iris = ['keep','drop']");
      database.command("sql", "INSERT INTO EntityType SET name = 'et1', id = 't1'");
      database.command("sql",
          "CREATE EDGE INSTANCE FROM (SELECT FROM Cust_Agent WHERE name = 'a1') TO (SELECT FROM EntityType WHERE name = 'et1')");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void removingTheSubtypeLabelKeepsTheInheritedBaseLabel() {
    // The reduced form from the issue. Was: type '~NO_LABEL~', labels [] - the node stopped being an Entity.
    database.transaction(() -> database.command("opencypher",
        "MATCH (entity:`Cust_Agent` {project_id: 'p1'}) SET entity:`Entity` REMOVE entity:`Cust_Agent` RETURN entity"));

    assertThat(typeOf("a1")).isEqualTo("Entity");
    assertThat(labels("MATCH (n {name:'a1'}) RETURN labels(n) AS l")).containsExactly("Entity");
    assertThat(count("MATCH (n:Entity) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:Cust_Agent) RETURN count(n) AS c")).isEqualTo(0);
  }

  @Test
  void theTransitionKeepsPropertiesAndConnectedEdges() {
    database.transaction(() -> database.command("opencypher",
        "MATCH (entity:`Cust_Agent` {project_id: 'p1'}) SET entity:`Entity` REMOVE entity:`Cust_Agent`"));

    assertThat(count("MATCH (n:Entity {project_id:'p1'}) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (:Entity {name:'a1'})-[:INSTANCE]->(:EntityType {id:'t1'}) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void theBatchQueryFromTheIssueLeavesEveryVertexAnEntity() {
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql",
            "INSERT INTO Cust_Agent SET project_id = 'p1', name = 'b" + i + "', class_iris = ['keep','drop']");
      database.command("sql",
          "CREATE EDGE INSTANCE FROM (SELECT FROM Cust_Agent WHERE name LIKE 'b%') TO (SELECT FROM EntityType WHERE name = 'et1')");
      // An edge between two vertices that are BOTH relabelled by the same clause: the second row must follow the
      // first row's rewrite instead of touching the record it displaced (issues #6312 / #6313).
      database.command("sql",
          "CREATE EDGE INSTANCE FROM (SELECT FROM Cust_Agent WHERE name = 'b0') TO (SELECT FROM Cust_Agent WHERE name = 'b1')");
    });

    database.transaction(() -> database.command("opencypher", """
        MATCH (entity:`Cust_Agent` {project_id: 'p1'})
        WITH entity LIMIT 100
        OPTIONAL MATCH (entity)-[instance:`INSTANCE`]->(entity_type:`EntityType` {id: 't1'})
        DELETE instance
        SET entity.class_iris = [ class_iri IN entity.class_iris WHERE class_iri <> 'drop' ]
        SET entity:`Entity`
        REMOVE entity:`Cust_Agent`
        """));

    assertThat(count("MATCH (n:Cust_Agent) RETURN count(n) AS c")).isEqualTo(0);
    assertThat(count("MATCH (n:Entity {project_id:'p1'}) RETURN count(n) AS c")).isEqualTo(6);
    assertThat(labels("MATCH (n {name:'b0'}) RETURN labels(n) AS l")).containsExactly("Entity");
    // The property rewrite in the same clause survived the type move.
    assertThat(labels("MATCH (n {name:'b0'}) RETURN n.class_iris AS l")).containsExactly("keep");
    // The edge the clause deleted is gone; the one it did not name still connects the two relabelled vertices.
    assertThat(count("MATCH (:Entity)-[:INSTANCE]->(:EntityType) RETURN count(*) AS c")).isEqualTo(0);
    assertThat(count("MATCH (:Entity {name:'b0'})-[:INSTANCE]->(:Entity {name:'b1'}) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void onlyTheNamedLabelIsCountedAsRemoved() {
    // Entity is kept, not removed: it must not be counted, and Cust_Agent must be.
    assertThat(labelsRemoved("MATCH (entity:`Cust_Agent`) REMOVE entity:`Cust_Agent`")).isEqualTo(1);
    assertThat(labels("MATCH (n {name:'a1'}) RETURN labels(n) AS l")).containsExactly("Entity");
  }

  @Test
  void aDeeperHierarchyKeepsTheNearestSurvivingAncestorOnly() {
    database.command("sql", "CREATE VERTEX TYPE Middle EXTENDS Entity");
    database.command("sql", "CREATE VERTEX TYPE Leaf EXTENDS Middle");
    database.transaction(() -> database.command("sql", "INSERT INTO Leaf SET name = 'l1'"));

    database.transaction(() -> database.command("opencypher", "MATCH (n:Leaf) REMOVE n:Leaf"));

    // Middle already implies Entity, so the rebuilt type is Middle and not an Entity~Middle that would have
    // flattened the EXTENDS chain (issue #6363).
    assertThat(typeOf("l1")).isEqualTo("Middle");
    assertThat(labels("MATCH (n {name:'l1'}) RETURN labels(n) AS l")).containsExactly("Entity", "Middle");
  }

  @Test
  void removingEveryLabelOfTheChainStillLeavesTheNodeUnlabelled() {
    database.transaction(
        () -> database.command("opencypher", "MATCH (n:Cust_Agent) REMOVE n:Cust_Agent:Entity"));

    assertThat(typeOf("a1")).isEqualTo(Labels.NO_LABEL_TYPE);
    assertThat(labels("MATCH (n {name:'a1'}) RETURN labels(n) AS l")).isEmpty();
  }

  @Test
  void removingOnlyTheInheritedLabelIsStillRefused() {
    // The counterpart the fix must not weaken: Cust_Agent IS-A Entity, so no type the vertex could be moved to
    // answers 'no' to :Entity while still answering 'yes' to :Cust_Agent.
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("opencypher", "MATCH (n:Cust_Agent) REMOVE n:Entity")))
        .hasMessageContaining("Entity")
        .hasMessageContaining("Cust_Agent");

    assertThat(typeOf("a1")).isEqualTo("Cust_Agent");
  }

  @Test
  void aMultiLabelSubtypeVertexKeepsItsSubtypeWhenTheExtraLabelGoes() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:Cust_Agent {name:'a1'}) SET n:Extra"));
    assertThat(typeOf("a1")).isEqualTo("Cust_Agent~Extra");

    database.transaction(() -> database.command("opencypher", "MATCH (n {name:'a1'}) REMOVE n:Extra"));

    assertThat(typeOf("a1")).isEqualTo("Cust_Agent");
    assertThat(labels("MATCH (n {name:'a1'}) RETURN labels(n) AS l")).containsExactly("Cust_Agent", "Entity");
  }

  private int labelsRemoved(final String command) {
    final int[] value = new int[] { -1 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", command)) {
        while (rs.hasNext())
          rs.next();
        value[0] = rs.getStatistics().map(QueryStatistics::getLabelsRemoved).orElse(-1);
      }
    });
    return value[0];
  }

  private String typeOf(final String name) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n {name:'" + name + "'}) RETURN n")) {
      final Document doc = (Document) rs.next().getProperty("n");
      return doc.getTypeName();
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> labels(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return (List<String>) rs.next().getProperty("l");
    }
  }

  private int count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("c")).intValue();
    }
  }
}
