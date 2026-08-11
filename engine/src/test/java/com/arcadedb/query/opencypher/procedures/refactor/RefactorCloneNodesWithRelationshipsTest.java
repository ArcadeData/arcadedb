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
package com.arcadedb.query.opencypher.procedures.refactor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the apoc.refactor.cloneNodesWithRelationships Cypher procedure
 * (registered as "refactor.cloneNodesWithRelationships").
 * <p>
 * The procedure mutates the graph, so every CALL runs inside an explicit transaction that is fully
 * consumed and committed before any assertion - see {@link RefactorMergeNodesTest} class Javadoc for
 * why CypherProcedure execution needs that.
 * </p>
 */
class RefactorCloneNodesWithRelationshipsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-refactor-clone-nodes");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void registeredUnderBothPlainAndApocPrefixedName() {
    assertThat(CypherProcedureRegistry.hasProcedure("refactor.cloneNodesWithRelationships")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.refactor.cloneNodesWithRelationships")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.refactor.cloneNodesWithRelationships"))
        .isSameAs(CypherProcedureRegistry.get("refactor.cloneNodesWithRelationships"));
  }

  @Test
  void clonesNodeWithSameTypeAndProperties() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("age", 30L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD input, output, error RETURN input, output, error");
    final Result result = rs.next();
    final Object error = result.getProperty("error");
    final Vertex input = result.getProperty("input");
    final Vertex output = result.getProperty("output");
    database.commit();

    assertThat(error).isNull();
    assertThat(output.getIdentity()).isNotEqualTo(input.getIdentity());
    assertThat(output.getTypeName()).isEqualTo("Person");
    assertThat(output.getString("name")).isEqualTo("A");
    assertThat(output.getLong("age")).isEqualTo(30L);
  }

  @Test
  void clonedRelationshipToExternalNodePointsAtOriginalExternalNode() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex external = database.newVertex("Person").set("name", "External").save();
    a.newEdge("KNOWS", external, "since", 2020L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD output RETURN output");
    final Vertex clone = rs.next().getProperty("output");
    database.commit();

    final ResultSet edges = database.query("opencypher",
        "MATCH (c:Person)-[r:KNOWS]->(e:Person {name:'External'}) WHERE id(c) = $cloneId RETURN r.since AS since",
        java.util.Map.of("cloneId", clone.getIdentity().toString()));
    assertThat(edges.hasNext()).isTrue();
    assertThat(edges.next().<Long>getProperty("since")).isEqualTo(2020L);

    // original relationship must be untouched
    final ResultSet originalEdges = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(e:Person {name:'External'}) RETURN r");
    assertThat(originalEdges.hasNext()).isTrue();
  }

  @Test
  void relationshipBetweenTwoClonedNodesConnectsTheTwoClones() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("KNOWS", b, "since", 2021L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.cloneNodesWithRelationships([a,b], {}) YIELD input, output RETURN input, output");
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    database.commit();

    Vertex cloneOfA = null;
    Vertex cloneOfB = null;
    for (final Result row : rows) {
      final Vertex input = row.getProperty("input");
      final Vertex output = row.getProperty("output");
      if ("A".equals(input.getString("name")))
        cloneOfA = output;
      else if ("B".equals(input.getString("name")))
        cloneOfB = output;
    }

    assertThat(cloneOfA).isNotNull();
    assertThat(cloneOfB).isNotNull();

    boolean cloneToCloneEdgeFound = false;
    for (final com.arcadedb.graph.Edge e : cloneOfA.getEdges(Vertex.DIRECTION.OUT, "KNOWS")) {
      if (e.getIn().equals(cloneOfB.getIdentity())) {
        cloneToCloneEdgeFound = true;
        assertThat(e.getLong("since")).isEqualTo(2021L);
      }
    }
    assertThat(cloneToCloneEdgeFound).isTrue();

    // original relationship between a and b must be untouched
    final ResultSet originalEdges = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(b:Person {name:'B'}) RETURN r");
    assertThat(originalEdges.hasNext()).isTrue();
  }
}
