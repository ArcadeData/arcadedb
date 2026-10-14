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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10048: {@code path.subgraphAll} answers every relationship between the nodes it returns, of any type, whatever the
 * {@code relationshipFilter} that found the nodes - as {@code apoc.path.subgraphAll} does. It used to answer only the
 * relationships its walk traversed.
 * <p>
 * Every expected answer below was produced by Neo4j 5.26 with APOC 5.26.30 on the same graph, built by the same
 * {@code CREATE}:
 * <pre>
 *   (s)-[:R]-&gt;(a), (s)-[:R]-&gt;(b), (a)-[:R]-&gt;(c), (b)-[:R]-&gt;(c), (c)-[:R]-&gt;(d), (d)-[:R]-&gt;(s), (a)-[:R2]-&gt;(c), (d)-[:L]-&gt;(d)
 * </pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10048SubgraphAllRelationshipsTest {
  private static final String START = "MATCH (s:U {name: 's'}), (a:U {name: 'a'}), (b:U {name: 'b'}), (c:U {name: 'c'}), (d:U {name: 'd'}) ";

  private static final List<String> EVERY_RELATIONSHIP = List.of("a-[R2]->c", "a-[R]->c", "b-[R]->c", "c-[R]->d", "d-[L]->d",
      "d-[R]->s", "s-[R]->a", "s-[R]->b");

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue10048-subgraph-all");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", """
          CREATE (s:U {name:'s'}), (a:U {name:'a'}), (b:U {name:'b'}), (c:U {name:'c'}), (d:U {name:'d'}),
                 (s)-[:R]->(a), (s)-[:R]->(b), (a)-[:R]->(c), (b)-[:R]->(c), (c)-[:R]->(d), (d)-[:R]->(s),
                 (a)-[:R2]->(c), (d)-[:L]->(d)
          """).close();
      database.command("opencypher", """
          CREATE (a:Q {name:'qa'})-[:R]->(b:Q {name:'qb'}), (b)-[:S]->(a), (b)-[:R]->(a)
          """).close();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void theIssueReproduction() {
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Q {name: 'qa'}) CALL path.subgraphAll(a, {relationshipFilter: 'R>'}) YIELD nodes, relationships "
            + "RETURN [n IN nodes | n.name] AS n, [r IN relationships | type(r)] AS r")) {
      final Result row = rs.next();
      assertThat((List<String>) row.getProperty("n")).containsExactlyInAnyOrder("qa", "qb");
      // The S relationship was not followed, and is between the two nodes: it is part of the subgraph
      assertThat((List<String>) row.getProperty("r")).containsExactlyInAnyOrder("R", "R", "S");
    }
  }

  @Test
  void everyRelationshipBetweenTheNodesWhateverTheFilter() {
    assertThat(relationships("{}")).containsExactlyElementsOf(EVERY_RELATIONSHIP);
    // Only R is followed, and the R2 and the L between nodes it reached come with them
    assertThat(relationships("{relationshipFilter: 'R>'}")).containsExactlyElementsOf(EVERY_RELATIONSHIP);
    assertThat(relationships("{relationshipFilter: 'R>', bfs: false}")).containsExactlyElementsOf(EVERY_RELATIONSHIP);
  }

  @Test
  void onlyTheRelationshipsBetweenTheReturnedNodes() {
    assertThat(nodes("{relationshipFilter: 'R>', maxLevel: 1}")).containsExactly("a", "b", "s");
    assertThat(relationships("{relationshipFilter: 'R>', maxLevel: 1}")).containsExactly("s-[R]->a", "s-[R]->b");

    // The start node is left out of the nodes, so the relationships that touch it are left out too
    assertThat(nodes("{relationshipFilter: 'R>', minLevel: 1}")).containsExactly("a", "b", "c", "d");
    assertThat(relationships("{relationshipFilter: 'R>', minLevel: 1}"))
        .containsExactly("a-[R2]->c", "a-[R]->c", "b-[R]->c", "c-[R]->d", "d-[L]->d");

    // The walk reaches nothing but the start node: its relationships to the nodes it did not reach are not reported
    assertThat(nodes("{relationshipFilter: 'R2'}")).containsExactly("s");
    assertThat(relationships("{relationshipFilter: 'R2'}")).isEmpty();
    assertThat(relationships("{relationshipFilter: 'L'}")).isEmpty();
  }

  @Test
  void aSelfLoopOfAReturnedNodeIsPartOfTheSubgraph() {
    assertThat(relationships("{relationshipFilter: 'R>'}")).contains("d-[L]->d");
  }

  @Test
  void theNodeListsAndTheFiltersDecideTheNodes() {
    assertThat(relationships("{endNodes: [c]}")).isEmpty();
    assertThat(nodes("{endNodes: [c, d]}")).containsExactly("c", "d");
    assertThat(relationships("{endNodes: [c, d]}")).containsExactly("c-[R]->d", "d-[L]->d");
    assertThat(relationships("{terminatorNodes: [c]}")).isEmpty();
    assertThat(relationships("{allowlistNodes: [s, a, c]}")).containsExactly("a-[R2]->c", "a-[R]->c", "s-[R]->a");
    assertThat(relationships("{denylistNodes: [c]}")).containsExactly("d-[L]->d", "d-[R]->s", "s-[R]->a", "s-[R]->b");
    assertThat(relationships("{denylistNodes: [c], relationshipFilter: 'R>'}")).containsExactly("s-[R]->a", "s-[R]->b");
    assertThat(relationships("{labelFilter: '>U'}")).containsExactly("a-[R2]->c", "a-[R]->c", "b-[R]->c", "c-[R]->d", "d-[L]->d");
  }

  @Test
  void limitBoundsTheNodesAndTheRelationshipsFollowThem() {
    assertThat(nodes("{relationshipFilter: 'R>', limit: 2}")).hasSize(2).contains("s");
    assertThat(relationships("{relationshipFilter: 'R>', limit: 0}")).isEmpty();
    assertThat(nodes("{relationshipFilter: 'R>', limit: 0}")).isEmpty();
    assertThat(nodes("{relationshipFilter: 'R>', limit: -1}")).hasSize(5);
    // The relationships are those between the two nodes kept
    final List<String> twoNodes = nodes("{relationshipFilter: 'R>', limit: 2}");
    for (final String relationship : relationships("{relationshipFilter: 'R>', limit: 2}")) {
      final String from = relationship.substring(0, 1);
      final String to = relationship.substring(relationship.length() - 1);
      assertThat(twoNodes).contains(from, to);
    }
  }

  @Test
  void theNodesOnlyFormStillBuildsNoRelationships() {
    try (final ResultSet rs = database.query("opencypher",
        START + "CALL path.subgraphAll(s, {relationshipFilter: 'R>'}) YIELD nodes RETURN size(nodes) AS n")) {
      assertThat((Number) rs.next().getProperty("n")).isEqualTo(5L);
    }
  }

  private List<String> nodes(final String config) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        START + "CALL path.subgraphAll(s, " + config + ") YIELD nodes, relationships RETURN nodes")) {
      for (final Object node : (List<?>) rs.next().getProperty("nodes"))
        names.add(((Vertex) node).getString("name"));
    }
    Collections.sort(names);
    return names;
  }

  /** The relationships as "from-[TYPE]->to", sorted. */
  private List<String> relationships(final String config) {
    final List<String> relationships = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        START + "CALL path.subgraphAll(s, " + config + ") YIELD nodes, relationships RETURN relationships")) {
      for (final Object relationship : (List<?>) rs.next().getProperty("relationships")) {
        final Edge edge = (Edge) relationship;
        relationships.add(edge.getOutVertex().getString("name") + "-[" + edge.getTypeName() + "]->" + edge.getInVertex().getString("name"));
      }
    }
    Collections.sort(relationships);
    return relationships;
  }
}
