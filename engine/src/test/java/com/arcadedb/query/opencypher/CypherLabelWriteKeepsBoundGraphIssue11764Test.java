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
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11764 (with #11670 and #11763): a label write rewrites the vertex under a new RID, and the row's bindings that held
 * the original - another alias, a list, a path - must follow it, or a later traversal from them misses the relationships
 * the rewrite moved, and a path reports a relationship that is no longer incident to its endpoint.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteKeepsBoundGraphIssue11764Test extends TestHelper {

  /** #11670, LEFT: no label write, the relationship is found from the node kept in the list. */
  @Test
  void withoutALabelWriteTheListBoundNodeReachesItsRelationship() {
    final List<Result> rows = rows("""
        CREATE (n:Person {id: 73})-[r:KNOWS {id: 10}]->(m:Person {id: 74})
        WITH n AS n1, n AS n2, [n] AS alias2, m
        WITH n1, alias2[0] AS n3, m
        OPTIONAL MATCH (n3)-[r:KNOWS]->(m)
        RETURN n1, n3, r, m""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("r")).isNotNull();
  }

  /** #11670, RIGHT: a temporary label set and removed through FOREACH must not lose the relationship. */
  @Test
  void aLabelRoundTripThroughForeachKeepsTheListBoundNodeConnected() {
    final List<Result> rows = rows("""
        CREATE (n:Person {id: 73})-[r:KNOWS {id: 10}]->(m:Person {id: 74})
        WITH n AS n1, n AS n2, [n] AS alias2, m
        FOREACH (x IN [n1] | SET x:__oracle_tmp)
        FOREACH (x IN [n2] | SET x:__oracle_tmp)
        FOREACH (x IN [n1] | REMOVE x:__oracle_tmp)
        FOREACH (x IN [n2] | REMOVE x:__oracle_tmp)
        WITH n1, alias2[0] AS n3, m
        OPTIONAL MATCH (n3)-[r:KNOWS]->(m)
        RETURN n1, n3, r, m""");
    assertThat(rows).hasSize(1);
    final Result row = rows.getFirst();
    assertThat(row.<Object>getProperty("r")).isNotNull();
    assertThat(row.<Object>getProperty("n3")).isNotNull();
    assertThat(identity(row, "n1")).isEqualTo(identity(row, "n3"));

    // The graph still holds the two nodes and the one relationship, none of them carrying the temporary label
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")).isEqualTo(1L);
    assertThat(count("MATCH (n:__oracle_tmp) RETURN count(n) AS c")).isEqualTo(0L);
  }

  /** A list holding the same node twice: the second iteration binds the record the first one produced. */
  @Test
  void aLaterIterationBindsTheRecordAnEarlierIterationReplaced() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})"));
    rows("""
        MATCH (n:Person {id: 1})
        FOREACH (x IN [n, n] | CREATE (:Seen {ls: labels(x)}) SET x:Tmp)""");
    final List<Result> seen = rows("MATCH (s:Seen) RETURN s.ls AS ls ORDER BY size(s.ls)");
    assertThat(seen).hasSize(2);
    assertThat(seen.get(0).<List<String>>getProperty("ls")).containsExactly("Person");
    assertThat(seen.get(1).<List<String>>getProperty("ls")).containsExactlyInAnyOrder("Person", "Tmp");
  }

  /**
   * Several rows hold the same node, and each one's FOREACH replaces it again: a row buffered before a later row's
   * replacement is served with the live record, not with the intermediate one the later row deleted.
   */
  @Test
  void aRowServedAfterALaterRowsLabelWriteHoldsTheLiveRecord() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})"));
    final List<Result> rows = rows("""
        MATCH (n:Person {id: 1})
        UNWIND [1, 2, 3] AS i
        WITH n, [n] AS held, i
        FOREACH (x IN [n] | SET x:Tmp REMOVE x:Tmp)
        WITH n, held, i
        OPTIONAL MATCH (h)-[r:KNOWS]->(m) WHERE h = held[0]
        RETURN i, n, held[0] AS h, r""");
    assertThat(rows).hasSize(3);
    final Object live = identity(rows.get(2), "n");
    for (final Result row : rows) {
      assertThat(identity(row, "n")).as("row %s", row).isEqualTo(live);
      assertThat(identity(row, "h")).as("row %s", row).isEqualTo(live);
      assertThat(row.<Object>getProperty("r")).as("row %s", row).isNotNull();
    }
    assertThat(count("MATCH (n:Person) RETURN count(n) AS c")).isEqualTo(2L);
  }

  /**
   * FOREACH buffers a batch of rows before serving them: a label write of a step downstream (a CALL body) replaces the node
   * the rows still buffered hold, and they are served with the live record.
   */
  @Test
  void aRowBufferedByForeachFollowsALabelWriteOfALaterStep() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})"));
    final List<Result> rows = rows("""
        MATCH (n:Person {id: 1})
        UNWIND [1, 2, 3] AS i
        WITH n, [n] AS held, i
        FOREACH (x IN [i] | SET n.seen = x)
        CALL (n) { SET n:Tmp REMOVE n:Tmp }
        WITH i, n, held
        OPTIONAL MATCH (h)-[r:KNOWS]->() WHERE h = held[0]
        RETURN i, n, held[0] AS h, r""");
    assertThat(rows).hasSize(3);
    final Object live = identity(rows.get(2), "n");
    for (final Result row : rows) {
      assertThat(identity(row, "n")).as("row %s", row).isEqualTo(live);
      assertThat(identity(row, "h")).as("row %s", row).isEqualTo(live);
      assertThat(row.<Object>getProperty("r")).as("row %s", row).isNotNull();
    }
    assertThat(count("MATCH (n:Person) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH (n:Tmp) RETURN count(n) AS c")).isEqualTo(0L);
  }

  /**
   * A CALL subquery serves the rows of a batch after running the body of each: a row produced before a later row's body
   * replaced the same node is served with the live record, not the intermediate one that body deleted.
   */
  @Test
  void aRowProducedByCallFollowsALaterRowsLabelWrite() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {id: 1})-[:KNOWS]->(:Person {id: 2})"));
    final List<Result> rows = rows("""
        MATCH (n:Person {id: 1})
        UNWIND [1, 2, 3] AS i
        WITH n, [n] AS held, i
        CALL (n) { SET n:Tmp REMOVE n:Tmp }
        WITH i, n, held
        OPTIONAL MATCH (h)-[r:KNOWS]->() WHERE h = held[0]
        RETURN i, n, held[0] AS h, r""");
    assertThat(rows).hasSize(3);
    final Object live = identity(rows.get(2), "n");
    for (final Result row : rows) {
      assertThat(identity(row, "n")).as("row %s", row).isEqualTo(live);
      assertThat(row.<Object>getProperty("r")).as("row %s", row).isNotNull();
    }
  }

  /** The same through a direct SET/REMOVE, without FOREACH: the alias in the list follows the rewrite. */
  @Test
  void aDirectLabelRoundTripKeepsTheListBoundNodeConnected() {
    final List<Result> rows = rows("""
        CREATE (n:Person {id: 73})-[r:KNOWS {id: 10}]->(m:Person {id: 74})
        WITH n AS n1, [n] AS alias2, m
        SET n1:__oracle_tmp
        REMOVE n1:__oracle_tmp
        WITH n1, alias2[0] AS n3, m
        OPTIONAL MATCH (n3)-[r:KNOWS]->(m)
        RETURN n1, n3, r""");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<Object>getProperty("r")).isNotNull();
  }

  /** #11763: after REMOVE on the path's end node, the path's relationship is still incident to it, on both sides. */
  @Test
  void aPathKeepsARelationshipIncidentToItsEndpointAfterALabelWrite() {
    for (final boolean barrier : new boolean[] { false, true }) {
      database.transaction(() -> database.command("opencypher", "MATCH (n) DETACH DELETE n"));
      database.transaction(() -> {
        database.command("opencypher", """
            CREATE (b:l0:l1:l10:l11:l2:l3:l4:l5:l6:l7:l8)-[:rt8]->
                   (a:l0:l1:l11:l2:l3:l4 {klist: ['if', '6qLOL', 'Ad']})""");
        database.command("opencypher", """
            MATCH (a:l0 {klist: ['if', '6qLOL', 'Ad']})
            CREATE (a)-[:rt0]->(a)""");
      });

      final String probeSet = barrier ? """
          FOREACH (probe0 IN CASE WHEN n0 IS NOT NULL AND valueType(n0) STARTS WITH 'NODE' THEN [n0] ELSE [] END |
            SET probe0:__oracle_tmp_node)
          FOREACH (probe1 IN CASE WHEN n1 IS NOT NULL AND valueType(n1) STARTS WITH 'NODE' THEN [n1] ELSE [] END |
            SET probe1:__oracle_tmp_node)
          """ : "";
      final String probeRemove = barrier ? """
          FOREACH (probe0 IN CASE WHEN n0 IS NOT NULL AND valueType(n0) STARTS WITH 'NODE' THEN [n0] ELSE [] END |
            REMOVE probe0:__oracle_tmp_node)
          FOREACH (probe1 IN CASE WHEN n1 IS NOT NULL AND valueType(n1) STARTS WITH 'NODE' THEN [n1] ELSE [] END |
            REMOVE probe1:__oracle_tmp_node)
          """ : "";

      final List<Result> rows = rows("""
          UNWIND ['load_1', 'load_2'] AS alias0
          OPTIONAL MATCH p0 = (n0:l2|l11|l4|l1|l0 {klist: ['if', '6qLOL', 'Ad']})-[:rt8|rt0*1]-(n1)
          WITH *
          WHERE n1 IS NOT NULL
          REMOVE n1:l3
          """ + probeSet + """
          WITH alias0, n0, n1, p0,
               [label IN labels(n0) WHERE label <> '__oracle_tmp_node'] AS n0Labels,
               [label IN labels(n1) WHERE label <> '__oracle_tmp_node'] AS n1Labels,
               n1 = nodes(p0)[size(nodes(p0)) - 1] AS pathEndMatchesN1,
               (n1 = startNode(relationships(p0)[0]) OR n1 = endNode(relationships(p0)[0])) AS edgeTouchesN1
          """ + probeRemove + """
          RETURN alias0, n0Labels, n1Labels, pathEndMatchesN1, edgeTouchesN1""");

      assertThat(rows).as("barrier=%s", barrier).hasSize(4);
      for (final Result row : rows) {
        assertThat(row.<Boolean>getProperty("pathEndMatchesN1")).as("barrier=%s %s", barrier, row).isTrue();
        assertThat(row.<Boolean>getProperty("edgeTouchesN1")).as("barrier=%s %s", barrier, row).isTrue();
      }
    }
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static Object identity(final Result row, final String column) {
    final Object value = row.getProperty(column);
    return value instanceof Identifiable identifiable ? identifiable.getIdentity() : value;
  }
}
