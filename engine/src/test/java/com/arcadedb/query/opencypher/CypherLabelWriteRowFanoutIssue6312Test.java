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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #6312 and #6313: a Cypher label write has to rewrite the vertex, because ArcadeDB reads a record's type
 * from the bucket it lives in and a label set is a type. The rewrite left every other reference to the node -
 * a second alias in the same row, the same node reached again by a later row of the same clause - pointing at the
 * deleted original, so the next REMOVE either failed to load the record (#6312) or followed its dangling edge-list
 * chunk on the way to deleting it a second time (#6313).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteRowFanoutIssue6312Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-labelwrite-6312").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void removeSurvivesUnwindFanoutOverTheSameNodes() {
    cypher("""
        CREATE (n2:l2:l11 {id:2,klist:['n2v']})-[:rt2]->
               (n1 {id:1,k11:'u'})-[:R {klist:[-1365794787,-485798932]}]->
               (n0 {id:0,k1:'v',klist:[1,2,3]})""");
    cypher("MATCH (n2 {id:2}) CREATE (:l11:l8:l1:l3:l5:l4:l9)-[:t]->(n2)");

    // One matched row, unwound into three by n0.klist: every row removes from the same two nodes, and the
    // first of them replaces n2's record.
    assertThat(count("""
        OPTIONAL MATCH p0 = (n2)-[:rt2]->(n1 {k11:'u'})-[{klist:[-1365794787,-485798932]}]->(n0),
          (:l11:l8:l1:l3:l5:l4:l9)-[]->(n2)
        WITH *
        WHERE n0 IS NOT NULL
        UNWIND coalesce(n0.klist, []) AS alias3
        WITH *
        WHERE n2 IS NOT NULL
        REMOVE n0.k1, n2.klist, n2:l2:l11
        RETURN count(*) AS row_count""")).isEqualTo(3);

    assertThat(rows("MATCH (n {id:2}) RETURN labels(n) AS v")).containsExactly("[]");
    assertThat(rows("MATCH (n {id:2}) RETURN n.klist AS v")).containsExactly("null");
    assertThat(rows("MATCH (n {id:0}) RETURN n.k1 AS v")).containsExactly("null");

    // The relabelled vertex keeps both sides of its topology.
    assertThat(rows("MATCH (n {id:2})-[:rt2]->(other) RETURN other.id AS v")).containsExactly("1");
    assertThat(rows("MATCH (source)-[:t]->(n {id:2}) RETURN size(labels(source)) AS v")).containsExactly("7");
  }

  @Test
  void removeLabelSurvivesTheSameNodeMatchedOnSeveralRows() {
    cypher("CREATE (n0 {k9:true})-[:rt7]->(n1:l1 {tag:'middle'})-[:X]->(n2 {tag:'end'})");
    cypher("MATCH (n1 {tag:'middle'}) CREATE (n1)-[:c]->(:Extra)");

    // Two rows - the middle node has two outgoing edges - both binding the same n0 and n1.
    assertThat(count("""
        UNWIND ['x'] AS alias0
        OPTIONAL MATCH (n2)<-[]-(n1)<-[:rt7]-(n0), p0=(n2)
        WHERE true
        WITH *
        WHERE n1 IS NOT NULL AND n0 IS NOT NULL
        REMOVE n0.k9, n1:l1
        RETURN count(*) AS row_count""")).isEqualTo(2);

    assertThat(rows("MATCH (n {tag:'middle'}) RETURN labels(n) AS v")).containsExactly("[]");
    assertThat(rows("MATCH (n)-[:rt7]->(m) RETURN m.tag AS v")).containsExactly("middle");
    assertThat(rows("MATCH (n {tag:'middle'})-[r]->(m) RETURN type(r) AS v")).containsExactlyInAnyOrder("X", "c");
    assertThat(rows("MATCH (n)-[:rt7]->() RETURN n.k9 AS v")).containsExactly("null");
  }

  @Test
  void removeLabelKeepsEdgeProperties() {
    cypher("CREATE (a:Keep:Drop {name:'a'})-[:E {w:7,tag:'out'}]->(b {name:'b'})");
    cypher("MATCH (a {name:'a'}) CREATE (c {name:'c'})-[:E {w:9,tag:'in'}]->(a)");
    cypher("MATCH (a {name:'a'}) CREATE (a)-[:E {w:11,tag:'loop'}]->(a)");

    cypher("MATCH (a:Drop) REMOVE a:Drop");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Keep]");
    assertThat(rows("MATCH ()-[r:E]->() RETURN r.tag + '=' + r.w AS v"))
        .containsExactlyInAnyOrder("out=7", "in=9", "loop=11");
    // The self-loop is migrated once, not once per direction.
    assertThat(rows("MATCH (n {name:'a'})-[r]->(n) RETURN count(r) AS v")).containsExactly("1");
  }

  @Test
  void addLabelKeepsEdgeProperties() {
    cypher("CREATE (a:Keep {name:'a'})-[:E {w:7,tag:'out'}]->(b {name:'b'})");
    cypher("MATCH (a {name:'a'}) CREATE (c {name:'c'})-[:E {w:9,tag:'in'}]->(a)");

    cypher("MATCH (a:Keep) SET a:Extra");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Extra, Keep]");
    assertThat(rows("MATCH ()-[r:E]->() RETURN r.tag + '=' + r.w AS v"))
        .containsExactlyInAnyOrder("out=7", "in=9");
  }

  @Test
  void aBoundPathFollowsItsRelabelledMembers() {
    cypher("CREATE (a:Keep:Drop {name:'a'})-[:E {w:7}]->(b {name:'b'})");

    // The path was bound before the label write, and the write moves one of its nodes. Reading the path
    // afterwards has to answer with the state that write left behind, not with the record it deleted.
    assertThat(writingRows("MATCH p = (a:Drop)-[:E]->(b) REMOVE a:Drop RETURN [n IN nodes(p) | labels(n)] AS v"))
        .containsExactly("[[Keep], []]");
    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Keep]");
    // The relationship the path carries is re-attached to the replacement, properties and all.
    assertThat(writingRows("MATCH p = (a:Keep)-[:E]->(b) REMOVE a:Keep RETURN [r IN relationships(p) | r.w] AS v"))
        .containsExactly("[7]");
  }

  @Test
  void aBoundLightweightEdgeFollowsTheEndpointThatMoved() {
    database.getSchema().buildEdgeType().withName("Light").withLightweight(true).create();
    cypher("CREATE (a:Keep:Drop {name:'a'})-[:Light]->(b {name:'b'})");

    // A lightweight edge is stored inside its two vertices, so relabelling one endpoint re-creates it. The row
    // still holding the original has to follow, or it answers with an endpoint that no longer exists.
    assertThat(writingRows("MATCH (a:Drop)-[r:Light]->(b) REMOVE a:Drop RETURN startNode(r).name + '->' + endNode(r).name AS v"))
        .containsExactly("a->b");
    assertThat(rows("MATCH (n {name:'a'})-[:Light]->(m) RETURN m.name AS v")).containsExactly("b");
  }

  @Test
  void aCollectedListFollowsItsRelabelledMembers() {
    cypher("CREATE (c:Gone {name:'c'})-[:E2]->(d {name:'d'})");

    assertThat(writingRows("MATCH (c:Gone) WITH collect(c) AS cs, c AS one REMOVE one:Gone RETURN [n IN cs | labels(n)] AS v"))
        .containsExactly("[[]]");
  }

  @Test
  void mergeOnMatchSetLabelKeepsEdgesAndRedirectsEveryAlias() {
    cypher("CREATE (a:Keep {name:'a'})-[:E {w:7}]->(b {name:'b'})");
    cypher("MATCH (a {name:'a'}) CREATE (c {name:'c'})-[:E {w:9}]->(a)");
    cypher("MATCH (a {name:'a'}) CREATE (a)-[:E {w:11}]->(a)");

    // ON MATCH SET n:Label rewrites the record exactly as a bare SET does. The row also binds the same node
    // under m, from the MATCH that fed the MERGE, and that alias has to follow it too.
    assertThat(writingRows("""
        MATCH (m {name:'a'})
        MERGE (n {name:'a'})
        ON MATCH SET n:Extra
        RETURN labels(m) AS v""")).containsExactly("[Extra, Keep]");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Extra, Keep]");
    // Edge properties survive, and the self-loop is one live edge rather than a dangling one.
    assertThat(rows("MATCH ()-[r:E]->() RETURN r.w AS v")).containsExactlyInAnyOrder("7", "9", "11");
    assertThat(rows("MATCH (n {name:'a'})-[r]->(n) RETURN count(r) AS v")).containsExactly("1");
    assertThat(rows("MATCH (n {name:'a'})-[:E]->(x) RETURN x.name AS v")).containsExactlyInAnyOrder("b", "a");
  }

  @Test
  void mergeOnMatchSetLabelSurvivesTheSameNodeOnSeveralRows() {
    cypher("CREATE (a:Keep {name:'a'})");

    // Three rows, all merging onto the same node: the first relabels it, the other two must recognise the
    // replacement rather than write through the record it deleted.
    assertThat(writingRows("""
        UNWIND [1,2,3] AS i
        MERGE (n {name:'a'})
        ON MATCH SET n:Extra
        RETURN labels(n) AS v""")).containsExactly("[Extra, Keep]", "[Extra, Keep]", "[Extra, Keep]");
    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Extra, Keep]");
  }

  @Test
  void removeLabelIsIdempotentAcrossRowsAndCountsOnce() {
    cypher("CREATE (a:Gone:Stay {name:'a'})");

    final int[] labelsRemoved = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a {name:'a'}) UNWIND [1,2,3] AS i REMOVE a:Gone RETURN count(*) AS row_count")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("row_count")).longValue()).isEqualTo(3);
        labelsRemoved[0] = rs.getStatistics().map(s -> s.getLabelsRemoved()).orElse(-1);
      }
    });

    assertThat(labelsRemoved[0]).isEqualTo(1);
    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Stay]");
  }

  @Test
  void addLabelIsIdempotentAcrossRowsAndCountsOnce() {
    // The mirror of the REMOVE case: SET runs the same replacement machinery, so a node relabelled on the
    // first row must be recognised as already carrying the label on the next two rather than replaced again.
    cypher("CREATE (a:Stay {name:'a'})");

    final int[] labelsAdded = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a {name:'a'}) UNWIND [1,2,3] AS i SET a:Extra RETURN count(*) AS row_count")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("row_count")).longValue()).isEqualTo(3);
        labelsAdded[0] = rs.getStatistics().map(s -> s.getLabelsAdded()).orElse(-1);
      }
    });

    assertThat(labelsAdded[0]).isEqualTo(1);
    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Extra, Stay]");
  }

  private long count(final String query) {
    final long[] value = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        value[0] = ((Number) rs.next().getProperty("row_count")).longValue();
      }
    });
    return value[0];
  }

  private List<String> rows(final String query) {
    return rows(query, false);
  }

  /**
   * Same as {@link #rows(String)} for a query that also writes, which the read-only entry point rejects.
   */
  private List<String> writingRows(final String query) {
    return rows(query, true);
  }

  private List<String> rows(final String query, final boolean writes) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = writes ? database.command("opencypher", query) : database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out.add(String.valueOf(r.<Object>getProperty("v")));
        }
      }
    });
    return out;
  }

  private void cypher(final String query) {
    database.transaction(() -> database.command("opencypher", query));
  }
}
