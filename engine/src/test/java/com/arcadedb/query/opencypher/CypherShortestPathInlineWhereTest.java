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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/5481
 * <p>
 * {@code shortestPath()} / {@code allShortestPaths()} have two independent evaluators: the
 * {@code MATCH p = shortestPath(...)} form goes through {@code ShortestPathStep} while the
 * expression form ({@code RETURN shortestPath(...)}) goes through {@code ShortestPathExpression}.
 * Both must apply the relationship inline property map AND the relationship inline {@code WHERE}
 * predicate to every relationship on the path. Before this fix three of the four combinations
 * silently ignored the constraint (#5096 fixed only the property map on the {@code MATCH} form).
 */
class CypherShortestPathInlineWhereTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Graph - two equal-length corridors between 1 and 3, distinguished only by the edge tag:
    //   1 --LINK{tag:'ok'}--> 2 --LINK{tag:'ok'}--> 3
    //   1 --LINK{tag:'bad'}-> 4 --LINK{tag:'bad'}-> 3
    // Node 4 is only reachable through a 'bad' edge, so a tag='ok' constraint makes it unreachable.
    database.transaction(() -> {
      final MutableVertex n1 = database.newVertex("Node").set("id", 1L).save();
      final MutableVertex n2 = database.newVertex("Node").set("id", 2L).save();
      final MutableVertex n3 = database.newVertex("Node").set("id", 3L).save();
      final MutableVertex n4 = database.newVertex("Node").set("id", 4L).save();

      n1.newEdge("LINK", n2, true, new Object[] { "tag", "ok" }).save();
      n2.newEdge("LINK", n3, true, new Object[] { "tag", "ok" }).save();
      n1.newEdge("LINK", n4, true, new Object[] { "tag", "bad" }).save();
      n4.newEdge("LINK", n3, true, new Object[] { "tag", "bad" }).save();
    });
  }

  // ==========================================================================================
  // MATCH form - handled by ShortestPathStep
  // ==========================================================================================

  /**
   * The exact reproduction from the issue: an unsatisfiable inline WHERE must yield no path.
   */
  @Test
  void matchFormUnsatisfiableInlineWhereFindsNoPath() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = shortestPath((a)-[r:LINK* WHERE 1 = 0]->(b))
            RETURN count(p) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
  }

  /**
   * Node 4 is only reachable via a tag='bad' edge, so the inline WHERE must exclude it entirely.
   * <p>
   * Note: the {@code *1..3} bounds spelled out here and below are not enforced by either shortestPath
   * evaluator (see the ShortestPathStep class Javadoc) - they read naturally but behave as a bare
   * {@code *}. These tests exercise the relationship constraints, not the hop bounds.
   */
  @Test
  void matchFormInlineWhereRejectsUnreachableTarget() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            MATCH p = shortestPath((a)-[r:LINK*1..3 WHERE r.tag = 'ok']->(b))
            RETURN p""");

    assertThat(rs.hasNext()).as("shortestPath must not cross tag='bad' edges when the inline WHERE excludes them")
        .isFalse();
  }

  @Test
  void matchFormInlineWhereSelectsMatchingCorridor() {
    assertThat(matchFormNodeIds("r.tag = 'ok'")).containsExactly(1L, 2L, 3L);
  }

  @Test
  void matchFormInlineWhereSelectsAlternateCorridor() {
    assertThat(matchFormNodeIds("r.tag = 'bad'")).containsExactly(1L, 4L, 3L);
  }

  @Test
  void matchFormInlineWhereSupportsCompoundPredicate() {
    assertThat(matchFormNodeIds("r.tag <> 'bad' AND r.tag IS NOT NULL")).containsExactly(1L, 2L, 3L);
  }

  @Test
  void matchFormInlineWhereSupportsParameters() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = shortestPath((a)-[r:LINK*1..3 WHERE r.tag = $tag]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""",
        Map.of("tag", "bad"));

    assertThat(rs.hasNext()).isTrue();
    assertThat(ids(rs)).containsExactly(1L, 4L, 3L);
  }

  /**
   * Property map and inline WHERE are independent constraints: both must hold on every hop.
   */
  @Test
  void matchFormAppliesPropertyMapAndInlineWhereTogether() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = shortestPath((a)-[r:LINK*1..3 {tag: 'ok'} WHERE r.tag = 'bad']->(b))
            RETURN count(p) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("c").longValue())
        .as("contradictory property map and inline WHERE cannot both be satisfied").isZero();
  }

  /**
   * A parameter map cannot be a predicate in a MATCH pattern, so the MATCH form rejects it up front
   * rather than dropping it: there is no silent-drop gap to close on this path.
   */
  @Test
  void matchFormRejectsParameterisedPropertyMap() {
    assertThatThrownBy(() -> database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            MATCH p = shortestPath((a)-[:LINK*1..3 $props]->(b))
            RETURN count(p) AS c""",
        Map.of("props", Map.of("tag", "ok"))))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Parameters cannot be used as predicates");
  }

  @Test
  void matchFormAllShortestPathsAppliesInlineWhere() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = allShortestPaths((a)-[r:LINK*1..3 WHERE r.tag = 'ok']->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(ids(rs)).containsExactly(1L, 2L, 3L);
    assertThat(rs.hasNext()).as("only the tag='ok' corridor qualifies").isFalse();
  }

  /**
   * Control: without any relationship constraint both corridors are co-shortest.
   */
  @Test
  void matchFormWithoutConstraintStillReturnsBothCorridors() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            MATCH p = allShortestPaths((a)-[:LINK*1..3]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids""");

    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  // ==========================================================================================
  // Expression form - handled by ShortestPathExpression
  // ==========================================================================================

  @Test
  void expressionFormPropertyMapRejectsUnreachableTarget() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            RETURN shortestPath((a)-[:LINK*1..3 {tag: 'ok'}]->(b)) AS p""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("p"))
        .as("the expression form must honour the inline property map too").isNull();
  }

  @Test
  void expressionFormInlineWhereRejectsUnreachableTarget() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            RETURN shortestPath((a)-[r:LINK*1..3 WHERE r.tag = 'ok']->(b)) AS p""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("p"))
        .as("the expression form must honour the inline WHERE predicate").isNull();
  }

  @Test
  void expressionFormUnsatisfiableInlineWhereFindsNoPath() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            RETURN shortestPath((a)-[r:LINK* WHERE 1 = 0]->(b)) AS p""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("p")).isNull();
  }

  @Test
  void expressionFormPropertyMapSelectsMatchingCorridor() {
    assertThat(expressionFormNodeIds("[:LINK*1..3 {tag: 'ok'}]")).containsExactly(1L, 2L, 3L);
  }

  @Test
  void expressionFormInlineWhereSelectsAlternateCorridor() {
    assertThat(expressionFormNodeIds("[r:LINK*1..3 WHERE r.tag = 'bad']")).containsExactly(1L, 4L, 3L);
  }

  @Test
  void expressionFormInlineWhereSupportsParameters() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 3})
            RETURN [n IN nodes(shortestPath((a)-[r:LINK*1..3 WHERE r.tag = $tag]->(b))) | n.id] AS ids""",
        Map.of("tag", "ok"));

    assertThat(rs.hasNext()).isTrue();
    assertThat(ids(rs)).containsExactly(1L, 2L, 3L);
  }

  @Test
  void expressionFormAppliesParameterisedPropertyMap() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            RETURN shortestPath((a)-[:LINK*1..3 $props]->(b)) AS p""",
        Map.of("props", Map.of("tag", "ok")));

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("p"))
        .as("a $props relationship property map must be enforced on the expression form too").isNull();
  }

  @Test
  void expressionFormAllShortestPathsAppliesInlineWhere() {
    final ResultSet rs = database.query("opencypher",
        """
            MATCH (a:Node {id: 1}), (b:Node {id: 4})
            RETURN allShortestPaths((a)-[r:LINK*1..3 WHERE r.tag = 'ok']->(b)) AS paths""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<List<Object>>getProperty("paths")).isEmpty();
  }

  /**
   * Control: the unconstrained expression form keeps returning the two-hop path.
   */
  @Test
  void expressionFormWithoutConstraintStillWorks() {
    assertThat(expressionFormNodeIds("[:LINK*1..3]")).hasSize(3).startsWith(1L).endsWith(3L);
  }

  // ==========================================================================================
  // Helpers
  // ==========================================================================================

  private List<Number> matchFormNodeIds(final String predicate) {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1}), (b:Node {id: 3})\n"
            + "MATCH p = shortestPath((a)-[r:LINK*1..3 WHERE " + predicate + "]->(b))\n"
            + "RETURN [n IN nodes(p) | n.id] AS ids");

    assertThat(rs.hasNext()).isTrue();
    return ids(rs);
  }

  private List<Number> expressionFormNodeIds(final String relationship) {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {id: 1}), (b:Node {id: 3})\n"
            + "RETURN [n IN nodes(shortestPath((a)-" + relationship + "->(b))) | n.id] AS ids");

    assertThat(rs.hasNext()).isTrue();
    return ids(rs);
  }

  @SuppressWarnings("unchecked")
  private static List<Number> ids(final ResultSet rs) {
    return (List<Number>) rs.next().<Object>getProperty("ids");
  }
}
