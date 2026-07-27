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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pattern comprehension must honor the relationship inline {@code WHERE} predicate, i.e. the
 * {@code WHERE r.tag = 'ok'} in {@code [(a)-[r:E WHERE r.tag = 'ok']->(b) | b]}. Over two
 * relationships tagged {@code 'ok'} and {@code 'bad'}, an always-false predicate must collect
 * nothing and a predicate on the relationship property must collect exactly the matching one -
 * the same results Neo4j produces, and the same semantics a regular {@code MATCH} already applies.
 * <p>
 * The comprehension-level {@code WHERE} (placed after the pattern) and the inline property-map form
 * {@code [:E {tag: 'ok'}]} are covered elsewhere; both are exercised here only as controls.
 */
class OpenCypherPatternComprehensionInlineWhereTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("C");
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("E");

    // a -[E {tag: 'ok'}]->  b
    // a -[E {tag: 'bad'}]-> b
    database.command("opencypher",
        """
        CREATE (a:A {v: 1}), (b:A {v: 2}), \
        (a)-[:E {tag: 'ok'}]->(b), \
        (a)-[:E {tag: 'bad'}]->(b)""");

    // A two-hop chain used only by the variable-length tests. Its nodes carry label C, so the
    // single-hop tests above - which all pin the far end to :A - are unaffected by it.
    //   a -ok-> mid -ok->  okEnd     (both hops satisfy the predicate)
    //   a -ok-> mid -bad-> badEnd    (second hop fails it)
    database.command("opencypher",
        """
        MATCH (a:A {v: 1}) \
        CREATE (mid:C {name: 'mid'}), (okEnd:C {name: 'okEnd'}), (badEnd:C {name: 'badEnd'}), \
        (a)-[:E {tag: 'ok'}]->(mid), \
        (mid)-[:E {tag: 'ok'}]->(okEnd), \
        (mid)-[:E {tag: 'bad'}]->(badEnd)""");

    // Nodes for the three-valued-logic tests, again on their own label so the tests above are
    // unaffected. 'tagged' is reached by an edge carrying tag and w; 'untagged' by an edge with no
    // tag property at all, so predicates over r.tag evaluate to NULL for it.
    database.command("opencypher",
        """
        MATCH (a:A {v: 1}) \
        CREATE (tagged:N {name: 'tagged'}), (untagged:N {name: 'untagged'}), \
        (a)-[:E {tag: 'ok', w: 1}]->(tagged), \
        (a)-[:E]->(untagged)""");
  }

  @Test
  void alwaysFalseInlineWherePredicateFiltersEverything() {
    // Neo4j returns 0: the always-false predicate discards both relationships.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE 1=0]->(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(0);
  }

  @Test
  void inlineWherePredicateOnRelationshipPropertyIsApplied() {
    // Neo4j returns 1: only the relationship tagged 'ok' survives.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE r.tag = 'ok']->(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
  }

  @Test
  void inlineWherePredicateProjectsTheMatchingRelationship() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE r.tag = 'ok']->(b:A) | r.tag] AS tags""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> tags = rs.next().getProperty("tags");
    assertThat(tags).containsExactly("ok");
  }

  @Test
  void inlineWherePredicateCombinesWithComprehensionLevelWhere() {
    // Both predicates apply: the inline one keeps 'ok', the outer one rejects every row.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE r.tag = 'ok']->(b:A) WHERE b.v = 99 | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(0);
  }

  @Test
  void inlineWherePredicateCanReferenceOuterBoundVariable() {
    // The predicate body must see variables bound outside the comprehension.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE a.v = 1]->(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(2);
  }

  @Test
  void inlineWherePredicateAppliesToEveryHopOfAVariableLengthPattern() {
    // Every relationship traversed by the var-length expansion must satisfy the predicate.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E*1..2 WHERE r.tag = 'ok']->(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
  }

  @Test
  void inlineWherePredicateTruncatesAVariableLengthPathAtTheFirstFailingHop() {
    // Over the chain a -ok-> mid -ok-> okEnd and mid -bad-> badEnd, a 2-hop expansion must keep
    // 'mid' and 'okEnd' and drop 'badEnd': the predicate has to be enforced on every hop, not just
    // the first. Without per-hop enforcement all three would be collected.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E*1..2 WHERE r.tag = 'ok']->(x:C) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactlyInAnyOrder("mid", "okEnd");
  }

  @Test
  void inlineWherePredicateAppliesToAnIncomingPattern() {
    // Traversed from the target side, the predicate must filter the same single relationship.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (b:A {v: 2})
        RETURN size([(b)<-[r:E WHERE r.tag = 'ok']-(a:A) | a]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
  }

  @Test
  void inlineWherePredicateAppliesToAnUndirectedPattern() {
    // Direction-agnostic expansion must still honor the predicate.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE r.tag = 'ok']-(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
  }

  @Test
  void inlineWherePredicateResolvesAQueryParameter() {
    // The predicate body must resolve $tag from the query parameters.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E WHERE r.tag = $tag]->(b:A) | b]) AS c""",
        Map.of("tag", "ok"));

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(1);
  }

  @Test
  void inlineWherePredicateSupportsConjunction() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE r.tag = 'ok' AND r.w = 1]->(x:N) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("tagged");
  }

  @Test
  void inlineWherePredicateSupportsDisjunction() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE r.tag = 'zzz' OR r.w = 1]->(x:N) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("tagged");
  }

  @Test
  void inlineWherePredicateExcludesRelationshipsWhoseComparisonIsNull() {
    // The 'untagged' edge has no tag property, so r.tag = 'ok' is NULL and the edge is excluded -
    // NULL is not a match in Cypher.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE r.tag = 'ok']->(x:N) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("tagged");
  }

  @Test
  void inlineWhereIsNullPredicateSelectsTheMissingProperty() {
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE r.tag IS NULL]->(x:N) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final List<Object> names = rs.next().getProperty("names");
    assertThat(names).containsExactly("untagged");
  }

  @Test
  void negatedInlineWherePredicateMatchesTheRegularMatchPath() {
    // Three-valued logic under NOT: for 'tagged' NOT(true) is false; for 'untagged' the comparison
    // is NULL and NOT NULL stays NULL, so neither is a match. The comprehension parses the predicate
    // through a different builder than MATCH does, so pin both engines to the same answer.
    final ResultSet comprehension = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN [(a)-[r:E WHERE NOT (r.tag = 'ok')]->(x:N) | x.name] AS names""");

    assertThat(comprehension.hasNext()).isTrue();
    final List<Object> names = comprehension.next().getProperty("names");

    final ResultSet match = database.query("opencypher",
        """
        MATCH (a:A {v: 1})-[r:E WHERE NOT (r.tag = 'ok')]->(x:N)
        RETURN collect(x.name) AS names""");

    assertThat(match.hasNext()).isTrue();
    final List<Object> matchNames = match.next().getProperty("names");

    assertThat(names).isEmpty();
    assertThat(names).isEqualTo(matchNames);
  }

  @Test
  void noInlineWherePredicateStillReturnsEveryRelationship() {
    // Control: without a predicate both relationships are collected.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[:E]->(b:A) | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(2);
  }

  @Test
  void comprehensionLevelWhereStillWorks() {
    // Control: the documented workaround must keep returning 0.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:A {v: 1})
        RETURN size([(a)-[r:E]->(b:A) WHERE 1=0 | b]) AS c""");

    assertThat(rs.hasNext()).isTrue();
    assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(0);
  }
}
