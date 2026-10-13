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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9940: the parser accepts the whole Cypher 25 grammar, so GQL / Cypher 25 constructs the engine did not implement were
 * silently dropped or answered wrongly. Each one is now either executed with its GQL / Cypher 25 semantics or refused with an
 * error that names it.
 * <p>
 * The graph: A, B, C, D with {@code A->B->C->D} and {@code A->D}, so A reaches D by a path of length 1 and one of length 3.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9940GqlConstructsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), (d:Person {name:'D'}), "
            + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), (a)-[:KNOWS]->(d)"));
  }

  private List<Map<String, Object>> rows(final String query) {
    return rows(query, Map.of());
  }

  private List<Map<String, Object>> rows(final String query, final Map<String, Object> parameters) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.toMap());
      }
    }
    return rows;
  }

  private List<Object> column(final String query, final String column) {
    return column(query, column, Map.of());
  }

  private List<Object> column(final String query, final String column, final Map<String, Object> parameters) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : rows(query, parameters))
      values.add(row.get(column));
    return values;
  }

  // 1. Path search prefixes

  @Test
  void pathSearchPrefixesOtherThanAllAreRefused() {
    for (final String prefix : new String[] { "ANY SHORTEST", "ALL SHORTEST", "SHORTEST 2", "SHORTEST 1 GROUPS", "SHORTEST GROUPS",
        "ANY", "ANY 2", "ANY SHORTEST PATH", "SHORTEST 2 PATHS" })
      assertThatThrownBy(() -> rows("MATCH p = " + prefix + " (a:Person {name:'A'})-[:KNOWS]->+(d:Person {name:'D'}) RETURN length(p) AS l"))
          .as(prefix)
          .hasMessageContaining("path search prefix")
          .hasMessageContaining("shortestPath()");
  }

  @Test
  void allPathsPrefixKeepsReturningEveryPath() {
    for (final String prefix : new String[] { "ALL", "ALL PATHS", "ALL TRAIL", "TRAIL", "ALL TRAIL PATHS" })
      assertThat(column("MATCH p = " + prefix + " (a:Person {name:'A'})-[:KNOWS]->+(d:Person {name:'D'}) RETURN length(p) AS l ORDER BY l",
          "l")).as(prefix).containsExactly(1L, 3L);
  }

  // 2. Match modes

  @Test
  void repeatableElementsMatchModeIsRefused() {
    for (final String mode : new String[] { "REPEATABLE ELEMENTS", "REPEATABLE ELEMENT", "REPEATABLE ELEMENT BINDINGS" })
      assertThatThrownBy(() -> rows("MATCH " + mode + " (a:Person {name:'A'})-[:KNOWS]->(b) RETURN b.name AS n"))
          .as(mode)
          .hasMessageContaining("match mode");

    assertThatThrownBy(() -> rows("MATCH (a:Person) WHERE EXISTS { REPEATABLE ELEMENTS (a)-[:KNOWS]->() } RETURN a.name AS n"))
        .hasMessageContaining("match mode");
  }

  @Test
  void differentRelationshipsMatchModeIsTheDefault() {
    for (final String mode : new String[] { "DIFFERENT RELATIONSHIPS", "DIFFERENT RELATIONSHIP", "DIFFERENT RELATIONSHIP BINDINGS" })
      assertThat(column("MATCH " + mode + " (a:Person {name:'A'})-[:KNOWS]->(b) RETURN b.name AS n ORDER BY n", "n"))
          .as(mode).containsExactly("B", "D");
  }

  // 3. LET

  @Test
  void letBindsVariables() {
    assertThat(column("LET x = 1 RETURN x", "x")).containsExactly(1L);
    assertThat(column("MATCH (n:Person) LET x = n.name + '!', y = x + '?' RETURN y ORDER BY y", "y"))
        .containsExactly("A!?", "B!?", "C!?", "D!?");
    // the incoming variables stay in scope, and a LET does not group the rows
    assertThat(column("MATCH (n:Person) LET len = size(n.name) RETURN n.name AS name, len ORDER BY name", "len"))
        .containsExactly(1L, 1L, 1L, 1L);
  }

  @Test
  void letRefusesAnAggregation() {
    assertThatThrownBy(() -> rows("MATCH (n:Person) LET c = count(n) RETURN c")).hasMessageContaining("LET");
  }

  // 4. USE

  @Test
  void useOfTheCurrentDatabaseRuns() {
    final String name = database.getName();
    assertThat(column("USE `" + name + "` MATCH (n:Person) RETURN count(n) AS c", "c")).containsExactly(4L);
    assertThat(column("USE GRAPH `" + name + "` MATCH (n:Person) RETURN count(n) AS c", "c")).containsExactly(4L);
    assertThat(column("USE `" + name + "` { MATCH (n:Person) RETURN count(n) AS c }", "c")).containsExactly(4L);
  }

  @Test
  void useOfAnotherGraphIsRefused() {
    assertThatThrownBy(() -> rows("USE otherGraph9940 MATCH (n:Person) RETURN count(n) AS c"))
        .hasMessageContaining("USE")
        .hasMessageContaining("otherGraph9940");
    assertThatThrownBy(() -> rows("USE otherGraph9940 { MATCH (n:Person) RETURN count(n) AS c }"))
        .hasMessageContaining("otherGraph9940");
  }

  @Test
  void aBracedQueryRunsItsBody() {
    assertThat(column("{ MATCH (n:Person) RETURN count(n) AS c }", "c")).containsExactly(4L);
  }

  // 5. Conditional query

  @Test
  void conditionalQueryRunsTheFirstTrueBranch() {
    final String query = "WHEN $x = 1 THEN RETURN 'one' AS r WHEN $x = 2 THEN RETURN 'two' AS r ELSE RETURN 'other' AS r";
    assertThat(column(query, "r", Map.of("x", 1))).containsExactly("one");
    assertThat(column(query, "r", Map.of("x", 2))).containsExactly("two");
    assertThat(column(query, "r", Map.of("x", 3))).containsExactly("other");
    assertThat(column("WHEN true THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r")).containsExactly(1L);
    // no branch taken and no ELSE: no rows
    assertThat(rows("WHEN false THEN RETURN 1 AS r")).isEmpty();
    // a branch is a full query
    assertThat(column("WHEN true THEN MATCH (n:Person) RETURN n.name AS r ORDER BY r ELSE RETURN 'none' AS r", "r"))
        .containsExactly("A", "B", "C", "D");
  }

  @Test
  void conditionalQueryRunsOnlyTheChosenWrite() {
    database.transaction(() -> database.command("opencypher",
        "WHEN false THEN CREATE (:Marker9940 {v:'then'}) ELSE CREATE (:Marker9940 {v:'else'})"));
    assertThat(column("MATCH (m:Marker9940) RETURN m.v AS v", "v")).containsExactly("else");
  }

  @Test
  void conditionalQueryConditionCanCallFunctions() {
    // a function call combined with AND / OR is evaluated directly by the logical expression, so the condition's context
    // must resolve functions the same way an execution plan does
    final String query = "WHEN toBoolean($f) AND $e THEN RETURN 'then' AS r ELSE RETURN 'else' AS r";
    assertThat(column(query, "r", Map.of("f", "true", "e", true))).containsExactly("then");
    assertThat(column(query, "r", Map.of("f", "false", "e", true))).containsExactly("else");
    assertThat(column("WHEN size($l) > 2 OR coalesce($x, false) THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r",
        Map.of("l", List.of(1, 2, 3), "x", false))).containsExactly(1L);
  }

  @Test
  void conditionalQueryNeedsTheParametersOfEveryBranch() {
    // parameters are checked for the whole query before a branch is chosen, as for any other query: a parameter that
    // only an untaken branch reads must still be bound
    assertThatThrownBy(() -> column("WHEN $x = 1 THEN RETURN $onlyHere AS r ELSE RETURN 'else' AS r", "r", Map.of("x", 2)))
        .hasMessageContaining("onlyHere");
  }

  @Test
  void conditionalQueryWithAWriteBranchAndNoReturnRuns() {
    database.transaction(() -> database.command("opencypher",
        "WHEN true THEN CREATE (:Marker9940b {v:'then'}) ELSE CREATE (:Marker9940b {v:'else'})"));
    assertThat(column("MATCH (m:Marker9940b) RETURN m.v AS v", "v")).containsExactly("then");
  }

  @Test
  void conditionalQueryBranchesMustReturnTheSameColumns() {
    assertThatThrownBy(() -> rows("WHEN true THEN RETURN 1 AS a ELSE RETURN 2 AS b")).hasMessageContaining("conditional");
  }

  @Test
  void nestedConditionalQueryIsRefused() {
    assertThatThrownBy(() -> rows("CALL { WHEN true THEN RETURN 1 AS r ELSE RETURN 2 AS r } RETURN r"))
        .hasMessageContaining("WHEN");
  }

  // 6. IS DIRECTED

  @Test
  void isDirectedIsRefused() {
    assertThatThrownBy(() -> rows("MATCH ()-[r:KNOWS]->() RETURN r IS DIRECTED AS d"))
        .hasMessageContaining("IS DIRECTED");
    assertThatThrownBy(() -> rows("MATCH ()-[r:KNOWS]->() WHERE r IS DIRECTED RETURN count(r) AS c"))
        .hasMessageContaining("IS DIRECTED");
    // a label that happens to be called DIRECTED is still reachable by quoting it
    assertThat(column("MATCH ()-[r:KNOWS]->() RETURN r IS `DIRECTED` AS d LIMIT 1", "d")).containsExactly(false);
  }

  // 7. IS NORMALIZED

  @Test
  void isNormalizedFollowsUnicodeNormalForms() {
    final String composed = "Å";            // A with ring above, one code point (NFC)
    final String decomposed = "Å";    // A followed by the combining ring (NFD)
    final Map<String, Object> params = Map.of("c", composed, "d", decomposed);

    final Map<String, Object> row = rows("RETURN $c IS NORMALIZED AS cDefault, $d IS NORMALIZED AS dDefault, "
        + "$d IS NFD NORMALIZED AS dNfd, $c IS NFD NORMALIZED AS cNfd, $c IS NOT NFKC NORMALIZED AS cNotNfkc, "
        + "$d IS NOT NORMALIZED AS dNot, $d IS NFKD NORMALIZED AS dNfkd, null IS NORMALIZED AS nul", params).getFirst();
    assertThat(row.get("cDefault")).isEqualTo(true);
    assertThat(row.get("dDefault")).isEqualTo(false);
    assertThat(row.get("dNfd")).isEqualTo(true);
    assertThat(row.get("cNfd")).isEqualTo(false);
    assertThat(row.get("cNotNfkc")).isEqualTo(false);
    assertThat(row.get("dNot")).isEqualTo(true);
    assertThat(row.get("dNfkd")).isEqualTo(true);
    assertThat(row.get("nul")).isNull();

    // as a predicate
    assertThat(column("UNWIND [$c, $d] AS s WITH s WHERE s IS NFC NORMALIZED RETURN size(s) AS len", "len", params))
        .containsExactly(1L);
    assertThat(column("UNWIND [$c, $d] AS s WITH s WHERE s IS NOT NFC NORMALIZED RETURN size(s) AS len", "len", params))
        .containsExactly(2L);
  }

  @Test
  void isNormalizedRefusesANonString() {
    assertThatThrownBy(() -> rows("RETURN 1 IS NORMALIZED AS n")).hasMessageContaining("NORMALIZED");
  }

  // 8. Path variable over a quantified path pattern with no outer hops

  @Test
  void pathVariableOverAQuantifiedGroupBindsTheWholePath() {
    final List<Map<String, Object>> rows = rows("MATCH p = (a:Person {name:'A'})((x)-[:KNOWS]->(y)){1,3}(b:Person {name:'D'}) "
        + "RETURN length(p) AS len, [n IN nodes(p) | n.name] AS names, size(relationships(p)) AS rels ORDER BY len");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).get("len")).isEqualTo(1L);
    assertThat(rows.get(0).get("names")).isEqualTo(List.of("A", "D"));
    assertThat(rows.get(0).get("rels")).isEqualTo(1L);
    assertThat(rows.get(1).get("len")).isEqualTo(3L);
    assertThat(rows.get(1).get("names")).isEqualTo(List.of("A", "B", "C", "D"));
    assertThat(rows.get(1).get("rels")).isEqualTo(3L);
  }

  // 9. Nested single quantified group

  @Test
  void nestedSingleQuantifiedGroupIsRefused() {
    assertThatThrownBy(() -> rows("MATCH (a)(((x)-[:KNOWS]->(y)){1,2}){1,2}(b) RETURN count(*) AS c"))
        .hasMessageContaining("nesting a quantified path pattern");
    assertThatThrownBy(() -> rows("MATCH (a)((()-[:KNOWS*1..2]->()){1,2})(b) RETURN count(*) AS c"))
        .hasMessageContaining("variable-length relationship inside a quantified path pattern");
  }

  // 10. PATH type

  @Test
  void aPathIsTypedPath() {
    final Map<String, Object> row = rows("MATCH p = (:Person {name:'A'})-[:KNOWS]->(:Person {name:'B'}) "
        + "RETURN p IS TYPED PATH AS typed, p IS :: PATH NOT NULL AS typedNotNull, valueType(p) AS vt, [1, 2] IS TYPED PATH AS list").getFirst();
    assertThat(row.get("typed")).isEqualTo(true);
    assertThat(row.get("typedNotNull")).isEqualTo(true);
    assertThat(row.get("vt")).isEqualTo("PATH NOT NULL");
    assertThat(row.get("list")).isEqualTo(false);
  }

  // 11. OFFSET written after LIMIT

  @Test
  void offsetWrittenAfterLimitIsAppliedBeforeIt() {
    // inserted out of order, so a skip applied before the sort drops the wrong row
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Item9940 {name:'D'}), (:Item9940 {name:'C'}), (:Item9940 {name:'B'}), (:Item9940 {name:'A'})"));

    for (final String query : new String[] {
        "MATCH (n:Item9940) RETURN n.name ORDER BY n.name LIMIT 2 OFFSET 1",
        "MATCH (n:Item9940) RETURN n.name ORDER BY n.name OFFSET 1 LIMIT 2",
        "MATCH (n:Item9940) RETURN n.name ORDER BY n.name LIMIT 2 SKIP 1",
        "UNWIND ['D', 'C', 'B', 'A'] AS name WITH {name: name} AS n RETURN n.name ORDER BY n.name LIMIT 2 OFFSET 1" })
      assertThat(column(query, "n.name")).as(query).containsExactly("B", "C");

    assertThatThrownBy(() -> rows("MATCH (n:Item9940) RETURN n.name ORDER BY n.name LIMIT 2 LIMIT 1"))
        .hasMessageContaining("RETURN");
  }

  // 12. Top-level parenthesized path with a WHERE and no quantifier

  @Test
  void unquantifiedParenthesizedPathBindsSingleElements() {
    assertThat(column("MATCH ((a)-[:KNOWS]->(b) WHERE a.name = 'A') RETURN b.name AS n ORDER BY n", "n"))
        .containsExactly("B", "D");
    assertThat(column("MATCH ((a:Person)-[r:KNOWS]->(b)) WHERE b.name = 'D' RETURN a.name AS n, type(r) AS t ORDER BY n", "n"))
        .containsExactly("A", "C");
    // juxtaposed with outer nodes, the boundary nodes are the same node: the outer constraints apply to the inner variables
    assertThat(column("MATCH (:Person {name:'A'})((a)-[:KNOWS]->(b) WHERE b.name <> 'D')() RETURN a.name + b.name AS n", "n"))
        .containsExactly("AB");
    assertThat(column("MATCH ()((a)-[:KNOWS]->(b))(:Person {name:'D'}) RETURN a.name AS n ORDER BY n", "n"))
        .containsExactly("A", "C");
    assertThat(column("MATCH (a)((a)-[:KNOWS]->(b))(b {name:'B'}) RETURN a.name AS n", "n")).containsExactly("A");
    // the same node under two different names is refused rather than bound to one of them only
    assertThatThrownBy(() -> rows("MATCH (x)((a)-[:KNOWS]->(b))() RETURN x, a"))
        .hasMessageContaining("parenthesized path");
  }

  // Subquery bodies: EXISTS { }, COUNT { }, COLLECT { } (bare-pattern and full-query forms) and CALL { }

  @Test
  void unsupportedConstructsInsideSubqueryBodiesAreRefused() {
    final String other = "otherGraph9940";
    final String[][] cases = {
        // path search prefix
        { "MATCH (a:Person {name:'A'}) WHERE EXISTS { ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) } RETURN a.name AS n", "path search prefix" },
        { "MATCH (a:Person {name:'A'}) WHERE EXISTS { MATCH ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) } RETURN a.name AS n", "path search prefix" },
        { "MATCH (a:Person {name:'A'}) RETURN COUNT { ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) } AS n", "path search prefix" },
        { "MATCH (a:Person {name:'A'}) RETURN COUNT { MATCH ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) } AS n", "path search prefix" },
        { "MATCH (a:Person {name:'A'}) RETURN COLLECT { MATCH p = ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) RETURN length(p) } AS n", "path search prefix" },
        { "MATCH (a:Person {name:'A'}) CALL (a) { MATCH p = ANY SHORTEST (a)-[:KNOWS]->+(:Person {name:'D'}) RETURN length(p) AS l } RETURN l", "path search prefix" },
        // match mode
        { "MATCH (a:Person) WHERE EXISTS { MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->() } RETURN a.name AS n", "match mode" },
        { "MATCH (a:Person) RETURN COUNT { REPEATABLE ELEMENTS (a)-[:KNOWS]->() } AS n", "match mode" },
        { "MATCH (a:Person) RETURN COUNT { MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->() } AS n", "match mode" },
        { "MATCH (a:Person) RETURN COLLECT { MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->(b) RETURN b.name } AS n", "match mode" },
        { "MATCH (a:Person) CALL (a) { MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->(b) RETURN b } RETURN b.name AS n", "match mode" },
        // USE of another graph
        { "MATCH (a:Person) WHERE EXISTS { USE " + other + " MATCH (a)-[:KNOWS]->() } RETURN a.name AS n", "USE " + other + " is not supported" },
        { "MATCH (a:Person) RETURN COUNT { USE " + other + " MATCH (a)-[:KNOWS]->() } AS n", "USE " + other + " is not supported" },
        { "MATCH (a:Person) RETURN COLLECT { USE " + other + " MATCH (a)-[:KNOWS]->(b) RETURN b.name } AS n", "USE " + other + " is not supported" },
        { "MATCH (a:Person) CALL (a) { USE " + other + " MATCH (a)-[:KNOWS]->(b) RETURN b } RETURN b.name AS n", "USE " + other + " is not supported" },
        // conditional query
        { "MATCH (a:Person) WHERE EXISTS { WHEN true THEN MATCH (a)-[:KNOWS]->() RETURN 1 AS x } RETURN a.name AS n", "conditional query" },
        { "MATCH (a:Person) RETURN COUNT { WHEN true THEN MATCH (a)-[:KNOWS]->() RETURN 1 AS x } AS n", "conditional query" },
        { "MATCH (a:Person) RETURN COLLECT { WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x } AS n", "conditional query" },
        { "MATCH (a:Person) CALL (a) { WHEN true THEN RETURN 1 AS x ELSE RETURN 2 AS x } RETURN x AS n", "conditional query" },
        // IS DIRECTED
        { "MATCH (a:Person) WHERE EXISTS { (a)-[r:KNOWS]->() WHERE r IS DIRECTED } RETURN a.name AS n", "directed predicate" },
        { "MATCH (a:Person) WHERE EXISTS { MATCH (a)-[r:KNOWS]->() WHERE r IS DIRECTED } RETURN a.name AS n", "directed predicate" },
        { "MATCH (a:Person) RETURN COUNT { (a)-[r:KNOWS]->() WHERE r IS DIRECTED } AS n", "directed predicate" },
        { "MATCH (a:Person) RETURN COUNT { MATCH (a)-[r:KNOWS]->() WHERE r IS DIRECTED } AS n", "directed predicate" },
        { "MATCH (a:Person) RETURN COLLECT { MATCH (a)-[r:KNOWS]->() RETURN r IS DIRECTED } AS n", "directed predicate" },
        { "MATCH (a:Person) CALL (a) { MATCH (a)-[r:KNOWS]->() RETURN r IS DIRECTED AS d } RETURN d AS n", "directed predicate" } };

    for (final String[] c : cases)
      // the refusal itself, not a syntax error that merely echoes the query text
      assertThatThrownBy(() -> rows(c[0])).as(c[0]).hasMessageContaining("FeatureNotImplemented").hasMessageContaining(c[1]);
  }

  @Test
  void letInsideSubqueryBodiesBindsVariables() {
    assertThat(column("MATCH (a:Person) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) LET n = b.name FILTER n = 'D' } "
        + "RETURN a.name AS n ORDER BY n", "n")).containsExactly("A", "C");
    assertThat(column("MATCH (a:Person) RETURN a.name AS a, COUNT { MATCH (a)-[:KNOWS]->(b) LET n = b.name FILTER n <> 'D' } "
        + "AS n ORDER BY a", "n")).containsExactly(1L, 1L, 0L, 0L);
    assertThat(column("MATCH (a:Person {name:'A'}) RETURN COLLECT { MATCH (a)-[:KNOWS]->(b) LET n = b.name + '!' "
        + "RETURN n ORDER BY n } AS n", "n")).containsExactly(List.of("B!", "D!"));
    assertThat(column("MATCH (a:Person {name:'A'}) CALL (a) { MATCH (a)-[:KNOWS]->(b) LET n = b.name + '!' RETURN n } "
        + "RETURN n ORDER BY n", "n")).containsExactly("B!", "D!");
  }

  // shortestPath with an inline node WHERE: the predicate is hoisted into the MATCH WHERE, and the pattern must stay a
  // shortest-path pattern while it is

  @Test
  void shortestPathWithInlineNodeWhereReturnsOnlyTheShortest() {
    assertThat(column("MATCH p = shortestPath((a {name:'A'})-[:KNOWS*]-(b WHERE b.name = 'D')) RETURN length(p) AS l", "l"))
        .containsExactly(1L);
    assertThat(column("MATCH p = allShortestPaths((a WHERE a.name = 'A')-[:KNOWS*]->(b {name:'D'})) RETURN length(p) AS l", "l"))
        .containsExactly(1L);
  }
}
