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
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
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
  void letRefusesToRedeclareAVariable() {
    // as Neo4j: a LET declares a new variable, it does not shadow one already in scope
    assertThatThrownBy(() -> rows("MATCH (n:Person) LET n = 1 RETURN n")).hasMessageContaining("Variable 'n' already declared");
    assertThatThrownBy(() -> rows("LET x = 1, x = 2 RETURN x")).hasMessageContaining("Variable 'x' already declared");
    assertThatThrownBy(() -> rows("MATCH (n:Person) WITH * LET n = 1 RETURN n")).hasMessageContaining("Variable 'n' already declared");
    // a WITH that drops the name frees it for a LET, and a plain WITH may still shadow
    assertThat(column("MATCH (n:Person {name:'A'}) WITH n.name AS name LET n = 1 RETURN n", "n")).containsExactly(1L);
    assertThat(column("MATCH (n:Person {name:'A'}) WITH *, 1 AS n RETURN n", "n")).containsExactly(1L);
  }

  @Test
  void letItemsReadEarlierItemsAndFollowAnAggregation() {
    assertThat(rows("LET x = 1, y = x + 1 RETURN x, y").getFirst()).containsEntry("x", 1L).containsEntry("y", 2L);
    assertThat(column("MATCH (n:Person) WITH count(n) AS c LET d = c * 2 RETURN d", "d")).containsExactly(8L);
  }

  @Test
  void letAfterUnwindOptionalMatchAndSubqueryKeepsTheScope() {
    assertThat(column("UNWIND [1, 2] AS i LET j = i * 10 RETURN i + j AS r ORDER BY r", "r")).containsExactly(11L, 22L);
    assertThat(column("MATCH (a:Person {name:'C'}) OPTIONAL MATCH (a)-[:KNOWS]->(b:Person {name:'A'}) "
        + "LET hasB = b IS NOT NULL RETURN a.name AS a, hasB", "hasB")).containsExactly(false);
    assertThat(column("MATCH (a:Person {name:'A'}) CALL (a) { MATCH (a)-[:KNOWS]->(x) RETURN count(x) AS out } "
        + "LET twice = out * 2 RETURN twice", "twice")).containsExactly(4L);
    assertThatThrownBy(() -> rows("UNWIND [1] AS i LET i = 2 RETURN i")).hasMessageContaining("Variable 'i' already declared");
  }

  @Test
  void aShortestPathSerializesToJsonAsAPath() {
    // Every Cypher path is one value since issue #9913, serialized as {_type, nodes, relationships, length}: a
    // shortestPath binding was an alternating list
    try (final ResultSet rs = database.query("opencypher",
        "MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'D'})) RETURN p")) {
      final Result row = rs.next();
      final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, row);
      final JSONObject path = json.getJSONObject("p");
      assertThat(path.getString("_type")).isEqualTo("path");
      assertThat(path.getInt("length")).isEqualTo(1);
      final JSONArray nodes = path.getJSONArray("nodes");
      assertThat(nodes.length()).isEqualTo(2);
      assertThat(nodes.getJSONObject(0).getString("name")).isEqualTo("A");
      assertThat(nodes.getJSONObject(1).getString("name")).isEqualTo("D");
      assertThat(path.getJSONArray("relationships").length()).isEqualTo(1);
      // and the JSON of the result itself
      assertThat(row.toJSON().getJSONObject("p").getJSONArray("nodes").length()).isEqualTo(2);
    }
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
  void useOfADottedOrCompositeNameIsRefused() {
    assertThatThrownBy(() -> rows("USE a.b MATCH (n:Person) RETURN count(n) AS c")).hasMessageContaining("USE");
    assertThatThrownBy(() -> rows("USE `" + database.getName() + "`.x MATCH (n:Person) RETURN count(n) AS c"))
        .hasMessageContaining("USE");
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
    // an unbound parameter in a condition is refused too, rather than read as null and falling through to ELSE
    assertThatThrownBy(() -> column("WHEN $unbound = 1 THEN RETURN 'then' AS r ELSE RETURN 'else' AS r", "r", Map.of()))
        .hasMessageContaining("unbound");
  }

  @Test
  void conditionalQueryConditionReadingAVariableIsRefusedAtParseTime() {
    // a conditional query is a whole statement: there is no row, so a condition may read parameters only
    assertThatThrownBy(() -> rows("WHEN n.x = 1 THEN RETURN 1 AS r ELSE RETURN 2 AS r"))
        .hasMessageContaining("the condition of a conditional query can read parameters only, not 'n'");
    assertThatThrownBy(() -> rows("MATCH (n) WITH n WHEN n.x = 1 THEN RETURN 1 AS r"))
        .hasMessageContaining("Unexpected input 'WHEN'");
  }

  @Test
  void explainOfAConditionalQueryShowsTheChosenBranch() {
    try (final ResultSet rs = database.query("opencypher",
        "EXPLAIN WHEN $x = 1 THEN MATCH (n:Person) RETURN n.name AS r ELSE RETURN 'none' AS r", Map.of("x", 1))) {
      assertThat(rs.getExecutionPlan()).isPresent();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("Person");
    }
  }

  @Test
  void conditionalQueryConditionWithASubquery() {
    // a subquery that names a variable is refused conservatively: the condition is checked to read parameters only
    assertThatThrownBy(() -> rows("WHEN EXISTS { MATCH (n:Person) } THEN RETURN 1 AS r ELSE RETURN 2 AS r"))
        .hasMessageContaining("the condition of a conditional query");
    // an uncorrelated subquery reads no row, so it runs: it is not ignored
    assertThat(column("WHEN COUNT { MATCH (:Person) } > 0 THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r")).containsExactly(1L);
    assertThat(column("WHEN COUNT { MATCH (:NoSuchType9940) } > 0 THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r")).containsExactly(2L);
    assertThat(column("WHEN EXISTS { MATCH (:Person) } THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r")).containsExactly(1L);
  }

  @Test
  void conditionalQueryWithAWriteBranchAndNoReturnRuns() {
    database.transaction(() -> database.command("opencypher",
        "WHEN true THEN CREATE (:Marker9940b {v:'then'}) ELSE CREATE (:Marker9940b {v:'else'})"));
    assertThat(column("MATCH (m:Marker9940b) RETURN m.v AS v", "v")).containsExactly("then");
  }

  @Test
  void conditionalBranchPlanKeysCannotBeQueryText() {
    // a branch's plan-cache key is "\0WHEN#i\0<query>" or "\0ELSE\0<query>": query text is trimmed (which drops the leading
    // NUL) and the lexer refuses the NUL that is then left inside the text, so a key can never be query text that plans.
    // A NUL inside a string literal is ordinary content
    assertThatThrownBy(() -> rows("\u0000WHEN#0\u0000RETURN 1 AS r")).isInstanceOf(Exception.class);
    assertThatThrownBy(() -> rows("\u0000ELSE\u0000RETURN 1 AS r")).isInstanceOf(Exception.class);
    assertThatThrownBy(() -> rows("RETURN 1\u0000 AS r")).hasMessageContaining("Unexpected input");
    assertThat(column("RETURN 'a\u0000b' AS r", "r")).containsExactly("a\u0000b");
    // the same conditional text runs each branch from its own cached plan
    final String query = "WHEN $x = 1 THEN RETURN 'one' AS r ELSE RETURN 'other' AS r";
    for (int i = 0; i < 3; i++) {
      assertThat(column(query, "r", Map.of("x", 1))).containsExactly("one");
      assertThat(column(query, "r", Map.of("x", 2))).containsExactly("other");
    }
  }

  @Test
  void isNormalizedColumnIsNamedAfterTheSourceText() {
    assertThat(rows("RETURN 'a' IS NORMALIZED").getFirst().keySet()).containsExactly("'a' IS NORMALIZED");
  }

  @Test
  void conditionalQueryBranchesWithAndWithoutAReturnAreRefused() {
    // the statement's columns are the first branch's, so a branch that returns nothing cannot sit next to one that does
    assertThatThrownBy(() -> rows("WHEN true THEN CREATE (:Marker9940c) ELSE RETURN 1 AS r")).hasMessageContaining("conditional");
    assertThatThrownBy(() -> rows("WHEN true THEN RETURN 1 AS r ELSE CREATE (:Marker9940c)")).hasMessageContaining("conditional");
    // a branch is a single query: a UNION inside one does not parse
    assertThatThrownBy(() -> rows("WHEN true THEN RETURN 1 AS r UNION RETURN 2 AS r ELSE RETURN 3 AS r")).isInstanceOf(Exception.class);
  }

  @Test
  void conditionalQueryWithANullConditionAndNoElseReturnsNoRows() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("x", null);
    assertThat(rows("WHEN $x THEN RETURN 1 AS r", parameters)).isEmpty();
    assertThat(column("WHEN $x THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r", parameters)).containsExactly(2L);
  }

  @Test
  void conditionalQueryConditionCanCallFunctionsThatReadTheDatabaseOrTheClock() {
    assertThat(column("WHEN datetime() > datetime('2000-01-01T00:00:00Z') THEN RETURN 'now' AS r ELSE RETURN 'past' AS r", "r"))
        .containsExactly("now");
    // inside an explicit transaction, the chosen branch writes in that transaction
    database.transaction(() -> database.command("opencypher",
        "WHEN toUpper($s) = 'YES' THEN CREATE (:Marker9940d {v: 1}) ELSE CREATE (:Marker9940d {v: 2})", Map.of("s", "yes")));
    assertThat(column("MATCH (m:Marker9940d) RETURN m.v AS v", "v")).containsExactly(1);
  }

  @Test
  void conditionalQueryWithANonBooleanConditionIsRefused() {
    assertThatThrownBy(() -> column("WHEN $x THEN RETURN 1 AS r ELSE RETURN 2 AS r", "r", Map.of("x", 5)))
        .hasMessageContaining("must be a BOOLEAN");
  }

  @Test
  void conditionalQueryBranchesMustReturnTheirColumnsInTheSameOrder() {
    // stricter than Neo4j, which reorders the columns of a later branch to the first branch's order: refused, never answered
    // with the columns in a different order
    assertThatThrownBy(() -> rows("WHEN true THEN RETURN 1 AS a, 2 AS b ELSE RETURN 2 AS b, 1 AS a"))
        .hasMessageContaining("same columns in the same order");
  }

  @Test
  void aConditionalQueryWithAWritingBranchIsNotIdempotent() {
    // query() runs idempotent statements only: a conditional query any of whose branches writes is a write statement,
    // even when the branch taken would only read
    assertThatThrownBy(() -> database.query("opencypher", "WHEN true THEN RETURN 1 AS r ELSE CREATE (:Marker9940e) RETURN 2 AS r"))
        .isInstanceOf(Exception.class);
    assertThat(column("MATCH (m:Marker9940e) RETURN count(m) AS c", "c")).containsExactly(0L);
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
  void isNormalizedOfANonStringIsNull() {
    // as Neo4j Cypher 25: only a string has a normal form, anything else is UNKNOWN, so a WHERE drops the row
    assertThat(rows("RETURN 1 IS NORMALIZED AS n, 1 IS NOT NORMALIZED AS m").getFirst()).containsEntry("n", null).containsEntry("m", null);
    assertThat(column("UNWIND [1, 'a'] AS x WITH x WHERE x IS NORMALIZED RETURN x", "x")).containsExactly("a");
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

  @Test
  void everyKindOfPathIsTypedPath() {
    // every path producer yields a path value, through COLLECT / UNWIND too
    for (final String query : new String[] {
        "MATCH p = (:Person {name:'A'})-[:KNOWS*1..3]->(:Person {name:'D'}) RETURN p IS TYPED PATH AS typed",
        "MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'D'})) RETURN p IS TYPED PATH AS typed",
        "MATCH p = allShortestPaths((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'D'})) RETURN p IS TYPED PATH AS typed",
        "MATCH p = (:Person {name:'A'})((x)-[:KNOWS]->(y)){1,3}(:Person {name:'D'}) RETURN p IS TYPED PATH AS typed",
        "MATCH p = (:Person {name:'A'})-[:KNOWS]->() WITH collect(p) AS ps UNWIND ps AS q RETURN q IS TYPED PATH AS typed" }) {
      final List<Object> typed = column(query, "typed");
      assertThat(typed).as(query).isNotEmpty();
      assertThat(typed).as(query).allMatch(Boolean.TRUE::equals);
    }
  }

  @Test
  void aListThatAlternatesVerticesAndEdgesIsNotAPath() {
    final Map<String, Object> row = rows("MATCH (a:Person {name:'A'})-[r:KNOWS]->(b:Person {name:'B'}) "
        + "RETURN [a, r, b] IS TYPED PATH AS typed, valueType([a, r, b]) AS vt").getFirst();
    assertThat(row.get("typed")).isEqualTo(false);
    assertThat((String) row.get("vt")).startsWith("LIST");
    // the shortestPath of the same hop is a PATH
    assertThat(column("MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'B'})) RETURN valueType(p) AS vt",
        "vt")).containsExactly("PATH NOT NULL");
  }

  @Test
  void aShortestPathStaysAPathThroughWithCollectAndUnwind() {
    for (final String query : new String[] {
        "MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'C'})) WITH p RETURN valueType(p) AS vt",
        "MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'C'})) WITH collect(p) AS ps UNWIND ps AS q "
            + "RETURN valueType(q) AS vt",
        "MATCH p = allShortestPaths((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'C'})) RETURN valueType(p) AS vt" })
      assertThat(column(query, "vt")).as(query).containsOnly("PATH NOT NULL");
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
    // ORDER BY written both before and after the RETURN's paging is the same part twice: refused, not merged
    assertThatThrownBy(() -> rows("MATCH (n:Item9940) RETURN n.name ORDER BY n.name LIMIT 2 ORDER BY n.name DESC"))
        .hasMessageContaining("RETURN");
  }

  @Test
  void trailingPageAfterDistinctOrAggregatingReturnSortsTheProjection() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Item9940b {g:'y', v:1}), (:Item9940b {g:'x', v:2}), (:Item9940b {g:'y', v:3}), (:Item9940b {g:'z', v:4})"));
    // DISTINCT, with the trailing ORDER BY naming the projected expression
    assertThat(column("MATCH (n:Item9940b) RETURN DISTINCT n.g LIMIT 2 ORDER BY n.g", "n.g")).containsExactly("x", "y");
    // aggregation, with the trailing ORDER BY naming the grouping key, SKIP and LIMIT applied after the sort
    assertThat(column("MATCH (n:Item9940b) RETURN n.g AS g, count(*) AS c LIMIT 2 ORDER BY g SKIP 1", "g"))
        .containsExactly("y", "z");
    // inside UNION branches and a CALL subquery, each RETURN pages its own rows
    assertThat(column("MATCH (n:Item9940b) RETURN n.v AS v LIMIT 1 ORDER BY v DESC UNION ALL "
        + "MATCH (n:Item9940b) RETURN n.v AS v LIMIT 1 ORDER BY v", "v")).containsExactly(4, 1);
    assertThat(column("CALL { MATCH (n:Item9940b) RETURN n.v AS v LIMIT 2 ORDER BY v DESC SKIP 1 } RETURN v", "v"))
        .containsExactly(3, 2);
    // aggregation, with the trailing ORDER BY naming the pre-aggregation expression the RETURN groups by
    assertThat(column("MATCH (n:Item9940b) RETURN n.g, count(*) AS c LIMIT 2 ORDER BY n.g", "n.g")).containsExactly("x", "y");
  }

  // 12. Top-level parenthesized path with a WHERE and no quantifier

  @Test
  void juxtaposedNodesOfAParenthesizedPathThatCannotBeMergedAreRefused() {
    // a node written next to a parenthesized path is the path's first node: it merges with it, or is refused by name
    for (final String[] refused : new String[][] {
        { "MATCH (a)((b)-[:KNOWS]->(c)) RETURN c.name AS n", "same node under two variables" },
        { "MATCH (a:Person|Admin)((a)-[:KNOWS]->(c)) RETURN c.name AS n", "label disjunction" },
        { "MATCH (a $props)((a)-[:KNOWS]->(c)) RETURN c.name AS n", "property-map parameter" },
        { "MATCH (a {name:'A'})((a {name:'B'})-[:KNOWS]->(c)) RETURN c.name AS n", "constrained by both node patterns" } })
      assertThatThrownBy(() -> rows(refused[0], Map.of("props", Map.of("name", "A")))).as(refused[0])
          .isInstanceOf(Exception.class).hasMessageContaining(refused[1]);
    // the constraints of both sides combine when they do not conflict
    assertThat(column("MATCH (a:Person)((a {name:'A'})-[:KNOWS]->(c)) RETURN c.name AS n ORDER BY n", "n")).containsExactly("B", "D");
  }

  @Test
  void aShortestPathIsNotTypedList() {
    final Map<String, Object> row = rows("MATCH p = shortestPath((:Person {name:'A'})-[:KNOWS*]->(:Person {name:'C'})) "
        + "RETURN p IS TYPED LIST<ANY> AS list, p IS TYPED PATH AS path").getFirst();
    assertThat(row.get("list")).isEqualTo(false);
    assertThat(row.get("path")).isEqualTo(true);
  }

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
