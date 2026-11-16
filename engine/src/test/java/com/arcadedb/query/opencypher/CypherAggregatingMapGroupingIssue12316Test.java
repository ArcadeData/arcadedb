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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12316: an aggregating map literal or map projection reading a variable that is not a grouping key folded every
 * input row into one group and read that variable from an arbitrary row.
 * <ul>
 *   <li>a map projection groups by its own variable, as Neo4j does ({@code RETURN p{.name, n: count(f)}}: one row per p);</li>
 *   <li>a map literal, a list, a {@code CASE} or a {@code reduce()} reading a non-grouping variable is ambiguous, as the
 *   arithmetic form already was (Neo4j 42I18);</li>
 *   <li>an aggregate inside a pattern comprehension is refused, as in a list comprehension.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherAggregatingMapGroupingIssue12316Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Person {name:'a', age: 30})-[:KNOWS]->(:Person {name:'b', age: 1}), (a)-[:KNOWS]->(:Person {name:'c', age: 2}),
               (d:Person {name:'d', age: 40})-[:KNOWS]->(:Person {name:'e', age: 3})"""));
  }

  @Test
  void aMapProjectionGroupsByItsVariable() {
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, n: count(f)} AS m ORDER BY m.name", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L), Map.of("name", "d", "n", 1L));

    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) WITH p{.name, n: count(f)} AS m RETURN m ORDER BY m.name", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L), Map.of("name", "d", "n", 1L));

    // The projection's own entries may read its variable in any shape: it is single-valued per group
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{up: toUpper(p.name), n: count(f) + p.age} AS m ORDER BY m.up",
        "m")).containsExactly(Map.of("up", "A", "n", 32L), Map.of("up", "D", "n", 41L));

    // Nested under a map literal or a function, the projection still groups
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN {who: p{.name, n: count(f)}} AS m ORDER BY m.who.name", "m"))
        .containsExactly(Map.of("who", Map.of("name", "a", "n", 2L)), Map.of("who", Map.of("name", "d", "n", 1L)));

    // As the initial value of a reduce(), the projection still groups
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN 0 AS k, reduce(m = p{.name, n: count(f)}, x IN [] | m) AS m ORDER BY m.name",
        "m")).containsExactly(Map.of("name", "a", "n", 2L), Map.of("name", "d", "n", 1L));
    // Under an index, a slice or a chained property access, the projection still groups
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN [p{.name, n: count(f)}][0] AS m ORDER BY m.name", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L), Map.of("name", "d", "n", 1L));
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, n: count(f)}.n AS m ORDER BY m", "m"))
        .containsExactly(1L, 2L);

    final List<Object> friends = values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, friends: collect(f.name)} AS m ORDER BY m.name",
        "m");
    assertThat(friends).hasSize(2);
    assertThat((List<Object>) ((Map<?, ?>) friends.get(0)).get("friends")).containsExactlyInAnyOrder("b", "c");
    assertThat((List<Object>) ((Map<?, ?>) friends.get(1)).get("friends")).containsExactly("e");
  }

  @Test
  void aMapProjectionNextToItsVariableOrOtherKeysGroupsOnce() {
    // Projected bare as well: still one group per p, and the hidden key is not added twice
    final List<Map<String, Object>> rows = rowMaps("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name AS name, p{n: count(f)} AS m ORDER BY name");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)).containsEntry("name", "a").containsEntry("m", Map.of("n", 2L));
    assertThat(rows.get(1)).containsEntry("name", "d").containsEntry("m", Map.of("n", 1L));
    // The hidden grouping key never reaches the row
    assertThat(rows.get(0).keySet()).containsExactlyInAnyOrder("name", "m");

    assertThat(rowMaps("MATCH (p:Person)-[:KNOWS]->(f) RETURN p, p{n: count(f)} AS m")).hasSize(2);

    // A map projection with no aggregate in it groups nothing
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN count(f) AS c", "c")).containsExactly(3L);
  }

  @Test
  void aMapLiteralReadingANonGroupingVariableIsAmbiguous() {
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN {name: p.name, n: count(f)} AS m");
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) WITH {name: p.name, n: count(f)} AS m RETURN m");
    // The arithmetic form was already refused
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name + count(f) AS m");
    // A list, a CASE, a reduce() reading a non-grouping variable
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN [p.name, count(f)] AS m");
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN CASE WHEN p.age > 35 THEN count(f) ELSE 0 END AS m");
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN reduce(s = count(f), x IN [p.age] | s + x) AS m");
    // A map projection entry reading another variable that is not a grouping key
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, other: f.name, n: count(*)} AS m");
    // Another property of a variable whose property is the key is not a key (Neo4j 42I18)
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, {old: p.age > 35, n: count(f)} AS m");
    // A scalar function or a predicate mixing a non-grouping property with an aggregate
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN {age: coalesce(p.age, count(f))} AS m");
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN coalesce(p.age, count(f)) AS m");
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name AS k, {isB: f.name IN ['b'], n: count(f)} AS m");
    // A logical operator over a non-grouping value, next to an aggregate or wrapping one
    assertAmbiguous("UNWIND [{v: true}, {v: false}] AS p RETURN {v: p.v AND true, n: count(*)} AS m");
    assertAmbiguous("UNWIND [{v: true}, {v: false}] AS p RETURN p.v OR count(*) > 0 AS m");
    // Only the exact property projected is a key: p.v varies within the group of p.k
    assertAmbiguous("UNWIND [{k: 1, v: 10}, {k: 1, v: 20}] AS p RETURN p.k AS k, {v: p.v, n: count(*)} AS m");
    assertAmbiguous("UNWIND [{k: 1, v: 10}, {k: 1, v: 20}] AS p RETURN p.k, p.v + count(*) AS m");
    // An output alias is not the incoming variable it shadows
    assertAmbiguous("UNWIND [{k: 1, v: 'a'}, {k: 1, v: 'b'}] AS p RETURN p.k AS p, {v: toUpper(p.v), n: count(*)} AS m");
    // A list next to an aggregate reading a non-grouping variable: group by it first
    assertAmbiguous("MATCH (p:Person)-[:KNOWS]->(f) RETURN collect(f) + [p] AS m");
    // The error names the variable and the way out
    assertThatThrownBy(() -> values("MATCH (p:Person)-[:KNOWS]->(f) RETURN collect(f) + [p] AS m", "m"))
        .hasMessageContaining("'p', which is not a grouping key").hasMessageContaining("WITH p, collect(...)");
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) WITH p, collect(f) AS fs RETURN size(fs + [p]) AS m ORDER BY m", "m"))
        .containsExactly(2L, 3L);
  }

  @Test
  void aMapLiteralOverGroupingKeysIsAccepted() {
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name AS k, {name: p.name, n: count(f)} AS m ORDER BY k", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L), Map.of("name", "d", "n", 1L));
    // A grouping-key property inside a larger expression (TCK Return6 [6])
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name AS k, {isA: p.name = 'a', n: count(f)} AS m ORDER BY k", "m"))
        .containsExactly(Map.of("isA", true, "n", 2L), Map.of("isA", false, "n", 1L));
    // A complex expression over a variable projected as itself is single-valued per group
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p, {up: toUpper(p.name), n: count(f)} AS m ORDER BY m.up", "m"))
        .containsExactly(Map.of("up", "A", "n", 2L), Map.of("up", "D", "n", 1L));
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p, p.age * 2 + count(f) AS m ORDER BY m", "m"))
        .containsExactly(62L, 81L);
    // Constants and the names reduce() binds are not grouping questions
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN {n: count(f), k: 'x'} AS m", "m")).containsExactly(Map.of("n", 3L, "k", "x"));
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN reduce(s = count(f), x IN [1, 2] | s + x) AS m", "m"))
        .containsExactly(6L);
  }

  @Test
  void anAggregateInAPatternComprehensionIsRefused() {
    assertThatThrownBy(() -> values("MATCH (n:Person) RETURN [(n)-[:KNOWS]->(x) | count(*)] AS m", "m"))
        .hasMessageContaining("pattern comprehension");
    assertThatThrownBy(() -> values("MATCH (n:Person) RETURN [x IN [1, 2] | count(*)] AS m", "m"))
        .hasMessageContaining("list comprehension");
    // An aggregate wrapped in a function inside a list comprehension is still one
    assertThatThrownBy(() -> values("MATCH (n:Person) RETURN [x IN [1, 2] | toString(count(*))] AS m", "m"))
        .hasMessageContaining("list comprehension");
  }

  private void assertAmbiguous(final String query) {
    assertThatThrownBy(() -> values(query, "m")).as(query).hasMessageContaining("AmbiguousAggregationExpression");
  }

  private List<Object> values(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }

  private List<Map<String, Object>> rowMaps(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
  }
}
