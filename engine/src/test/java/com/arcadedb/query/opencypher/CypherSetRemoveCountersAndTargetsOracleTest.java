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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests for the SET / REMOVE write counters and a few write targets the mutation run of SetStep, RemoveStep and
 * SetClauseApplier left unpinned. Every counter expectation was read off Neo4j 5.26 (community) on the same statement
 * sequence, so a mutation of how a SET or REMOVE counts what it did is caught here, not by a statement that merely
 * succeeds. Where ArcadeDB answers differently from Neo4j on purpose (a null dynamic key is a no-op, a write through a
 * deleted node is refused), the test says so and cites the code that documents it.
 */
class CypherSetRemoveCountersAndTargetsOracleTest extends TestHelper {

  private QueryStatistics statsOf(final String cypher) {
    return statsOf(cypher, Map.of());
  }

  private QueryStatistics statsOf(final String cypher, final Map<String, Object> params) {
    final QueryStatistics[] stats = new QueryStatistics[1];
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", cypher, params);
      while (rs.hasNext())
        rs.next();
      final Optional<QueryStatistics> s = rs.getStatistics();
      assertThat(s).isPresent();
      stats[0] = s.get();
    });
    return stats[0];
  }

  private Object valueOf(final String cypher, final String column) {
    final Object[] value = new Object[1];
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", cypher);
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      value[0] = row.getProperty(column);
    });
    return value[0];
  }

  /** The properties of the node as a map whose whole numbers are all longs: a literal in a pattern and one in an expression differ in width. */
  @SuppressWarnings("unchecked")
  private Map<String, Object> propertiesOf(final String cypher) {
    final Map<String, Object> normalized = new HashMap<>();
    ((Map<String, Object>) valueOf(cypher, "p")).forEach((k, v) -> normalized.put(k, v instanceof Number n && !(v instanceof Double) ? n.longValue() : v));
    return normalized;
  }

  @Test
  void mergeOnMatchSetOfAnUnchangedValueIsNotWrittenButStillCounted() {
    // The write is skipped (issue #4474) but the assignment is reported, as Neo4j reports it: the first run creates the node
    // (2 properties: id, x), the second finds x already 5 and still counts the assignment
    assertThat(statsOf("MERGE (n:T {id:1}) ON CREATE SET n.x = 5 ON MATCH SET n.x = 5").getPropertiesSet()).isEqualTo(2);

    final QueryStatistics again = statsOf("MERGE (n:T {id:1}) ON CREATE SET n.x = 5 ON MATCH SET n.x = 5");

    assertThat(again.getPropertiesSet()).isEqualTo(1);
    assertThat(again.getNodesCreated()).isEqualTo(0);
    assertThat(((Number) valueOf("MATCH (n:T {id:1}) RETURN n.x AS x", "x")).longValue()).isEqualTo(5L);
  }

  @Test
  void mergeOnMatchSetNullRemovesAnExistingPropertyAndCountsOnlyThat() {
    statsOf("CREATE (:T {id:1, x:5})");

    // x exists: removing it is one counted change; the property is gone afterwards
    assertThat(statsOf("MERGE (n:T {id:1}) ON MATCH SET n.x = null").getPropertiesSet()).isEqualTo(1);
    assertThat(valueOf("MATCH (n:T {id:1}) RETURN n.x AS x", "x")).isNull();
    assertThat(propertiesOf("MATCH (n:T {id:1}) RETURN properties(n) AS p")).isEqualTo(Map.of("id", 1L));

    // x is absent now: removing it again is no write and nothing to count
    assertThat(statsOf("MERGE (n:T {id:1}) ON MATCH SET n.x = null").getPropertiesSet()).isEqualTo(0);
  }

  @Test
  void mergeOnMatchMapSetCountsAssignmentsAndTheNullsThatRemoveAHeldProperty() {
    statsOf("CREATE (:T {id:1, z:1})");

    // y is assigned (1) and z, which the node holds, is removed (1)
    assertThat(statsOf("MERGE (n:T {id:1}) ON MATCH SET n += {y: 1, z: null}").getPropertiesSet()).isEqualTo(2);
    assertThat(propertiesOf("MATCH (n:T {id:1}) RETURN properties(n) AS p")).isEqualTo(Map.of("id", 1L, "y", 1L));

    // SET n = map: id and y are written (2); the property the map leaves out is removed (counted)
    statsOf("MATCH (n:T {id:1}) SET n.extra = 'e'");
    assertThat(statsOf("MERGE (n:T {id:1}) ON MATCH SET n = {id: 1, y: 2}").getPropertiesSet()).isEqualTo(3);
    assertThat(propertiesOf("MATCH (n:T {id:1}) RETURN properties(n) AS p")).isEqualTo(Map.of("id", 1L, "y", 2L));
  }

  @Test
  void removeCountsEachExistingPropertyOnceAndAnAbsentOneNotAtAll() {
    statsOf("CREATE (:T {id:1})");

    // one assignment plus one removal of the property just written; zz never existed
    assertThat(statsOf("MATCH (n:T {id:1}) SET n.i = 5 REMOVE n.i, n.zz").getPropertiesSet()).isEqualTo(2);
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE n.zz").getPropertiesSet()).isEqualTo(0);
    assertThat(propertiesOf("MATCH (n:T {id:1}) RETURN properties(n) AS p")).isEqualTo(Map.of("id", 1L));
  }

  @Test
  void labelWritesCountEveryNewLabelAndEveryRemovedLabelOnce() {
    statsOf("CREATE (:T {id:1})");

    assertThat(statsOf("MATCH (n:T {id:1}) SET n:A:B").getLabelsAdded()).isEqualTo(2);

    // removing two labels is two removals; a label the node does not carry (A is already gone) counts nothing
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE n:A:B").getLabelsRemoved()).isEqualTo(2);
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE n:A:A").getLabelsRemoved()).isEqualTo(0);

    // a label named twice in one clause is one label, for the write and for the removal
    assertThat(statsOf("MATCH (n:T {id:1}) SET n:A:A").getLabelsAdded()).isEqualTo(1);
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE n:A:A").getLabelsRemoved()).isEqualTo(1);
    assertThat(valueOf("MATCH (n:T {id:1}) RETURN labels(n) AS l", "l")).isEqualTo(List.of("T"));
  }

  @Test
  void relationshipMapWritesCountAssignmentsAndSkipNullEntries() {
    statsOf("CREATE (:T {id:2})-[:E]->(:T {id:3})");

    // u and g are stored, f is null: 2 properties, on the first write and on the identical second one
    final Map<String, Object> params = Map.of("m", Map.of("u", "a", "g", List.of(1)));
    assertThat(statsOf("MATCH ()-[e:E]->() SET e = $m", params).getPropertiesSet()).isEqualTo(2);
    assertThat(statsOf("MATCH ()-[e:E]->() SET e = $m", params).getPropertiesSet()).isEqualTo(2);
    assertThat(statsOf("MATCH ()-[e:E]->() SET e += $m", params).getPropertiesSet()).isEqualTo(2);
    assertThat(statsOf("MATCH ()-[e:E]->() SET e.f = null, e.u = $x", Map.of("x", "a")).getPropertiesSet()).isEqualTo(1);
    assertThat(valueOf("MATCH ()-[e:E]->() RETURN properties(e) AS p", "p")).isEqualTo(Map.of("u", "a", "g", List.of(1)));
  }

  @Test
  void nullDynamicKeyWritesAndRemovesNothing() {
    // Documented in SetClauseApplier.applyPropertySet and RemoveStep.applyRemoveOperations ("a null key is a no-op"). Neo4j raises
    // a type error for it instead: this is a deliberate ArcadeDB difference, not an oracle result
    statsOf("CREATE (:T {id:1, y:1})");
    final Map<String, Object> params = new HashMap<>();
    params.put("k", null);

    assertThat(statsOf("MATCH (n:T {id:1}) SET n[$k] = 3", params).getPropertiesSet()).isEqualTo(0);
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE n[$k]", params).getPropertiesSet()).isEqualTo(0);
    assertThat(statsOf("MERGE (n:T {id:1}) ON MATCH SET n[$k] = 3", params).getPropertiesSet()).isEqualTo(0);
    assertThat(propertiesOf("MATCH (n:T {id:1}) RETURN properties(n) AS p")).isEqualTo(Map.of("id", 1L, "y", 1L));
  }

  @Test
  void removeThroughACaseExpressionTargetOnADeletedNodeIsRefusedAndTheDeleteRolledBack() {
    // Issue #5795 refuses a write through a node deleted earlier in the statement (Neo4j accepts it); the expression target of a
    // REMOVE resolves through RemoveStep.resolveTarget, a path the plain-variable REMOVE test does not reach
    statsOf("CREATE (:T {id:1, v:0})");

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T {id:1}) DELETE t WITH t REMOVE (CASE WHEN true THEN t END).v")))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("DeletedEntityAccess");

    assertThat(((Number) valueOf("MATCH (t:T {id:1}) RETURN t.v AS v", "v")).longValue()).isEqualTo(0L);
  }

  @Test
  void removeAndSetThroughAnExpressionTargetCountAsPropertyWritesLikeNeo4j() {
    statsOf("CREATE (:T {id:1, q:2})");

    assertThat(statsOf("MATCH (n:T {id:1}) SET (CASE WHEN n.q = 2 THEN n END).r = 1").getPropertiesSet()).isEqualTo(1);
    // the CASE yields null: the clause writes nothing
    assertThat(statsOf("MATCH (n:T {id:1}) SET (CASE WHEN n.q = 99 THEN n END).r = 2").getPropertiesSet()).isEqualTo(0);
    assertThat(((Number) valueOf("MATCH (n:T {id:1}) RETURN n.r AS r", "r")).longValue()).isEqualTo(1L);
    assertThat(statsOf("MATCH (n:T {id:1}) REMOVE (CASE WHEN n.q = 2 THEN n END).r").getPropertiesSet()).isEqualTo(1);
    assertThat(valueOf("MATCH (n:T {id:1}) RETURN n.r AS r", "r")).isNull();
  }
}
