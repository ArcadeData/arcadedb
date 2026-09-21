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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the apoc.refactor.mergeNodes Cypher procedure (registered as "refactor.mergeNodes").
 * <p>
 * The procedure mutates the graph, so every CALL runs inside an explicit transaction that is fully
 * consumed and committed before any assertion - CypherProcedure execution is lazy (it runs on the
 * first {@code ResultSet.next()} pull) and is not auto-committed by the engine the way built-in write
 * clauses like SET/DELETE/CREATE are, so leaving it for an assertion to trigger would both run it
 * outside the transaction and, on assertion failure, leave the transaction open for teardown.
 * </p>
 */
class RefactorMergeNodesTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-refactor-merge-nodes");
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
    assertThat(CypherProcedureRegistry.hasProcedure("refactor.mergeNodes")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.refactor.mergeNodes")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.refactor.mergeNodes")).isSameAs(CypherProcedureRegistry.get("refactor.mergeNodes"));
  }

  @Test
  void survivorIsTheFirstNodeAndAbsorbedNodeIsDeleted() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    database.commit();
    final String aId = a.getIdentity().toString();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CALL apoc.refactor.mergeNodes([a,b], {}) YIELD node RETURN node");
    final Result result = rs.next();
    final String survivorId = result.getVertex().get().getIdentity().toString();
    database.commit();

    assertThat(survivorId).isEqualTo(aId);
    assertThatThrownBy(() -> database.lookupByRID(b.getIdentity(), true))
        .isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  void overwritePolicyLetsAbsorbedValueWin() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("age", 30L).save();
    database.newVertex("Person").set("name", "B").set("age", 25L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'overwrite'}) YIELD node RETURN node.age AS age");
    final Long age = rs.next().getProperty("age");
    database.commit();

    assertThat(age).isEqualTo(25L);
  }

  @Test
  void discardPolicyKeepsSurvivorsOriginalValue() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("age", 30L).save();
    database.newVertex("Person").set("name", "B").set("age", 25L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'discard'}) YIELD node RETURN node.age AS age");
    final Long age = rs.next().getProperty("age");
    database.commit();

    assertThat(age).isEqualTo(30L);
  }

  @Test
  void combinePolicyProducesListOfBothValues() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "x").save();
    database.newVertex("Person").set("name", "B").set("tag", "y").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(java.util.List.of("x", "y"));
  }

  @Test
  void unknownPropertiesPolicyThrows() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.newVertex("Person").set("name", "B").save();
    database.commit();

    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'bogus'}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void combinePolicyAccumulatesAcrossMultipleAbsorbedNodes() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "x").save();
    database.newVertex("Person").set("name", "B").set("tag", "y").save();
    database.newVertex("Person").set("name", "C").set("tag", "z").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}) "
            + "CALL apoc.refactor.mergeNodes([a,b,c], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(java.util.List.of("x", "y", "z"));
  }

  @Test
  void edgesFromAbsorbedNodeAreRewiredToSurvivor() {
    database.begin();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    database.newVertex("Person").set("name", "A").save();
    final MutableVertex c = database.newVertex("Person").set("name", "C").save();
    b.newEdge("KNOWS", c, "since", 2020L).save();
    database.commit();

    // properties: 'discard' keeps the survivor's own 'name' (both nodes carry that property, and
    // the default 'overwrite' policy would otherwise rename the survivor to 'B' along with everything
    // else absorbed from it) so the post-merge node stays reachable by MATCH {name:'A'}.
    database.begin();
    database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'discard'}) YIELD node RETURN node").next();
    database.commit();

    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(c:Person {name:'C'}) RETURN r.since AS since");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("since")).isEqualTo(2020L);
  }

  @Test
  void edgeBetweenMergedNodesBecomesSelfRelationshipOnSurvivor() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("KNOWS", b, "since", 2020L).save();
    database.commit();

    database.begin();
    database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'discard'}) YIELD node RETURN node").next();
    database.commit();

    final ResultSet rs = database.query("opencypher", "MATCH (a:Person {name:'A'})-[r:KNOWS]->(a) RETURN r");
    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void fewerThanTwoNodesThrows() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.commit();

    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.mergeNodes([a], {}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void duplicateAbsorbedNodeInTheListIsDeduplicatedNotCrashed() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    database.commit();
    final String aId = a.getIdentity().toString();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CALL apoc.refactor.mergeNodes([a,b,b], {}) YIELD node RETURN node");
    final String survivorId = rs.next().getVertex().get().getIdentity().toString();
    database.commit();

    assertThat(survivorId).isEqualTo(aId);
    assertThatThrownBy(() -> database.lookupByRID(b.getIdentity(), true))
        .isInstanceOf(RecordNotFoundException.class);
  }

  @Test
  void nodesCollapsingToFewerThanTwoAfterDeduplicationThrows() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.commit();

    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.mergeNodes([a,a], {}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void threeWayMergeRewiresEdgesFromEveryAbsorbedNode() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex c = database.newVertex("Person").set("name", "C").save();
    final MutableVertex d = database.newVertex("Person").set("name", "D").save();
    b.newEdge("KNOWS", d, "since", 2020L).save();
    c.newEdge("KNOWS", d, "since", 2021L).save();
    database.commit();

    // properties: 'discard' keeps the survivor's own 'name' - see edgesFromAbsorbedNodeAreRewiredToSurvivor
    // above for why the default 'overwrite' policy would otherwise break the post-merge MATCH {name:'A'}.
    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}) "
            + "CALL apoc.refactor.mergeNodes([a,b,c], {properties: 'discard'}) YIELD node RETURN node");
    final String survivorId = rs.next().getVertex().get().getIdentity().toString();
    database.commit();

    assertThat(survivorId).isEqualTo(a.getIdentity().toString());
    assertThatThrownBy(() -> database.lookupByRID(b.getIdentity(), true)).isInstanceOf(RecordNotFoundException.class);
    assertThatThrownBy(() -> database.lookupByRID(c.getIdentity(), true)).isInstanceOf(RecordNotFoundException.class);

    final ResultSet rewiredEdges = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(d:Person {name:'D'}) RETURN r.since AS since ORDER BY r.since");
    assertThat(rewiredEdges.hasNext()).isTrue();
    assertThat(rewiredEdges.next().<Long>getProperty("since")).isEqualTo(2020L);
    assertThat(rewiredEdges.hasNext()).isTrue();
    assertThat(rewiredEdges.next().<Long>getProperty("since")).isEqualTo(2021L);
    assertThat(rewiredEdges.hasNext()).isFalse();
  }

  /**
   * Issue #7427: APOC declares {@code apoc.refactor.mergeNodes(nodes :: LIST<NODE>, config = {} :: MAP)}. Omitting
   * the config must merge with the documented default property policy ({@code overwrite}), not be rejected.
   */
  @Test
  void mergesNodesWithTheDefaultPolicyWhenTheConfigArgumentIsOmitted() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").set("age", 30L).save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").set("age", 25L).save();
    database.commit();
    final String aId = a.getIdentity().toString();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CALL apoc.refactor.mergeNodes([a,b]) YIELD node "
            + "RETURN node");
    final Vertex survivor = rs.next().getVertex().get();
    final String survivorId = survivor.getIdentity().toString();
    final Long age = survivor.getLong("age");
    database.commit();

    assertThat(survivorId).isEqualTo(aId);
    // "overwrite" is the default policy, so the absorbed node's value wins - exactly as with an explicit {}
    assertThat(age).isEqualTo(25L);
    assertThatThrownBy(() -> database.lookupByRID(b.getIdentity(), true)).isInstanceOf(RecordNotFoundException.class);
  }

  /**
   * Issue #7427, the direct-caller entry point: {@code execute()} is public, so the arity gate every caller passes
   * through is {@code validateArgs}. It has to accept 1 and 2 arguments and keep rejecting 0 and 3.
   */
  @Test
  void arityGateAcceptsOneOrTwoArgumentsAndStillRejectsTheRest() {
    final CypherProcedure procedure = CypherProcedureRegistry.get("apoc.refactor.mergeNodes");

    assertThat(procedure.getMinArgs()).isEqualTo(1);
    assertThat(procedure.getMaxArgs()).isEqualTo(2);

    assertThatCode(() -> procedure.validateArgs(new Object[] { List.of() })).doesNotThrowAnyException();
    assertThatCode(() -> procedure.validateArgs(new Object[] { List.of(), Map.of() })).doesNotThrowAnyException();

    assertThatThrownBy(() -> procedure.validateArgs(new Object[] {}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 1-2 arguments but got 0");
    assertThatThrownBy(() -> procedure.validateArgs(new Object[] { List.of(), Map.of(), Map.of() }))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 1-2 arguments but got 3");
  }
}
