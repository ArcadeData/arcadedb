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

  /**
   * Issue #7428: APOC's contract for the 'combine' strategy is "if the values are the same, keep one;
   * otherwise merge into a list". Two nodes that agree on a property must therefore leave the survivor
   * with the scalar it already had, not with a two-element list of duplicates.
   */
  @Test
  void combinePolicyCollapsesTwoEqualScalarValuesToTheScalar() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "Review").save();
    database.newVertex("Person").set("name", "B").set("tag", "Review").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo("Review");
  }

  /**
   * Issue #7428, verbatim reporter shape: the node list arrives from collect() rather than as a literal,
   * and the config map carries an extra key the procedure does not read.
   */
  @Test
  void combinePolicyCollapsesEqualValuesWhenTheNodeListComesFromCollect() {
    database.begin();
    database.newVertex("Person").set("name", "Review").save();
    database.newVertex("Person").set("name", "Review").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (s:Person {name:'Review'}) WITH collect(s) AS nodes "
            + "CALL apoc.refactor.mergeNodes(nodes, {properties:'combine', mergeRels:true}) YIELD node RETURN node.name AS name");
    final Object name = rs.next().getProperty("name");
    database.commit();

    assertThat(name).isEqualTo("Review");
  }

  /**
   * Three absorbed nodes that all agree: the collapse has to survive every iteration of the merge loop,
   * not just the first, so the survivor never grows a list at all.
   */
  @Test
  void combinePolicyCollapsesThreeEqualScalarValuesToTheScalar() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "x").save();
    database.newVertex("Person").set("name", "B").set("tag", "x").save();
    database.newVertex("Person").set("name", "C").set("tag", "x").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}) "
            + "CALL apoc.refactor.mergeNodes([a,b,c], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo("x");
  }

  /**
   * The survivor already holds the list an earlier iteration accumulated, so the repeated value has to be
   * matched against that list - and the first-seen order is what survives.
   */
  @Test
  void combinePolicySkipsAValueTheAccumulatedListAlreadyHolds() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "x").save();
    database.newVertex("Person").set("name", "B").set("tag", "y").save();
    database.newVertex("Person").set("name", "C").set("tag", "x").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}) "
            + "CALL apoc.refactor.mergeNodes([a,b,c], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(List.of("x", "y"));
  }

  /** The survivor's property is a list the user stored, and the absorbed scalar is already one of its elements. */
  @Test
  void combinePolicySkipsAnAbsorbedScalarAlreadyInTheSurvivorList() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", List.of("x", "y")).save();
    database.newVertex("Person").set("name", "B").set("tag", "x").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(List.of("x", "y"));
  }

  /** The absorbed node's property is itself a list that overlaps the survivor's scalar. */
  @Test
  void combinePolicySkipsAbsorbedListElementsAlreadyPresent() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", "x").save();
    database.newVertex("Person").set("name", "B").set("tag", List.of("x", "z")).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(List.of("x", "z"));
  }

  /**
   * Deliberate consequence of the APOC contract, pinned here so it is a decision and not a surprise: when the
   * merge leaves exactly one distinct value the property becomes that value, even where both contributions
   * were single-element lists.
   */
  @Test
  void combinePolicyCollapsesEqualSingleElementListsToTheScalar() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", List.of("x")).save();
    database.newVertex("Person").set("name", "B").set("tag", List.of("x")).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo("x");
  }

  /**
   * A property only the absorbed node carries never reaches the combine branch: it is copied across verbatim,
   * so a list stays the list it was and is not collapsed by the new de-duplication.
   */
  @Test
  void combinePolicyCopiesAPropertyOnlyTheAbsorbedNodeCarriesVerbatim() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.newVertex("Person").set("name", "B").set("tag", List.of("x")).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(List.of("x"));
  }

  /**
   * A property both nodes carry as null reaches the combine branch like any other: one distinct value, so the
   * survivor keeps the null rather than the two-element list of nulls the branch used to build. Pinned because
   * the de-duplication's membership test has to stay null-safe and the collapsed null has to survive save().
   */
  @Test
  void combinePolicyCollapsesAPropertyBothNodesCarryAsNull() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", null).save();
    database.newVertex("Person").set("name", "B").set("tag", null).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isNull();
  }

  /**
   * An empty list contributes no values, so the merge sees exactly one distinct value and the survivor ends up
   * with the scalar rather than with a one-element list. Raised in review on #7428 as an untested corner of the
   * "distinct values seen" semantics; pinned here so the answer is on the record either way.
   */
  @Test
  void combinePolicyTreatsAnEmptyListAsContributingNothing() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", List.of()).save();
    database.newVertex("Person").set("name", "B").set("tag", "x").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo("x");
  }

  /**
   * Both sides empty leaves no distinct value at all, so the property stays the empty list it was - the collapse to
   * a scalar applies to exactly one distinct value, not to fewer.
   */
  @Test
  void combinePolicyLeavesTwoEmptyListsAsAnEmptyList() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("tag", List.of()).save();
    database.newVertex("Person").set("name", "B").set("tag", List.of()).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.tag AS tag");
    final Object tag = rs.next().getProperty("tag");
    database.commit();

    assertThat(tag).isEqualTo(List.of());
  }

  /**
   * Issue #8099, follow-up to #7428: a property whose value is a Java array - the shape a vector embedding takes -
   * is not a {@code List}, so the de-duplication above never saw it; array {@code equals} is identity, so two
   * nodes carrying the "same" embedding as two distinct {@code float[]} instances used to produce a two-element
   * list of duplicates instead of collapsing like every other equal contribution does.
   */
  @Test
  void combinePolicyCollapsesEqualFloatArraysToTheArray() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("vec", new float[] { 1f, 2f }).save();
    database.newVertex("Person").set("name", "B").set("vec", new float[] { 1f, 2f }).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.vec AS vec");
    final Object vec = rs.next().getProperty("vec");
    database.commit();

    assertThat(vec).isInstanceOf(float[].class);
    assertThat((float[]) vec).containsExactly(1f, 2f);
  }

  /**
   * Two genuinely different arrays are kept as two separate entries, each the single array it was - not
   * flattened element-by-element the way a {@code List} property is, since concatenating two embeddings would
   * not be a merge a caller could make sense of.
   */
  @Test
  void combinePolicyKeepsDistinctFloatArraysAsSeparateListElements() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("vec", new float[] { 1f, 2f }).save();
    database.newVertex("Person").set("name", "B").set("vec", new float[] { 3f, 4f }).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.vec AS vec");
    final Object vec = rs.next().getProperty("vec");
    database.commit();

    assertThat(vec).isInstanceOf(List.class);
    final List<?> combined = (List<?>) vec;
    assertThat(combined).hasSize(2);
    assertThat((float[]) combined.get(0)).containsExactly(1f, 2f);
    assertThat((float[]) combined.get(1)).containsExactly(3f, 4f);
  }

  /** Same content-equality collapse, on a different element type, to pin that the fix is not float[]-specific. */
  @Test
  void combinePolicyCollapsesEqualIntArraysToTheArray() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("vec", new int[] { 1, 2, 3 }).save();
    database.newVertex("Person").set("name", "B").set("vec", new int[] { 1, 2, 3 }).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.mergeNodes([a,b], {properties: 'combine'}) YIELD node RETURN node.vec AS vec");
    final Object vec = rs.next().getProperty("vec");
    database.commit();

    assertThat(vec).isInstanceOf(int[].class);
    assertThat((int[]) vec).containsExactly(1, 2, 3);
  }
}
