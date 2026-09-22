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
package com.arcadedb.query.opencypher.procedures.merge;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8103: APOC declares {@code apoc.merge.relationship(startNode, relationshipType, identProps,
 * onCreateProps, endNode, onMatchProps = {})}, six parameters, while ArcadeDB accepted exactly five - the
 * six-argument form was rejected with {@code Procedure 'merge.relationship' expects 5 arguments but got 6},
 * so there was no way to say "set these properties when the relationship already existed".
 * <p>
 * {@code onMatchProps} is applied on the match branch only, mirroring how {@code onCreateProps} is applied on
 * the create branch.
 */
class MergeRelationshipOnMatchPropsTest {
  /** Exactly one (a, b) pair, so every CALL below merges against the same relationship. */
  private static final String MATCH_PAIR = "MATCH (a:A), (b:B)\n";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/merge-relationship-on-match-props-8103").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:B {id: 2})").close());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The headline of the issue: the second call finds the relationship the first one created and applies
   * {@code onMatchProps} to it, without creating a second edge.
   */
  @Test
  void onMatchPropsIsAppliedToTheRelationshipThatAlreadyExisted() {
    merge("{}", "{created: true}", null);
    merge("{}", "{created: true}", "{seen: 'again'}");

    assertThat(edgeCount()).isEqualTo(1);
    assertThat(edgeProperty("seen")).isEqualTo("again");
    assertThat(edgeProperty("created")).isEqualTo(true);
  }

  /** APOC applies {@code onMatchProps} only when the relationship already existed. The create branch ignores it. */
  @Test
  void onMatchPropsIsIgnoredOnTheCreateBranch() {
    merge("{}", "{created: true}", "{seen: 'again'}");

    assertThat(edgeCount()).isEqualTo(1);
    assertThat(edgeProperty("created")).isEqualTo(true);
    assertThat(edgeProperty("seen")).isNull();
  }

  /** On the match branch {@code onMatchProps} wins over whatever the create branch had written. */
  @Test
  void onMatchPropsOverwritesAValueTheCreateBranchHadSet() {
    merge("{}", "{state: 'new'}", null);
    assertThat(edgeProperty("state")).isEqualTo("new");

    merge("{}", "{state: 'new'}", "{state: 'seen'}");

    assertThat(edgeCount()).isEqualTo(1);
    assertThat(edgeProperty("state")).isEqualTo("seen");
  }

  /** An empty or omitted {@code onMatchProps} leaves the matched relationship exactly as it was. */
  @Test
  void anEmptyOnMatchPropsLeavesTheMatchedRelationshipUntouched() {
    merge("{}", "{state: 'new'}", null);

    merge("{}", "{state: 'ignored-on-match'}", "{}");
    assertThat(edgeProperty("state")).isEqualTo("new");

    merge("{}", "{state: 'ignored-on-match'}", null);
    assertThat(edgeProperty("state")).isEqualTo("new");
    assertThat(edgeCount()).isEqualTo(1);
  }

  /** The identProps still select the relationship: a different match map merges a separate edge. */
  @Test
  void onMatchPropsDoesNotDisturbTheIdentPropsLookup() {
    merge("{kind: 'x'}", "{}", "{seen: 1}");
    merge("{kind: 'y'}", "{}", "{seen: 1}");
    assertThat(edgeCount()).isEqualTo(2);

    merge("{kind: 'x'}", "{}", "{seen: 2}");
    assertThat(edgeCount()).as("the third call matched the 'x' edge rather than creating one").isEqualTo(2);

    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH ()-[r:KNOWS]->() WHERE r.kind = 'x' RETURN r.seen AS seen")) {
      assertThat(((Number) resultSet.next().getProperty("seen")).intValue()).isEqualTo(2);
    }
  }

  /** The {@code apoc.}-prefixed alias resolves to the same procedure, so it takes the six-argument form too. */
  @Test
  void theApocPrefixedNameTakesTheSixArgumentForm() {
    database.command("opencypher", MATCH_PAIR
        + "CALL apoc.merge.relationship(a, 'KNOWS', {}, {}, b, {}) YIELD rel RETURN rel").close();
    database.command("opencypher", MATCH_PAIR
        + "CALL apoc.merge.relationship(a, 'KNOWS', {}, {}, b, {seen: true}) YIELD rel RETURN rel").close();

    assertThat(edgeCount()).isEqualTo(1);
    assertThat(edgeProperty("seen")).isEqualTo(true);
  }

  /** The row the CALL yields carries the property {@code onMatchProps} just set, not a pre-update snapshot. */
  @Test
  void theYieldedRelationshipCarriesTheOnMatchProperties() {
    merge("{}", "{}", null);

    try (final ResultSet resultSet = database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'KNOWS', {}, {}, b, {seen: 'now'}) YIELD rel RETURN rel.seen AS seen")) {
      assertThat((String) resultSet.next().getProperty("seen")).isEqualTo("now");
    }
  }

  /** {@code onMatchProps} is a map, and a non-map is the caller's mistake rather than a silently ignored argument. */
  @Test
  void onMatchPropsMustBeAMap() {
    merge("{}", "{}", null);

    assertThatThrownBy(() -> database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'KNOWS', {}, {}, b, 'not-a-map') YIELD rel RETURN rel").close())
        .hasStackTraceContaining("onMatchProps");
  }

  /**
   * An edge of a {@code LIGHTWEIGHT} type has no record, so there is nothing for {@code onMatchProps} to write to.
   * The call has to say so - naming the procedure and the argument - rather than fail with the bare
   * "Lightweight edges cannot be modified" that {@code ImmutableLightEdge.modify()} raises, and rather than
   * silently drop the properties the caller asked for.
   */
  @Test
  void onMatchPropsOnALightweightEdgeTypeIsRefusedByName() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("FOLLOWS").withLightweight(true).create());

    database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'FOLLOWS', {}, {}, b) YIELD rel RETURN rel").close();

    assertThatThrownBy(() -> database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'FOLLOWS', {}, {}, b, {seen: true}) YIELD rel RETURN rel").close())
        .hasStackTraceContaining("LIGHTWEIGHT")
        .hasStackTraceContaining("onMatchProps");
  }

  /** The same type with an empty onMatchProps writes nothing, so it still merges. */
  @Test
  void anEmptyOnMatchPropsIsFineOnALightweightEdgeType() {
    database.transaction(
        () -> database.getSchema().buildEdgeType().withName("FOLLOWS").withLightweight(true).create());

    database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'FOLLOWS', {}, {}, b, {}) YIELD rel RETURN rel").close();
    database.command("opencypher", MATCH_PAIR
        + "CALL merge.relationship(a, 'FOLLOWS', {}, {}, b, {}) YIELD rel RETURN rel").close();

    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH ()-[r:FOLLOWS]->() RETURN count(*) AS c")) {
      assertThat(((Number) resultSet.next().getProperty("c")).longValue()).isEqualTo(1);
    }
  }

  /**
   * The direct-caller entry point: {@code execute()} is public, so the gate every caller passes through is
   * {@code validateArgs}. It has to accept 5 and 6 arguments and keep rejecting 4 and 7.
   */
  @Test
  void arityGateAcceptsFiveOrSixArgumentsAndStillRejectsTheRest() {
    final CypherProcedure procedure = CypherProcedureRegistry.get("apoc.merge.relationship");

    assertThat(procedure.getMinArgs()).isEqualTo(5);
    assertThat(procedure.getMaxArgs()).isEqualTo(6);

    assertThatCode(() -> procedure.validateArgs(new Object[5])).doesNotThrowAnyException();
    assertThatCode(() -> procedure.validateArgs(new Object[6])).doesNotThrowAnyException();

    assertThatThrownBy(() -> procedure.validateArgs(new Object[4]))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 5-6 arguments but got 4");
    assertThatThrownBy(() -> procedure.validateArgs(new Object[7]))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 5-6 arguments but got 7");
  }

  private void merge(final String identProps, final String onCreateProps, final String onMatchProps) {
    final String call = onMatchProps == null ?
        "CALL merge.relationship(a, 'KNOWS', " + identProps + ", " + onCreateProps + ", b)" :
        "CALL merge.relationship(a, 'KNOWS', " + identProps + ", " + onCreateProps + ", b, " + onMatchProps + ")";
    database.command("opencypher", MATCH_PAIR + call + " YIELD rel RETURN rel").close();
  }

  private long edgeCount() {
    try (final ResultSet resultSet = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(*) AS c")) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }

  private Object edgeProperty(final String name) {
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH ()-[r:KNOWS]->() RETURN r." + name + " AS v")) {
      return resultSet.next().getProperty("v");
    }
  }
}
