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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8100: a label carrying {@link Labels#LABEL_SEPARATOR} used to be accepted on the write paths that turn a
 * label into a <i>vertex type</i>, and the separator is precisely the character that encodes the boundary between
 * the labels of a composite type.
 * <p>
 * Accepting it breaks three things at once, all reproduced in the issue: {@code [A~B, C]} and {@code [A, B~C]}
 * both name the type {@code A~B~C}, so the second write silently matched the node the first one created; the
 * pre-existing composite then gained {@code A~B} and {@code B~C} as further supertypes, so the type encoded four
 * labels instead of two; and {@code labels(n)} started reporting the synthetic composite name itself, which
 * {@link Labels#getLabels}'s contract says never leaves that method, because the type no longer matched the sorted
 * join of its own supertypes.
 * <p>
 * {@code Labels.appendDynamicLabels} already refused such a label, but only for the Cypher 25 dynamic label
 * expressions {@code SET n:$(expr)} / {@code REMOVE n:$(expr)}. Every other write path reached
 * {@link Labels#ensureCompositeType} without passing that guard, so the check now lives in
 * {@code ensureCompositeType} itself - the one method all of them create their type through - and this test drives
 * it once per entry point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelSeparatorValidationIssue8100Test {
  private static final String MERGE_NODE = "CALL merge.node($labels, $match, {}) YIELD node RETURN labels(node) AS l";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-separator-8100");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // The reported entry point: the merge.node procedure
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The issue's own repro. Both calls used to succeed, both landed on {@code A~B~C}, and the second one returned
   * the node the first one had created.
   */
  @Test
  void mergeNodeRefusesALabelCarryingTheSeparator() {
    assertThatThrownBy(() -> mergeNode(List.of("A~B", "C"), Map.of("id", 1)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("A~B")
        .hasMessageContaining(Labels.LABEL_SEPARATOR);

    assertThatThrownBy(() -> mergeNode(List.of("A", "B~C"), Map.of("id", 1)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("B~C");

    assertThat(typeNames())
        .as("a refused merge.node must not have created any type at all")
        .doesNotContain("A~B", "B~C", "A~B~C");
  }

  /** A single label is the same hazard: {@code A~B} alone collides with the composite of {@code A} and {@code B}. */
  @Test
  void mergeNodeRefusesASingleLabelCarryingTheSeparator() {
    assertThatThrownBy(() -> mergeNode(List.of("A~B"), Map.of("id", 1)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("A~B");

    assertThat(typeNames()).doesNotContain("A~B");
  }

  /**
   * {@code extractLabels} used to call {@code item.toString()} on whatever the caller passed, so a number became a
   * type name - the same coercion {@code appendDynamicLabels} has refused since #7059.
   */
  @Test
  void mergeNodeRefusesANonStringLabel() {
    assertThatThrownBy(() -> mergeNode(List.of(1), Map.of("id", 1)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("merge.node");

    assertThat(typeNames()).doesNotContain("1");
  }

  /** A blank label is not a usable type name either, and used to reach the schema as one. */
  @Test
  void mergeNodeRefusesABlankLabel() {
    assertThatThrownBy(() -> mergeNode(List.of("  "), Map.of("id", 1)))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("merge.node");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The sibling entry points the completeness sweep found: every other path that turns a label into a type
  // ---------------------------------------------------------------------------------------------------------

  /** {@code CREATE (n:`A~B`)} parses - the back-ticks make it a legal identifier - and used to create the type. */
  @Test
  void createRefusesALabelCarryingTheSeparator() {
    assertThatThrownBy(() -> command("CREATE (n:`A~B` {id: 1})"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("A~B");

    assertThat(typeNames()).doesNotContain("A~B");
  }

  /** The same pattern on MERGE, which has its own copy of the create path. */
  @Test
  void mergeRefusesALabelCarryingTheSeparator() {
    assertThatThrownBy(() -> command("MERGE (n:`C~D` {id: 1})"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("C~D");

    assertThat(typeNames()).doesNotContain("C~D");
  }

  /**
   * {@code SET n:`E~F`} on a node that already has a label is the worst of the three: it built the composite
   * {@code E~F~Z}, which is also the name the ordinary label set {@code [E, F, Z]} computes.
   */
  @Test
  void setRefusesALabelCarryingTheSeparator() {
    command("CREATE (n:Z {id: 1})");

    assertThatThrownBy(() -> command("MATCH (n:Z {id: 1}) SET n:`E~F`"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("E~F");

    assertThat(typeNames()).doesNotContain("E~F", "E~F~Z");
    assertThat(labelsOf(1)).containsExactly("Z");
  }

  /**
   * The guard #7059 added for {@code SET n:$(expr)} still fires. This PR moves the separator check into a helper
   * both paths call, so the dynamic path is the regression counterweight for that move.
   */
  @Test
  void aDynamicSetLabelStillRefusesTheSeparator() {
    command("CREATE (n:Z {id: 1})");

    assertThatThrownBy(() -> command("MATCH (n:Z {id: 1}) SET n:$('G~H')"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("G~H");

    assertThat(typeNames()).doesNotContain("G~H");
  }

  /**
   * {@code CREATE INDEX} / {@code CREATE CONSTRAINT} auto-create the type they name, and do it directly rather
   * than through {@code ensureCompositeType}, so they are the one write path the central guard does not reach.
   */
  @Test
  void createIndexRefusesToAutoCreateALabelCarryingTheSeparator() {
    assertThatThrownBy(() -> command("CREATE INDEX FOR (n:`A~B`) ON (n.id)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("A~B");

    assertThat(typeNames()).doesNotContain("A~B");
  }

  @Test
  void createConstraintRefusesToAutoCreateALabelCarryingTheSeparator() {
    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:`C~D`) REQUIRE n.id IS UNIQUE"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("C~D");

    assertThat(typeNames()).doesNotContain("C~D");
  }

  /**
   * The counterweight to the two above: a composite type that genuinely exists is named with the separator by
   * construction, and indexing it is the legitimate use of such a name. The guard sits inside the auto-create
   * branch so this keeps working.
   */
  @Test
  void anIndexOnAnExistingCompositeTypeIsStillAccepted() {
    command("CREATE (n:A:C {id: 1})");
    assertThat(typeNames()).contains("A~C");

    command("CREATE INDEX FOR (n:`A~C`) ON (n.id)");

    assertThat(rows("MATCH (n:A) RETURN n.id AS r")).containsExactly(1);
  }

  /**
   * Raised by CodeRabbit on this PR: {@code TypeBuilder.create()} refuses an empty type name but not a
   * whitespace-only one, so such a type can exist in a database written by SQL or the Java API. The DDL
   * auto-create therefore checks for a blank label before it checks whether the type exists - otherwise indexing
   * it would be the one way left to name a label no other openCypher path accepts.
   */
  @Test
  void createIndexRefusesABlankLabelEvenWhenThatTypeAlreadyExists() {
    database.getSchema().createVertexType("  ");
    assertThat(typeNames()).contains("  ");

    assertThatThrownBy(() -> command("CREATE INDEX FOR (n:`  `) ON (n.id)"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("blank");
  }

  // ---------------------------------------------------------------------------------------------------------
  // What must keep working
  // ---------------------------------------------------------------------------------------------------------

  /** The composite machinery itself is untouched: two ordinary labels still merge onto one node and one type. */
  @Test
  void anOrdinaryCompositeStillMergesOntoTheSameNode() {
    assertThat(mergeNode(List.of("A", "C"), Map.of("id", 1))).containsExactly("A", "C");
    assertThat(mergeNode(List.of("A", "C"), Map.of("id", 1))).containsExactly("A", "C");

    assertThat(rows("MATCH (n:A) RETURN n.id AS r")).containsExactly(1);
    assertThat(typeNames()).contains("A~C");
  }

  /** And the ordinary Cypher write paths still create, merge and relabel with plain labels. */
  @Test
  void ordinaryLabelsStillWriteThroughEveryPath() {
    command("CREATE (n:P {id: 1})");
    command("MERGE (n:Q {id: 2})");
    command("MATCH (n:P {id: 1}) SET n:R");

    assertThat(labelsOf(1)).containsExactly("P", "R");
    assertThat(labelsOf(2)).containsExactly("Q");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The deliberate behaviour change, on data that already carries such a label
  // ---------------------------------------------------------------------------------------------------------

  /**
   * The boundary of the fix, and the reason it validates introduced labels rather than the resulting label set.
   * A type created outside openCypher under a name carrying the separator - {@code CREATE VERTEX TYPE `a~b`} in
   * SQL - is a label a vertex legitimately already answers to, and #6363 requires that openCypher keep reading and
   * relabelling such a vertex. Validating inside {@code ensureCompositeType}, which is handed the resulting set,
   * broke exactly that, so the guard moved to the points a statement introduces a label.
   */
  @Test
  void relabellingANodeThatAlreadyCarriesASeparatorLabelStillWorks() {
    database.getSchema().createVertexType("E~F");
    database.transaction(() -> database.newVertex("E~F").set("id", 1).save());

    command("MATCH (n {id: 1}) SET n:Z");

    assertThat(labelsOf(1)).containsExactly("E~F", "Z");
  }

  /** Introducing such a label in the same SET is still refused, even on that vertex. */
  @Test
  void introducingASeparatorLabelOnSuchANodeIsStillRefused() {
    database.getSchema().createVertexType("E~F");
    database.transaction(() -> database.newVertex("E~F").set("id", 1).save());

    assertThatThrownBy(() -> command("MATCH (n {id: 1}) SET n:`G~H`"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("G~H");

    assertThat(typeNames()).doesNotContain("G~H");
  }

  /** Removing the offending label is still possible, which is what makes the data recoverable in place. */
  @Test
  void removingASeparatorLabelFromSuchANodeStillWorks() {
    database.getSchema().createVertexType("E~F");
    database.transaction(() -> database.newVertex("E~F").set("id", 1).save());

    command("MATCH (n {id: 1}) REMOVE n:`E~F`");

    assertThat(labelsOf(1)).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private List<Object> mergeNode(final List<?> labels, final Map<String, Object> matchProps) {
    try (final ResultSet resultSet = database.command("opencypher", MERGE_NODE,
        Map.of("labels", labels, "match", matchProps))) {
      return (List<Object>) resultSet.next().getProperty("l");
    }
  }

  private void command(final String query) {
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    });
  }

  private List<Object> rows(final String query) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty("r"));
    }
    return values;
  }

  @SuppressWarnings("unchecked")
  private List<Object> labelsOf(final Object id) {
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (n {id: $v}) RETURN labels(n) AS l", Map.of("v", id))) {
      final Result result = resultSet.next();
      return (List<Object>) result.getProperty("l");
    }
  }

  private List<Object> typeNames() {
    final List<Object> names = new ArrayList<>();
    for (final var type : database.getSchema().getTypes())
      names.add(type.getName());
    return names;
  }
}
