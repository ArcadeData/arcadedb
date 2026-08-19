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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6395: a node whose only label is literally {@code V} or {@code Vertex} used to report no labels, and a
 * label write on it dropped the label instead of keeping it.
 * <p>
 * The engine used those two names for two different things at once - the type a node lands in when it has no
 * labels at all, and an ordinary user label a query is free to write - and {@code Labels.getLabels} could only
 * tell them apart by name, so it filtered the name and the genuine label disappeared with it. Two directions were
 * weighed in the issue: narrow the special case (make {@code RemoveStep} strip to whatever {@code CreateStep}
 * uses, so there is at least only one unlabelled type), or delete it by reserving an internal name no user type
 * can collide with. This test pins the second: {@link Labels#NO_LABEL_TYPE} - {@code ~NO_LABEL~} - is not a name
 * any Cypher label can ever equal, because {@link Labels#LABEL_SEPARATOR} already cannot appear inside a single
 * label, so {@code V} and {@code Vertex} become ordinary labels and the special-casing in
 * {@code Labels.getLabels}/{@code getOwnLabels} is unconditional rather than narrowed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherReservedNoLabelSentinelIssue6395Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-no-label-sentinel-6395");
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
  // 1. The label is no longer lost on read
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aNodeWhoseOnlyLabelIsVReportsIt() {
    command("CREATE (:V {k:'onlyV'})");

    assertThat(labelsOf("k", "onlyV")).containsExactly("V");
    assertThat(scalar("MATCH (n:V) RETURN n.k AS r")).isEqualTo("onlyV");
    assertThat(scalar("MATCH (n) WHERE n:V RETURN n.k AS r")).isEqualTo("onlyV");
  }

  @Test
  void aNodeWhoseOnlyLabelIsVertexReportsIt() {
    command("CREATE (:Vertex {k:'onlyVertex'})");

    assertThat(labelsOf("k", "onlyVertex")).containsExactly("Vertex");
    assertThat(scalar("MATCH (n:Vertex) RETURN n.k AS r")).isEqualTo("onlyVertex");
  }

  @Test
  void aCompositeCarryingVStillReportsAllThreeLabels() {
    // The exact shape the openCypher TCK writes (CREATE (b:U:V:W:X:Y:Z)) and the case the issue explicitly says
    // must keep behaving exactly as it does today: V is not the WHOLE label set here, so this passed before the
    // fix and must go on passing after it.
    command("CREATE (:U:V:W {k:'composite'})");

    assertThat(labelsOf("k", "composite")).containsExactlyInAnyOrder("U", "V", "W");
  }

  // ---------------------------------------------------------------------------------------------------------
  // 2. Adding a label no longer removes one
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void addingALabelToAVOnlyNodeKeepsV() {
    command("CREATE (:V {k:'onlyV'})");

    command("MATCH (n {k:'onlyV'}) SET n:Extra");

    assertThat(labelsOf("k", "onlyV")).containsExactlyInAnyOrder("Extra", "V");
    assertThat(scalar("MATCH (n:V) RETURN count(n) AS r")).isEqualTo(1L);
    assertThat(scalar("MATCH (n:Extra) RETURN n.k AS r")).isEqualTo("onlyV");
  }

  // ---------------------------------------------------------------------------------------------------------
  // 3. There is one unlabelled type, not two - stripped-to-zero and created-unlabelled behave identically
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void aNodeStrippedOfItsLastLabelAndOneCreatedUnlabelledAnswerTheSamePredicates() {
    command("CREATE (:Foo {k:'stripped'}), ({k:'born-plain'})");
    command("MATCH (n:Foo) REMOVE n:Foo");

    assertThat(labelsOf("k", "stripped")).isEmpty();
    assertThat(labelsOf("k", "born-plain")).isEmpty();

    // Neither answers to :V or :Vertex - those are ordinary labels now, and nobody wrote them.
    assertThat(rows("MATCH (n:V) RETURN n.k AS r")).isEmpty();
    assertThat(rows("MATCH (n:Vertex) RETURN n.k AS r")).isEmpty();

    // Both are reached by the same unlabelled-node query, and nothing else is.
    assertThat(rows("MATCH (n) WHERE size(labels(n)) = 0 RETURN n.k AS r"))
        .containsExactlyInAnyOrder("stripped", "born-plain");
  }

  // ---------------------------------------------------------------------------------------------------------
  // The counterweight: a real label named V or Vertex is written and read like any other, on every path
  // ---------------------------------------------------------------------------------------------------------

  @Test
  void vAndVertexBehaveAsOrdinaryLabelsOnEveryPath() {
    command("CREATE (:V {k:'v1'}), (:Vertex {k:'vx1'}), (:Other {k:'o1'})");

    assertThat(rows("MATCH (n:V) RETURN n.k AS r")).containsExactly("v1");
    assertThat(rows("MATCH (n:Vertex) RETURN n.k AS r")).containsExactly("vx1");
    assertThat(rows("MATCH (n:V|Vertex) RETURN n.k AS r")).containsExactlyInAnyOrder("v1", "vx1");
    assertThat(rows("MATCH (n) WHERE n:Other RETURN n.k AS r")).containsExactly("o1");

    command("MATCH (n:V {k:'v1'}) REMOVE n:V");
    assertThat(labelsOf("k", "v1")).isEmpty();
    assertThat(rows("MATCH (n:V) RETURN n.k AS r")).isEmpty();
  }

  @Test
  void theSentinelItselfIsNotWritableAsALabelViaCreate() {
    // Found in code review: CREATE (:`~NO_LABEL~`) bypasses the composite-name protection entirely, because a
    // single label is never run through isLabelComposite - it lands directly on the sentinel type, which
    // isBaseVertexTypeName then filters, reopening the exact V/Vertex collision this class exists to close under
    // a name a query merely has to spell correctly instead of guess.
    assertThatThrownBy(() -> command("CREATE (:`~NO_LABEL~` {k:'x'})"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(Labels.NO_LABEL_TYPE);

    assertThat(rows("MATCH (n {k:'x'}) RETURN n.k AS r"))
        .as("the rejected CREATE must not have left a node behind")
        .isEmpty();
  }

  @Test
  void theSentinelItselfIsNotWritableAsALabelViaSet() {
    // A vertex already typed as the sentinel: SET-ing the same label it already carries is Cypher's ordinary
    // no-op (SET n:Existing on a vertex that already answers to Existing changes nothing), and instanceOf
    // correctly says an unlabelled vertex's own type already IS the sentinel - so this path never reaches
    // ensureCompositeType with a label list at all, and is the counterweight asserting that.
    command("CREATE (n {k:'already-unlabelled'})");
    command("MATCH (n {k:'already-unlabelled'}) SET n:`~NO_LABEL~`");
    assertThat(labelsOf("k", "already-unlabelled")).isEmpty();

    // A vertex that genuinely gains a new label: this is the path that must be rejected, since it is the one
    // that would otherwise build a composite naming the sentinel.
    command("CREATE (n:Foo {k:'x'})");

    assertThatThrownBy(() -> command("MATCH (n:Foo {k:'x'}) SET n:`~NO_LABEL~`"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(Labels.NO_LABEL_TYPE);

    // The rejected SET must not have relabelled the node either.
    assertThat(labelsOf("k", "x")).containsExactly("Foo");
  }

  @Test
  void theSentinelItselfIsNotWritableAsALabelViaMerge() {
    assertThatThrownBy(() -> command("MERGE (n:`~NO_LABEL~` {k:'x'})"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining(Labels.NO_LABEL_TYPE);
  }

  // ---------------------------------------------------------------------------------------------------------

  private void command(final String query) {
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    });
  }

  private Object scalar(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      return resultSet.next().getProperty("r");
    }
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
  private List<Object> labelsOf(final String matchProperty, final Object value) {
    try (final ResultSet resultSet = database.query("opencypher",
        "MATCH (n {" + matchProperty + ": $v}) RETURN labels(n) AS l", java.util.Map.of("v", value))) {
      final Result result = resultSet.next();
      return (List<Object>) result.getProperty("l");
    }
  }
}
