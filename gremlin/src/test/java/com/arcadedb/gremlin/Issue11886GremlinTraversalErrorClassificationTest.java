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
package com.arcadedb.gremlin;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11886: a traversal TinkerPop refuses for its own shape, or for the type of the values it meets, is an error in the
 * request, like the Gremlin syntax errors already answered as client errors (#5191, #5201). They surfaced as the raw
 * {@code ClassCastException}, {@code NullPointerException}, {@code VerificationException} or {@code IllegalStateException} of the
 * step, or wrapped in a plain {@link CommandExecutionException}, which every protocol answers as a server fault (HTTP 500).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11886GremlinTraversalErrorClassificationTest {
  private static final String DB_PATH = "./target/testgremlin11886";

  private static ArcadeGraph graph;
  private static String      vertexId;

  @BeforeAll
  static void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    graph = ArcadeGraph.open(DB_PATH);
    graph.getDatabase().getSchema().createVertexType("V");
    graph.getDatabase().getSchema().createEdgeType("E");
    graph.getDatabase().transaction(() -> {
      final var a = graph.getDatabase().newVertex("V").set("x", 1).save();
      final var b = graph.getDatabase().newVertex("V").set("x", 2).save();
      a.newEdge("E", b).save();
      vertexId = a.getIdentity().toString();
    });
  }

  @AfterAll
  static void tearDown() {
    if (graph != null)
      graph.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @ParameterizedTest
  @ValueSource(strings = { //
      "g.V('#ID').max()", // a vertex is not Comparable
      "g.V().sum()", // a vertex is not a Number
      "g.E().mean().fold()", // an edge is not a Number
      "g.V().inV()", // inV() takes edges
      "g.V('#ID').label().values('x')", // a label is not an element
      "g.V().hasLabel('V').sample(1).property(2147483648, 'i')", // a property key is a string
      "g.V().emit()", // repeat() without a traversal
      "g.V().bothE(null, 'x')", // TinkerPop itself cannot sort a null among the labels of a step
      "g.V().out(null, 'E')", //
      "g.V().hasLabel('V').otherV()", // no previous vertex in the path
      "g.V('#ID').to('V')", // to() cannot follow GraphStep
  })
  void traversalErrorIsAClientError(final String traversal) {
    final String query = traversal.replace("#ID", vertexId);
    final Throwable error = catchThrowable(() -> drain(query));
    assertThat(error).as(query).isNotNull();
    assertThat(ErrorCategory.of(error)).as("%s answered %s", query, error).isIn(ErrorCategory.PARSING, ErrorCategory.VALIDATION);
    assertThat(error.getMessage()).as(query).startsWith("Invalid gremlin traversal: ");
  }

  @Test
  void theTinkerPopMessageIsKept() {
    assertThatThrownBy(() -> drain("g.V().emit()")).hasMessageContaining("repeat()-traversal was not defined");
    assertThatThrownBy(() -> drain("g.V('" + vertexId + "').to('V')")).hasMessageContaining("to() step cannot follow");
    assertThatThrownBy(() -> drain("g.V().hasLabel('V').otherV()")).hasMessageContaining("previous vertex");
    // a ClassCastException names the type met and the type needed, without the module and class loader detail
    assertThatThrownBy(() -> drain("g.V().sum()")).hasMessageContaining("java.lang.Number").hasMessageNotContaining("module");
  }

  @Test
  void aRuntimeErrorThatIsNoTraversalErrorKeepsItsClassification() {
    // .next() on an empty traversal is a runtime failure of a valid traversal, a CommandExecutionException (issue #5219)
    final Throwable error = catchThrowable(() -> drain("g.V().hasLabel('V').has('x', 99).next()"));
    assertThat(error).isInstanceOf(CommandExecutionException.class).hasRootCauseInstanceOf(NoSuchElementException.class);
    assertThat(ErrorCategory.of(error)).isEqualTo(ErrorCategory.SERVER);
  }

  /**
   * A step naming the null label alone matches nothing, as TinkerGraph, the reference implementation, reads it: no edge carries it.
   * It reached the edge-type lookups of the engine and failed them with a {@code NullPointerException}. A null among other labels
   * is refused by TinkerPop itself, which sorts the labels of a step, and is therefore a client error (see above).
   */
  @Test
  void aNullLabelAloneMatchesNoEdge() {
    assertThat(drain("g.V().out(null)")).isZero();
    assertThat(drain("g.V().outE(null)")).isZero();
    assertThat(drain("g.V().both(null)")).isZero();
    assertThat(drain("g.V().in(null)")).isZero();
    assertThat(drain("g.V().where(__.outE(null).count().is(0))")).isEqualTo(2);
  }

  @Test
  void validTraversalsStillRun() {
    assertThat(drain("g.V().values('x').sum()")).isEqualTo(1);
    assertThat(drain("g.V().outE().inV()")).isEqualTo(1);
    assertThat(drain("g.V().outE().otherV()")).isEqualTo(1);
    assertThat(drain("g.V().bothE('E')")).isEqualTo(2);
  }

  private static int drain(final String query) {
    int count = 0;
    try (final ResultSet resultSet = graph.gremlin(query).execute()) {
      while (resultSet.hasNext()) {
        resultSet.next();
        count++;
      }
    }
    return count;
  }
}
