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

import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7394 item 7: the Gremlin sibling of the double execution #7330 removed from OpenCypher.
 * <p>
 * {@code $profileExecution} means "time this statement", not "run it differently" - that is what #7330
 * settled for OpenCypher, and {@code ServerDatabase} injects the flag for <b>every</b> statement, in every
 * language, while the server profiler is recording. {@link ArcadeGremlin#execute()} answered it by running
 * the statement a second time with {@code .profile()} appended and draining that run to build the execution
 * plan. For a read that costs double; for a mutation the second run <i>applies the mutation again</i>, so
 * switching on a diagnostic silently doubled every Gremlin write on the server.
 * <p>
 * The tests below pin both halves of the fix: a mutating traversal is executed exactly once and reports no
 * plan, and a read-only traversal still gets its plan (Studio's {@code profileExecution: "detailed"} is
 * unchanged for reads, which is the behaviour the skip must not take away).
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
 */
class Issue7394GremlinProfileDoesNotDoubleMutateTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-7394-profile");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("Knows");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  private long countPersons() {
    try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT count(*) AS total FROM Person")) {
      return rs.next().getProperty("total");
    }
  }

  @Test
  void anAddVertexUnderProfileExecutionCreatesOneVertexNotTwo() {
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin("g.addV('Person').property('name','Alice')")
          .setParameters(Map.of("$profileExecution", true))
          .execute()) {
        // The traversal is lazy: draining it is what the caller does, and it is the run that must be the
        // only one to reach storage.
        rs.stream().forEach(r -> {
        });
      }
    });

    assertThat(countPersons()).isEqualTo(1L);
  }

  @Test
  void anAddEdgeUnderProfileExecutionCreatesOneEdgeNotTwo() {
    graph.getDatabase().transaction(() -> {
      graph.addVertex("Person").property("name", "Alice");
      graph.addVertex("Person").property("name", "Bob");
    });

    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin(
              "g.V().has('Person','name','Alice').as('a')"
                  + ".V().has('Person','name','Bob').as('b')"
                  + ".addE('Knows').from('a').to('b')")
          .setParameters(Map.of("$profileExecution", true))
          .execute()) {
        rs.stream().forEach(r -> {
        });
      }
    });

    try (final ResultSet rs = graph.getDatabase().query("sql", "SELECT count(*) AS total FROM Knows")) {
      assertThat((Long) rs.next().getProperty("total")).isEqualTo(1L);
    }
  }

  @Test
  void aMutatingTraversalReportsNoExecutionPlanRatherThanBuyingOneWithASecondMutation() {
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin("g.addV('Person').property('name','Carol')")
          .setParameters(Map.of("$profileExecution", true))
          .execute()) {
        rs.stream().forEach(r -> {
        });
        assertThat(rs.getExecutionPlan()).isEmpty();
      }
    });

    assertThat(countPersons()).isEqualTo(1L);
  }

  @Test
  void aReadOnlyTraversalStillGetsItsExecutionPlan() {
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Dave"));

    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person')")
        .setParameters(Map.of("$profileExecution", true))
        .execute()) {
      rs.stream().forEach(r -> {
      });
      assertThat(rs.getExecutionPlan()).isPresent();
    }
  }

  /**
   * The flag is consumed, not left in the bindings. It is not a Gremlin variable, and leaving it there
   * would bind {@code $profileExecution} into the script engine as if the caller had written it.
   */
  @Test
  void theFlagIsRemovedFromTheParametersEvenWhenTheProfilingPassIsSkipped() {
    final ArcadeGremlin gremlin = graph.gremlin("g.addV('Person')");
    gremlin.setParameters(Map.of("$profileExecution", true));

    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = gremlin.execute()) {
        rs.stream().forEach(r -> {
        });
      }
    });

    assertThat(gremlin.getParameters()).doesNotContainKey("$profileExecution");
  }
}
