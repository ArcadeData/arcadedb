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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9335: {@code ArcadeFilterByTypeStep} resolved the schema in its constructor, which runs when the traversal is compiled.
 * A type created by an earlier step of the same traversal ({@code addV('X')}) was therefore unknown to the step, which then
 * emitted nothing for the whole life of the traversal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9335FilterByTypeReadsSchemaAtExecutionTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-9335-lazy-type");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void hasLabelSeesATypeCreatedEarlierInTheSameTraversal() {
    assertThat(count("g.addV('NewA').V().hasLabel('NewA').count()")).isEqualTo(1L);
    assertThat(graph.getDatabase().getSchema().existsType("NewA")).isTrue();
  }

  @Test
  void hasLabelFollowedByHasSeesATypeCreatedEarlierInTheSameTraversal() {
    assertThat(count("g.addV('NewB').property('k',1).V().hasLabel('NewB').has('k',1).count()")).isEqualTo(1L);
  }

  @Test
  void valuesAreReturnedForATypeCreatedEarlierInTheSameTraversal() {
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin("g.addV('NewC').property('k',1).V().hasLabel('NewC').values('k')").execute()) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("result")).intValue()).isEqualTo(1);
      }
    });
  }

  @Test
  void hasLabelOnAnAbsentTypeStillMatchesNothing() {
    assertThat(count("g.V().hasLabel('Missing').count()")).isZero();
  }

  @Test
  void hasLabelOnAnExistingTypeStillWorks() {
    graph.getDatabase().getSchema().createVertexType("P");
    graph.getDatabase().transaction(() -> graph.getDatabase().newVertex("P").set("n", 9).save());
    assertThat(count("g.addV('P').property('n',9).V().hasLabel('P').count()")).isEqualTo(2L);
  }

  private long count(final String query) {
    final long[] result = new long[1];
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin(query).execute()) {
        result[0] = ((Number) rs.next().getProperty("result")).longValue();
      }
    });
    return result[0];
  }
}
