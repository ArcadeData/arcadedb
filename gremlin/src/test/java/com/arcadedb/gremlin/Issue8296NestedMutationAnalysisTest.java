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

import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8296: {@link ArcadeGremlin#parse()} decided whether a traversal writes by scanning only the root step list, so a
 * write nested in a child traversal ({@code coalesce()}, {@code union()}, {@code sideEffect()}...) was analyzed as
 * read-only. The HA follower uses exactly that verdict to decide whether to forward a command to the leader, so the
 * canonical Gremlin upsert ran locally on a follower, outside Raft.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8296NestedMutationAnalysisTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-8296-analysis");
    graph.getDatabase().getSchema().createVertexType("P");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void aTopLevelWriteIsAWrite() {
    assertWrite("g.addV('P').property('name','direct')", OperationType.CREATE);
  }

  @Test
  void theCanonicalUpsertIsAWrite() {
    assertWrite("g.V().hasLabel('P').has('name','up').fold().coalesce(__.unfold(), __.addV('P').property('name','up'))",
        OperationType.CREATE);
  }

  @Test
  void aWriteInsideSideEffectIsAWrite() {
    assertWrite("g.V().hasLabel('P').sideEffect(__.addV('P').property('name','se'))", OperationType.CREATE);
  }

  @Test
  void aWriteInsideUnionIsAWrite() {
    assertWrite("g.inject(1).union(__.addV('P').property('name','un'))", OperationType.CREATE);
  }

  @Test
  void aDropTwoLevelsDownIsADelete() {
    assertWrite("g.V().hasLabel('P').local(__.choose(__.has('name','x'), __.drop()))", OperationType.DELETE);
  }

  @Test
  void aNestedPropertyUpdateIsAnUpdate() {
    assertWrite("g.V().hasLabel('P').sideEffect(__.property('name','y'))", OperationType.UPDATE);
  }

  @Test
  void aMergeIsACreateAndAnUpdate() {
    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin("g.inject(1).union(__.mergeV([(T.label):'P', name:'m']))").parse();
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE, OperationType.UPDATE).doesNotContain(OperationType.READ);
  }

  @Test
  void anIoReadLoadsIntoTheGraphSoItIsAWrite() {
    assertWrite("g.io('/nonexistent/in.xml').read()", OperationType.CREATE);
  }

  @Test
  void anIoWriteOnlyReadsTheGraph() {
    assertRead("g.io('/nonexistent/out.xml').write()");
  }

  @Test
  void aPureReadStaysARead() {
    assertRead("g.V().hasLabel('P').count()");
    assertRead("g.V().hasLabel('P').union(__.out(), __.in()).fold().coalesce(__.unfold(), __.constant(0))");
  }

  @Test
  void aNestedWriteUnderProfileExecutionRunsOnce() {
    // THE PROFILING HALF OF THE ISSUE: SINCE #7408 THE PLAN IS COLLECTED FROM THE ONE RUN THE CALLER DRAINS, SO A
    // NESTED WRITE IS NOT APPLIED TWICE EITHER. PINNED HERE ONE NESTING LEVEL DOWN
    graph.getDatabase().transaction(() -> {
      try (final ResultSet rs = graph.gremlin("g.inject(1).union(__.addV('P').property('name','un'))")
          .setParameters(Map.of("$profileExecution", true)).execute()) {
        assertThat(rs.stream().count()).isEqualTo(1L);
      }
    });
    assertThat(graph.getDatabase().countType("P", true)).isEqualTo(1L);
  }

  private void assertWrite(final String query, final OperationType expected) {
    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin(query).parse();
    assertThat(analyzed.isIdempotent()).as(query).isFalse();
    assertThat(analyzed.getOperationTypes()).as(query).contains(expected).doesNotContain(OperationType.READ);
  }

  private void assertRead(final String query) {
    final QueryEngine.AnalyzedQuery analyzed = graph.gremlin(query).parse();
    assertThat(analyzed.isIdempotent()).as(query).isTrue();
    assertThat(analyzed.getOperationTypes()).as(query).containsExactly(OperationType.READ);
  }
}
