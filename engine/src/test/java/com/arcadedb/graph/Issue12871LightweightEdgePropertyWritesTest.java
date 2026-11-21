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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #12871: a property write on an edge of a {@code LIGHTWEIGHT} type is refused on every path with the same
 * {@link IllegalArgumentException} openCypher {@code CREATE} raises, the type the HTTP layer answers 400 for (the exception or
 * its direct cause), and an empty {@code CONTENT} map, which describes no property, creates the edge as openCypher
 * {@code CREATE (a)-[:K {}]->(b)} and the batch loader already do.
 */
class Issue12871LightweightEdgePropertyWritesTest extends TestHelper {

  private static final String REFUSAL = "is declared LIGHTWEIGHT, so its edges cannot have properties";

  @Override
  protected void beginTest() {
    database.command("sql", "create vertex type P").close();
    database.command("sql", "create edge type K lightweight").close();
    database.transaction(() -> {
      database.command("sql", "create vertex P set k = 1").close();
      database.command("sql", "create vertex P set k = 2").close();
    });
  }

  @Test
  void anEmptyContentCreatesTheEdge() {
    database.transaction(() -> database.command("sql", "create edge K from (select from P where k = 1) to (select from P where k = 2) content {}")
        .close());

    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void anEmptyContentParameterCreatesTheEdge() {
    database.transaction(() -> database.command("sql",
        "create edge K from (select from P where k = 1) to (select from P where k = 2) content :props", Map.of("props", Map.of()))
        .close());

    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void aContentWithAPropertyIsRefused() {
    assertRefused("sql", "create edge K from (select from P where k = 1) to (select from P where k = 2) content {\"x\": 1}");
    assertThat(edgeCount()).isZero();
  }

  @Test
  void aContentParameterWithAPropertyIsRefused() {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> database.command("sql",
        "create edge K from (select from P where k = 1) to (select from P where k = 2) content :props",
        Map.of("props", Map.of("x", 1))).close()));

    assertTheRefusal(thrown);
    assertThat(edgeCount()).isZero();
  }

  @Test
  void aCreateEdgeSetIsRefused() {
    assertRefused("sql", "create edge K from (select from P where k = 1) to (select from P where k = 2) set x = 1");
    assertThat(edgeCount()).isZero();
  }

  @Test
  void anUpdateSetIsRefused() {
    createEdge();
    assertRefused("sql", "update K set x = 1");
  }

  @Test
  void aCypherSetPropertyIsRefused() {
    createEdge();
    assertRefused("opencypher", "MATCH (:P {k: 1})-[r:K]->() SET r.x = 5");
  }

  @Test
  void aCypherSetMergeMapIsRefused() {
    createEdge();
    assertRefused("opencypher", "MATCH (:P {k: 1})-[r:K]->() SET r += {y: 1}");
  }

  @Test
  void aCypherSetReplaceMapIsRefused() {
    createEdge();
    assertRefused("opencypher", "MATCH (:P {k: 1})-[r:K]->() SET r = {y: 1}");
  }

  @Test
  void aCypherSetOnTheEdgeJustCreatedIsRefused() {
    assertRefused("opencypher", "MATCH (a:P {k: 1}), (b:P {k: 2}) CREATE (a)-[r:K]->(b) SET r.x = 1");
    assertThat(edgeCount()).isZero();
  }

  @Test
  void theEdgeItselfRefusesAPropertyWithTheSameException() {
    database.transaction(() -> {
      final MutableEdge created = database.lookupByRID(vertex(1), true).asVertex().modify().newEdge("K", vertex(2));
      assertTheRefusal(catchThrowable(() -> created.set("x", 1)));
      assertTheRefusal(catchThrowable(() -> created.fromMap(Map.of("x", 1))));

      // an empty map describes no property, as for GraphEngine.newEdge
      assertThat(created.fromMap(Map.of())).isSameAs(created);
      assertThat(created.set(Map.of())).isSameAs(created);
      created.merge(Map.of());
    });

    database.transaction(() -> {
      final Edge persisted = database.lookupByRID(vertex(1), true).asVertex().getEdges(Vertex.DIRECTION.OUT, "K").iterator().next();
      assertTheRefusal(catchThrowable(persisted::modify));
    });
  }

  private void assertRefused(final String language, final String statement) {
    assertTheRefusal(catchThrowable(() -> database.transaction(() -> database.command(language, statement).close())));
  }

  /**
   * The refusal, as the HTTP layer reads it: the thrown exception or its direct cause is the {@link IllegalArgumentException}
   * that names the type.
   */
  private static void assertTheRefusal(final Throwable thrown) {
    assertThat(thrown).as("the statement must be refused").isNotNull();
    final Throwable refusal = thrown instanceof IllegalArgumentException ? thrown : thrown.getCause();
    assertThat(refusal).as("refused with %s", thrown).isInstanceOf(IllegalArgumentException.class);
    assertThat(refusal.getMessage()).contains("Edge type 'K' " + REFUSAL);
  }

  private void createEdge() {
    database.transaction(
        () -> database.command("sql", "create edge K from (select from P where k = 1) to (select from P where k = 2)").close());
    assertThat(edgeCount()).isEqualTo(1);
  }

  private RID vertex(final int k) {
    try (final ResultSet rs = database.query("sql", "select from P where k = ?", k)) {
      return rs.next().getIdentity().get();
    }
  }

  private long edgeCount() {
    try (final ResultSet rs = database.query("sql", "select count(*) as c from K")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
