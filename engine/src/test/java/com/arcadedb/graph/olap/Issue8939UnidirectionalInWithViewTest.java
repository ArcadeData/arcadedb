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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #8939: {@code in()} and {@code both()} over a UNIDIRECTIONAL edge type answered the incoming
 * vertices from the analytical view's reverse index, so the same query returned different rows with and without a view
 * and disagreed with {@code inE()} and the vertex API. A view only accelerates: called on its own the functions answer
 * what the vertex stores, a pattern answers the incoming side, in both states.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8939UnidirectionalInWithViewTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL");
    database.getSchema().createEdgeType("B");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("name", "a").save();
      final MutableVertex b = database.newVertex("V").set("name", "b").save();
      a.newEdge("U", b);
      a.newEdge("B", b);
    });
  }

  private long sql(final String projection) {
    return ((Number) database.query("sql", "SELECT " + projection + " AS n FROM V WHERE name = 'b'").next().getProperty("n")).longValue();
  }

  private void assertSameAnswers(final String state) {
    assertThat(sql("in('U').size()")).as(state + " in('U')").isZero();
    assertThat(sql("both('U').size()")).as(state + " both('U')").isZero();
    assertThat(sql("inE('U').size()")).as(state + " inE('U')").isZero();
    // a bidirectional type is answered as ever
    assertThat(sql("in('B').size()")).as(state + " in('B')").isEqualTo(1);
    assertThat(sql("both('B').size()")).as(state + " both('B')").isEqualTo(1);

    final Vertex b = database.query("sql", "SELECT FROM V WHERE name = 'b'").next().getVertex().get();
    long api = 0;
    for (final Vertex ignored : b.getVertices(Vertex.DIRECTION.IN, "U"))
      api++;
    assertThat(api).as(state + " API").isZero();

    // a pattern asks which edges end in the vertex, with or without a view
    assertThat(database.query("opencypher", "MATCH (b:V {name: 'b'})<-[:U]-(a) RETURN count(*) AS n").next().<Number>getProperty("n").longValue())
        .as(state + " Cypher").isEqualTo(1);
    assertThat(database.query("sql", "MATCH {type: V, as: b, where: (name = 'b')}.in('U'){as: a} RETURN a.name AS n").next().<String>getProperty("n"))
        .as(state + " SQL MATCH").isEqualTo("a");
  }

  @Test
  void inAndBothAnswerTheSameWithAndWithoutAView() throws Exception {
    assertSameAnswers("no view");

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g8939 VERTEX TYPES (V) EDGE TYPES (U, B)");
    try {
      GraphAnalyticalViewRegistry.get(database, "g8939").awaitReady(60, TimeUnit.SECONDS);
      assertSameAnswers("with view");
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW g8939");
    }

    assertSameAnswers("view dropped");
  }
}
