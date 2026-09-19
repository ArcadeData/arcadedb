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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7914 made {@code RemoteVertex.isConnectedTo} BIND its edge type instead of interpolating it into the
 * string literal argument of {@code both()}/{@code out()}/{@code in()}. The unit test for that change asserts on
 * the SQL text and the captured parameters against a mocked database, which cannot tell whether the statement
 * actually runs - and a bound parameter in a FUNCTION ARGUMENT is a less common position than in a WHERE clause
 * (PR #7942 review).
 * <p>
 * So this executes it against a real database. The graph traversal functions take their labels from
 * ALREADY-EVALUATED argument values ({@code SQLFunctionMove.execute} reads {@code iParameters}), so a parameter
 * is exactly as good as a literal there - this pins that, and would fail loudly if the parameter ever stopped
 * being resolved before the function sees it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7914BoundEdgeTypeArgumentTest extends TestHelper {

  @Test
  void aBoundEdgeTypeResolvesTheSameWayAsALiteralOne() {
    database.command("sql", "create vertex type Node");
    database.command("sql", "create edge type Linked");
    database.command("sql", "create edge type Ignored");

    database.transaction(() -> {
      database.command("sql", "create vertex Node set n = 1");
      database.command("sql", "create vertex Node set n = 2");
      database.command("sql", "create vertex Node set n = 3");
      database.command("sql",
          "create edge Linked from (select from Node where n = 1) to (select from Node where n = 2)");
      database.command("sql",
          "create edge Ignored from (select from Node where n = 1) to (select from Node where n = 3)");
    });

    // the literal spelling, which is what the client used to send
    final long viaLiteral = countNeighbours("select both('Linked') as vertices from Node where n = 1", Map.of());

    // ...and the bound spelling the fix sends instead
    final long viaParameter = countNeighbours("select both(:edgeType) as vertices from Node where n = 1",
        Map.of("edgeType", "Linked"));

    assertThat(viaParameter)
        .as("a bound edge type must select the same neighbours as the literal one")
        .isEqualTo(viaLiteral)
        .isEqualTo(1);

    // and it really is filtering: the other edge type answers its own neighbour, not both
    assertThat(countNeighbours("select both(:edgeType) as vertices from Node where n = 1",
        Map.of("edgeType", "Ignored"))).isEqualTo(1);
  }

  /**
   * The full shape {@code isConnectedTo} sends, parameter included: the neighbour set of one vertex filtered by
   * edge type, then tested with CONTAINS.
   */
  @Test
  void theIsConnectedToShapeRunsWithABoundEdgeType() {
    database.command("sql", "create vertex type V2");
    database.command("sql", "create edge type E2");

    database.transaction(() -> {
      database.command("sql", "create vertex V2 set n = 1");
      database.command("sql", "create vertex V2 set n = 2");
      database.command("sql", "create edge E2 from (select from V2 where n = 1) to (select from V2 where n = 2)");
    });

    final Object from = database.query("sql", "select @rid as rid from V2 where n = 1").next().getProperty("rid");
    final Object to = database.query("sql", "select @rid as rid from V2 where n = 2").next().getProperty("rid");

    final ResultSet connected = database.query("sql",
        "select from ( select both(:edgeType) as vertices from " + from + " ) where vertices contains " + to,
        Map.of("edgeType", "E2"));

    assertThat(connected.hasNext()).as("the bound edge type must not stop the traversal from matching").isTrue();
  }

  /**
   * The shape {@code countEdges} and the {@code getEdges}/{@code getVertices} family send: SEVERAL bound type
   * names in one traversal call, which is where those two sites used to write single-quoted literals.
   */
  @Test
  void severalBoundTypesFilterTheTraversalTheSameWayLiteralsDid() {
    database.command("sql", "create vertex type N3");
    database.command("sql", "create edge type A3");
    database.command("sql", "create edge type B3");
    database.command("sql", "create edge type C3");

    database.transaction(() -> {
      database.command("sql", "create vertex N3 set n = 1");
      for (int i = 2; i <= 4; i++)
        database.command("sql", "create vertex N3 set n = " + i);
      database.command("sql", "create edge A3 from (select from N3 where n = 1) to (select from N3 where n = 2)");
      database.command("sql", "create edge B3 from (select from N3 where n = 1) to (select from N3 where n = 3)");
      database.command("sql", "create edge C3 from (select from N3 where n = 1) to (select from N3 where n = 4)");
    });

    final long viaLiterals = count("select both('A3', 'B3').size() as count from N3 where n = 1", Map.of());
    final long viaParameters = count("select both(:t0, :t1).size() as count from N3 where n = 1",
        Map.of("t0", "A3", "t1", "B3"));

    assertThat(viaParameters)
        .as("two bound type names must filter exactly as the two literals did")
        .isEqualTo(viaLiterals)
        .isEqualTo(2);
  }

  private long count(final String sql, final Map<String, Object> params) {
    return database.query("sql", sql, params).next().<Number>getProperty("count").longValue();
  }

  private long countNeighbours(final String sql, final Map<String, Object> params) {
    final ResultSet result = database.query("sql", sql, params);
    assertThat(result.hasNext()).isTrue();
    final Object vertices = result.next().getProperty("vertices");
    return vertices instanceof Iterable<?> iterable ?
        StreamSupport.stream(iterable.spliterator(), false).count() :
        vertices == null ? 0 : 1;
  }
}
