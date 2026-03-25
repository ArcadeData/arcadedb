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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the query optimizer optimization that converts edge type scans with @out/@in RID filter
 * into vertex-centric edge traversal. E.g.:
 * <pre>SELECT FROM EdgeType WHERE @out = #X:Y</pre>
 * becomes equivalent to:
 * <pre>SELECT expand(outE('EdgeType')) FROM #X:Y</pre>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class EdgeTypeVertexRidOptimizationTest extends TestHelper {

  private RID v1Rid;
  private RID v2Rid;
  private RID v3Rid;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("Knows");
      database.getSchema().createEdgeType("Follows");

      final MutableVertex v1 = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex v2 = database.newVertex("Person").set("name", "Bob").save();
      final MutableVertex v3 = database.newVertex("Person").set("name", "Charlie").save();

      v1Rid = v1.getIdentity();
      v2Rid = v2.getIdentity();
      v3Rid = v3.getIdentity();

      // Alice -Knows-> Bob, Alice -Knows-> Charlie, Bob -Knows-> Charlie
      v1.newEdge("Knows", v2, "since", 2020);
      v1.newEdge("Knows", v3, "since", 2021);
      v2.newEdge("Knows", v3, "since", 2022);

      // Alice -Follows-> Bob
      v1.newEdge("Follows", v2);
    });
  }

  @Test
  void testSelectFromEdgeWhereOutEqualsRid() {
    database.transaction(() -> {
      // This should use vertex-centric traversal, not full edge type scan
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @out = " + v1Rid);

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getElement().get().asEdge().getOut()).isEqualTo(v1Rid);
        count++;
      }
      assertThat(count).isEqualTo(2); // Alice knows Bob and Charlie
    });
  }

  @Test
  void testSelectFromEdgeWhereInEqualsRid() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @in = " + v3Rid);

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getElement().get().asEdge().getIn()).isEqualTo(v3Rid);
        count++;
      }
      assertThat(count).isEqualTo(2); // Bob and Alice both know Charlie
    });
  }

  @Test
  void testSelectFromEdgeWhereOutEqualsRidWithAdditionalFilter() {
    database.transaction(() -> {
      // @out = RID AND since > 2020
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @out = " + v1Rid + " AND since > 2020");

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getElement().get().asEdge().getOut()).isEqualTo(v1Rid);
        assertThat((int) r.getProperty("since")).isGreaterThan(2020);
        count++;
      }
      assertThat(count).isEqualTo(1); // Only Alice->Charlie (since=2021)
    });
  }

  @Test
  void testSelectFromEdgeWhereOutAndInEqualsRid() {
    database.transaction(() -> {
      // Both @out and @in specified — the optimizer picks the first one (@out)
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @out = " + v1Rid + " AND @in = " + v2Rid);

      int count = 0;
      while (result.hasNext()) {
        final Result r = result.next();
        assertThat(r.getElement().get().asEdge().getOut()).isEqualTo(v1Rid);
        assertThat(r.getElement().get().asEdge().getIn()).isEqualTo(v2Rid);
        count++;
      }
      assertThat(count).isEqualTo(1); // Alice->Bob
    });
  }

  @Test
  void testSelectFromEdgeWhereOutEqualsRidString() {
    database.transaction(() -> {
      // RID as quoted string (common in HTTP API calls)
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @out = \"" + v1Rid + "\"");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void testExecutionPlanUsesVertexFetch() {
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT FROM Knows WHERE @out = " + v1Rid + " AND since > 2020");

      // Check that the execution plan uses FetchEdgesFromVertexStep
      final ExecutionPlan plan = result.getExecutionPlan().get();
      final String planStr = plan.prettyPrint(0, 2);
      assertThat(planStr).contains("FETCH EDGES FROM VERTEX");

      // Consume the result
      while (result.hasNext())
        result.next();
    });
  }
}
