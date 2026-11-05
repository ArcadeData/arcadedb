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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10807: since #10684 a bare {@code out} / {@code in} on an edge with no property of that name reads its endpoint, exactly
 * as {@code @out} / {@code @in} do, but only {@code WHERE @out = #rid} was rewritten into a walk of the vertex's adjacency list;
 * {@code WHERE out = #rid} scanned the whole edge type. The planner now rewrites the bare name too, while the dictionary does not
 * know it: a name no record stores and no type declares reads the endpoint on every edge. Once a record or a type uses it, the scan
 * stays and the stored property wins.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10807BareEndpointNameAdjacencyWalkTest extends TestHelper {
  private static final String WALK = "FETCH EDGES FROM VERTEX";

  private RID a;
  private RID b;
  private RID c;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("E");
    database.getSchema().createEdgeType("E2").addSuperType("E");
    database.transaction(() -> {
      final MutableVertex va = database.newVertex("V1").set("name", "a").save();
      final MutableVertex vb = database.newVertex("V1").set("name", "b").save();
      final MutableVertex vc = database.newVertex("V1").set("name", "c").save();
      va.newEdge("E", vb).set("qty", 1).save();
      va.newEdge("E2", vc).set("qty", 2).save();
      vb.newEdge("E", vc).set("qty", 3).save();
      a = va.getIdentity();
      b = vb.getIdentity();
      c = vc.getIdentity();
    });
  }

  @Test
  void aBareEndpointNameWalksTheAdjacencyListLikeTheAttribute() {
    assertThat(database.getSchema().getDictionary().getIdByName("out", false)).as("the fixture stores no property named out")
        .isNegative();

    for (final String endpoint : new String[] { "out", "in" }) {
      final boolean out = "out".equals(endpoint);
      final RID vertex = out ? a : c;
      final String bare = "SELECT FROM E WHERE " + endpoint + " = " + vertex;
      final String attribute = "SELECT FROM E WHERE @" + endpoint + " = " + vertex;

      assertThat(plan(bare)).as(bare).contains(WALK);
      assertThat(qtys(bare)).as(bare).isEqualTo(qtys(attribute)).containsExactly(out ? new Integer[] { 1, 2 } : new Integer[] { 2, 3 });
    }

    // the remaining conditions stay as a filter, and a parameter is resolved like a literal
    final String filtered = "SELECT FROM E WHERE out = " + a + " AND qty > 1";
    assertThat(plan(filtered)).contains(WALK);
    assertThat(qtys(filtered)).containsExactly(2);
    try (final ResultSet rs = database.query("sql", "SELECT FROM E WHERE in = ?", c)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains(WALK);
      final Set<Integer> qtys = new TreeSet<>();
      rs.forEachRemaining(r -> qtys.add(r.getProperty("qty")));
      assertThat(qtys).containsExactly(2, 3);
    }
  }

  /**
   * A record that stores a property named {@code out} makes the name ambiguous: the scan comes back, and the stored value wins on
   * that record. Asked of the same statement text that was rewritten before the write, which pins that the rewrite is never
   * served from the plan cache.
   */
  @Test
  void aStoredPropertyNamedLikeTheEndpointKeepsTheScan() {
    final String query = "SELECT FROM E WHERE out = " + a;
    assertThat(plan(query)).contains(WALK);
    assertThat(qtys(query)).containsExactly(1, 2);

    // b -> c, with a stored property out that names a: by its endpoints it is not an edge out of a, by its property it is
    database.transaction(
        () -> b.asVertex().modify().newEdge("E", c.asVertex()).set("qty", 4).set("out", a).save());

    assertThat(database.getSchema().getDictionary().getIdByName("out", false)).isNotNegative();
    assertThat(plan(query)).doesNotContain(WALK);
    assertThat(qtys(query)).as("the stored property wins on the record that has it").containsExactly(1, 2, 4);
    assertThat(qtys("SELECT FROM E WHERE @out = " + a)).as("the attribute is still the endpoint").containsExactly(1, 2);
    // the other name is still unused, so it keeps the walk
    assertThat(plan("SELECT FROM E WHERE in = " + c)).contains(WALK);
  }

  /** A property of that name declared by any type, here a subtype, keeps the scan too: on its edges the name reads the property. */
  @Test
  void aDeclaredPropertyNamedLikeTheEndpointKeepsTheScan() {
    database.getSchema().getType("E2").createProperty("in", Type.STRING);

    final String query = "SELECT FROM E WHERE in = " + c;
    assertThat(plan(query)).doesNotContain(WALK);
    assertThat(qtys(query)).as("on E2 the declared, unset, property reads null").containsExactly(3);
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private Set<Integer> qtys(final String query) {
    final Set<Integer> qtys = new TreeSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.forEachRemaining(r -> qtys.add(r.getProperty("qty")));
    }
    return qtys;
  }
}
