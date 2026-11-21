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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A property declared with column types that differ across the types of a view is kept out of its columns. A third type
 * declaring the first type again brought the name back: with {@code A.p INTEGER}, {@code B.p DOUBLE} and {@code C.p INTEGER}
 * the view built an {@code INT} column for {@code p}.
 */
class GavPropertyTypeConflictTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("sqlscript", """
        CREATE VERTEX TYPE A; CREATE PROPERTY A.p INTEGER;
        CREATE VERTEX TYPE B; CREATE PROPERTY B.p DOUBLE;
        CREATE VERTEX TYPE C; CREATE PROPERTY C.p INTEGER;
        CREATE EDGE TYPE R; CREATE PROPERTY R.w INTEGER;
        CREATE EDGE TYPE S; CREATE PROPERTY S.w DOUBLE;
        CREATE EDGE TYPE T; CREATE PROPERTY T.w INTEGER;""");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("A").set("p", 1).save();
      final MutableVertex b = database.newVertex("B").set("p", 2.5).save();
      database.newVertex("B").set("p", 3.0).save();
      final MutableVertex c = database.newVertex("C").set("p", 4).save();
      a.newEdge("R", b, "w", 1);
      b.newEdge("S", c, "w", 2.5);
      c.newEdge("T", a, "w", 3);
    });
  }

  @Test
  void aVertexPropertyConflictStaysAcrossALaterType() throws Exception {
    assertNoColumnFor(new String[] { "A", "B", "C" }, 4);
  }

  /** With no other declared property the build samples records for schemaless ones: a conflicting name is not one of them. */
  @Test
  void aVertexPropertyConflictIsNotSampledBack() throws Exception {
    assertNoColumnFor(new String[] { "A", "B" }, 3);
  }

  @Test
  void anEdgePropertyConflictStaysAcrossALaterType() throws Exception {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("gv").withVertexTypes("A", "B", "C")
        .withEdgeTypes("R", "S", "T").withProperties().withEdgeProperties("w").build();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    assertThat(view.hasEdgeProperties()).as("an edge column for w").isFalse();
  }

  private void assertNoColumnFor(final String[] types, final int vertices) throws Exception {
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("gv").withVertexTypes(types)
        .withProperties("p").build();
    assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
    final Map<RID, Object> fromView = new HashMap<>();
    for (final String type : types)
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + type)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final RID rid = row.getIdentity().get();
          fromView.put(rid, view.getProperty(view.getNodeId(rid), "p"));
        }
      }
    // No column holds p: the view answers none of them, and a reader goes to the records
    assertThat(fromView).hasSize(vertices);
    assertThat(fromView.values()).as("p as the view holds it, per vertex " + fromView).containsOnlyNulls();
  }
}
