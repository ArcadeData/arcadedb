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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9248: a Postgres-style {@code $N} parameter was read only by the {@code Result} overload of
 * {@code BaseExpression.execute}, so every path evaluating through the {@code Identifiable} overload got null.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9248DollarParameterTest extends TestHelper {

  private void setup(final boolean index) {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.sku STRING");
    if (index)
      database.command("sql", "CREATE INDEX ON T (sku) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.name STRING");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("T").set("sku", "S" + i).save();
      final MutableVertex a = database.newVertex("V").set("name", "A").save();
      final MutableVertex b = database.newVertex("V").set("name", "B").save();
      database.newVertex("V").set("name", "C").save();
      a.newEdge("E", b);
    });
  }

  @Test
  void createEdgeSet() {
    setup(false);
    final Object[] from = new Object[3];
    database.transaction(() -> {
      from[0] = database.query("sql", "SELECT FROM V WHERE name = 'B'").next().getIdentity().get();
      from[1] = database.query("sql", "SELECT FROM V WHERE name = 'C'").next().getIdentity().get();
      from[2] = 9;
      try (final ResultSet rs = database.command("sql", "CREATE EDGE E FROM $1 TO $2 SET w = $3", from)) {
        final Edge e = rs.next().getEdge().get();
        assertThat(e.get("w")).isEqualTo(9);
      }
    });
  }

  @Test
  void functionArgumentOnRecord() {
    setup(false);
    try (final ResultSet rs = database.query("sql", "SELECT sku FROM T WHERE sku = coalesce($1, 'S1')", "S2")) {
      assertThat(rs.next().<String>getProperty("sku")).isEqualTo("S2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void functionArgumentOnRecordWithIndex() {
    setup(true);
    try (final ResultSet rs = database.query("sql", "SELECT sku FROM T WHERE sku = coalesce($1, 'S1')", "S2")) {
      assertThat(rs.next().<String>getProperty("sku")).isEqualTo("S2");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void functionInProjection() {
    setup(false);
    try (final ResultSet rs = database.query("sql", "SELECT sku, coalesce($1, 'none') AS v FROM T WHERE sku = 'S1'", "x")) {
      assertThat(rs.next().<String>getProperty("v")).isEqualTo("x");
    }
  }

  @Test
  void matchFilterAfterHop() {
    setup(false);
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: V, as: a, where: (name = $1)}.out('E'){as: b, where: (name = $2)} RETURN b.name AS name", "A", "B")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("B");
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
