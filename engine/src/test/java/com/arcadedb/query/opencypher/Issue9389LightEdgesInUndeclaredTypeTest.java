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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9389: a one-hop {@code count(*)} over an unnamed relationship answered 0 for light edges stored in an edge type that does
 * not declare LIGHTWEIGHT, because the planner took the record counter of the type (0) for proof that the type is empty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("deprecation")
class Issue9389LightEdgesInUndeclaredTypeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE Empty");
    database.transaction(() -> {
      final RID a = database.newVertex("V").save().getIdentity();
      final RID b = database.newVertex("V").save().getIdentity();
      final RID c = database.newVertex("V").save().getIdentity();
      a.asVertex().modify().newLightEdge("E", b);
      b.asVertex().modify().newLightEdge("E", c);
    });
  }

  @Test
  void oneHopCountStarSeesLightEdgesOfUndeclaredType() {
    assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (a:V)-[r:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(b) AS n")).isEqualTo(2L);
    assertThat(count("MATCH (a:V)-[:E]->(b:V)-[:E]->(c:V) RETURN count(*) AS n")).isEqualTo(1L);
  }

  @Test
  void oneHopCountStarSeesLightEdgesAfterReopen() {
    reopenDatabase();
    assertThat(count("MATCH (a:V)-[:E]->(b:V) RETURN count(*) AS n")).isEqualTo(2L);
  }

  @Test
  void typeWithNoEdgeStillCountsZero() {
    assertThat(count("MATCH (a:V)-[:Empty]->(b:V) RETURN count(*) AS n")).isEqualTo(0L);
    assertThat(count("MATCH (a:V)-[:Missing]->(b:V) RETURN count(*) AS n")).isEqualTo(0L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
