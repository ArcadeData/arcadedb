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
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9249: a MATCH node filter that reads {@code $matched} only inside a nested statement was planned as independent of
 * the aliases it reads, and an IN subquery on a node after a hop ran without the enclosing context.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9249MatchedInSubqueryTest extends TestHelper {

  private void setup() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      final MutableVertex[] v = new MutableVertex[6];
      for (int i = 0; i < 6; i++)
        v[i] = database.newVertex("P").set("name", "p" + i).set("n", i % 3).set("g", i % 2).save();
      for (int i = 0; i < 6; i++)
        v[i].newEdge("K", v[(i + 1) % 6]);
    });
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void disjointInSubquery() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: (n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(12);
  }

  @Test
  void disjointWithMatchedReadOutside() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: ($matched.a.g >= 0 AND n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(12);
  }

  @Test
  void disjointSubquerySizeTested() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: ((SELECT FROM P WHERE g = $matched.a.g).size() > 0)} RETURN count(*) AS c")).isEqualTo(12);
  }

  @Test
  void oneHopInSubquery() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: (n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void oneHopWithMatchedReadOutside() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: ($matched.a.g >= 0 AND n IN (SELECT n FROM P WHERE g = $matched.a.g))} RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void oneHopSubquerySizeTested() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}.out('K'){as: b, where: ((SELECT FROM P WHERE g = $matched.a.g).size() > 0)} RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void literalControl() {
    setup();
    assertThat(count("MATCH {type: P, as: a, where: (n = 2)}, {type: P, as: b, where: (n IN (SELECT n FROM P WHERE g = 0))} RETURN count(*) AS c")).isEqualTo(12);
  }
}
