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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #13470: a WHERE conjunct that reads a named path variable and the anchor node was attached to the
 * anchor scan, where the path is not bound yet, so the scan dropped every row.
 */
class CypherPathPredicateNotPushedToAnchorIssue13470Test extends TestHelper {
  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (img:Img13470 {id: 'i'}), (a:Lay13470 {id: 'a'}), (b:Lay13470 {id: 'b'}), (d:Lay13470 {id: 'd'})
        CREATE (img)-[:HEAD]->(a), (img)-[:TAIL]->(d), (a)-[:NEXT]->(b), (b)-[:NEXT]->(d),
               (img)-[:HAS_LAYER]->(a), (img)-[:HAS_LAYER]->(b), (img)-[:HAS_LAYER]->(d)
        """).close());
  }

  private List<Long> lengths(final String where) {
    final List<Long> lengths = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (img:Img13470)-[:HEAD]->(h:Lay13470) "
        + "MATCH (img)-[:TAIL]->(t:Lay13470) MATCH p=(h)-[:NEXT*1..]->(t) " + where + " RETURN length(p) AS l")) {
      while (rs.hasNext())
        lengths.add(rs.next().<Number>getProperty("l").longValue());
    }
    return lengths;
  }

  @Test
  void aPathPredicateReadingTheAnchorIsDecidedOnThePath() {
    assertThat(lengths("WHERE ALL(x IN nodes(p) WHERE (img)-[:HAS_LAYER]->(x))")).containsExactly(2L);
  }

  @Test
  void aPathPredicateThatFailsFiltersThePathOut() {
    assertThat(lengths("WHERE ALL(x IN nodes(p) WHERE (img)-[:HAS_LAYER]->(x) AND x.id <> 'b')")).isEmpty();
  }

  @Test
  void aPathPredicateReadingTheAnchorPropertyIsDecidedOnThePath() {
    assertThat(lengths("WHERE img.id = 'i' AND length(p) = 2")).containsExactly(2L);
    assertThat(lengths("WHERE img.id = 'i' AND length(p) = 3")).isEmpty();
  }
}
