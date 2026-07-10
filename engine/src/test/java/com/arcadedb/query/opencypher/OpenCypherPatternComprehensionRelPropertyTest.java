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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5139: pattern comprehensions must honor inline relationship
 * property filters ({@code {prop: value}}), both for single-hop and variable-length patterns.
 * Neo4j and Memgraph enforce these filters; ArcadeDB previously ignored them inside pattern
 * comprehensions while enforcing them correctly in regular MATCH.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherPatternComprehensionRelPropertyTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-pc-rel-property").create();
    database.getSchema().createVertexType("VZ");
    database.getSchema().createEdgeType("VE");

    // a -[w:1]-> b -[w:2]-> c
    // a -[w:3]-> d -[w:4]-> c
    // No path from a to c has all relationships with w = 1.
    database.command("opencypher",
        """
        CREATE (a:VZ {name: 'a'}), \
        (b:VZ {name: 'b'}), \
        (c:VZ {name: 'c'}), \
        (d:VZ {name: 'd'}), \
        (a)-[:VE {w: 1}]->(b), \
        (b)-[:VE {w: 2}]->(c), \
        (a)-[:VE {w: 3}]->(d), \
        (d)-[:VE {w: 4}]->(c)""");
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void variableLengthInlineRelPropertyFilterIsEnforced() {
    // No path a -> c has every relationship with w = 1, so the comprehension must be empty.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:VZ {name: 'a'}), (c:VZ {name: 'c'})
        RETURN [p = (a)-[:VE*1..4 {w: 1}]->(c) | length(p)] AS lens""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> lens = result.getProperty("lens");
    assertThat(lens).isEmpty();
  }

  @Test
  void singleHopInlineRelPropertyFilterIsEnforced() {
    // No outgoing :VE relationship from a has w = 999, so the comprehension must be empty.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:VZ {name: 'a'})
        RETURN [(a)-[:VE {w: 999}]->(x) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> names = result.getProperty("names");
    assertThat(names).isEmpty();
  }

  @Test
  void singleHopInlineRelPropertyFilterMatchesSubset() {
    // Only the a -[w:1]-> b edge qualifies.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:VZ {name: 'a'})
        RETURN [(a)-[:VE {w: 1}]->(x) | x.name] AS names""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> names = result.getProperty("names");
    assertThat(names).containsExactly("b");
  }

  @Test
  void variableLengthInlineRelPropertyFilterMatchesQualifyingPaths() {
    // b -[w:2]-> c is the only edge with w = 2; the single-hop path b -> c qualifies.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (b:VZ {name: 'b'}), (c:VZ {name: 'c'})
        RETURN [p = (b)-[:VE*1..4 {w: 2}]->(c) | length(p)] AS lens""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> lens = result.getProperty("lens");
    assertThat(lens).containsExactly(1L);
  }

  @Test
  void patternComprehensionExplicitWherePathPredicateStillWorks() {
    // Control: the explicit WHERE form must keep returning empty.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:VZ {name: 'a'}), (c:VZ {name: 'c'})
        RETURN [
          p = (a)-[:VE*1..4]->(c)
          WHERE all(e IN relationships(p) WHERE e.w = 1)
          | length(p)
        ] AS lens""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> lens = result.getProperty("lens");
    assertThat(lens).isEmpty();
  }

  @Test
  void noInlineFilterReturnsAllPaths() {
    // Sanity: without the inline filter both length-2 paths are returned.
    final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:VZ {name: 'a'}), (c:VZ {name: 'c'})
        RETURN [p = (a)-[:VE*1..4]->(c) | length(p)] AS lens""");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    final List<Object> lens = new ArrayList<>(result.getProperty("lens"));
    assertThat(lens).containsExactly(2L, 2L);
  }
}
