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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #13133: a string literal in the property map of a MERGE pattern was read as the name of a variable of
 * the row, or as {@code variable.property}, so {@code UNWIND ['zz'] AS x MERGE (n:L {name: 'x'})} stored {@code zz}. A literal is a
 * value: the expected results are those of Neo4j 5.26 and of CREATE.
 */
class CypherMergeLiteralStringIssue13133Test extends TestHelper {

  @Test
  void aLiteralNamingABoundVariableIsStoredAsWritten() {
    final List<Object> names = names("UNWIND ['zz'] AS x MERGE (n:L {name: 'x'}) RETURN n.name AS v");
    assertThat(names).containsExactly("x");
    assertThat(names("MATCH (n:L) RETURN n.name AS v")).containsExactly("x");
  }

  @Test
  void aLiteralOfTheFormVariableDotPropertyIsStoredAsWritten() {
    assertThat(names("UNWIND [{k: 'zz'}] AS row MERGE (n:M {name: 'row.k'}) RETURN n.name AS v")).containsExactly("row.k");
    assertThat(names("UNWIND [{k: 'zz'}] AS row MERGE (n:N {name: \"row.k\"}) RETURN n.name AS v")).containsExactly("row.k");
  }

  @Test
  void aLiteralStartingWithTheNameOfABoundNodeDoesNotReadItsProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:E {k: 1})"));
    assertThat(names("MATCH (e:E) MERGE (n:Doc {title: 'e.g'}) RETURN n.title AS v")).containsExactly("e.g");
    assertThat(names("MATCH (e:E) MERGE (n:Doc {title: 'e.k'}) RETURN n.title AS v")).containsExactly("e.k");
  }

  @Test
  void aLiteralOnARelationshipIsStoredAsWritten() {
    final List<Object> names = names("UNWIND ['zz'] AS x MERGE (a:P {k: 1})-[r:R {name: 'x'}]->(b:P {k: 2}) RETURN r.name AS v");
    assertThat(names).containsExactly("x");
    assertThat(names("MATCH ()-[r:R]->() RETURN r.name AS v")).containsExactly("x");
  }

  @Test
  void theMergedNodeIsFoundAgainByTheSameStatement() {
    for (int i = 0; i < 3; i++)
      assertThat(names("UNWIND ['zz'] AS x MERGE (n:L {name: 'x'}) RETURN n.name AS v")).containsExactly("x");
    assertThat(names("MATCH (n:L) RETURN count(n) AS v")).containsExactly(1L);
    assertThat(names("MATCH (n:L {name: 'x'}) RETURN n.name AS v")).containsExactly("x");
  }

  @Test
  void aLiteralInAPathMergeWithABoundEndpointIsStoredAsWritten() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {k: 1})"));
    assertThat(names("MATCH (a:P {k: 1}) UNWIND ['zz'] AS x MERGE (a)-[r:R]->(b:Q {name: 'x'}) RETURN b.name AS v")).containsExactly("x");
    assertThat(names("MATCH (a:P {k: 1}) UNWIND ['zz'] AS x MERGE (a)-[r:R]->(b:Q {name: 'x'}) RETURN b.name AS v")).containsExactly("x");
    assertThat(names("MATCH (b:Q) RETURN count(b) AS v")).containsExactly(1L);
  }

  @Test
  void aParameterAndAVariableStillFeedTheMap() {
    final Map<String, Object> params = Map.of("p", "param");
    assertThat(names("UNWIND ['var'] AS x MERGE (n:V {a: x, b: $p, c: 'x'}) RETURN n.a AS v", params)).containsExactly("var");
    assertThat(names("MATCH (n:V) RETURN n.b AS v")).containsExactly("param");
    assertThat(names("MATCH (n:V) RETURN n.c AS v")).containsExactly("x");
  }

  private List<Object> names(final String query) {
    return names(query, Map.of());
  }

  private List<Object> names(final String query, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty("v"));
        }
      }
    });
    return values;
  }
}
