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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12338: the value of a Cypher 25 dynamic label or type {@code $(expr)} on a READ path must be a
 * {@code STRING NOT NULL} or a {@code LIST<STRING NOT NULL> NOT NULL}, as the Cypher manual states and Neo4j enforces. Before,
 * {@code null} added no label (so a missing {@code $label} parameter matched the whole graph) and any other value was
 * {@code toString()}-ed into a label name. Also covers {@code $any(...)}, which the read paths treated as {@code $all(...)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDynamicLabelValueIssue12338Test extends TestHelper {

  @BeforeEach
  void setUpGraph() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:A {x:1}), (b:B {x:2}), (ab:A:B {x:3}), (a)-[:R]->(b), (b)-[:S]->(ab)").close());
  }

  private long count(final String query, final Object... params) {
    final Map<String, Object> map = new HashMap<>();
    for (int i = 0; i < params.length; i += 2)
      map.put((String) params[i], params[i + 1]);
    try (final ResultSet rs = database.query("opencypher", query, map)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private void assertTypeError(final String query, final Object... params) {
    assertThatThrownBy(() -> count(query, params)).as(query)
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("dynamic label");
  }

  @Test
  void nullIsATypeErrorOnEveryReadPath() {
    assertTypeError("MATCH (n:$(null)) RETURN count(n) AS c");
    assertTypeError("MATCH (n) WHERE n:$(null) RETURN count(n) AS c");
    assertTypeError("MATCH ()-[r:$(null)]->() RETURN count(r) AS c");
    assertTypeError("MATCH (a:A)-[:R]->(b:$(null)) RETURN count(b) AS c");
    assertTypeError("MATCH (a:A)-[:R*1..2]->(b:$(null)) RETURN count(b) AS c");
    assertTypeError("MATCH (a:A)-[:$(null)*1..2]->(b) RETURN count(b) AS c");
    assertTypeError("MATCH (n) WHERE n:A|$(null) RETURN count(n) AS c");
  }

  @Test
  void aNullOrMissingParameterDoesNotMatchTheWholeGraph() {
    assertTypeError("MATCH (n:$($label)) RETURN count(n) AS c", "label", null);
    final Map<String, Object> nullLabel = new HashMap<>();
    nullLabel.put("label", null);
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (n:$($label)) DETACH DELETE n",
        nullLabel).close())).isInstanceOf(CommandExecutionException.class).hasMessageContaining("dynamic label");
    assertThatThrownBy(() -> count("MATCH (n:$($label)) RETURN count(n) AS c")).isInstanceOf(CommandParameterMissingException.class);
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(3);
  }

  @Test
  void aNonStringIsATypeErrorNotALabelName() {
    assertTypeError("MATCH (n:$(1)) RETURN count(n) AS c");
    assertTypeError("MATCH (n) WHERE n:$(true) RETURN count(n) AS c");
    assertTypeError("MATCH ()-[r:$({a: 1})]->() RETURN count(r) AS c");
  }

  @Test
  void aListWithANullOrANonStringEntryIsATypeError() {
    assertTypeError("MATCH (n:$(['A', null])) RETURN count(n) AS c");
    assertTypeError("MATCH (n) WHERE n:$(['A', 1]) RETURN count(n) AS c");
    assertTypeError("MATCH (n:$($labels)) RETURN count(n) AS c", "labels", Arrays.asList("A", null));
  }

  @Test
  void validValuesStillMatch() {
    assertThat(count("MATCH (n:$('A')) RETURN count(n) AS c")).isEqualTo(2);
    assertThat(count("MATCH (n:$(['A', 'B'])) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH (n:$($labels)) RETURN count(n) AS c", "labels", List.of("A", "B"))).isEqualTo(1);
    assertThat(count("MATCH (n) WHERE n:$('B') RETURN count(n) AS c")).isEqualTo(2);
    assertThat(count("MATCH ()-[r:$('R')]->() RETURN count(r) AS c")).isEqualTo(1);
    // an empty list is the conjunction of no label, true for every node, as $all([]) is in Neo4j
    assertThat(count("MATCH (n:$([])) RETURN count(n) AS c")).isEqualTo(3);
    assertThat(count("MATCH (n) WHERE n:$([]) RETURN count(n) AS c")).isEqualTo(3);
    // A|$([]) is A OR the empty conjunction, true for every node, on the anchor scan, the hop target and the WHERE alike
    assertThat(count("MATCH (n:Nope|$([])) RETURN count(n) AS c")).isEqualTo(3);
    assertThat(count("MATCH (n) WHERE n:Nope|$([]) RETURN count(n) AS c")).isEqualTo(3);
    assertThat(count("MATCH (a:A)-[:R]->(b:Nope|$([])) RETURN count(b) AS c")).isEqualTo(1);
    assertThat(count("MATCH (a:A)-[*1..2]->(b:Nope|$([])) RETURN count(b) AS c")).isEqualTo(2);
    // a label nobody carries matches nothing
    assertThat(count("MATCH (n:$('Nope')) RETURN count(n) AS c")).isZero();
  }

  @Test
  void aNullElementStaysNullNotAnError() {
    // the element, not the dynamic label, is null: three-valued logic, no type error
    assertThat(count("OPTIONAL MATCH (n:Nope) WITH n WHERE (n:$('A')) IS NULL RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void quantifiedPathNodesCannotDropADynamicOperand() {
    // a dynamic label of a node inside a quantified path pattern is either honoured per repetition or refused, never dropped
    assertThat(count("MATCH (a:A) ((x)-[:R]->(y:!$('B'))){1,1} (z) RETURN count(*) AS c")).isZero();
  }

  @Test
  void anyIsADisjunction() {
    assertThat(count("MATCH (n:$any(['A', 'B'])) RETURN count(n) AS c")).isEqualTo(3);
    assertThat(count("MATCH (n) WHERE n:$any(['A', 'B']) RETURN count(n) AS c")).isEqualTo(3);
    assertThat(count("MATCH (n:$all(['A', 'B'])) RETURN count(n) AS c")).isEqualTo(1);
    assertThat(count("MATCH ()-[r:$any(['R', 'S'])]->() RETURN count(r) AS c")).isEqualTo(2);
    // the disjunction of no label is false
    assertThat(count("MATCH (n:$any([])) RETURN count(n) AS c")).isZero();
    assertTypeError("MATCH (n:$any(null)) RETURN count(n) AS c");
  }
}
