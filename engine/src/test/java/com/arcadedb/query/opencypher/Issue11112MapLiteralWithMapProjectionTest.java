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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11112: a map literal whose value was a map projection collapsed to that projection, losing the
 * literal's own keys and every other entry ({@code { a: n { .k }, b: 1 }} answered {@code {k: 1}}). The expected values are
 * Neo4j 5.26's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11112MapLiteralWithMapProjectionTest extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:Qa {k: 1})<-[:QR {y: 1}]-(:Qb {t: 'x'})"));
  }

  @Test
  void aMapLiteralKeepsItsKeyAroundAProjection() {
    assertThat(single("MATCH (n:Qa) RETURN { a: n { .k } } AS m")).isEqualTo(Map.of("a", Map.of("k", 1)));
  }

  @Test
  void aMapLiteralKeepsItsOtherEntries() {
    assertThat(single("MATCH (n:Qa) RETURN { a: n { .k }, b: 1 } AS m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
    assertThat(single("MATCH (n:Qa) RETURN { b: 1, a: n { .k } } AS m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
  }

  @Test
  void aProjectionInsideAMapLiteralInsideAProjection() {
    assertThat(single("MATCH (n:Qa) RETURN n { c: { e: n { .k } } } AS m")).isEqualTo(Map.of("c", Map.of("e", Map.of("k", 1))));
  }

  @Test
  void aConnectionShapeWithAPatternComprehension() {
    assertThat(single("MATCH (n:Qa) RETURN n { c: { edges: [(n)<-[e0:QR]-(n0:Qb) | { node: n0 { .t }, p: e0 { .y } }] } } AS m"))
        .isEqualTo(Map.of("c", Map.of("edges", List.of(Map.of("node", Map.of("t", "x"), "p", Map.of("y", 1))))));
  }

  @Test
  void aListOfProjectionsInsideAMapLiteral() {
    assertThat(single("MATCH (n:Qa) RETURN { list: [n { .k }, n { .k }] } AS m"))
        .isEqualTo(Map.of("list", List.of(Map.of("k", 1), Map.of("k", 1))));
  }

  @Test
  void aProjectionWithAComputedFieldInsideAMapLiteral() {
    assertThat(single("MATCH (n:Qa) RETURN { a: n { .k, doubled: n.k * 2, s: toString(n.k) } } AS m"))
        .isEqualTo(Map.of("a", Map.of("k", 1, "doubled", 2L, "s", "1")));
  }

  @Test
  void aMapLiteralInWithAndInAFunctionArgument() {
    assertThat(single("MATCH (n:Qa) WITH { a: n { .k }, b: 1 } AS m RETURN m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
    final Result row = row("MATCH (n:Qa) RETURN keys({ a: n { .k }, b: 1 }) AS m");
    assertThat(row.<List<String>>getProperty("m")).containsExactlyInAnyOrder("a", "b");
  }

  @Test
  void aProjectionNextToAFunctionCallAListAndAPropertyAccess() {
    assertThat(single("MATCH (n:Qa) RETURN { s: toString(n.k), l: [1, 2], a: n { .k } } AS m"))
        .isEqualTo(Map.of("s", "1", "l", List.of(1L, 2L), "a", Map.of("k", 1)));
    assertThat(single("MATCH (n:Qa) RETURN { a: n { .k } }.a AS m")).isEqualTo(Map.of("k", 1));
  }

  @Test
  void aProjectionThroughAVariableStillWorks() {
    assertThat(single("MATCH (n:Qa) WITH n { .k } AS p RETURN { a: p } AS m")).isEqualTo(Map.of("a", Map.of("k", 1)));
  }

  @Test
  void aTopLevelProjectionStillWorks() {
    assertThat(single("MATCH (n:Qa) RETURN n { .k, c: { e: 1 } } AS m")).isEqualTo(Map.of("k", 1, "c", Map.of("e", 1L)));
  }

  private Object single(final String query) {
    return row(query).getProperty("m");
  }

  private Result row(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).as(query).isFalse();
      return row;
    }
  }
}
