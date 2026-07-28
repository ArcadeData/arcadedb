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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5477: {@code size()} answered {@code null} for an argument that has no size at all
 * ({@code size(42)}, {@code size(true)}), which a client cannot tell apart from legal Cypher null propagation, so a
 * wrong query looked like a successful one. Those arguments are now a client-facing type error, as in Neo4j and
 * Memgraph. Sibling of issue #5476, which did the same for {@code head()}, {@code last()} and {@code tail()}.
 *
 * <p>Input domain: {@code size()} counts characters of a STRING and entries of a LIST or a MAP. Maps are counted rather
 * than rejected, following Memgraph. {@code size(null)} still answers {@code null}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSizeArgumentIssue5477Test extends TestHelper {

  // ===================== arguments that have no size =====================

  @Test
  void sizeOfIntegerIsATypeError() {
    // The reproducer from the issue: RETURN size(42) used to answer {"r": null}.
    assertThatThrownBy(() -> consume("RETURN size(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()")
        .hasMessageContaining("INTEGER");
  }

  @Test
  void sizeOfFloatIsATypeError() {
    assertThatThrownBy(() -> consume("RETURN size(3.14) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()")
        .hasMessageContaining("FLOAT");
  }

  @Test
  void sizeOfBooleanIsATypeError() {
    assertThatThrownBy(() -> consume("RETURN size(true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void sizeOfNodeIsATypeError() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5477 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5477) RETURN size(n) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()");
  }

  @Test
  void sizeOfIntegerPropertyIsARejectedAtRuntime() {
    // The type is known only while the query runs, so this exercises SizeFunction itself rather than the
    // statically-known-argument check in the validator.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5477 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5477) RETURN size(n.age) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()")
        .hasMessageContaining("INTEGER");
  }

  @Test
  void sizeOfIntegerLiteralFailsEvenWhenNoRowMatches() {
    // Neo4j rejects an out-of-domain literal before running the query, so it fails even where the function would
    // never be called.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5477 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5477) WHERE n.name = 'nobody' RETURN size(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("size()");
  }

  // ===================== arguments that do have a size =====================

  @Test
  void sizeOfNullStaysNull() {
    // Null propagation is legal Cypher and must not be turned into an error.
    assertThat(single("RETURN size(null) AS r")).isNull();
  }

  @Test
  void sizeOfStringAndListStillWorks() {
    assertThat(single("RETURN size('abc') AS r")).isEqualTo(3L);
    assertThat(single("RETURN size('') AS r")).isEqualTo(0L);
    assertThat(single("RETURN size([1,2,3]) AS r")).isEqualTo(3L);
    assertThat(single("RETURN size([]) AS r")).isEqualTo(0L);
  }

  @Test
  void sizeOfMapCountsItsEntries() {
    // Neo4j has no size() for maps; Memgraph counts the entries, which is the more useful answer.
    assertThat(single("RETURN size({a: 1, b: 2}) AS r")).isEqualTo(2L);
    assertThat(single("RETURN size({}) AS r")).isEqualTo(0L);
  }

  @Test
  void sizeOfCollectedRowsAndPropertiesStillWorks() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Issue5477 {name: 'a', tags: ['x', 'y']})");
      database.command("opencypher", "CREATE (:Issue5477 {name: 'b', tags: ['z']})");
    });
    assertThat(single("MATCH (n:Issue5477) RETURN size(collect(n.name)) AS r")).isEqualTo(2L);
    assertThat(single("MATCH (n:Issue5477 {name: 'a'}) RETURN size(n.tags) AS r")).isEqualTo(2L);
    assertThat(single("MATCH (n:Issue5477 {name: 'a'}) RETURN size(n.name) AS r")).isEqualTo(1L);
  }

  @Test
  void sizeOfArrayParameterStillWorks() {
    try (final ResultSet rs = database.query("opencypher", "RETURN size($list) AS r", Map.of("list", new int[] { 7, 8, 9 }))) {
      assertThat(((Number) rs.next().getProperty("r")).longValue()).isEqualTo(3L);
    }
  }

  // ===================== sibling functions that used to answer HTTP 500 =====================

  @Test
  void isEmptyOfIntegerIsAClientTypeError() {
    // isEmpty() already rejected the argument, but as a CommandExecutionException, which the HTTP layer reports as a
    // 500 server failure. An argument outside the input domain is the client's mistake: 400.
    assertThatThrownBy(() -> consume("RETURN isEmpty(42) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("isEmpty()");
  }

  @Test
  void isEmptyStillWorksOnStringListAndMap() {
    assertThat(single("RETURN isEmpty('') AS r")).isEqualTo(true);
    assertThat(single("RETURN isEmpty([1]) AS r")).isEqualTo(false);
    assertThat(single("RETURN isEmpty({}) AS r")).isEqualTo(true);
  }

  @Test
  void rangeWithFloatBoundIsAClientTypeError() {
    assertThatThrownBy(() -> consume("RETURN range(1.5, 3) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("range()")
        .hasMessageContaining("FLOAT");
  }

  @Test
  void rangeStillWorksOnIntegers() {
    assertThat(single("RETURN range(1, 3) AS r")).isEqualTo(List.of(1L, 2L, 3L));
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
