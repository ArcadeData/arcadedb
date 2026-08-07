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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5801: {@code reverse()} answered {@code null} for a scalar argument outside its
 * input domain ({@code reverse(5)}, {@code reverse(true)}), which a client cannot tell apart from legal Cypher
 * null propagation ({@code reverse(null)}), so a type violation looked like a successful null result. Same class
 * of defect as #5477 ({@code size()}) and #5476 ({@code head()}/{@code last()}/{@code tail()}); this fix follows
 * their sibling {@code isEmpty()}'s precedent of a runtime-only check via {@link
 * com.arcadedb.function.cypher.CypherFunctionHelper#typeMismatch}.
 *
 * <p>Input domain: {@code reverse()} accepts a {@code STRING} or a {@code LIST<ANY>}, matching Neo4j.
 * {@code reverse(null)} still answers {@code null}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherReverseArgumentIssue5801Test extends TestHelper {

  // ===================== arguments outside the input domain =====================

  @Test
  void reverseOfIntegerIsATypeError() {
    // The reproducer from the issue: RETURN reverse(5) used to answer {"value": null}.
    assertThatThrownBy(() -> consume("RETURN reverse(5) AS value"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("reverse()")
        .hasMessageContaining("INTEGER");
  }

  @Test
  void reverseOfFloatIsATypeError() {
    assertThatThrownBy(() -> consume("RETURN reverse(3.14) AS value"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("reverse()")
        .hasMessageContaining("FLOAT");
  }

  @Test
  void reverseOfBooleanIsATypeError() {
    assertThatThrownBy(() -> consume("RETURN reverse(true) AS value"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("reverse()")
        .hasMessageContaining("BOOLEAN");
  }

  @Test
  void reverseOfNodeIsATypeError() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5801 {name: 'a'})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5801) RETURN reverse(n) AS value"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("reverse()");
  }

  @Test
  void reverseOfIntegerPropertyIsRejectedAtRuntime() {
    // The type is known only while the query runs, so this exercises ReverseFunction itself rather than any
    // parse-time literal check.
    database.transaction(() -> database.command("opencypher", "CREATE (:Issue5801 {name: 'a', age: 42})"));
    assertThatThrownBy(() -> consume("MATCH (n:Issue5801) RETURN reverse(n.age) AS value"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("reverse()")
        .hasMessageContaining("INTEGER");
  }

  // ===================== arguments that remain valid =====================

  @Test
  void reverseOfNullStaysNull() {
    // Null propagation is legal Cypher and must not be turned into an error.
    assertThat(single("RETURN reverse(null) AS value")).isNull();
  }

  @Test
  void reverseOfStringStillWorks() {
    assertThat(single("RETURN reverse('palindrome') AS value")).isEqualTo("emordnilap");
    assertThat(single("RETURN reverse('') AS value")).isEqualTo("");
  }

  @Test
  void reverseOfListStillWorks() {
    assertThat(single("RETURN reverse([1, 2, 3]) AS value")).isEqualTo(List.of(3L, 2L, 1L));
    assertThat(single("RETURN reverse([]) AS value")).isEqualTo(List.of());
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("value");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
