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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #5476: the list functions head(), last() and tail() accepted a non-list
 * argument (INTEGER, FLOAT, BOOLEAN, STRING, MAP, NODE, ...) and silently answered null, so a wrong
 * query looked like a successful query returning no value.
 * <p>
 * Reference semantics (Neo4j / Memgraph, the OpenCypher reference implementations): the signature is
 * {@code head(list :: LIST<ANY>) :: ANY}, so a non-list argument is a client-facing type error. Only
 * {@code null} propagates to {@code null} (Cypher null semantics), and an empty list answers
 * {@code null} for head()/last() and an empty list for tail().
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherListFunctionArgumentIssue5476Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypherlistarg5476").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:Sample {num: 42, txt: 'abc', tags: ['x', 'y']})"));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void headRejectsScalarLiterals() {
    assertTypeError("RETURN head(42) AS r", "head");
    assertTypeError("RETURN head(3.14) AS r", "head");
    assertTypeError("RETURN head(true) AS r", "head");
    assertTypeError("RETURN head('abc') AS r", "head");
    assertTypeError("RETURN head({a: 1}) AS r", "head");
  }

  @Test
  void lastRejectsScalarLiterals() {
    assertTypeError("RETURN last(42) AS r", "last");
    assertTypeError("RETURN last(3.14) AS r", "last");
    assertTypeError("RETURN last(true) AS r", "last");
    assertTypeError("RETURN last('abc') AS r", "last");
  }

  @Test
  void tailRejectsScalarLiterals() {
    assertTypeError("RETURN tail(42) AS r", "tail");
    assertTypeError("RETURN tail(3.14) AS r", "tail");
    assertTypeError("RETURN tail(true) AS r", "tail");
    assertTypeError("RETURN tail('abc') AS r", "tail");
  }

  @Test
  void reportedTypeNameIsTheCypherOne() {
    final Throwable thrown = catchThrowable(() -> consume("RETURN head(42) AS r"));
    assertThat(thrown).isNotNull();
    assertThat(rootMessage(thrown)).contains("INTEGER").contains("LIST");
  }

  @Test
  void typeErrorIsClassifiedAsAClientError() {
    // CommandParsingException (CommandSemanticException extends it) is what the HTTP layer maps to 400
    // instead of a misleading 500: see AbstractServerHttpHandler and issues #5191 / #5203.
    final Throwable thrown = catchThrowable(() -> consume("RETURN head(42) AS r"));
    assertThat(thrown).isNotNull();
    assertThat(hasInChain(thrown, CommandParsingException.class)).isTrue();
  }

  @Test
  void nonListPropertyValueIsRejectedAtRuntime() {
    assertTypeError("MATCH (n:Sample) RETURN head(n.num) AS r", "head");
    assertTypeError("MATCH (n:Sample) RETURN head(n.txt) AS r", "head");
    assertTypeError("MATCH (n:Sample) RETURN last(n.num) AS r", "last");
    assertTypeError("MATCH (n:Sample) RETURN tail(n.num) AS r", "tail");
  }

  @Test
  void nodeArgumentIsRejected() {
    assertTypeError("MATCH (n:Sample) RETURN head(n) AS r", "head");
  }

  @Test
  void nullPropagatesToNull() {
    assertThat(single("RETURN head(null) AS r")).isNull();
    assertThat(single("RETURN last(null) AS r")).isNull();
    assertThat(single("RETURN tail(null) AS r")).isNull();
    assertThat(single("MATCH (n:Sample) RETURN head(n.missing) AS r")).isNull();
  }

  @Test
  void validListArgumentsKeepWorking() {
    assertThat(single("RETURN head([42]) AS r")).isEqualTo(42L);
    assertThat(single("RETURN head([1, 2, 3]) AS r")).isEqualTo(1L);
    assertThat(single("RETURN last([1, 2, 3]) AS r")).isEqualTo(3L);
    assertThat(single("RETURN tail([1, 2, 3]) AS r")).isEqualTo(List.of(2L, 3L));
    assertThat(single("RETURN head([]) AS r")).isNull();
    assertThat(single("RETURN last([]) AS r")).isNull();
    assertThat(single("RETURN tail([]) AS r")).isEqualTo(List.of());
    assertThat(single("MATCH (n:Sample) RETURN head(n.tags) AS r")).isEqualTo("x");
    assertThat(single("MATCH (n:Sample) RETURN last(n.tags) AS r")).isEqualTo("y");
    assertThat(single("MATCH (n:Sample) RETURN tail(n.tags) AS r")).isEqualTo(List.of("y"));
    assertThat(single("MATCH (n:Sample) RETURN head(collect(n.num)) AS r")).isEqualTo(42);
  }

  @Test
  void arrayParameterIsStillAccepted() {
    final Map<String, Object> params = Map.of("list", new int[] { 7, 8, 9 });
    try (final ResultSet rs = database.query("opencypher", "RETURN head($list) AS h, last($list) AS l, tail($list) AS t", params)) {
      final var row = rs.next();
      assertThat((Object) row.getProperty("h")).isEqualTo(7);
      assertThat((Object) row.getProperty("l")).isEqualTo(9);
      assertThat((Object) row.getProperty("t")).isEqualTo(List.of(8, 9));
    }
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void assertTypeError(final String query, final String functionName) {
    final Throwable thrown = catchThrowable(() -> consume(query));
    assertThat(thrown).as("query '%s' must be rejected", query).isNotNull();
    assertThat(rootMessage(thrown)).as("query '%s'", query).contains(functionName + "()");
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private static String rootMessage(final Throwable thrown) {
    final StringBuilder buffer = new StringBuilder();
    for (Throwable current = thrown; current != null; current = current.getCause())
      buffer.append(current.getMessage()).append(" | ");
    return buffer.toString();
  }

  private static boolean hasInChain(final Throwable thrown, final Class<? extends Throwable> type) {
    for (Throwable current = thrown; current != null; current = current.getCause())
      if (type.isInstance(current))
        return true;
    return false;
  }
}
