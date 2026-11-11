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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11451: the value grammar missed two parts of the GraphQL specification. An integer literal was parsed into a Java
 * {@code int} whatever the argument it was written for, so a 13-digit timestamp for a {@code Long} or {@code ID} argument failed
 * the whole document with a raw {@code NumberFormatException}, which also escaped the read-only classification of
 * {@code analyze()}. The 32-bit bound belongs to {@code Int} and is checked where the declared type is known. And there was no
 * {@code null} literal and no block string ({@code """..."""}), both values of the specification since its first edition.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11451ValueLiteralsTest extends AbstractGraphQLTest {
  private static final long TS = 1_700_000_000_000L;

  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    database.getSchema().getOrCreateVertexType("Event");
    database.newVertex("Event").set("id", "e1").set("ts", TS).set("name", "Mr. brain").save();
    database.newVertex("Event").set("id", "e2").set("ts", 5L).save();
    database.newVertex("Event").set("id", "e3").set("ts", -TS).set("name", "say \"\"\" hi").save();
    database.command("graphql", """
        type Event {
          id: String
          ts: Long
          name: String
        }

        type Query {
          byTs(ts: Long): [Event]
          byTsId(ts: ID): [Event]
          byTsInt(ts: Int): [Event]
          byTsFloat(ts: Float): [Event]
          byTsDirective(ts: Long): [Event] @sql(statement: "select from Event where ts = :ts order by id")
          byName(name: String): [Event]
          byNameDefaultNull(name: String = null): [Event]
          byNames(names: [String]): [Event] @sql(statement: "select from Event where name in :names order by id")
          byNameDirective(name: String): [Event] @sql(statement: \"""
            select from Event
            where name = :name
            order by id
            \""")
        }""");
  }

  @ParameterizedTest
  @ValueSource(strings = { "byTs", "byTsId", "byTsDirective" })
  void integerLiteralBeyond32BitsReachesALongOrIdArgument(final String field) {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ " + field + "(ts: 1700000000000) { id } }", Map.of())).containsExactly("e1");
      assertThat(ids(database, "{ " + field + "(ts: -1700000000000) { id } }", Map.of())).containsExactly("e3");
      assertThat(ids(database, "{ " + field + "(ts: 5) { id } }", Map.of())).containsExactly("e2");
      // the literal and the variable are the same value
      assertThat(ids(database, "query q($t: Long) { " + field + "(ts: $t) { id } }", Map.of("t", TS))).containsExactly("e1");
      return null;
    });
  }

  @Test
  void integerLiteralBeyond32BitsIsRefusedForAnIntArgumentWithTheReason() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> ids(database, "{ byTsInt(ts: 1700000000000) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("Int").hasMessageContaining("1700000000000");
      assertThat(ids(database, "{ byTsInt(ts: 5) { id } }", Map.of())).containsExactly("e2");
      // an integer is a valid Float input value, whatever its width
      assertThat(ids(database, "{ byTsFloat(ts: 1700000000000) { id } }", Map.of())).containsExactly("e1");
      return null;
    });
  }

  @Test
  void integerLiteralBeyond64BitsIsRefusedForALongArgument() {
    executeTest(database -> {
      defineTypes(database);
      assertThatThrownBy(() -> ids(database, "{ byTs(ts: 12345678901234567890) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("Long").hasMessageContaining("12345678901234567890");
      return null;
    });
  }

  @Test
  void bothEntryPointsAndAnalyzeAgreeOnAWideLiteral() {
    executeTest(database -> {
      defineTypes(database);
      final QueryEngine engine = database.getQueryEngine("graphql");
      assertThat(engine.analyze("{ byTs(ts: 1700000000000) { id } }").isIdempotent()).isTrue();
      assertThat(engine.analyze("{ byTs(ts: 123456789012345678901234567890) { id } }").isIdempotent()).isTrue();
      // a syntax error still classifies fail-closed
      assertThat(engine.analyze("{ byTs(ts: ").isIdempotent()).isFalse();

      final List<String> viaCommand = new ArrayList<>();
      try (final ResultSet resultSet = database.command("graphql", "{ byTs(ts: 1700000000000) { id } }")) {
        while (resultSet.hasNext())
          viaCommand.add(resultSet.next().getProperty("id"));
      }
      assertThat(viaCommand).containsExactly("e1");
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byName", "byNameDirective" })
  void nullLiteralIsTheSameValueAsAnExplicitNullVariable(final String field) {
    executeTest(database -> {
      defineTypes(database);
      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("n", null);
      final List<String> viaVariable = ids(database, "query q($n: String) { " + field + "(name: $n) { id } }", explicitNull);

      assertThat(ids(database, "{ " + field + "(name: null) { id } }", Map.of())).isEqualTo(viaVariable);
      assertThat(database.getQueryEngine("graphql").analyze("{ " + field + "(name: null) { id } }").isIdempotent()).isTrue();
      return null;
    });
  }

  @Test
  void nullLiteralAsADefaultValue() {
    executeTest(database -> {
      defineTypes(database);
      final Map<String, Object> explicitNull = new HashMap<>();
      explicitNull.put("n", null);
      final List<String> explicit = ids(database, "query q($n: String) { byName(name: $n) { id } }", explicitNull);

      // an argument default of null is an explicit null, and so is a variable default of null
      assertThat(ids(database, "{ byNameDefaultNull { id } }", Map.of())).isEqualTo(explicit);
      assertThat(ids(database, "query q($n: String = null) { byName(name: $n) { id } }", Map.of())).isEqualTo(explicit);

      assertThatThrownBy(() -> ids(database, "query q($n: String! = null) { byName(name: $n) { id } }", Map.of())).isInstanceOf(
          CommandParsingException.class).hasMessageContaining("$n");
      return null;
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "byName", "byNameDirective" })
  void blockStringLiteral(final String field) {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ " + field + "(name: \"\"\"Mr. brain\"\"\") { id } }", Map.of())).containsExactly("e1");
      // the common indentation and the blank first and last lines are not part of the value
      assertThat(ids(database, "{ " + field + "(name: \"\"\"\n      Mr. brain\n    \"\"\") { id } }", Map.of())).containsExactly("e1");
      // \""" is the one escape of a block string
      assertThat(ids(database, "{ " + field + "(name: \"\"\"say \\\"\"\" hi\"\"\") { id } }", Map.of())).containsExactly("e3");
      return null;
    });
  }

  @Test
  void listLiteralReachesTheStatementAsTheValuesOfItsItems() {
    executeTest(database -> {
      defineTypes(database);
      assertThat(ids(database, "{ byNames(names: [\"Mr. brain\", \"nobody\"]) { id } }", Map.of())).containsExactly("e1");
      // a variable inside a list literal is substituted too
      assertThat(ids(database, "query q($n: String) { byNames(names: [$n, \"nobody\"]) { id } }", Map.of("n", "Mr. brain")))
          .containsExactly("e1");
      return null;
    });
  }

  private static List<String> ids(final Database database, final String query, final Map<String, Object> variables) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet resultSet = database.query("graphql", query, variables)) {
      while (resultSet.hasNext())
        ids.add(resultSet.next().getProperty("id"));
    }
    return ids;
  }
}
