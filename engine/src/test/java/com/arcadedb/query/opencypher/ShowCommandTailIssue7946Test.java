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
import com.arcadedb.query.opencypher.query.ShowCommandTail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #7946: the {@code YIELD}/{@code WHERE} tail of a {@code SHOW} command was parsed and then
 * dropped, so {@code SHOW DATABASES WHERE name = $dbName} - the Neo4j driver's own multi-database bootstrap check -
 * answered with every database on the server whatever was bound to {@code $dbName}, and could never report that a
 * database did not exist.
 * <p>
 * The tail is applied by {@link ShowCommandTail}, which hands the rows back to the openCypher engine, so what a
 * predicate means here is what it means in a query. These tests exercise it directly on a table of rows, which is
 * what every {@code SHOW} command answered outside a query plan produces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ShowCommandTailIssue7946Test {
  private static final List<String> FIELDS = List.of("name", "type", "access", "default");

  private static final List<List<Object>> ROWS = List.of(
      List.of("pipeshub_test", "standard", "read-write", true),
      List.of("beer", "standard", "read-only", false),
      List.of("system", "system", "read-write", false));

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-7946");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aWhereOnAParameterKeepsOnlyTheMatchingDatabase() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES WHERE name = $dbName", Map.of("dbName", "beer"));

    assertThat(table.fields()).isEqualTo(FIELDS);
    assertThat(table.rows()).hasSize(1);
    assertThat(table.rows().getFirst().getFirst()).isEqualTo("beer");
  }

  /**
   * The failure the issue is really about: the existence check must be able to answer "no".
   */
  @Test
  void aWhereThatMatchesNothingAnswersNothing() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES WHERE name = $dbName",
        Map.of("dbName", "totally_bogus_never_created_xyz"));

    assertThat(table.fields()).isEqualTo(FIELDS);
    assertThat(table.rows()).isEmpty();
  }

  @Test
  void aCommandWithNoTailIsLeftAlone() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES", Map.of());

    assertThat(ShowCommandTail.hasTail("SHOW DATABASES")).isFalse();
    assertThat(table.fields()).isEqualTo(FIELDS);
    assertThat(table.rows()).isEqualTo(ROWS);
  }

  @Test
  void aWhereMayUseAnyOperatorTheEngineHas() {
    assertThat(apply("SHOW DATABASES WHERE name STARTS WITH 'pipes'", Map.of()).rows()).hasSize(1);
    assertThat(apply("SHOW DATABASES WHERE type <> 'system'", Map.of()).rows()).hasSize(2);
    assertThat(apply("SHOW DATABASES WHERE name IN ['beer', 'system']", Map.of()).rows()).hasSize(2);
    assertThat(apply("SHOW DATABASES WHERE `default`", Map.of()).rows()).hasSize(1);
  }

  @Test
  void aYieldProjectsTheColumnsItNames() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD name, type", Map.of());

    assertThat(table.fields()).containsExactly("name", "type");
    assertThat(table.rows()).hasSize(3);
    assertThat(table.rows().getFirst()).containsExactly("pipeshub_test", "standard");
  }

  @Test
  void aYieldRenamesWithAs() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD name AS db WHERE db = 'beer'", Map.of());

    assertThat(table.fields()).containsExactly("db");
    assertThat(table.rows()).hasSize(1);
    assertThat(table.rows().getFirst()).containsExactly("beer");
  }

  @Test
  void aYieldHonoursOrderBySkipAndLimit() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD name ORDER BY name SKIP 1 LIMIT 1", Map.of());

    assertThat(table.rows()).hasSize(1);
    assertThat(table.rows().getFirst()).containsExactly("pipeshub_test");
  }

  @Test
  void aYieldMayBeFollowedByItsOwnReturn() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD name, type RETURN count(name) AS total", Map.of());

    assertThat(table.fields()).containsExactly("total");
    assertThat(table.rows()).hasSize(1);
    assertThat(((Number) table.rows().getFirst().getFirst()).longValue()).isEqualTo(3L);
  }

  @Test
  void aYieldOfEverythingKeepsEveryColumn() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD * WHERE name = 'system'", Map.of());

    assertThat(table.fields()).containsExactlyInAnyOrderElementsOf(FIELDS);
    assertThat(table.rows()).hasSize(1);
  }

  /**
   * A {@code WHERE} that appears inside a value is a value: the tail is found with the openCypher lexer, not by
   * searching the text.
   */
  @Test
  void aWhereInsideAStringLiteralIsNotATail() {
    assertThat(ShowCommandTail.hasTail("SHOW DATABASES YIELD name WHERE name = 'where'")).isTrue();
    assertThat(ShowCommandTail.hasTail("CALL dbms.components()")).isFalse();
  }

  @Test
  void aReturnMayOrderAndLimitTheProjection() {
    final ShowCommandTail.Table table = apply("SHOW DATABASES YIELD name RETURN name ORDER BY name DESC LIMIT 2",
        Map.of());

    assertThat(table.fields()).containsExactly("name");
    assertThat(table.rows()).hasSize(2);
    assertThat(table.rows().getFirst()).containsExactly("system");
  }

  /**
   * A UNION is several commands, each with a tail of its own, and each answered separately by whoever produced its
   * rows - the shape Neo4j Desktop sends to read the schema. There is no single table for a trailing clause to
   * apply to, so the command is left exactly as it was answered before.
   */
  @Test
  void aUnionIsLeftAlone() {
    final String query = "CALL db.labels() YIELD label RETURN collect(label) AS result "
        + "UNION CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) AS result";

    assertThat(ShowCommandTail.hasTail(query)).isFalse();
    assertThat(ShowCommandTail.apply(database, query, FIELDS, ROWS, Map.of()).rows()).isEqualTo(ROWS);
  }

  /**
   * A tail that cannot be parsed is a client error, and reaches the client as one instead of being swallowed
   * along with the filtering it asked for.
   */
  @Test
  void aBrokenTailIsReported() {
    assertThatThrownBy(() -> apply("SHOW DATABASES WHERE name ==== 'beer'", Map.of()))
        .isInstanceOf(CommandParsingException.class);
  }

  private ShowCommandTail.Table apply(final String query, final Map<String, Object> parameters) {
    return ShowCommandTail.apply(database, query, FIELDS, ROWS, parameters);
  }
}
