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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;

/**
 * Issue #4141 (ISO GQL / Cypher 25 cleanup): deprecated/legacy syntax must be rejected with a clear,
 * actionable error pointing at the supported replacement.
 * <ul>
 *   <li>{@code PERIODIC COMMIT} -&gt; {@code CALL { ... } IN TRANSACTIONS}</li>
 *   <li>legacy {@code {param}} -&gt; {@code $param}</li>
 * </ul>
 * Also guards against false positives: map projections ({@code n{name}}) and brace-block expressions
 * ({@code COUNT { ... }}) must still parse, and the modern {@code $param} form must keep working.
 */
public class Issue4141DeprecatedSyntaxTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue4141DeprecatedSyntax");
    if (factory.exists())
      factory.open().drop(); // defend against a leftover db from a previously interrupted run
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (p:Person {name: 'Bob', age: 25})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---- Deprecated syntax is rejected with an actionable message -------------------------------

  @Test
  void periodicCommitIsRejectedWithHint() {
    assertThatThrownBy(() ->
        database.command("opencypher", "USING PERIODIC COMMIT 500 MATCH (p:Person) RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("PERIODIC COMMIT")
        .hasMessageContaining("IN TRANSACTIONS");
  }

  @Test
  void legacyParameterIsRejectedWithHint() {
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) WHERE p.name = {name} RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("{name}")
        .hasMessageContaining("$name");
  }

  @Test
  void legacyParameterIsRejectedEvenWhenSuppliedAsArg() {
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) WHERE p.name = {name} RETURN p", Map.of("name", "Alice")))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("$name");
  }

  // ---- Supported syntax keeps working (no false positives) ------------------------------------

  @Test
  void modernDollarParameterStillWorks() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age",
        Map.of("name", "Alice"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Integer) rs.next().getProperty("age")).isEqualTo(30);
    }
  }

  @Test
  void mapProjectionIsNotMistakenForLegacyParameter() {
    // 'p{name}' has the same token shape as a legacy parameter ('{' name '}') but is a map projection
    // (variable LCURLY variable RCURLY). It must parse, projecting the in-scope 'name' variable.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' WITH p, p.name AS name RETURN p{name} AS proj")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> proj = rs.next().getProperty("proj");
      assertThat(proj).containsEntry("name", "Alice");
    }
  }

  @Test
  void mapLiteralIsNotMistakenForLegacyParameter() {
    try (final ResultSet rs = database.query("opencypher", "RETURN {name: 'Alice'} AS m")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> m = rs.next().getProperty("m");
      assertThat(m).containsEntry("name", "Alice");
    }
  }

  @Test
  void existsBraceBlockIsNotMistakenForLegacyParameter() {
    // 'EXISTS { ... }' is a brace-block expression; the following '{' must not trigger the legacy-param check.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE EXISTS { (p) } RETURN p.name AS name ORDER BY name")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  void countBraceBlockIsNotMistakenForLegacyParameter() {
    // 'COUNT { ... }' is a brace-block expression; the following '{' must not trigger the legacy-param check.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) RETURN p.name AS name, COUNT { (p) } AS c ORDER BY name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }
}
