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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  @Test
  void legacyParameterAfterMapMergeIsRejected() {
    // 'SET n += {param}' - the map-merge operator '+=' is a value position.
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) SET p += {props} RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("{props}")
        .hasMessageContaining("$props");
  }

  @Test
  void legacyParameterAfterInequalityIsRejected() {
    // '!=' is the deprecated inequality operator but still a value position.
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) WHERE p.name != {name} RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("{name}")
        .hasMessageContaining("$name");
  }

  @Test
  void legacyParameterAfterModuloIsRejected() {
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) WHERE p.age % {mod} = 0 RETURN p"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("{mod}")
        .hasMessageContaining("$mod");
  }

  @Test
  void legacyParameterAfterConcatenationIsRejected() {
    assertThatThrownBy(() ->
        database.command("opencypher", "MATCH (p:Person) RETURN p.name || {suffix} AS x"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("{suffix}")
        .hasMessageContaining("$suffix");
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
  void nestedMapLiteralIsNotMistakenForLegacyParameter() {
    // A nested map literal with a real value ('{outer: {inner: p.name}}') must parse: the inner '}' is
    // preceded by 'p . name', so the token before it is not '{' and the legacy-param check does not fire.
    // (A bare '{inner}' after a colon is not a valid map value, so COLON as a value position is safe.)
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) WHERE p.name = 'Alice' RETURN {outer: {inner: p.name}} AS nested")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> nested = rs.next().getProperty("nested");
      final Map<String, Object> inner = (Map<String, Object>) nested.get("outer");
      assertThat(inner).containsEntry("inner", "Alice");
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

  @Test
  void mapProjectionOnKeywordNamedVariableIsNotMistakenForLegacyParameter() {
    // 'type' is a keyword token but a legal variable name, so 'type{name}' is a map projection whose '{'
    // is preceded by the variable (not a value operator) and must not be flagged as a legacy parameter.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (type:Person) WHERE type.name = 'Alice' WITH type, type.name AS name RETURN type{name} AS proj")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> proj = rs.next().getProperty("proj");
      assertThat(proj).containsEntry("name", "Alice");
    }
  }

  @Test
  void periodicCommitInsideStringLiteralIsNotFlagged() {
    // The token scan runs on the default channel, so 'PERIODIC COMMIT' inside a string literal (a single
    // string token) is not two keyword tokens and must not be flagged.
    try (final ResultSet rs = database.query("opencypher", "RETURN 'PERIODIC COMMIT' AS s")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("s")).isEqualTo("PERIODIC COMMIT");
    }
  }
}
