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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.function.procedure.Procedure;
import com.arcadedb.query.opencypher.procedures.merge.MergeNode;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8124: the "optional trailing argument, resolve to APOC's default" step is one helper,
 * {@link Procedure#argOrDefault}, instead of a private copy per procedure family - and the copies disagreed on an
 * explicit {@code null}. The shared rule: an explicit {@code null} reads as the default, exactly as an absent argument.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8124ProcedureArgOrDefaultTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue8124-arg-or-default");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void argOrDefaultReadsAbsentAndNullAsTheDefault() {
    final Procedure procedure = new MergeNode();
    final Map<String, Object> defaultValue = Map.of();
    final Map<String, Object> given = Map.of("k", 1);

    assertThat(procedure.argOrDefault(new Object[] { "a" }, 1, defaultValue)).isSameAs(defaultValue);
    assertThat(procedure.argOrDefault(new Object[] { "a", null }, 1, defaultValue)).isSameAs(defaultValue);
    assertThat(procedure.argOrDefault(new Object[] { "a", given }, 1, defaultValue)).isSameAs(given);
    assertThat(procedure.argOrDefault(null, 0, defaultValue)).isSameAs(defaultValue);
    assertThat(procedure.argOrDefault(new Object[] { "a" }, 1, null)).isNull();
  }

  @Test
  void mergeNodeTakesAnExplicitNullAsTheEmptyMap() {
    assertThat(single("CALL merge.node(['Person'], {name: 'John'}, null, null) YIELD node RETURN node.name AS v")).isEqualTo("John");
    // Matched this time: the explicit null onMatchProps changes nothing
    assertThat(single("CALL merge.node(['Person'], {name: 'John'}, null, null) YIELD node RETURN node.name AS v")).isEqualTo("John");
    assertThat(number("MATCH (p:Person) RETURN count(p) AS v")).isEqualTo(1L);
  }

  @Test
  void mergeRelationshipTakesAnExplicitNullAsTheEmptyMap() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'A'}), (:Person {name: 'B'})").close());
    final String merge = """
        MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'})
        CALL merge.relationship(a, 'KNOWS', {}, {since: 1}, b, null) YIELD rel RETURN rel.since AS v""";
    assertThat(number(merge)).isEqualTo(1L);
    assertThat(number(merge)).isEqualTo(1L);
    // The five-argument form still answers the same
    assertThat(number("""
        MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'})
        CALL merge.relationship(a, 'KNOWS', {}, {since: 1}, b) YIELD rel RETURN rel.since AS v""")).isEqualTo(1L);
  }

  @Test
  void doWhenTakesAnExplicitNullAsTheEmptyMap() {
    assertThat(number("CALL do.when(true, 'RETURN 7 AS x', '', null) YIELD value RETURN value.x AS v")).isEqualTo(7L);
    assertThat(number("CALL do.when(true, 'RETURN 7 AS x', '') YIELD value RETURN value.x AS v")).isEqualTo(7L);
  }

  @Test
  void refactorTakesAnExplicitNullAsTheEmptyMap() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'A'}), (:Person {name: 'B'})").close());
    assertThat(number("""
        MATCH (p:Person) WITH collect(p) AS nodes
        CALL refactor.mergeNodes(nodes, null) YIELD node RETURN count(node) AS v""")).isEqualTo(1L);
    assertThat(number("MATCH (p:Person) RETURN count(p) AS v")).isEqualTo(1L);
  }

  private Object single(final String query) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty("v"));
        }
      }
    });
    assertThat(values).hasSize(1);
    return values.getFirst();
  }

  private long number(final String query) {
    return ((Number) single(query)).longValue();
  }
}
