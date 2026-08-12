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
package com.arcadedb.query.opencypher.procedures.control;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the apoc.do.when Cypher procedure (registered as "do.when").
 */
class DoWhenTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-do-when");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void registeredUnderBothPlainAndApocPrefixedName() {
    assertThat(CypherProcedureRegistry.hasProcedure("do.when")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.do.when")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.do.when")).isSameAs(CypherProcedureRegistry.get("do.when"));
  }

  @Test
  void trueConditionRunsIfQuery() {
    final ResultSet rs = database.query("opencypher",
        "CALL apoc.do.when(true, 'RETURN 1 AS x', 'RETURN 2 AS x', {}) YIELD value RETURN value");

    assertThat(rs.hasNext()).isTrue();
    final Map<String, Object> value = (Map<String, Object>) rs.next().getProperty("value");
    assertThat(value.get("x")).isEqualTo(1L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void falseConditionRunsElseQuery() {
    final ResultSet rs = database.query("opencypher",
        "CALL apoc.do.when(false, 'RETURN 1 AS x', 'RETURN 2 AS x', {}) YIELD value RETURN value");

    assertThat(rs.hasNext()).isTrue();
    final Map<String, Object> value = (Map<String, Object>) rs.next().getProperty("value");
    assertThat(value.get("x")).isEqualTo(2L);
  }

  @Test
  void emptyElseQueryOnFalseConditionYieldsNoRows() {
    final ResultSet rs = database.query("opencypher",
        "CALL apoc.do.when(false, 'RETURN 1 AS x', '', {}) YIELD value RETURN value");

    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void paramsAreBoundIntoSubQuery() {
    final ResultSet rs = database.query("opencypher",
        "CALL apoc.do.when(true, 'RETURN $name AS greeting', '', {name: 'hello'}) YIELD value RETURN value");

    final Map<String, Object> value = (Map<String, Object>) rs.next().getProperty("value");
    assertThat(value.get("greeting")).isEqualTo("hello");
  }

  @Test
  void writeSubQueryIsPersisted() {
    final ResultSet rs = database.query("opencypher",
        "CALL apoc.do.when(true, \"CREATE (n:Person {name: 'Bob'}) RETURN n\", '', {}) YIELD value RETURN value");
    assertThat(rs.hasNext()).isTrue();
    rs.next();

    final ResultSet check = database.query("opencypher", "MATCH (p:Person {name: 'Bob'}) RETURN p");
    assertThat(check.hasNext()).isTrue();
  }

  @Test
  void wrongArgumentCountThrows() {
    assertThatThrownBy(() -> database.query("opencypher", "CALL apoc.do.when(true, 'RETURN 1') YIELD value RETURN value").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void nonBooleanConditionThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "CALL apoc.do.when(1, 'RETURN 1 AS x', '', {}) YIELD value RETURN value").hasNext())
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nonStringIfQueryThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "CALL apoc.do.when(true, 1, '', {}) YIELD value RETURN value").hasNext())
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nonStringElseQueryThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "CALL apoc.do.when(false, 'RETURN 1 AS x', 1, {}) YIELD value RETURN value").hasNext())
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nonMapParamsThrows() {
    assertThatThrownBy(() -> database.query("opencypher",
        "CALL apoc.do.when(true, 'RETURN 1 AS x', '', 'not-a-map') YIELD value RETURN value").hasNext())
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }
}
