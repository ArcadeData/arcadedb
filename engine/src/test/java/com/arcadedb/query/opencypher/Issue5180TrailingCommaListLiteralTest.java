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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5180: a trailing comma in a Cypher list literal
 * ({@code [1, 2,]}) must be accepted and parse identically to {@code [1, 2]}, matching
 * Neo4j and Memgraph. ArcadeDB previously rejected it with a syntax error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5180TrailingCommaListLiteralTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./databases/test-5180-trailing-comma-list").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void trailingCommaInListLiteralIsAccepted() {
    try (final ResultSet rs = database.query("opencypher", "RETURN [1, 2,] AS x")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((List<Object>) r.getProperty("x")).containsExactly(1L, 2L);
    }
  }

  @Test
  void listLiteralWithoutTrailingCommaStillWorks() {
    try (final ResultSet rs = database.query("opencypher", "RETURN [1, 2] AS x")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((List<Object>) r.getProperty("x")).containsExactly(1L, 2L);
    }
  }

  @Test
  void singleElementWithTrailingComma() {
    try (final ResultSet rs = database.query("opencypher", "RETURN [42,] AS x")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((List<Object>) r.getProperty("x")).containsExactly(42L);
    }
  }

  @Test
  void emptyListStillWorks() {
    try (final ResultSet rs = database.query("opencypher", "RETURN [] AS x")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((List<Object>) r.getProperty("x")).isEmpty();
    }
  }

  @Test
  void trailingCommaWithMixedElements() {
    try (final ResultSet rs = database.query("opencypher", "RETURN [1, 'a', true,] AS x")) {
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      assertThat((List<Object>) r.getProperty("x")).containsExactly(1L, "a", true);
    }
  }
}
