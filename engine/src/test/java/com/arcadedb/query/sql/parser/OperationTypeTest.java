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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.OperationType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests semantic operation type detection for SQL statements via the parser AST.
 */
class OperationTypeTest {

  private static Statement parse(final String query) {
    try {
      return new SqlParser(null, query).Parse();
    } catch (final ParseException e) {
      throw new RuntimeException("Failed to parse: " + query, e);
    }
  }

  @Test
  void selectIsRead() {
    final Statement stmt = parse("SELECT FROM Person");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.READ);
    assertThat(stmt.isIdempotent()).isTrue();
  }

  @Test
  void matchIsRead() {
    final Statement stmt = parse("MATCH {type: Person, as: p} RETURN p");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void traverseIsRead() {
    final Statement stmt = parse("TRAVERSE out() FROM #1:0");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void insertIsCreate() {
    final Statement stmt = parse("INSERT INTO Person SET name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.CREATE);
  }

  @Test
  void insertCaseInsensitive() {
    final Statement stmt = parse("insert into Person set name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.CREATE);
  }

  @Test
  void createVertexIsCreate() {
    final Statement stmt = parse("CREATE VERTEX Person SET name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.CREATE);
  }

  @Test
  void createEdgeIsCreate() {
    final Statement stmt = parse("CREATE EDGE Knows FROM #1:0 TO #2:0");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.CREATE);
  }

  @Test
  void updateIsUpdate() {
    final Statement stmt = parse("UPDATE Person SET name = 'Jane' WHERE name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.UPDATE);
  }

  @Test
  void upsertIsCreateAndUpdate() {
    final Statement stmt = parse("UPDATE Person SET name = 'John' UPSERT WHERE name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE);
  }

  @Test
  void deleteIsDelete() {
    final Statement stmt = parse("DELETE FROM Person WHERE name = 'John'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.DELETE);
  }

  @Test
  void createVertexTypeIsSchema() {
    final Statement stmt = parse("CREATE VERTEX TYPE MyVertex");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void createEdgeTypeIsSchema() {
    final Statement stmt = parse("CREATE EDGE TYPE MyEdge");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void createDocumentTypeIsSchema() {
    final Statement stmt = parse("CREATE DOCUMENT TYPE MyDoc");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void alterTypeIsSchema() {
    final Statement stmt = parse("ALTER TYPE Person CUSTOM myAttr = 'test'");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void dropTypeIsSchema() {
    final Statement stmt = parse("DROP TYPE Person IF EXISTS");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void createIndexIsSchema() {
    final Statement stmt = parse("CREATE INDEX ON Person (name) UNIQUE");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void dropIndexIsSchema() {
    final Statement stmt = parse("DROP INDEX `Person[name]`");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void createPropertyIsSchema() {
    final Statement stmt = parse("CREATE PROPERTY Person.age INTEGER");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.SCHEMA);
  }

  @Test
  void explainIsRead() {
    final Statement stmt = parse("EXPLAIN SELECT FROM Person");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void profileIsRead() {
    final Statement stmt = parse("PROFILE SELECT FROM Person");
    assertThat(stmt.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void moveVertexIsCreateUpdateAndDelete() {
    final Statement stmt = parse("MOVE VERTEX (SELECT FROM V LIMIT 1) TO TYPE:Person");
    assertThat(stmt.getOperationTypes()).containsExactlyInAnyOrder(OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE);
  }
}
