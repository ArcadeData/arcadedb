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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4222.
 * <p>
 * After {@code CREATE INDEX} on a Cypher node property, integer values were
 * silently coerced to strings on subsequent writes, and index-backed property
 * lookups returned empty. The trigger was the implicit property declaration
 * created by {@code CREATE INDEX} which hard-coded {@code Type.STRING} even
 * when the existing data already used a numeric type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4222CreateIndexCoercesIntegerToStringTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4222-create-index-coerces-int").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void integerPropertyValueIsPreservedAfterCreateIndex() {
    // PART 1 (control): no index. Integer round-trips correctly.
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");

    assertIntegerLookupsAllReturnOne("Part 1 (no index)");

    // PART 2 (broken before the fix): same sequence, but a CREATE INDEX inserted in the middle.
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.uuid)");
    database.command("opencypher", "MATCH (n) DETACH DELETE n");
    database.command("opencypher", "CREATE (a:Person {uuid: 1})");

    assertIntegerLookupsAllReturnOne("Part 2 (with CREATE INDEX)");
  }

  @Test
  void integerPropertyValueIsPreservedWhenIndexIsCreatedOnEmptyType() {
    // No data when the index is created => fall back to a type that does not coerce numbers.
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (n:Account) ON (n.id)");
    database.command("opencypher", "CREATE (a:Account {id: 42})");

    final ResultSet rs = database.query("opencypher", "MATCH (n:Account {id: 42}) RETURN n.id AS u");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    final Object value = r.getProperty("u");
    assertThat(value).isInstanceOf(Number.class);
    assertThat(((Number) value).longValue()).isEqualTo(42L);
  }

  private void assertIntegerLookupsAllReturnOne(final String description) {
    // 1. plain label match: returned uuid must still be a number (not "1").
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - label match has row").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - uuid retains numeric type").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).as(description + " - uuid value preserved").isEqualTo(1L);
    }

    // 2. inline property filter: must find the node by integer literal.
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person {uuid: 1}) RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - inline property filter finds node").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - inline lookup uuid numeric").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).isEqualTo(1L);
    }

    // 3. WHERE equality with integer literal: must find the node.
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Person) WHERE n.uuid = 1 RETURN n.uuid AS u")) {
      assertThat(rs.hasNext()).as(description + " - WHERE filter finds node").isTrue();
      final Object value = rs.next().getProperty("u");
      assertThat(value).as(description + " - WHERE lookup uuid numeric").isInstanceOf(Number.class);
      assertThat(((Number) value).longValue()).isEqualTo(1L);
    }
  }
}
