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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9169 asks whether an id that is only read, updated and deleted by equality should use UNIQUE_HASH. This pins the
 * part the answer depends on: every one of those statements, from SQL and from openCypher, is answered by the hash index
 * (and not by a scan) and gives the right result, including the delete, for the unique key of a graph and of a table.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9169HashIndexEqualityLookupTest extends TestHelper {
  private static final int ROWS = 2_000;

  @Test
  void sqlEqualityStatementsUseTheHashIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Crud");
      database.command("sql", "CREATE PROPERTY Crud.ckey LONG");
      database.command("sql", "CREATE INDEX ON Crud (ckey) UNIQUE_HASH").close();
      for (int i = 0; i < ROWS; i++)
        database.command("sql", "INSERT INTO Crud SET ckey = ?, pkey = ?, qty = ?", i, i * 2, i % 7).close();
    });

    assertThat(plan("sql", "EXPLAIN SELECT ckey, pkey, qty FROM Crud WHERE ckey = 42")).contains("FETCH FROM INDEX Crud[ckey]");
    assertThat(plan("sql", "EXPLAIN DELETE FROM Crud WHERE ckey = 42")).contains("FETCH FROM INDEX Crud[ckey]");

    try (final ResultSet rs = database.query("sql", "SELECT ckey, pkey, qty FROM Crud WHERE ckey = :c", Map.of("c", 42))) {
      assertThat(rs.next().<Integer>getProperty("pkey")).isEqualTo(84);
      assertThat(rs.hasNext()).isFalse();
    }

    database.transaction(() -> database.command("sql", "DELETE FROM Crud WHERE ckey = :c", Map.of("c", 42)).close());
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Crud WHERE ckey = 42")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Crud")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(ROWS - 1);
    }
  }

  @Test
  void cypherEqualityStatementsUseTheHashIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.command("sql", "CREATE INDEX ON Person (id) UNIQUE_HASH").close();
      for (int i = 0; i < ROWS; i++)
        database.command("opencypher", "CREATE (:Person {id: $id, name: 'n' + $id, age: $age})",
            Map.of("id", i, "age", i % 90)).close();
    });

    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person) WHERE p.id = 42 RETURN p.name AS name")) {
      rs.stream().count();
      assertThat(rs.getExecutionPlan()).isPresent();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("NodeIndexSeek");
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = $id RETURN p.name AS name, p.age AS age",
        Map.of("id", 42))) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("n42");
      assertThat(rs.hasNext()).isFalse();
    }

    database.transaction(() -> database.command("opencypher", "MATCH (q:Person) WHERE q.id = $id DETACH DELETE q",
        Map.of("id", 42)).close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = 42 RETURN count(p) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Person")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(ROWS - 1);
    }
  }

  private String plan(final String language, final String statement) {
    try (final ResultSet rs = database.command(language, statement)) {
      final StringBuilder sb = new StringBuilder();
      while (rs.hasNext()) {
        final Result r = rs.next();
        sb.append(r.toJSON());
      }
      return sb.toString();
    }
  }
}
