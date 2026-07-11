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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5224: UNION with mismatched implicit column names must fail with a clean
 * query-validation error, not an HTTP 500 "Error on transaction commit".
 */
class Issue5224UnionColumnNamesTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5224").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mismatchedImplicitColumnNamesRejected() {
    database.transaction(() -> {
      database.command("cypher", "CREATE (a:A {id: 1}), (b:B {id: 2})");
    });

    database.transaction(() -> {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("cypher",
            "MATCH (a:A) RETURN a.id UNION MATCH (b:B) RETURN b.id")) {
          rs.stream().count();
        }
      }).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("same return column names");
    });
  }

  @Test
  void mismatchedImplicitColumnNamesExactReporterQuery() {
    database.transaction(() -> {
      database.command("cypher", "CREATE (a:A {id: 1}), (b:B {id: 2})");
    });

    // Exact reporter text: multi-line branches with a trailing semicolon.
    final String q = "MATCH (a:A)\nRETURN a.id\nUNION\nMATCH (b:B)\nRETURN b.id;";
    database.transaction(() -> {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("cypher", q)) {
          rs.stream().count();
        }
      }).isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("same return column names");
    });
  }

  @Test
  void matchingExplicitAliasesWork() {
    database.transaction(() -> {
      database.command("cypher", "CREATE (a:A {id: 1}), (b:B {id: 2})");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("cypher",
          "MATCH (a:A) RETURN a.id AS x UNION MATCH (b:B) RETURN b.id AS x")) {
        assertThat(rs.stream().count()).isEqualTo(2);
      }
    });
  }
}
