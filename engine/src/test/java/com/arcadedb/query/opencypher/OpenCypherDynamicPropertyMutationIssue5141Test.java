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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5141: dynamic bracket property mutations
 * ({@code SET n[key] = value} and {@code REMOVE n[key]}) were silently ignored.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherDynamicPropertyMutationIssue5141Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-5141").create();
    database.getSchema().createVertexType("DS");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void setBracketLiteralKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (d:DS {name: 'dyn'})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (d:DS {name: 'dyn'}) SET d['propA'] = 'hello' RETURN d.propA AS propA");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("propA")).isEqualTo("hello");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (d:DS {name: 'dyn'}) RETURN d.propA AS propA");
    assertThat(verify.next().<String>getProperty("propA")).isEqualTo("hello");
  }

  @Test
  void setBracketComputedKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (d:DS {name: 'dyn'})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (d:DS {name: 'dyn'}) WITH d, 'key_' + toString(1) AS k SET d[k] = 'world' RETURN d[k] AS value");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("world");
    });

    final ResultSet verify = database.query("opencypher", "MATCH (d:DS {name: 'dyn'}) RETURN d.key_1 AS value");
    assertThat(verify.next().<String>getProperty("value")).isEqualTo("world");
  }

  @Test
  void removeBracketLiteralKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (d:DS {name: 'dyn', toRemove: 42})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (d:DS {name: 'dyn'}) REMOVE d['toRemove'] RETURN d.toRemove AS toRemove");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("toRemove")).isNull();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (d:DS {name: 'dyn'}) RETURN d.toRemove AS toRemove");
    assertThat(verify.next().<Object>getProperty("toRemove")).isNull();
  }

  @Test
  void removeBracketComputedKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (d:DS {name: 'dyn', toRemove: 42})"));

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (d:DS {name: 'dyn'}) WITH d, 'to' + 'Remove' AS k REMOVE d[k] RETURN d.toRemove AS toRemove");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("toRemove")).isNull();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (d:DS {name: 'dyn'}) RETURN d.toRemove AS toRemove");
    assertThat(verify.next().<Object>getProperty("toRemove")).isNull();
  }

  @Test
  void setBracketNullKeyRemovesProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (d:DS {name: 'dyn', toRemove: 42})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (d:DS {name: 'dyn'}) SET d['toRemove'] = null"));

    final ResultSet verify = database.query("opencypher", "MATCH (d:DS {name: 'dyn'}) RETURN d.toRemove AS toRemove");
    assertThat(verify.next().<Object>getProperty("toRemove")).isNull();
  }
}
