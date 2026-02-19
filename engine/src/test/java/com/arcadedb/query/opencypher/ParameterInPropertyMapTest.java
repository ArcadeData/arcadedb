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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3476
 * Cypher: Fail to match when key is a param.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ParameterInPropertyMapTest {

  private static final String DB_PATH = "./target/testParamPropertyMap";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().getOrCreateVertexType("USER_RIGHTS");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:USER_RIGHTS {user_name: 'random_username_123'}) RETURN n");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void matchWithParameterInPropertyMap() {
    // This is the exact scenario from issue #3476:
    // MATCH with inline property map using a parameter should work
    final ResultSet result = database.query("opencypher",
        "MATCH (n:USER_RIGHTS {user_name: $username}) RETURN n",
        Map.of("username", "random_username_123"));

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    final Vertex vertex = row.getProperty("n");
    assertThat(vertex.getString("user_name")).isEqualTo("random_username_123");
    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void matchWithParameterInPropertyMapNoMatch() {
    // Parameter value that doesn't match any node
    final ResultSet result = database.query("opencypher",
        "MATCH (n:USER_RIGHTS {user_name: $username}) RETURN n",
        Map.of("username", "nonexistent_user"));

    assertThat(result.hasNext()).isFalse();
  }

  @Test
  void mergeWithParameterInPropertyMap() {
    // MERGE should find existing node when parameter matches
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (t:USER_RIGHTS {user_name: $username}) RETURN t",
          Map.of("username", "random_username_123"));

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = row.getProperty("t");
      assertThat(vertex.getString("user_name")).isEqualTo("random_username_123");
      assertThat(result.hasNext()).isFalse();
    });

    // Verify no duplicate was created
    final ResultSet allResults = database.query("opencypher",
        "MATCH (n:USER_RIGHTS) RETURN n");
    int count = 0;
    while (allResults.hasNext()) {
      allResults.next();
      count++;
    }
    assertThat(count).isEqualTo(1);
  }

  @Test
  void mergeWithParameterCreatesNewNode() {
    // MERGE with non-matching parameter should create a new node
    database.transaction(() -> {
      final ResultSet result = database.command("opencypher",
          "MERGE (t:USER_RIGHTS {user_name: $username}) RETURN t",
          Map.of("username", "new_user_456"));

      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();
      final Vertex vertex = row.getProperty("t");
      assertThat(vertex.getString("user_name")).isEqualTo("new_user_456");
    });

    // Verify two nodes now exist
    final ResultSet allResults = database.query("opencypher",
        "MATCH (n:USER_RIGHTS) RETURN n");
    int count = 0;
    while (allResults.hasNext()) {
      allResults.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }
}
