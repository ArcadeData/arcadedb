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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3132 - ID() function in WHERE clause with IN operator.
 *
 * Error: 'java.lang.UnsupportedOperationException', 'Function evaluation requires StatelessFunction'
 * Query: MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id
 *
 * The problem was that InExpression was calling expression.evaluate() directly on
 * FunctionCallExpression, which throws UnsupportedOperationException. Additionally,
 * when using parameter lists, the Collection values need to be expanded.
 */
class Issue3132Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-3132").create();
    database.getSchema().createVertexType("CHUNK");

    // Create test data
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        database.command("opencypher", "CREATE (n:CHUNK {text: 'Chunk " + i + "'})");
      }
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testIdFunctionInReturnClause() {
    // Verify ID() function works in RETURN clause (baseline test)
    ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id, n.text AS text");

    int count = 0;
    while (result.hasNext()) {
      Result r = result.next();
      String id = (String) r.getProperty("id");
      String text = (String) r.getProperty("text");
      assertThat(id).isNotNull();
      assertThat(text).isNotNull();
      count++;
    }

    assertThat(count).isEqualTo(5);
  }

  @Test
  void testIdFunctionInWhereClauseWithInOperator() {
    // Get all CHUNK node IDs
    ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id");
    List<String> allIds = new ArrayList<>();
    while (result.hasNext()) {
      String id = (String) result.next().getProperty("id");
      allIds.add(id);
    }

    assertThat(allIds).hasSize(5);

    // Test ID() function in WHERE clause with IN operator using subset - THIS WAS THE ISSUE
    List<String> queryIds = List.of(allIds.get(0), allIds.get(2), allIds.get(4));

    result = database.query("opencypher",
        "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
        Map.of("ids", queryIds));

    List<String> returnedIds = new ArrayList<>();
    List<String> texts = new ArrayList<>();
    while (result.hasNext()) {
      Result r = result.next();
      String id = (String) r.getProperty("id");
      String text = (String) r.getProperty("text");
      returnedIds.add(id);
      texts.add(text);
    }

    // Verify we got exactly the nodes we asked for
    assertThat(returnedIds).hasSize(3);
    assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(queryIds);
    assertThat(texts).containsExactlyInAnyOrder("Chunk 0", "Chunk 2", "Chunk 4");
  }

  @Test
  void testIdFunctionWithAllIds() {
    // Get all IDs
    ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id");
    List<String> allIds = new ArrayList<>();
    while (result.hasNext()) {
      String id = (String) result.next().getProperty("id");
      allIds.add(id);
    }

    // Test with all IDs (in different order than created)
    List<String> queryIds = List.of(allIds.get(2), allIds.get(1), allIds.get(0), allIds.get(4), allIds.get(3));

    result = database.query("opencypher",
        "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
        Map.of("ids", queryIds));

    List<String> returnedIds = new ArrayList<>();
    while (result.hasNext()) {
      Result r = result.next();
      String id = (String) r.getProperty("id");
      returnedIds.add(id);
    }

    // Should return all 5 chunks
    assertThat(returnedIds).hasSize(5);
    assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(allIds);
  }

  @Test
  void testIdFunctionWithTwoIds() {
    // Get all IDs
    ResultSet result = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id ORDER BY id");
    List<String> allIds = new ArrayList<>();
    while (result.hasNext()) {
      String id = (String) result.next().getProperty("id");
      allIds.add(id);
    }

    // Test with just 2 IDs
    List<String> queryIds = List.of(allIds.get(1), allIds.get(3));

    result = database.query("opencypher",
        "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text as text, ID(n) as id",
        Map.of("ids", queryIds));

    List<String> returnedIds = new ArrayList<>();
    while (result.hasNext()) {
      Result r = result.next();
      String id = (String) r.getProperty("id");
      returnedIds.add(id);
    }

    // Should return exactly 2 chunks
    assertThat(returnedIds).hasSize(2);
    assertThat(returnedIds).containsExactlyInAnyOrderElementsOf(queryIds);
  }
}
