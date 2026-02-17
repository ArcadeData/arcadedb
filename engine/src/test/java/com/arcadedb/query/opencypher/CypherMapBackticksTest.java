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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for map literals with backticked keys.
 * Ensures that backticks are properly stripped from map keys in RETURN clauses.
 */
class CypherMapBackticksTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcyphermapbackticks").create();
    database.getSchema().createVertexType("DOCUMENT");
    database.getSchema().createVertexType("CHUNK");
    database.getSchema().createEdgeType("HAS_CHUNK");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mapLiteralWithBacktickedKeys() {
    // Create test data
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (d:DOCUMENT {name: 'Doc1'})-[:HAS_CHUNK]->(c:CHUNK {text: 'Content', llm_flag: true})");
    });

    // Query with map literal using backticked key (fixed direction to match CREATE)
    final ResultSet result = database.query("opencypher",
      "MATCH (d:DOCUMENT)-->(c:CHUNK) WHERE c.llm_flag = true " +
      "RETURN collect({`@rid`: ID(c), text: c.text}) AS chunks");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.hasProperty("chunks")).isTrue();

    final List<Map<String, Object>> chunks = (List<Map<String, Object>>) row.getProperty("chunks");
    assertThat(chunks).isNotEmpty();

    final Map<String, Object> firstChunk = chunks.get(0);

    // The key should be "@rid" without backticks
    assertThat(firstChunk).containsKey("@rid");
    assertThat(firstChunk).doesNotContainKey("`@rid`");
    assertThat(firstChunk).containsKey("text");
    assertThat(firstChunk.get("text")).isEqualTo("Content");
  }

  @Test
  void mapLiteralWithMultipleBacktickedKeys() {
    // Create test data
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (d:DOCUMENT {name: 'Doc1'})-[:HAS_CHUNK]->(c:CHUNK {text: 'Content'})");
    });

    // Query with multiple backticked keys (fixed direction to match CREATE)
    final ResultSet result = database.query("opencypher",
      "MATCH (d:DOCUMENT)-->(c:CHUNK) " +
      "RETURN {`@rid`: ID(c), `@type`: 'chunk', normalKey: c.text} AS data");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.hasProperty("data")).isTrue();

    final Map<String, Object> data = (Map<String, Object>) row.getProperty("data");

    // All keys should be without backticks
    assertThat(data).containsKey("@rid");
    assertThat(data).doesNotContainKey("`@rid`");
    assertThat(data).containsKey("@type");
    assertThat(data).doesNotContainKey("`@type`");
    assertThat(data).containsKey("normalKey");
    assertThat(data.get("@type")).isEqualTo("chunk");
    assertThat(data.get("normalKey")).isEqualTo("Content");
  }

  @Test
  void mapLiteralWithEscapedBackticks() {
    // Create test data
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (n:DOCUMENT {name: 'Test'})");
    });

    // Query with escaped backticks (`` -> `) in key name
    final ResultSet result = database.query("opencypher",
      "MATCH (n:DOCUMENT) " +
      "RETURN {`key``with``backticks`: n.name} AS data");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.hasProperty("data")).isTrue();

    final Map<String, Object> data = (Map<String, Object>) row.getProperty("data");

    // The key should have single backticks (`` escaped to `)
    assertThat(data).containsKey("key`with`backticks");
    assertThat(data.get("key`with`backticks")).isEqualTo("Test");
  }

  @Test
  void createWithBacktickedMapKeys() {
    // Test CREATE clause with backticked keys in map literal
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (n:DOCUMENT {`@special`: 'value1', normalKey: 'value2'})");
    });

    // Verify the properties were stored with correct key names (without backticks)
    final ResultSet result = database.query("opencypher",
      "MATCH (n:DOCUMENT) RETURN n.`@special` AS special, n.normalKey AS normal");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((Object) row.getProperty("special")).isEqualTo("value1");
    assertThat((Object) row.getProperty("normal")).isEqualTo("value2");
  }

  @Test
  void mapProjectionWithBacktickedKeys() {
    // Test map projection with backticked keys
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (n:DOCUMENT {name: 'Doc1', id: 123})");
    });

    // Query with map projection using backticked key
    final ResultSet result = database.query("opencypher",
      "MATCH (n:DOCUMENT) " +
      "RETURN n{.name, `@id`: n.id} AS data");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.hasProperty("data")).isTrue();

    final Map<String, Object> data = (Map<String, Object>) row.getProperty("data");

    // The keys should be without backticks
    assertThat(data).containsKey("name");
    assertThat(data).containsKey("@id");
    assertThat(data).doesNotContainKey("`@id`");
    assertThat(data.get("name")).isEqualTo("Doc1");
    assertThat(data.get("@id")).isEqualTo(123);
  }

  @Test
  void propertyAccessWithBackticks() {
    // Test that property access with backticks works correctly
    database.transaction(() -> {
      database.command("opencypher",
        "CREATE (n:DOCUMENT {`@id`: 'test123', `@type`: 'document', name: 'Test'})");
    });

    // Access properties using backticks
    final ResultSet result = database.query("opencypher",
      "MATCH (n:DOCUMENT) " +
      "RETURN n.`@id` AS id, n.`@type` AS type, n.name AS name");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat((Object) row.getProperty("id")).isEqualTo("test123");
    assertThat((Object) row.getProperty("type")).isEqualTo("document");
    assertThat((Object) row.getProperty("name")).isEqualTo("Test");
  }
}
