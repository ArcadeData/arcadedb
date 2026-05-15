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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4210.
 * <p>
 * UNWIND $batch MERGE with a property value that is a single {@code '} character
 * (or any string that starts AND ends with a quote) must not throw a
 * {@code StringIndexOutOfBoundsException} ({@code Range [1, 0) out of bounds for length 1}).
 */
class Issue4210MergeUnwindSingleQuoteTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4210-merge-unwind-single-quote").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeWithSingleQuotePropertyValueDoesNotCrash() {
    final String query = """
        UNWIND $batch AS BatchEntry
        MERGE (n:NER {identity: BatchEntry.identity, name: BatchEntry.name})
        ON CREATE SET n._temp_created = true
        ON MATCH SET n._temp_created = false
        WITH n, n._temp_created AS created
        REMOVE n._temp_created
        RETURN ID(n) AS id, created
        """;

    final List<Map<String, Object>> batch = new ArrayList<>();

    final Map<String, Object> entry1 = new HashMap<>();
    entry1.put("identity", "normal");
    entry1.put("name", "normal");
    batch.add(entry1);

    // Single quote character: the original crash case
    final Map<String, Object> entryQuote = new HashMap<>();
    entryQuote.put("identity", "'");
    entryQuote.put("name", "'");
    batch.add(entryQuote);

    // Starts and ends with quote but length > 1
    final Map<String, Object> entryQuotedString = new HashMap<>();
    entryQuotedString.put("identity", "'wrapped'");
    entryQuotedString.put("name", "'wrapped'");
    batch.add(entryQuotedString);

    // Contains a quote but not both start+end
    final Map<String, Object> entryPartialQuote = new HashMap<>();
    entryPartialQuote.put("identity", "it's");
    entryPartialQuote.put("name", "it's");
    batch.add(entryPartialQuote);

    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(4);
      }
    });

    // Second run - all must match (ON MATCH path)
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        int count = 0;
        while (rs.hasNext()) {
          final var row = rs.next();
          assertThat(row.<Boolean>getProperty("created")).isFalse();
          count++;
        }
        assertThat(count).isEqualTo(4);
      }
    });
  }

  @Test
  void mergeWithQuotePropertyPreservesExactValue() {
    final String query = """
        UNWIND $batch AS entry
        MERGE (n:Token {value: entry.value})
        RETURN n.value AS val
        """;

    final Map<String, Object> entry = new HashMap<>();
    entry.put("value", "'");
    final List<Object> batch = List.of(entry);

    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    database.transaction(() -> database.command("opencypher", query, params).close());

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Token) RETURN n.value AS val")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("val")).isEqualTo("'");
    }
  }

  @Test
  void matchNodeWithSingleQuotePropertyViaUnwindDoesNotCrash() {
    // Creates nodes and then MATCHes them using UNWIND-supplied property values.
    // MatchNodeStep.matchesProperties must not strip quotes from evaluated values.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag {name: \"'\"})");
      database.command("opencypher", "CREATE (:Tag {name: \"'wrapped'\"})");
      database.command("opencypher", "CREATE (:Tag {name: 'plain'})");
    });

    final List<Map<String, Object>> batch = List.of(
        Map.of("name", "'"),
        Map.of("name", "'wrapped'"),
        Map.of("name", "plain")
    );
    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    try (final ResultSet rs = database.query("opencypher",
        "UNWIND $batch AS item MATCH (n:Tag {name: item.name}) RETURN n.name AS name", params)) {
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(3);
    }
  }

  @Test
  void variableLengthPathEndpointWithSingleQuotePropertyDoesNotCrash() {
    // ExpandPathStep.matchesTargetProperties: VLP endpoint with a quote-containing literal.
    // Pre-fix, quote-stripping would have transformed the literal "'" into the empty string,
    // causing zero matches; post-fix, the literal is compared as-is.
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Node {tag: 'start'})");
      database.command("opencypher", "CREATE (:Node {tag: \"'\"})");
      database.command("opencypher",
          "MATCH (a:Node {tag: 'start'}), (b:Node {tag: \"'\"}) CREATE (a)-[:LINK]->(b)");
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH (:Node {tag: 'start'})-[:LINK*1..3]->(m:Node {tag: \"'\"}) RETURN m.tag AS tag")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("tag")).isEqualTo("'");
    }
  }

  @Test
  void matchRelationshipWithQuoteLiteralEdgePropertyDoesNotCrash() {
    // MatchRelationshipStep.matchesEdgeProperties: inline edge property filter with a literal
    // value that starts and ends with a quote character. Pre-fix this would have been stripped
    // to the empty string; post-fix it round-trips correctly.
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (:Person {name: 'Bob'})");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {note: \"'\"}]->(b)");
    });

    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[r:KNOWS {note: \"'\"}]->() RETURN r.note AS note")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("note")).isEqualTo("'");
    }
  }

  @Test
  void matchRelationshipEndpointWithSingleQuotePropertyViaUnwindDoesNotCrash() {
    // Creates edges and MATCHes them using UNWIND-supplied endpoint node property filters.
    // MatchRelationshipStep.matchesTargetProperties evaluates expression values and must not
    // strip quotes when the evaluated value starts and ends with a quote character.
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (:Person {name: \"'\"})");
      database.command("opencypher", "CREATE (:Person {name: 'plain'})");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: \"'\"}) CREATE (a)-[:KNOWS]->(b)");
      database.command("opencypher",
          "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'plain'}) CREATE (a)-[:KNOWS]->(b)");
    });

    final List<Map<String, Object>> batch = List.of(
        Map.of("name", "'"),
        Map.of("name", "plain")
    );
    final Map<String, Object> params = new HashMap<>();
    params.put("batch", batch);

    try (final ResultSet rs = database.query("opencypher",
        "UNWIND $batch AS item MATCH (:Person)-[:KNOWS]->(m:Person {name: item.name}) RETURN m.name AS name", params)) {
      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isNotNull();
        count++;
      }
      assertThat(count).isEqualTo(2);
    }
  }
}
