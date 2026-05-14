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
}
