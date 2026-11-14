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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11575: MERGE and the inline map of a MATCH compared a stored list with the pattern's list through
 * Java {@code equals}, so a list of {@code Long} never matched the same list of {@code Integer}, and a primitive array
 * (what a numeric JSON array nested in an HTTP parameter becomes) matched nothing, not even an identical array: every
 * {@code UNWIND $rows AS row MERGE (n:T {key: row.key})} run created another node. Lists now compare element by element,
 * as {@code =} compares them.
 */
class CypherMergeNumericListKeyIssue11575Test extends TestHelper {

  @Test
  void mergeOnAPrimitiveArrayKeyFindsTheNodeItCreated() {
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("key", new long[] { 1, 2 })));
    for (int i = 0; i < 3; i++)
      database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row MERGE (n:T {key: row.key})", params));
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(1L);

    final Map<String, Object> doubles = Map.of("rows", List.of(Map.of("key", new double[] { 1.5, 2.5 })));
    for (int i = 0; i < 3; i++)
      database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row MERGE (n:D {key: row.key})", doubles));
    assertThat(count("MATCH (n:D) RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void mergeOnAListKeyMatchesAcrossNumberClasses() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:T {key: [1, 2]})"));
    database.transaction(() -> database.command("opencypher", "MERGE (n:T {key: $key})", Map.of("key", List.of(1, 2))));
    database.transaction(() -> database.command("opencypher", "MERGE (n:T {key: $key})", Map.of("key", new long[] { 1, 2 })));
    database.transaction(() -> database.command("opencypher", "MERGE (n:T {key: [1.0, 2.0]})"));
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(1L);

    database.transaction(() -> database.command("opencypher", "MERGE (n:T {key: [1, 2, 3]})"));
    database.transaction(() -> database.command("opencypher", "MERGE (n:T {key: [2, 1]})"));
    assertThat(count("MATCH (n:T) RETURN count(n) AS c")).isEqualTo(3L);
  }

  @Test
  void relationshipMergeOnAPrimitiveArrayKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A {id: 1}), (:A {id: 2})"));
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("key", new long[] { 1, 2 })));
    for (int i = 0; i < 3; i++)
      database.transaction(() -> database.command("opencypher",
          "UNWIND $rows AS row MATCH (a:A {id: 1}), (b:A {id: 2}) MERGE (a)-[r:R {key: row.key}]->(b)", params));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c")).isEqualTo(1L);
  }

  @Test
  void inlineMatchFindsAStoredPrimitiveArray() {
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("key", new long[] { 1, 2 })));
    database.transaction(() -> database.command("opencypher", "UNWIND $rows AS row CREATE (n:T {key: row.key})", params));
    assertThat(count("UNWIND $rows AS row MATCH (n:T {key: row.key}) RETURN count(n) AS c", params)).isEqualTo(1L);
    assertThat(count("MATCH (n:T {key: [1, 2]}) RETURN count(n) AS c", Map.of())).isEqualTo(1L);
    assertThat(count("MATCH (n:T {key: [1, 2, 3]}) RETURN count(n) AS c", Map.of())).isEqualTo(0L);
  }

  @Test
  void mergeOnABinaryKey() {
    final Map<String, Object> params = Map.of("key", new byte[] { 1, 2, 3 });
    for (int i = 0; i < 3; i++)
      database.transaction(() -> database.command("opencypher", "MERGE (n:B {key: $key})", params));
    assertThat(count("MATCH (n:B) RETURN count(n) AS c")).isEqualTo(1L);
  }

  @Test
  void mergeOnANestedListKeyMatchesAcrossNumberClasses() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:N {key: [[1, 2]]})"));
    database.transaction(() -> database.command("opencypher", "MERGE (n:N {key: $key})", Map.of("key", List.of(List.of(1, 2)))));
    database.transaction(() -> database.command("opencypher", "MERGE (n:N {key: $key})", Map.of("key", List.of(new long[] { 1, 2 }))));
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(1L);

    database.transaction(() -> database.command("opencypher", "MERGE (n:N {key: $key})", Map.of("key", List.of(List.of(2, 1)))));
    assertThat(count("MATCH (n:N) RETURN count(n) AS c")).isEqualTo(2L);
  }

  /** BINARY stays separate from numeric lists: a byte[] key never matches a list or long[] holding the same values. */
  @Test
  void aBinaryKeyNeverMatchesANumericListWithTheSameValues() {
    database.transaction(() -> database.command("opencypher", "MERGE (n:B {key: $key})", Map.of("key", new byte[] { 1, 2, 3 })));
    database.transaction(() -> database.command("opencypher", "MERGE (n:B {key: [1, 2, 3]})"));
    database.transaction(() -> database.command("opencypher", "MERGE (n:B {key: $key})", Map.of("key", new long[] { 1, 2, 3 })));
    // the list and the long[] are one numeric key; the byte[] is a second, separate node
    assertThat(count("MATCH (n:B) RETURN count(n) AS c")).isEqualTo(2L);
  }

  /** A native temporal parameter compares with a stored temporal inside a list as it does alone, and as {@code =} does. */
  @Test
  void aListOfNativeTemporalsMatchesAsEqualsDoes() {
    final LocalDateTime local = LocalDateTime.of(2024, 5, 6, 7, 8, 9);
    final Instant instant = local.toInstant(ZoneOffset.UTC);
    database.transaction(() -> database.command("opencypher", "CREATE (:E {d: $d, l: $l})",
        Map.of("d", local, "l", List.of(local))));
    final Map<String, Object> params = Map.of("d", instant, "l", List.of(instant));
    assertThat(count("MATCH (n:E) WHERE n.d = $d RETURN count(n) AS c", params)).isEqualTo(1L);
    assertThat(count("MATCH (n:E) WHERE n.l = $l RETURN count(n) AS c", params)).isEqualTo(1L);
    assertThat(count("MATCH (n:E {d: $d}) RETURN count(n) AS c", params)).isEqualTo(1L);
    assertThat(count("MATCH (n:E {l: $l}) RETURN count(n) AS c", params)).isEqualTo(1L);
    database.transaction(() -> database.command("opencypher", "MERGE (n:E {l: $l})", params));
    assertThat(count("MATCH (n:E) RETURN count(n) AS c")).isEqualTo(1L);
  }

  private long count(final String query) {
    return count(query, Map.of());
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
