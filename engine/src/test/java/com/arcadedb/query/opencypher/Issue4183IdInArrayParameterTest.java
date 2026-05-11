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
 * Engine-level reproducer for the e2e-python failure
 * {@code test_psycopg2_cypher_with_array_parameter_in_clause}.
 * <p>
 * After issue #4183 the {@code id()} function returns a Long-encoded RID. Engine callers that
 * round-trip {@code id()} via an {@code IN} parameter must therefore preserve the numeric type;
 * the wire-layer fix in {@code PostgresNetworkExecutor.getColumns} advertises Long columns as
 * INT8 so Postgres clients keep the type. This test pins the engine-level contract: a {@code List}
 * of {@code Long} ids and the legacy {@code List} of RID-strings both match every row, while a
 * {@code List} of numeric strings deliberately does <strong>not</strong> match - Cypher TCK
 * requires {@code 5 IN ["5"]} to return false, so numeric strings must not coerce into ids at
 * the comparator level.
 */
class Issue4183IdInArrayParameterTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-4183-in-array").create();
    database.getSchema().createVertexType("CHUNK");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:CHUNK {text: 'chunk1'})");
      database.command("opencypher", "CREATE (n:CHUNK {text: 'chunk2'})");
      database.command("opencypher", "CREATE (n:CHUNK {text: 'chunk3'})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * The shape an engine-API caller (and any wire client that preserves the numeric column type)
   * passes back: a {@code List<Long>} of encoded ids. Must match every row.
   */
  @Test
  void idInArrayParameterOfLongsMatchesAll() {
    final List<Long> ids = collectIdsAsLong();
    assertThat(ids).hasSize(3);

    final List<String> texts = runInQuery(ids);
    assertThat(texts).containsExactlyInAnyOrder("chunk1", "chunk2", "chunk3");
  }

  /**
   * Legacy {@code #bucketId:offset} RID-string shape. The cross-type coercion in
   * {@link com.arcadedb.query.opencypher.ast.InExpression} preserves this path so clients that
   * still send id values as RID strings keep working after #4183.
   */
  @Test
  void idInArrayParameterOfRidStringsMatchesAll() {
    final List<String> ids = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher", "MATCH (n:CHUNK) RETURN elementId(n) AS id ORDER BY n.text")) {
      while (rs.hasNext())
        ids.add((String) rs.next().getProperty("id"));
    }
    assertThat(ids).hasSize(3);

    final List<String> texts = runInQuery(ids);
    assertThat(texts).containsExactlyInAnyOrder("chunk1", "chunk2", "chunk3");
  }

  /**
   * Negative case: a {@code List} of numeric Strings (decimal form of the Long-encoded id) must
   * <strong>not</strong> match. Cypher TCK requires {@code 5 IN ["5"]} to return false; coercing
   * numeric strings into ids would silently break that contract. This is precisely why the
   * regression must be fixed at the wire layer (column type INT8) rather than the comparator.
   */
  @Test
  void idInArrayParameterOfNumericStringsMatchesNothing() {
    final List<String> ids = new ArrayList<>();
    for (final Long v : collectIdsAsLong())
      ids.add(Long.toString(v));
    assertThat(ids).hasSize(3);

    final List<String> texts = runInQuery(ids);
    assertThat(texts).isEmpty();
  }

  private List<Long> collectIdsAsLong() {
    final List<Long> ids = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher", "MATCH (n:CHUNK) RETURN ID(n) AS id ORDER BY n.text")) {
      while (rs.hasNext())
        ids.add(((Number) rs.next().getProperty("id")).longValue());
    }
    return ids;
  }

  private List<String> runInQuery(final List<?> ids) {
    final List<String> texts = new ArrayList<>();
    try (ResultSet rs = database.query("opencypher",
        "MATCH (n:CHUNK) WHERE ID(n) IN $ids RETURN n.text AS text",
        Map.of("ids", ids))) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        texts.add(r.getProperty("text"));
      }
    }
    return texts;
  }
}
