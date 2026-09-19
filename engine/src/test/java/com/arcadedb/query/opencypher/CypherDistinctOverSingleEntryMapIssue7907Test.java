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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7907, end to end. The openCypher map-literal evaluator takes a single-entry fast path into
 * {@code com.arcadedb.utility.SingletonMap}, whose {@code hashCode()} dereferenced the value without a null guard,
 * so any hashing operator over a one-entry map literal with a null value - {@code DISTINCT},
 * {@code collect(DISTINCT ...)}, {@code count(DISTINCT ...)} - died with a raw NullPointerException reaching the
 * client. The identical query with a second map entry succeeded, because arity 2+ builds a {@code LinkedHashMap}.
 * <p>
 * Neo4j, the openCypher reference implementation, answers all of these: a map is an ordinary value, a null entry in
 * one is ordinary, and two maps that are equal collapse under DISTINCT. So the arity-1 behaviour was ours alone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDistinctOverSingleEntryMapIssue7907Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-7907-distinct-singleton-map");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      final MutableDocument alice = database.newVertex("Person");
      alice.set("name", "Alice");
      alice.set("city", "Rome");
      alice.save();

      // No 'city' at all: n.city evaluates to null, which is the common way to reach a null-valued map entry.
      final MutableDocument bob = database.newVertex("Person");
      bob.set("name", "Bob");
      bob.save();
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
  void distinctOverASingleEntryMapLiteralWithANullValue() {
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Person) RETURN DISTINCT {city: n.city} AS m")) {
      final List<Object> maps = rs.stream().map(r -> r.getProperty("m")).map(Object.class::cast).toList();
      assertThat(maps).hasSize(2);
    }
  }

  @Test
  void collectDistinctOverASingleEntryMapLiteralWithANullValue() {
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Person) RETURN collect(DISTINCT {city: n.city}) AS m")) {
      assertThat(rs.hasNext()).isTrue();
      final List<?> collected = rs.next().getProperty("m");
      assertThat(collected).hasSize(2);
    }
  }

  @Test
  void countDistinctOverRepeatedNullValuedMaps() {
    try (final ResultSet rs = database.query("cypher", "UNWIND [{a: null}, {a: null}] AS m RETURN count(DISTINCT m) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }

  /**
   * Pure literals, no data at all: the narrowest form of the defect.
   */
  @Test
  void distinctOverAPureNullValuedMapLiteral() {
    try (final ResultSet rs = database.query("cypher", "RETURN DISTINCT {a: null} AS m")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> m = rs.next().getProperty("m");
      assertThat(m).hasSize(1).containsEntry("a", null);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * The arity-2 form always worked; kept so the fast path and the general path stay pinned to the same answer.
   */
  @Test
  void theArityTwoFormStillAgrees() {
    try (final ResultSet rs = database.query("cypher", "RETURN DISTINCT {a: null, b: null} AS m")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Map<?, ?>) rs.next().getProperty("m")).hasSize(2);
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
