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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5008: when a range index exists on the compared temporal property, the
 * query planner picks {@code NodeIndexRangeScan}, whose {@code compareValues()} compared the stored
 * value against the resolved predicate bound with a raw {@link Comparable#compareTo} - bypassing the
 * temporal normalization added in #4906. A native {@code java.time} parameter (e.g. a datetime sent
 * over Bolt) therefore threw {@code ClassCastException} instead of comparing against the stored temporal.
 * <p>
 * Same scenario as {@link OpenCypherTemporalParameterTest} but with an LSM index on the property so the
 * index-scan path (not the general {@code ComparisonExpression} path) is exercised.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRangeIndexTemporalTest {

  private static final ZonedDateTime BEFORE    = ZonedDateTime.of(2026, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime REFERENCE = ZonedDateTime.of(2026, 7, 2, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime AFTER     = ZonedDateTime.of(2026, 7, 3, 0, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void upperBoundNativeDatetimeParameterUsesIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE valid_at <= $ref : upper-bound-only path (full iterator + compareValues filter).
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid", REFERENCE))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void lowerBoundNativeDatetimeParameterUsesIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE valid_at >= $ref : lower-bound-only path (iterator from key).
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at >= $ref RETURN e.uuid AS uuid", REFERENCE))
          .containsExactlyInAnyOrder("reference", "after");
    });
  }

  @Test
  void bothBoundsNativeDatetimeParametersUseIndex() {
    withDatabase(database -> {
      createEpisodes(database);
      // WHERE $lo <= valid_at <= $hi : both-bounds range() path.
      assertThat(query(database,
          "MATCH (e:Episodic) WHERE e.valid_at >= $lo AND e.valid_at <= $hi RETURN e.uuid AS uuid",
          Map.of("lo", BEFORE, "hi", REFERENCE)))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void acceptsDifferentNativeParameterTypesOnIndexedProperty() {
    withDatabase(database -> {
      createEpisodes(database);
      // java.util.Date, OffsetDateTime and Instant params against the indexed datetime property.
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          Date.from(REFERENCE.toInstant())))
          .containsExactlyInAnyOrder("before", "reference");
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          REFERENCE.toOffsetDateTime()))
          .containsExactlyInAnyOrder("before", "reference");
      assertThat(query(database, "MATCH (e:Episodic) WHERE e.valid_at <= $ref RETURN e.uuid AS uuid",
          REFERENCE.toInstant()))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  private static void createEpisodes(final Database database) {
    database.transaction(() -> {
      createEpisode(database, "before", BEFORE);
      createEpisode(database, "reference", REFERENCE);
      createEpisode(database, "after", AFTER);
    });
  }

  private static void createEpisode(final Database database, final String uuid, final Object validAt) {
    database.command("opencypher", "CREATE (e:Episodic {uuid: $uuid, valid_at: $valid_at})",
        Map.of("uuid", uuid, "valid_at", validAt));
  }

  private static Set<String> query(final Database database, final String cypher, final Object ref) {
    return query(database, cypher, Map.of("ref", ref));
  }

  private static Set<String> query(final Database database, final String cypher, final Map<String, Object> params) {
    final Set<String> uuids = new HashSet<>();
    final ResultSet rs = database.query("opencypher", cypher, params);
    while (rs.hasNext()) {
      final Result row = rs.next();
      uuids.add(row.getProperty("uuid"));
    }
    return uuids;
  }

  private static void withDatabase(final Consumer<Database> test) {
    final String path = "./target/testrangeindextemporal_" + System.nanoTime();
    final Database database = new DatabaseFactory(path).create();
    try {
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final VertexType type = schema.createVertexType("Episodic");
        // DATETIME_MICROS stores as LocalDateTime, matching the ClassCastException in the issue's stack trace.
        type.createProperty("valid_at", Type.DATETIME_MICROS);
        type.createProperty("uuid", Type.STRING);
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Episodic", "valid_at");
      });
      test.accept(database);
    } finally {
      database.drop();
    }
  }
}
