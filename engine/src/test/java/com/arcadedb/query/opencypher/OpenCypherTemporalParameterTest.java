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
 * Regression tests for issue #4905: a native {@code java.time} / {@code java.util.Date} temporal query
 * parameter must participate in temporal comparison against a stored temporal, so a plain
 * {@code WHERE e.valid_at <= $reference_time} matches without any {@code datetime(...)} wrapping.
 * <p>
 * This is the exact graphiti {@code retrieve_episodes()} scenario. Before the fix the parameter reached
 * the comparison as a raw java value (never a {@link com.arcadedb.query.opencypher.temporal.CypherTemporalValue}),
 * so the temporal branch was skipped and the predicate silently matched nothing.
 */
class OpenCypherTemporalParameterTest {

  private static final ZonedDateTime BEFORE    = ZonedDateTime.of(2026, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime REFERENCE = ZonedDateTime.of(2026, 7, 2, 0, 0, 0, 0, ZoneOffset.UTC);
  private static final ZonedDateTime AFTER     = ZonedDateTime.of(2026, 7, 3, 0, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void nativeDatetimeParameterMatchesStoredNativeDatetime() {
    withDatabase(database -> {
      // Store valid_at as native datetimes (what the Bolt driver now sends after #4905's wire fix).
      createEpisode(database, "before", BEFORE);
      createEpisode(database, "reference", REFERENCE);
      createEpisode(database, "after", AFTER);

      // Plain comparison, native ZonedDateTime parameter - no datetime() wrapping.
      final Set<String> matched = queryValidAtLE(database, REFERENCE);
      assertThat(matched).containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void nativeDatetimeParameterMatchesStoredIsoString() {
    withDatabase(database -> {
      // Store valid_at as ISO-8601 strings (pre-wire-fix data / string writes). convertFromStorage
      // parses them back into temporals on read, and the native parameter is coerced to match.
      createEpisode(database, "before", BEFORE.toString());
      createEpisode(database, "reference", REFERENCE.toString());
      createEpisode(database, "after", AFTER.toString());

      final Set<String> matched = queryValidAtLE(database, REFERENCE);
      assertThat(matched).containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void acceptsDifferentNativeParameterTypes() {
    withDatabase(database -> {
      createEpisode(database, "before", BEFORE);
      createEpisode(database, "reference", REFERENCE);
      createEpisode(database, "after", AFTER);

      // java.util.Date parameter
      assertThat(queryValidAtLE(database, Date.from(REFERENCE.toInstant())))
          .containsExactlyInAnyOrder("before", "reference");
      // OffsetDateTime parameter
      assertThat(queryValidAtLE(database, REFERENCE.toOffsetDateTime()))
          .containsExactlyInAnyOrder("before", "reference");
      // Instant parameter
      assertThat(queryValidAtLE(database, REFERENCE.toInstant()))
          .containsExactlyInAnyOrder("before", "reference");
    });
  }

  @Test
  void mismatchedTemporalGranularityDoesNotMatch() {
    withDatabase(database -> {
      // Store valid_at as a date-only value (reads back as CypherDate).
      createEpisode(database, "reference", REFERENCE.toLocalDate());

      // Comparing a CypherDate against a full ZonedDateTime is a different-temporal-type comparison:
      // ordering (<=) yields null -> no match, equality (=) yields false -> no match. Exercises the
      // IllegalArgumentException branch in compareValuesTernary without throwing.
      final Set<String> ordered = queryValidAtLE(database, REFERENCE);
      assertThat(ordered).isEmpty();

      final Set<String> equal = new HashSet<>();
      final ResultSet rs = database.query("opencypher",
          "MATCH (e:Episodic) WHERE e.valid_at = $reference_time RETURN e.uuid AS uuid",
          Map.of("reference_time", REFERENCE));
      while (rs.hasNext())
        equal.add(rs.next().getProperty("uuid"));
      assertThat(equal).isEmpty();
    });
  }

  private static void createEpisode(final Database database, final String uuid, final Object validAt) {
    database.transaction(() ->
        database.command("opencypher", "CREATE (e:Episodic {uuid: $uuid, valid_at: $valid_at})",
            Map.of("uuid", uuid, "valid_at", validAt)));
  }

  private static Set<String> queryValidAtLE(final Database database, final Object referenceTime) {
    final Set<String> uuids = new HashSet<>();
    final ResultSet rs = database.query("opencypher",
        "MATCH (e:Episodic) WHERE e.valid_at <= $reference_time RETURN e.uuid AS uuid",
        Map.of("reference_time", referenceTime));
    while (rs.hasNext()) {
      final Result row = rs.next();
      uuids.add(row.getProperty("uuid"));
    }
    return uuids;
  }

  private static void withDatabase(final Consumer<Database> test) {
    final String path = "./target/testtemporalparam_" + System.nanoTime();
    final Database database = new DatabaseFactory(path).create();
    try {
      database.getSchema().getOrCreateVertexType("Episodic");
      test.accept(database);
    } finally {
      database.drop();
    }
  }
}
