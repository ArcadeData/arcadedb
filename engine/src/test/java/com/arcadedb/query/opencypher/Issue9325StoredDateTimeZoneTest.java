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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9325: a stored DATETIME has no zone on disk, so the engine materializes one. With
 * {@code arcadedb.dateTimeImplementation=java.time.ZonedDateTime} that zone was {@code UTC} while a {@code Z} literal's is
 * {@code Z}, and a stored value compared strictly below the literal for its own instant. With {@code java.util.Date} or
 * {@code java.time.Instant} it matched the {@code Z} spelling but not the same instant in another zone, because the
 * zone-less adoption rule never fired for a value already wrapped by the property read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9325StoredDateTimeZoneTest extends TestHelper {
  private static final String Z     = "datetime('2026-01-01T11:00:00Z')";
  private static final String PLUS1 = "datetime('2026-01-01T12:00:00+01:00')";

  @Test
  void zonedDateTimeImplementationStoredValueEqualsItsOwnInstant() {
    run(ZonedDateTime.class, () -> {
      assertThat(count("e.ts = " + Z)).isEqualTo(1L);
      assertThat(count("e.ts >= " + Z)).isEqualTo(1L);
      assertThat(count("e.ts <= " + Z)).isEqualTo(1L);
      assertThat(count("e.ts < datetime('2026-01-01T11:00:01Z')")).isEqualTo(1L);
      assertThat(count("e.ts = datetime('2026-01-01T11:00:00[UTC]')")).isEqualTo(1L);
      assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
      assertThat(count("e.ts = datetime('2026-01-01T11:00:01Z')")).isEqualTo(0L);
    });
  }

  @Test
  void dateImplementationStoredValueEqualsItsInstantInAnyZone() {
    run(Date.class, () -> {
      assertThat(count("e.ts = " + Z)).isEqualTo(1L);
      assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
      assertThat(count("e.ts >= " + PLUS1)).isEqualTo(1L);
      assertThat(count("e.ts < " + PLUS1)).isEqualTo(0L);
    });
  }

  @Test
  void instantImplementationStoredValueEqualsItsInstantInAnyZone() {
    run(Instant.class, () -> {
      assertThat(count("e.ts = " + Z)).isEqualTo(1L);
      assertThat(count("e.ts = " + PLUS1)).isEqualTo(1L);
    });
  }

  @Test
  void flagDoesNotLeakIntoTwoStoredValues() {
    run(ZonedDateTime.class, () -> {
      database.transaction(() -> database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save());
      try (final ResultSet rs = database.query("opencypher", "MATCH (a:E), (b:E) WHERE a.ts = b.ts RETURN count(*) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(4L);
      }
    });
  }

  @Test
  void storedValuesStillGroupAndDistinctAsOne() {
    run(ZonedDateTime.class, () -> {
      database.transaction(() -> database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save());
      try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) RETURN count(DISTINCT e.ts) AS c")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      }
    });
  }

  @Test
  void derivedValueIsAnOrdinaryZonedDateTime() {
    run(ZonedDateTime.class, () -> {
      // e.ts + PT0S is a new value: it keeps the materialized UTC zone and is NOT zone-less, so it matches only its own zone spelling
      assertThat(count("e.ts + duration('PT0S') = " + Z)).isEqualTo(1L);
      assertThat(count("e.ts + duration('PT0S') = " + PLUS1)).isEqualTo(0L);
    });
  }

  private void run(final Class<?> implementation, final Runnable body) {
    final var serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateTimeImplementation();
    try {
      serializer.setDateTimeImplementation(implementation);
      database.transaction(() -> {
        database.getSchema().createVertexType("E").createProperty("ts", Type.DATETIME);
        database.newVertex("E").set("ts", ZonedDateTime.parse("2026-01-01T11:00:00Z")).save();
      });
      body.run();
    } finally {
      serializer.setDateTimeImplementation(previous);
    }
  }

  private long count(final String predicate) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (e:E) WHERE " + predicate + " RETURN count(e) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
