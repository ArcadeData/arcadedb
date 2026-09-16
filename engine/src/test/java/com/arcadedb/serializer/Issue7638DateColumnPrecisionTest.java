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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7638: a {@code DATE} column lost or mis-formatted its precision on the way out to
 * JSON, in three related ways with one root cause between them.
 * <ol>
 *   <li><b>The root cause.</b> {@code arcadedb.dateImplementation=java.util.Date} was not honoured on read,
 *   because it was not honoured on WRITE either: the write path coerces a value into the column's configured Java
 *   class ({@code Type.getJavaImplementation}), and {@code BinaryTypes.getTypeFromValue} then classified any
 *   {@code java.util.Date} as {@code DATETIME} regardless of what the schema declared - so under that setting
 *   every value written to a DATE column was stored as epoch millis under a DATE declaration, and read back as
 *   the {@code dateTimeImplementation} (a {@code LocalDateTime}) instead. The four {@code java.time} branches
 *   beside it already asked the schema; the {@code Date}/{@code Calendar} one did not.</li>
 *   <li><b>The column-list projection.</b> {@code SELECT d FROM T} produces a non-element {@link Result}, so
 *   {@code JsonSerializer.serializeResult()} resolved no schema type for any property. Harmless while the value's
 *   Java class says what the column was - and a {@code java.util.Date} does not, so a genuine DATE came out as
 *   {@code "2026-06-12 00:00:00"}. The projection now records which column each value was read from.</li>
 *   <li><b>{@code serializeDocument()}'s formatting slot.</b> It set {@code JSONObject}'s date slot to the
 *   DATE-TIME pattern while {@code serializeResult()} set it to the date one, so the same column rendered
 *   differently through {@code GET /document/&#123;db&#125;/&#123;rid&#125;} than through a query.</li>
 * </ol>
 * A fourth, found while fixing the first: {@code JSONObject}'s {@code Date} branch renders in the JVM's DEFAULT
 * zone, while a DATE is stored as a day count and materialised as UTC midnight - so west of Greenwich every date
 * came out a day early. A DATE-typed value is now formatted explicitly, UTC-anchored, rather than left to that
 * dispatch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7638DateColumnPrecisionTest extends TestHelper {
  private static final LocalDate DAY = LocalDate.of(2026, 6, 12);

  /**
   * The root cause, at the storage layer: with {@code dateImplementation=java.util.Date} a DATE column must still
   * be stored and read back AS a date. Before the fix the value came back as a {@code LocalDateTime}, which is the
   * DATETIME materialisation - proof the column had silently become a DATETIME on disk.
   */
  @Test
  void aDateColumnIsStillADateWhenTheConfiguredImplementationIsJavaUtilDate() {
    withDateImplementation(Date.class, () -> {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638Date").createProperty("d", Type.DATE);
        database.newDocument("Issue7638Date").set("d", DAY).save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Date")) {
          final Object read = rs.next().getProperty("d");
          assertThat(read).as("the configured dateImplementation must be honoured on read").isInstanceOf(Date.class);
          assertThat(((Date) read).getTime()).as("a DATE is UTC midnight of the stored day")
              .isEqualTo(DAY.toEpochDay() * 86_400_000L);
        }
      });
    });
  }

  /**
   * The same for {@code java.util.Calendar}, the other configurable non-{@code java.time} representation: it took
   * the identical branch and so had the identical defect.
   */
  @Test
  void aDateColumnIsStillADateWhenTheConfiguredImplementationIsCalendar() {
    withDateImplementation(Calendar.class, () -> {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638Cal").createProperty("d", Type.DATE);
        database.newDocument("Issue7638Cal").set("d", DAY).save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Cal")) {
          final Object read = rs.next().getProperty("d");
          assertThat(read).isInstanceOf(Calendar.class);
          assertThat(((Calendar) read).getTimeInMillis()).isEqualTo(DAY.toEpochDay() * 86_400_000L);
        }
      });
    });
  }

  /**
   * The reported symptom, on all three JSON surfaces at once: a column list, a {@code SELECT *} row and the
   * document endpoint must all answer the same date-only string. The column list is the case #7638 reported; the
   * document one is its point 3; {@code SELECT *} is the control that was already right and must stay right.
   */
  @Test
  void everyJsonSurfaceAnswersTheSameDateOnlyString() {
    withDateImplementation(Date.class, () -> {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638Json").createProperty("d", Type.DATE);
        database.newDocument("Issue7638Json").set("d", DAY).save();
      });

      database.transaction(() -> {
        final JsonSerializer serializer = new JsonSerializer(database);

        try (final ResultSet rs = database.query("sql", "SELECT d FROM Issue7638Json")) {
          final JSONObject json = serializer.serializeResult(database, rs.next());
          assertThat(json.getString("d")).as("a column list must not invent a time of day").isEqualTo("2026-06-12");
        }

        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Json")) {
          final Result row = rs.next();
          assertThat(serializer.serializeResult(database, row).getString("d")).isEqualTo("2026-06-12");
          assertThat(serializer.serializeDocument(row.toElement()).getString("d"))
              .as("serializeDocument() used the DATE-TIME slot, so the document endpoint disagreed with the query")
              .isEqualTo("2026-06-12");
        }
      });
    });
  }

  /**
   * An alias must carry the source column's type with it: the value published under {@code x} is still the value
   * of the DATE column {@code d}, so it has to render the same way.
   */
  @Test
  void anAliasedColumnKeepsItsSourceColumnsType() {
    withDateImplementation(Date.class, () -> {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638Alias").createProperty("d", Type.DATE);
        database.newDocument("Issue7638Alias").set("d", DAY).save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT d AS x FROM Issue7638Alias")) {
          final Result row = rs.next();
          assertThat(row.getPropertyType("x")).isEqualTo(Type.DATE);
          assertThat(new JsonSerializer(database).serializeResult(database, row).getString("x")).isEqualTo("2026-06-12");
        }
      });
    });
  }

  /**
   * The projection must only claim a type for a value that really is the column's. A computed expression over the
   * same column is not, and reporting DATE for it would push the DATE formatting onto whatever it produced.
   */
  @Test
  void aComputedProjectionClaimsNoSourceColumnType() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Issue7638Computed");
      type.createProperty("d", Type.DATE);
      type.createProperty("n", Type.INTEGER);
      database.newDocument("Issue7638Computed").set("d", DAY).set("n", 7).save();
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT n + 1 AS computed, d FROM Issue7638Computed")) {
        final Result row = rs.next();
        assertThat(row.getPropertyType("computed")).as("an expression has no source column").isNull();
        assertThat(row.getPropertyType("d")).as("a plain column reference does").isEqualTo(Type.DATE);
        assertThat(row.getPropertyType("absent")).isNull();
      }
    });
  }

  /**
   * The default representation must be untouched: {@code java.time.LocalDate} carries "this is a date" in its own
   * class, so it never needed the schema and must keep rendering exactly as it did.
   */
  @Test
  void theDefaultLocalDateRepresentationIsUnchanged() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Issue7638Default").createProperty("d", Type.DATE);
      database.newDocument("Issue7638Default").set("d", DAY).save();
    });

    database.transaction(() -> {
      final JsonSerializer serializer = new JsonSerializer(database);
      try (final ResultSet rs = database.query("sql", "SELECT d FROM Issue7638Default")) {
        assertThat(serializer.serializeResult(database, rs.next()).getString("d")).isEqualTo("2026-06-12");
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Default")) {
        final Result row = rs.next();
        assertThat((Object) row.getProperty("d")).isInstanceOf(LocalDate.class);
        assertThat(serializer.serializeResult(database, row).getString("d")).isEqualTo("2026-06-12");
        assertThat(serializer.serializeDocument(row.toElement()).getString("d")).isEqualTo("2026-06-12");
      }
    });
  }

  /**
   * A DATETIME column configured with {@code java.util.Date} must keep its time of day - that is #7610's fix, and
   * the reason the {@code Date}-versus-DATE distinction has to be made from the schema rather than from the class.
   * Pinned here as well, because #7638 moves the decision that makes it.
   */
  @Test
  void aDateTimeColumnBackedByJavaUtilDateStillKeepsItsTimeOfDay() {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateTimeImplementation();
    try {
      serializer.setDateTimeImplementation(Date.class);

      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638DateTime").createProperty("dt", Type.DATETIME);
        database.command("sql", "INSERT INTO Issue7638DateTime SET dt = date('2026-06-12 15:30:00.250', 'yyyy-MM-dd HH:mm:ss.SSS')");
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638DateTime")) {
          final Result row = rs.next();
          assertThat((Object) row.getProperty("dt")).isInstanceOf(Date.class);
          assertThat(new JsonSerializer(database).serializeResult(database, row).getString("dt"))
              .isEqualTo("2026-06-12 15:30:00.250");
        }
      });
    } finally {
      serializer.setDateTimeImplementation(previous);
    }
  }

  /**
   * A day before the epoch. Integer division truncates towards zero, so {@code Date.getTime() / MS_IN_A_DAY} put
   * every instant of 1969-12-31 on day 0 - the bug was unreachable while no DATE column could hold a {@code Date},
   * and reachable the moment one could.
   */
  @Test
  void aPreEpochDateRoundTripsOnTheDayItBelongsTo() {
    withDateImplementation(Date.class, () -> {
      final LocalDate beforeEpoch = LocalDate.of(1969, 12, 31);
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638PreEpoch").createProperty("d", Type.DATE);
        database.newDocument("Issue7638PreEpoch").set("d", beforeEpoch).save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638PreEpoch")) {
          final Result row = rs.next();
          assertThat(((Date) row.getProperty("d")).getTime()).isEqualTo(beforeEpoch.toEpochDay() * 86_400_000L);
          assertThat(new JsonSerializer(database).serializeResult(database, row).getString("d")).isEqualTo("1969-12-31");
        }
      });
    });
  }

  /**
   * The rendering must not depend on the machine's time zone. A DATE is a day count materialised as UTC midnight,
   * and {@code JSONObject}'s own {@code Date} branch renders in the DEFAULT zone - so anywhere west of Greenwich
   * the date came out one day early before the value was formatted explicitly.
   */
  @Test
  void theRenderedDateDoesNotDependOnTheMachineTimeZone() {
    final TimeZone previousZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      withDateImplementation(Date.class, () -> {
        database.transaction(() -> {
          database.getSchema().createDocumentType("Issue7638Zone").createProperty("d", Type.DATE);
          database.newDocument("Issue7638Zone").set("d", DAY).save();
        });

        database.transaction(() -> {
          final JsonSerializer serializer = new JsonSerializer(database);
          try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Zone")) {
            final Document document = rs.next().toElement();
            assertThat(serializer.serializeDocument(document).getString("d")).isEqualTo("2026-06-12");
          }
        });
      });
    } finally {
      TimeZone.setDefault(previousZone);
    }
  }

  /**
   * The scope of the {@code serializeDocument()} change, pinned deliberately rather than left implied. A
   * {@code java.util.Date} on a SCHEMALESS property used to skip {@code formatTemporalForPrecision} entirely and
   * reach {@code JSONObject}'s {@code Date} branch, which renders in the JVM's DEFAULT zone. It is now formatted
   * UTC-anchored - which is what {@code serializeResult()} has done for the identical value since #7610, and what
   * the write side does ({@code Type#convertToDate} anchors to UTC), so the old answer was the outlier: the same
   * document disagreed with itself between the document endpoint and a query, and the document one moved with the
   * machine's time zone.
   * <p>
   * Both surfaces are asserted from a non-UTC default zone, so a regression to zone-dependent rendering fails here
   * rather than only on a CI machine that happens not to run in UTC.
   */
  @Test
  void anUndeclaredDatePropertyIsAlsoUtcAnchored() {
    final TimeZone previousZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

      // 2026-06-12T02:30:00Z - a UTC instant that falls on the PREVIOUS day in the default zone, so a
      // system-zone rendering cannot accidentally agree with a UTC one
      final Date instant = new Date(1781231400000L);
      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638Schemaless");
        database.newDocument("Issue7638Schemaless").set("whenever", instant).save();
      });

      database.transaction(() -> {
        final JsonSerializer serializer = new JsonSerializer(database);
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638Schemaless")) {
          final Result row = rs.next();
          assertThat(serializer.serializeDocument(row.toElement()).getString("whenever"))
              .as("the document endpoint must not render an undeclared Date in the machine's time zone")
              .isEqualTo("2026-06-12 02:30:00");
          assertThat(serializer.serializeResult(database, row).getString("whenever"))
              .as("and it must answer exactly what a query already answered for the same value")
              .isEqualTo("2026-06-12 02:30:00");
        }
      });
    } finally {
      TimeZone.setDefault(previousZone);
    }
  }

  private void withDateImplementation(final Class<?> implementation, final Runnable body) {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateImplementation();
    try {
      serializer.setDateImplementation(implementation);
      body.run();
    } finally {
      serializer.setDateImplementation(previous);
    }
  }
}
