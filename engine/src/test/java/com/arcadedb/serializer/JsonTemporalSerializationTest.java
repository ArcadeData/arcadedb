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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the serialization of DATE and DATETIME values to JSON and back, one nested class per reported issue.
 */
class JsonTemporalSerializationTest {
  /**
   * Regression for #10486: a record's JSON did not read back for {@code DATETIME_SECOND}, {@code DATETIME_MICROS} and {@code DATETIME_NANOS}.
   * {@code toJSON()} wrote the value as a count in the column's own unit and {@code fromJSON()} reads a number as epoch milliseconds,
   * as it does everywhere a number meets a datetime (SQL, the HTTP API, the remote client), so a 2020 timestamp came back in 1970, in
   * the year 51972 or in the year 50004715. A number is milliseconds on the way out too now, which a second-precision column loses
   * nothing to; a column of microseconds or nanoseconds, which milliseconds would truncate, is written as the text the HTTP API
   * answers for it, with all its digits.
   */
  @Nested
  class JsonDatetimePrecisionRoundTrip extends TestHelper {
    private static final LocalDateTime SECONDS = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
    private static final LocalDateTime MILLIS  = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);
    private static final LocalDateTime MICROS  = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_000);
    private static final LocalDateTime NANOS   = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_789);

    private static final long MILLIS_OF_SECONDS = 1_577_934_245_000L;

    @Test
    void everyPrecisionRoundTripsThroughTheRecordJson() {
      assertRoundTrip(Type.DATETIME, MILLIS);
      assertRoundTrip(Type.DATETIME_SECOND, SECONDS);
      assertRoundTrip(Type.DATETIME_MICROS, MICROS);
      assertRoundTrip(Type.DATETIME_NANOS, NANOS);
    }

    /** The reproduction of the issue: a whole second of 2020 in each unit. */
    @Test
    void aWholeSecondOfEveryPrecisionComesBackInTheSameYear() {
      for (final Type type : new Type[] { Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS })
        assertRoundTrip(type, SECONDS);
    }

    @Test
    void aSecondPrecisionColumnWritesEpochMillisecondsLikeADatetime() {
      final MutableDocument doc = newDocument(Type.DATETIME_SECOND, SECONDS);
      assertThat(doc.toJSON(false).getLong("p")).isEqualTo(MILLIS_OF_SECONDS);
    }

    @Test
    void aMicrosecondOrNanosecondColumnWritesTextWithAllItsDigits() {
      assertThat(newDocument(Type.DATETIME_MICROS, MICROS).toJSON(false).getString("p")).endsWith(".123456");
      assertThat(newDocument(Type.DATETIME_NANOS, NANOS).toJSON(false).getString("p")).endsWith(".123456789");
    }

    @Test
    void aDatetimeColumnKeepsItsEpochMillisecondsNumber() {
      assertThat(newDocument(Type.DATETIME, MILLIS).toJSON(false).getLong("p")).isEqualTo(MILLIS_OF_SECONDS + 123);
    }

    /** The same record read from disk renders like the one that was just built. */
    @Test
    void aPersistedRecordRoundTripsToo() {
      for (final Type type : new Type[] { Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
        final LocalDateTime value = type == Type.DATETIME_SECOND ? SECONDS : type == Type.DATETIME_MICROS ? MICROS : NANOS;
        final DocumentType documentType = documentType(type);
        final AtomicReference<RID> rid = new AtomicReference<>();
        database.transaction(() -> rid.set(database.newDocument(documentType.getName()).set("p", value).save().getIdentity()));

        final Document stored = database.lookupByRID(rid.get(), true).asDocument();
        final MutableDocument back = database.newDocument(documentType.getName());
        back.fromJSON(new JSONObject(stored.toJSON(false).toString()));
        assertThat(back.get("p")).as(type.name()).isEqualTo(value);
      }
    }

    /** What every client already sends - a number of milliseconds - is still read as milliseconds for any precision. */
    @Test
    void aNumberIsStillReadAsEpochMilliseconds() {
      for (final Type type : new Type[] { Type.DATETIME, Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
        final MutableDocument back = database.newDocument(documentType(type).getName());
        back.fromJSON(new JSONObject("{\"p\":" + MILLIS_OF_SECONDS + "}"));
        assertThat(back.get("p")).as(type.name()).isEqualTo(SECONDS);
      }
    }

    /**
     * A database whose datetimes are {@code ZonedDateTime} and whose schema zone is not UTC: the text of a micros/nanos column carries
     * no offset and is read back in the schema zone, so the value has to be rendered in that zone, not in the zone it happens to hold.
     */
    @Test
    void aZonedDatetimeInAnotherZoneThanTheSchemaRoundTripsToTheSameInstant() {
      final ZoneId schemaZone = ZoneId.of("America/New_York");
      database.getSchema().setZoneId(schemaZone);
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);

      for (final Type type : new Type[] { Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
        final ZonedDateTime utc = ZonedDateTime.of(type == Type.DATETIME_MICROS ? MICROS : NANOS, ZoneOffset.UTC);
        final MutableDocument source = database.newDocument(documentType(type).getName()).set("p", utc);

        final MutableDocument back = database.newDocument(source.getTypeName());
        back.fromJSON(new JSONObject(source.toJSON(false).toString()));
        assertThat(((ZonedDateTime) back.get("p")).toInstant()).as(type.name()).isEqualTo(utc.toInstant());
      }
    }

    @Test
    void aNullDatetimeStaysNull() {
      final DocumentType type = documentType(Type.DATETIME_NANOS);
      final MutableDocument doc = database.newDocument(type.getName());
      assertThat(doc.toJSON(false).has("p")).isFalse();
    }

    private void assertRoundTrip(final Type type, final LocalDateTime value) {
      final MutableDocument source = newDocument(type, value);
      final String json = source.toJSON(false).toString();

      final MutableDocument back = database.newDocument(source.getTypeName());
      back.fromJSON(new JSONObject(json));
      assertThat(back.get("p")).as("%s through %s", type, json).isEqualTo(value);
    }

    private MutableDocument newDocument(final Type type, final LocalDateTime value) {
      return database.newDocument(documentType(type).getName()).set("p", value);
    }

    private DocumentType documentType(final Type type) {
      final String name = "D" + type.name();
      if (database.getSchema().existsType(name))
        return database.getSchema().getType(name);
      final DocumentType documentType = database.getSchema().createDocumentType(name);
      documentType.createProperty("p", type);
      return documentType;
    }
  }

  /**
   * Regression for #10865: a property declared on a SUPERTYPE was invisible to the JSON serializer, which looked the declaration up on
   * the record's own type only. A {@code java.util.Date} for an inherited DATE was written as epoch milliseconds, which the
   * server reads as epoch days, and an inherited DATETIME_MICROS / DATETIME_SECOND lost the column's precision in the output.
   * The second half: a DATE fed the epoch milliseconds of a UTC midnight (what {@code JSONObject} writes for a {@code LocalDate}
   * query parameter, and what an HTTP client sends for a date) was read as a count of days and refused as out of range.
   */
  @Nested
  class InheritedDateAndMidnightMillis extends TestHelper {
    private static final LocalDate DAY        = LocalDate.of(2024, 1, 15);
    private static final long      DAY_MILLIS = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

    @BeforeEach
    void createSchema() {
      database.transaction(() -> {
        final DocumentType own = database.getSchema().createDocumentType("Own");
        own.createProperty("fecha", Type.DATE);
        own.createProperty("micros", Type.DATETIME_MICROS);
        own.createProperty("seconds", Type.DATETIME_SECOND);

        final DocumentType base = database.getSchema().createDocumentType("Base");
        base.createProperty("fecha", Type.DATE);
        base.createProperty("micros", Type.DATETIME_MICROS);
        base.createProperty("seconds", Type.DATETIME_SECOND);
        database.getSchema().createDocumentType("Child").addSuperType("Base");
        database.getSchema().createDocumentType("GrandChild").addSuperType("Child");
      });
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // map2json (the remote client's write path, and every toJSON() of a record)
    // ---------------------------------------------------------------------------------------------------------------------

    @Test
    void map2jsonEncodesAnInheritedDateAsEpochDays() {
      final Date date = Date.from(DAY.atTime(15, 30).toInstant(ZoneOffset.UTC));
      final Map<String, Object> map = new HashMap<>();
      map.put("fecha", date);

      for (final String typeName : new String[] { "Own", "Child", "GrandChild" }) {
        final JSONObject json = new JsonSerializer(database).map2json(map, database.getSchema().getType(typeName), false);
        assertThat(json.getLong("fecha")).as(typeName).isEqualTo(DAY.toEpochDay());
      }
    }

    @Test
    void map2jsonTypeHintNamesTheInheritedDeclaration() {
      final Map<String, Object> map = new HashMap<>();
      map.put("fecha", Date.from(DAY.atStartOfDay().toInstant(ZoneOffset.UTC)));

      final JSONObject json = new JsonSerializer(database).setIncludeTypeHints(true)
          .map2json(map, database.getSchema().getType("Child"), false);
      assertThat(json.getString("@props")).isEqualTo("fecha:" + Type.DATE.getId());
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // serializeDocument / serializeResult projection (the HTTP API's rendering of a record)
    // ---------------------------------------------------------------------------------------------------------------------

    @Test
    void serializeDocumentFormatsAnInheritedColumnWithItsDeclaredPrecision() {
      final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
      final Document own = saved("Own", value);
      final Document child = saved("Child", value);
      final Document grandChild = saved("GrandChild", value);

      final JsonSerializer serializer = new JsonSerializer(database);
      final JSONObject expected = serializer.serializeDocument(own);
      assertThat(expected.getString("micros")).endsWith(".123000");

      for (final Document doc : new Document[] { child, grandChild }) {
        final JSONObject json = serializer.serializeDocument(doc);
        assertThat(json.get("micros")).as(doc.getTypeName()).isEqualTo(expected.get("micros"));
        assertThat(json.get("seconds")).as(doc.getTypeName()).isEqualTo(expected.get("seconds"));
        assertThat(json.get("fecha")).as(doc.getTypeName()).isEqualTo(expected.get("fecha"));
      }
    }

    @Test
    void serializeResultProjectionFormatsAnInheritedColumnWithItsDeclaredPrecision() {
      final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
      final Document own = saved("Own", value);
      final Document child = saved("Child", value);

      final JsonSerializer serializer = new JsonSerializer(database);
      final Object ownMicros = projectedMicros(serializer, own);
      assertThat(ownMicros.toString()).endsWith(".123000");
      assertThat(projectedMicros(serializer, child)).isEqualTo(ownMicros);
    }

    @Test
    void serializeResultOfAWholeRecordFormatsAnInheritedColumnWithItsDeclaredPrecision() {
      final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_000_000);
      saved("Own", value);
      saved("Child", value);

      final JsonSerializer serializer = new JsonSerializer(database);
      final Object ownMicros = selectedMicros(serializer, "Own");
      assertThat(ownMicros.toString()).endsWith(".123000");
      assertThat(selectedMicros(serializer, "Child")).isEqualTo(ownMicros);
    }

    @Test
    void jsonGraphSerializerEncodesAnInheritedColumnWithItsDeclaredPrecision() {
      final LocalDateTime value = LocalDateTime.of(2024, 1, 15, 10, 20, 30, 123_456_000);
      final Document own = saved("Own", value);
      final Document child = saved("Child", value);

      final JsonGraphSerializer serializer = JsonGraphSerializer.createJsonGraphSerializer().setPrecisionAwareTemporals(true);
      final Object ownMicros = serializer.serializeGraphElement(own).getJSONObject("p").get("micros");
      assertThat(serializer.serializeGraphElement(child).getJSONObject("p").get("micros")).isEqualTo(ownMicros);
    }

    // ---------------------------------------------------------------------------------------------------------------------
    // DATE conversion of a number (the server's read of a LocalDate query parameter / HTTP date)
    // ---------------------------------------------------------------------------------------------------------------------

    @Test
    void dateParameterSentAsMidnightMillisIsTheSameDay() {
      database.transaction(() -> database.command("sql", "INSERT INTO Own SET fecha = ?", DAY_MILLIS));
      database.transaction(() -> database.command("sql", "INSERT INTO Child SET fecha = ?", DAY_MILLIS));
      database.transaction(() -> database.command("sql", "INSERT INTO Own CONTENT {\"fecha\": " + DAY_MILLIS + "}"));

      assertThat(dates("Own")).containsExactly(DAY, DAY);
      assertThat(dates("Child")).containsExactly(DAY);
    }

    @Test
    void dateStillReadsAPlainNumberAsEpochDays() {
      assertThat(written(DAY.toEpochDay())).isEqualTo(DAY);
      assertThat(written(0L)).isEqualTo(LocalDate.of(1970, 1, 1));
      assertThat(written(-1L)).isEqualTo(LocalDate.of(1969, 12, 31));
      assertThat(written(String.valueOf(DAY.toEpochDay()))).isEqualTo(DAY);
    }

    @Test
    void dateReadsTheMillisOfAnyUtcMidnight() {
      assertThat(written(DAY_MILLIS)).isEqualTo(DAY);
      assertThat(written((int) 86_400_000)).isEqualTo(LocalDate.of(1970, 1, 2));
      assertThat(written(String.valueOf(DAY_MILLIS))).isEqualTo(DAY);
      // the first days after the epoch, whose millis are small enough to be a valid day count too
      assertThat(written(86_400_000L)).isEqualTo(LocalDate.of(1970, 1, 2));
      // before the epoch
      final LocalDate preEpoch = LocalDate.of(1960, 5, 1);
      assertThat(written(preEpoch.toEpochDay() * 86_400_000L)).isEqualTo(preEpoch);
    }

    @Test
    void dateReadsMillisThatAreNotAMidnightAsTheirDay() {
      // no day count any class of a DATE can hold, so the milliseconds it is: the day they fall in, as a java.util.Date, Calendar
      // or LocalDateTime implementation of the DATE reads them (issue #11025, it used to be refused here and stored there)
      assertThat(written(DAY_MILLIS + 1)).isEqualTo(DAY);
      assertThat(written(DAY_MILLIS + 86_399_999L)).isEqualTo(DAY);
    }

    @Test
    void listOfDateReadsTheMillisOfAUtcMidnight() {
      database.transaction(() -> {
        database.getSchema().getType("Own").createProperty("fechas", Type.LIST, "DATE");
        database.command("sql", "INSERT INTO Own SET fechas = ?", List.of(DAY_MILLIS, DAY.toEpochDay() + 1));
      });
      try (final ResultSet rs = database.query("sql", "SELECT fechas FROM Own")) {
        final List<Object> read = rs.next().getProperty("fechas");
        assertThat(read).containsExactly(DAY, DAY.plusDays(1));
      }
    }

    @Test
    void aComparisonStillReadsTheNumberAsDays() {
      database.transaction(() -> database.newDocument("Own").set("fecha", DAY).save());
      // only a WRITE reads the millis of a midnight as that day: a number compared with a date never equals it (#8913)
      try (final ResultSet rs = database.query("sql", "SELECT FROM Own WHERE fecha = ?", DAY_MILLIS)) {
        assertThat(rs.hasNext()).isFalse();
      }
      assertThat(Type.convert(database, DAY.toEpochDay(), LocalDate.class)).isEqualTo(DAY);
    }

    /**
     * The DATE a value becomes when written into a DATE property.
     */
    private LocalDate written(final Object value) {
      final LocalDate[] result = new LocalDate[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Own").set("fecha", value);
        result[0] = doc.getLocalDate("fecha");
      });
      return result[0];
    }

    private Document saved(final String typeName, final LocalDateTime value) {
      final Document[] result = new Document[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(typeName);
        doc.set("fecha", value.toLocalDate());
        doc.set("micros", value);
        doc.set("seconds", value.withNano(0));
        doc.save();
        result[0] = doc;
      });
      return result[0];
    }

    /**
     * The branch of serializeResult an openCypher {@code RETURN n} takes: the row is the element, tagged with its projection name.
     */
    private Object projectedMicros(final JsonSerializer serializer, final Document document) {
      final ResultInternal row = new ResultInternal(document);
      row.setMetadata("_projectionName", "n");
      return serializer.serializeResult(database, row).get("micros");
    }

    private Object selectedMicros(final JsonSerializer serializer, final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + typeName)) {
        return serializer.serializeResult(database, rs.next()).get("micros");
      }
    }

    private List<LocalDate> dates(final String typeName) {
      final List<LocalDate> result = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT fecha FROM " + typeName)) {
        while (rs.hasNext())
          result.add(rs.next().getProperty("fecha"));
      }
      return result;
    }
  }

  /**
   * Regression for #7638: a {@code DATE} column lost or mis-formatted its precision on the way out to
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
   */
  @Nested
  class DateColumnPrecision extends TestHelper {
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
     * A day before the epoch. Integer division truncates TOWARDS ZERO, so {@code getTime() / MS_IN_A_DAY} put a
     * pre-epoch instant on the day AFTER the one it belongs to.
     * <p>
     * THE INSTANT IS DELIBERATELY NOT MIDNIGHT. At exactly midnight the division has no remainder and truncation
     * and flooring agree, so a test written on {@code toEpochDay() * MS_IN_A_DAY} passes with or without the fix -
     * this test was written that way at first and proved nothing (caught in review). Noon is the case that
     * separates them: 1969-12-31T12:00Z is day -1 floored and day 0 truncated.
     */
    @Test
    void aPreEpochDateRoundTripsOnTheDayItBelongsTo() {
      withDateImplementation(Date.class, () -> {
        final LocalDate beforeEpoch = LocalDate.of(1969, 12, 31);
        final Date middayBeforeEpoch = new Date(beforeEpoch.toEpochDay() * 86_400_000L + 43_200_000L);
        database.transaction(() -> {
          database.getSchema().createDocumentType("Issue7638PreEpoch").createProperty("d", Type.DATE);
          database.newDocument("Issue7638PreEpoch").set("d", middayBeforeEpoch).save();
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
     * The same truncation, on the DEFAULT configuration rather than the {@code java.util.Date} one - so this is not
     * a corner of an opt-in setting. {@code Type.getJavaImplementation} answers {@code LocalDate} for a DATE column
     * by default, so {@code document.set("d", someJavaUtilDate)} coerces through {@code Type.convert}, which did the
     * identical truncating division. Found in review, next to the instance this issue had already fixed.
     */
    @Test
    void aPreEpochJavaUtilDateOnADefaultDateColumnKeepsItsDay() {
      final LocalDate beforeEpoch = LocalDate.of(1969, 12, 31);
      final Date middayBeforeEpoch = new Date(beforeEpoch.toEpochDay() * 86_400_000L + 43_200_000L);

      database.transaction(() -> {
        database.getSchema().createDocumentType("Issue7638PreEpochDefault").createProperty("d", Type.DATE);
        database.newDocument("Issue7638PreEpochDefault").set("d", middayBeforeEpoch).save();
      });

      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Issue7638PreEpochDefault")) {
          final Result row = rs.next();
          assertThat((Object) row.getProperty("d")).isEqualTo(beforeEpoch);
          assertThat(new JsonSerializer(database).serializeResult(database, row).getString("d")).isEqualTo("1969-12-31");
        }
      });

      // And the shared conversion both the write path and BinaryComparator.temporalAsLong go through
      final Calendar calendar = Calendar.getInstance();
      calendar.setTime(middayBeforeEpoch);
      assertThat(DateUtils.dateToEpochDays(middayBeforeEpoch)).isEqualTo(beforeEpoch.toEpochDay());
      assertThat(DateUtils.dateToEpochDays(calendar)).isEqualTo(beforeEpoch.toEpochDay());
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

    /**
     * Found by CodeRabbit's review. {@code SELECT *, n + 1 AS d} keeps the backing element AND publishes
     * a COMPUTED value under a name the element also has, so a type lookup that consulted the element first answered
     * with column {@code d}'s declared DATE for a value that is an integer - and would have applied DATE formatting
     * to any temporal the expression happened to produce. The projection is now asked first and says "mine, and no
     * source column", which is a different answer from "never mentioned".
     */
    @Test
    void aComputedAliasCollidingWithABackingColumnDoesNotInheritItsType() {
      withDateImplementation(Date.class, () -> {
        database.transaction(() -> {
          final DocumentType type = database.getSchema().createDocumentType("Issue7638Collision");
          type.createProperty("d", Type.DATE);
          type.createProperty("n", Type.INTEGER);
          database.newDocument("Issue7638Collision").set("d", DAY).set("n", 7).save();
        });

        database.transaction(() -> {
          try (final ResultSet rs = database.query("sql", "SELECT *, n + 1 AS d FROM Issue7638Collision")) {
            final Result row = rs.next();
            assertThat((Object) row.getProperty("d")).as("the row publishes the computed value, not the column")
                .isEqualTo(8);
            assertThat(row.getPropertyType("d"))
                .as("so it must not report the backing DATE column's type for it").isNull();
            assertThat(new JsonSerializer(database).serializeResult(database, row).get("d")).isEqualTo(8);
          }

          // A COMPUTED TEMPORAL, which is the case that actually reaches the formatter. The integer above proves
          // the type lookup but not the formatting: formatTemporalForPrecision ignores a non-temporal value, so
          // that assertion passed even while JsonSerializer was still resolving DATE for this alias by falling
          // back to the element on the deliberate null. A datetime aliased onto the DATE column's name must keep
          // its time of day rather than being rendered date-only.
          try (final ResultSet rs = database.query("sql",
              "SELECT *, date('2026-06-12 15:30:00', 'yyyy-MM-dd HH:mm:ss') AS d FROM Issue7638Collision")) {
            final Result row = rs.next();
            assertThat(row.getPropertyType("d")).isNull();
            assertThat(new JsonSerializer(database).serializeResult(database, row).getString("d"))
                .as("a computed timestamp must not inherit the colliding DATE column's date-only formatting")
                .startsWith("2026-06-12 15:30:00");
          }

          // The control: with no collision, `*` still reports the backing column's type
          try (final ResultSet rs = database.query("sql", "SELECT * FROM Issue7638Collision")) {
            final Result row = rs.next();
            assertThat(row.getPropertyType("d")).isEqualTo(Type.DATE);
            assertThat(new JsonSerializer(database).serializeResult(database, row).getString("d")).isEqualTo("2026-06-12");
          }
        });
      });
    }

    /**
     * The FOURTH site of the same truncating division, found in review after the commit that claimed to have fixed
     * "everywhere it happens, not just one of three" - it was one of four. {@code DateUtils.getDate} is reached
     * through {@code asDate()}/{@code asDateTime()}/{@code date()}/{@code sysdate()} under the DEFAULT
     * implementations, so neither branch needs special configuration to hit.
     * <p>
     * The {@code LocalDateTime} branch was the worse of the two: Java's {@code %} keeps the DIVIDEND's sign, so a
     * pre-epoch timestamp with a sub-second component produced a negative {@code nanoOfSecond}, which
     * {@code LocalDateTime.ofEpochSecond} validates and rejects - it threw {@code DateTimeException} rather than
     * merely answering the wrong instant.
     */
    @Test
    void getDateFloorsPreEpochInstantsRatherThanTruncatingOrThrowing() {
      assertThat(DateUtils.getDate("1969-12-31T12:00:00Z", LocalDate.class))
          .as("noon before the epoch belongs to 1969-12-31, not to day 0")
          .isEqualTo(LocalDate.of(1969, 12, 31));

      assertThat(DateUtils.getDate("1969-12-31T12:00:00.250Z", LocalDateTime.class))
          .as("a pre-epoch instant with a sub-second component used to throw DateTimeException")
          .isEqualTo(LocalDateTime.of(1969, 12, 31, 12, 0, 0, 250_000_000));

      // Post-epoch is unchanged, so the fix cannot have been a blanket shift
      assertThat(DateUtils.getDate("2026-06-12T15:30:00.250Z", LocalDateTime.class))
          .isEqualTo(LocalDateTime.of(2026, 6, 12, 15, 30, 0, 250_000_000));
      assertThat(DateUtils.getDate("2026-06-12T12:00:00Z", LocalDate.class)).isEqualTo(DAY);
    }

    /**
     * The sweep the previous rounds kept failing to finish: no millis-to-days or millis-to-seconds division in the
     * engine may use {@code /} or {@code %}, because every one of them is this bug waiting for a pre-epoch value.
     * Four sites were found one at a time across three review rounds; this fails on the fifth rather than waiting
     * for someone to notice it.
     */
    @Test
    void noEngineSourceStillDividesMillisByHandInsteadOfFlooring() throws Exception {
      final List<String> offenders = new ArrayList<>();
      final Path engineSources = Path.of("src", "main", "java");
      try (final Stream<Path> files = Files.walk(engineSources)) {
        for (final Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
          int lineNumber = 0;
          for (final String line : Files.readAllLines(file)) {
            lineNumber++;
            final String code = line.trim();
            if (code.startsWith("//") || code.startsWith("*"))
              continue;
            if (code.matches(".*[/%]\\s*(DateUtils\\.)?MS_IN_A_DAY.*")
                || code.matches(".*timestamp\\s*[/%]\\s*1_000\\b.*"))
              offenders.add(file + ":" + lineNumber + "  " + code);
          }
        }
      }

      assertThat(offenders)
          .as("use Math.floorDiv/floorMod - '/' and '%' truncate towards zero and put every pre-epoch instant on "
              + "the wrong day, or produce a negative nanoOfSecond that LocalDateTime rejects outright")
          .isEmpty();
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

  /**
   * Regression for #8158: with {@code arcadedb.dateTimeImplementation} set to {@code java.util.Date} or {@code java.util.Calendar}, a
   * {@code DATETIME_MICROS}/{@code DATETIME_NANOS} column was written successfully (the write side converts those
   * columns to {@link LocalDateTime} regardless of the setting) but every read threw inside
   * {@code DateUtils.dateTime()}, was swallowed by the corrupt-property recovery arm and surfaced as {@code null}. The
   * read side now materialises a sub-millisecond column as {@link LocalDateTime} under a class that cannot carry it,
   * while {@code DATETIME} and {@code DATETIME_SECOND} keep honouring the setting.
   */
  @Nested
  class SubMillisDateTimeImplementation extends TestHelper {
    private static final LocalDateTime MICROS = LocalDateTime.parse("2024-02-29T13:45:10.123456");
    private static final LocalDateTime NANOS  = LocalDateTime.parse("2024-02-29T13:45:10.123456789");

    @ParameterizedTest
    @ValueSource(strings = { "java.util.Date", "java.util.Calendar" })
    void settingChangedAfterTheWriteStillReadsSubMillisColumns(final String implementation) {
      createSchema();
      insert();
      try {
        setImplementation(implementation);
        assertSubMillisColumns();
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "java.util.Date", "java.util.Calendar" })
    void settingInPlaceBeforeTheWriteStillReadsSubMillisColumns(final String implementation) {
      setImplementation(implementation);
      try {
        createSchema();
        insert();
        assertSubMillisColumns();
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    @Test
    void millisAndSecondColumnsStillHonourTheSetting() {
      createSchema();
      insert();
      try {
        setImplementation("java.util.Date");
        try (final ResultSet rs = database.query("sql", "SELECT dt, dts FROM T")) {
          final Result row = rs.next();
          assertThat(row.<Object>getProperty("dt")).isInstanceOf(Date.class);
          assertThat(row.<Object>getProperty("dts")).isInstanceOf(Date.class);
        }
        setImplementation("java.util.Calendar");
        try (final ResultSet rs = database.query("sql", "SELECT dt, dts FROM T")) {
          final Result row = rs.next();
          assertThat(row.<Object>getProperty("dt")).isInstanceOf(Calendar.class);
          assertThat(row.<Object>getProperty("dts")).isInstanceOf(Calendar.class);
        }
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    private void createSchema() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.dtm DATETIME_MICROS");
      database.command("sql", "CREATE PROPERTY T.dtn DATETIME_NANOS");
      database.command("sql", "CREATE PROPERTY T.dt DATETIME");
      database.command("sql", "CREATE PROPERTY T.dts DATETIME_SECOND");
    }

    private void insert() {
      database.transaction(() -> database.command("sql",
          "INSERT INTO T SET dtm = '2024-02-29 13:45:10.123456', dtn = '2024-02-29 13:45:10.123456789', "
              + "dt = '2024-02-29 13:45:10.123', dts = '2024-02-29 13:45:10'"));
    }

    private void setImplementation(final String implementation) {
      database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` '" + implementation + "'");
    }

    private void assertSubMillisColumns() {
      try (final ResultSet rs = database.query("sql", "SELECT dtm, dtn FROM T")) {
        final Result row = rs.next();
        assertThat(row.<Object>getProperty("dtm")).isEqualTo(MICROS);
        assertThat(row.<Object>getProperty("dtn")).isEqualTo(NANOS);
      }

      try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
        final Document doc = rs.next().getRecord().get().asDocument();
        assertThat(doc.get("dtm")).isEqualTo(MICROS);
        assertThat(doc.get("dtn")).isEqualTo(NANOS);
      }
    }
  }

  /**
   * Regression for #8253: {@code DATETIME_SECOND} stores epoch SECONDS. {@code DateUtils.dateTime()} fed that value straight into
   * {@code new Date(millis)} / {@code Calendar.setTimeInMillis(millis)}, both of which always expect MILLIS, so the
   * value came back 1000 times too small under {@code java.util.Date}/{@code java.util.Calendar} - silently, no
   * exception and no corrupt-property warning.
   */
  @Nested
  class DatetimeSecondPrecision extends TestHelper {
    private static final LocalDateTime EXPECTED = LocalDateTime.parse("2024-02-29T13:45:10");

    @Test
    void dateImplementationReadsBackTheCorrectInstant() {
      createSchema();
      insert();
      try {
        setImplementation("java.util.Date");
        try (final ResultSet rs = database.query("sql", "SELECT dts FROM T")) {
          final Result row = rs.next();
          final Date value = row.getProperty("dts");
          assertThat(value.toInstant()).isEqualTo(EXPECTED.toInstant(ZoneOffset.UTC));
        }
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    @Test
    void calendarImplementationReadsBackTheCorrectInstant() {
      createSchema();
      insert();
      try {
        setImplementation("java.util.Calendar");
        try (final ResultSet rs = database.query("sql", "SELECT dts FROM T")) {
          final Result row = rs.next();
          final Calendar value = row.getProperty("dts");
          assertThat(value.toInstant()).isEqualTo(EXPECTED.toInstant(ZoneOffset.UTC));
        }
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    @Test
    void localDateTimeImplementationStillReadsTheCorrectInstant() {
      createSchema();
      insert();
      try (final ResultSet rs = database.query("sql", "SELECT dts FROM T")) {
        final Result row = rs.next();
        assertThat(row.<LocalDateTime>getProperty("dts")).isEqualTo(EXPECTED);
      }
    }

    @Test
    void instantImplementationStillReadsTheCorrectInstant() {
      createSchema();
      insert();
      try {
        setImplementation("java.time.Instant");
        try (final ResultSet rs = database.query("sql", "SELECT dts FROM T")) {
          final Result row = rs.next();
          assertThat(row.<Instant>getProperty("dts")).isEqualTo(EXPECTED.toInstant(ZoneOffset.UTC));
        }
      } finally {
        setImplementation("java.time.LocalDateTime");
      }
    }

    private void createSchema() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.dts DATETIME_SECOND");
    }

    private void insert() {
      database.transaction(() -> database.command("sql", "INSERT INTO T SET dts = '2024-02-29 13:45:10'"));
    }

    private void setImplementation(final String implementation) {
      database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` '" + implementation + "'");
    }
  }

  /**
   * Regression for #9005: a node returned whole by openCypher ({@code RETURN n}) lost the fractional seconds of
   * an undeclared datetime property when serialized to JSON, while SQL and the declared property kept them.
   */
  @Nested
  class CypherNodeUndeclaredDatetime extends TestHelper {
    @Test
    void undeclaredDatetimeKeepsItsPrecisionWhenTheNodeIsReturnedWhole() {
      final LocalDateTime t = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_000);
      database.transaction(() -> {
        database.getSchema().createVertexType("V9005").createProperty("declared", Type.DATETIME_MICROS);
        database.newVertex("V9005").set("declared", t).set("undeclared", t).save();
      });

      final JsonSerializer serializer = new JsonSerializer(database);
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:V9005) RETURN n")) {
        final JSONObject json = serializer.serializeResult(database, rs.next());
        assertThat(json.getString("declared")).isEqualTo("2026-10-03 12:34:56.123456");
        assertThat(json.getString("undeclared")).isEqualTo("2026-10-03 12:34:56.123456");
      }
    }
  }

  /**
   * Regression for #9011: a datetime inside a LIST arrived as epoch milliseconds and inside a MAP without fractional
   * seconds, so two instants a microsecond apart became the same value.
   */
  @Nested
  class TemporalInCollection extends TestHelper {
    private static final LocalDateTime A = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_000);
    private static final LocalDateTime B = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_457_000);

    @Test
    void temporalsInListAndMapKeepTheirPrecision() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("T9011").createProperty("one", Type.DATETIME_MICROS);
        database.getSchema().getType("T9011").createProperty("many", Type.LIST);
        final Map<String, Object> byKey = new LinkedHashMap<>();
        byKey.put("k", A);
        database.newDocument("T9011").set("one", A).set("many", List.of(A, B)).set("byKey", byKey).save();
      });

      final JsonSerializer serializer = new JsonSerializer(database);
      try (final ResultSet rs = database.query("sql", "SELECT one, many, byKey FROM T9011")) {
        final JSONObject json = serializer.serializeResult(database, rs.next());
        assertThat(json.getString("one")).isEqualTo("2026-10-03 12:34:56.123456");
        final JSONArray many = json.getJSONArray("many");
        assertThat(many.getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
        assertThat(many.getString(1)).isEqualTo("2026-10-03 12:34:56.123457");
        assertThat(json.getJSONObject("byKey").getString("k")).isEqualTo("2026-10-03 12:34:56.123456");
      }

      try (final ResultSet rs = database.query("sql", "SELECT FROM T9011")) {
        final JSONObject json = serializer.serializeResult(database, rs.next());
        assertThat(json.getJSONArray("many").getString(1)).isEqualTo("2026-10-03 12:34:56.123457");
        assertThat(json.getJSONObject("byKey").getString("k")).isEqualTo("2026-10-03 12:34:56.123456");
      }
    }

    @Test
    void wholeSecondTemporalInListIsAStringToo() {
      final LocalDateTime whole = LocalDateTime.of(2026, 10, 3, 12, 34, 56);
      database.getSchema().createDocumentType("T9011b");
      database.transaction(() -> database.newDocument("T9011b").set("many", List.of(whole)).save());
      final JsonSerializer serializer = new JsonSerializer(database);
      try (final ResultSet rs = database.query("sql", "SELECT many FROM T9011b")) {
        assertThat(serializer.serializeResult(database, rs.next()).getJSONArray("many").getString(0)).isEqualTo("2026-10-03 12:34:56");
      }
    }

    @Test
    void temporalsInNestedContainersAndOtherTemporalClassesKeepTheirPrecision() {
      database.getSchema().createDocumentType("T9011c");
      database.transaction(() -> {
        final Map<String, Object> byKey = new LinkedHashMap<>();
        byKey.put("l", List.of(A, B));
        database.newDocument("T9011c").set("nested", List.of(List.of(A), List.of(B))).set("map", byKey)
            .set("instants", List.of(A.toInstant(ZoneOffset.UTC))).save();
      });
      final JsonSerializer serializer = new JsonSerializer(database);
      try (final ResultSet rs = database.query("sql", "SELECT nested, map, instants FROM T9011c")) {
        final JSONObject json = serializer.serializeResult(database, rs.next());
        assertThat(json.getJSONArray("nested").getJSONArray(1).getString(0)).isEqualTo("2026-10-03 12:34:56.123457");
        assertThat(json.getJSONObject("map").getJSONArray("l").getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
        assertThat(json.getJSONArray("instants").getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
      }
    }

    @Test
    void calendarInAListIsFormattedWithTheDateTimePattern() {
      final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTimeInMillis(1_791_030_896_123L);
      database.getSchema().createDocumentType("T9011d");
      database.transaction(() -> database.newDocument("T9011d").set("many", List.of(calendar)).save());
      try (final ResultSet rs = database.query("sql", "SELECT many FROM T9011d")) {
        final Object element = new JsonSerializer(database).serializeResult(database, rs.next()).getJSONArray("many").get(0);
        assertThat(element).isInstanceOf(String.class);
        assertThat((String) element).startsWith("2026-10-03 12:34:56");
      }
    }
  }

  /**
   * Regression for #9324: with {@code arcadedb.dateImplementation=java.time.LocalDateTime} a DATE (a count of days) was read as if it
   * were milliseconds, so every DATE answered a 1970 timestamp and could not be written back.
   */
  @Nested
  class DateLocalDateTime extends TestHelper {
    private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

    @Test
    void dateReadsAsMidnightOfThatDay() {
      assertThat(DateUtils.date(database, DAY.toEpochDay(), LocalDateTime.class)).isEqualTo(DAY.atStartOfDay());
      assertThat(DateUtils.date(database, -1L, LocalDateTime.class)).isEqualTo(LocalDate.of(1969, 12, 31).atStartOfDay());
      assertThat(DateUtils.date(database, LocalDate.of(1960, 5, 1).toEpochDay(), LocalDateTime.class))
          .isEqualTo(LocalDate.of(1960, 5, 1).atStartOfDay());
    }

    @Test
    void storedDateRoundTripsUnderLocalDateTimeImplementation() {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateImplementation();
      try {
        serializer.setDateImplementation(LocalDateTime.class);
        database.transaction(() -> {
          database.getSchema().createDocumentType("T").createProperty("D", Type.DATE);
          database.newDocument("T").set("D", DAY).save();
        });

        database.transaction(() -> {
          final MutableDocument doc = database.iterateType("T", true).next().asDocument().modify();
          final Object read = doc.get("D");
          assertThat(read).isEqualTo(DAY.atStartOfDay());
          // read-modify-write must not fail
          doc.set("D", read).save();
          assertThat(doc.get("D")).isEqualTo(DAY.atStartOfDay());
        });
      } finally {
        serializer.setDateImplementation(previous);
      }
    }

    @Test
    void numberToLocalDateTimeStaysOnEpochMillis() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Ms").createProperty("dt", Type.DATETIME);
        database.newDocument("Ms").set("dt", 1791000000000L).save();
      });
      final Object read = database.iterateType("Ms", true).next().asDocument().get("dt");
      assertThat(read).isEqualTo(LocalDateTime.of(2026, 10, 3, 4, 0));
    }

    @Test
    void dateColumnTruncatesToDaysUnderDatetimeImplementations() {
      database.getSchema().createDocumentType("Dy").createProperty("d", Type.DATE);
      final LocalDateTime withTime = LocalDateTime.of(2024, 2, 29, 13, 45, 10);
      for (final Object value : new Object[] { withTime, withTime.toInstant(ZoneOffset.UTC), withTime.atZone(ZoneOffset.UTC) }) {
        database.transaction(() -> database.newDocument("Dy").set("d", value).save());
      }
      final List<Object> read = new ArrayList<>();
      database.iterateType("Dy", true).forEachRemaining(r -> read.add(r.asDocument().get("d")));
      assertThat(read).hasSize(3).allMatch(DAY::equals);
    }
  }
}
