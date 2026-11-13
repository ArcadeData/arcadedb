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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: property value conversion.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class PropertyValueConversionTest {
  /**
   * Regression for #8090: space separated fractional date time.
   *
   * Reproduces issue #8090: a datetime literal that uses a space separator AND a fractional-second part
   * ({@code '2024-02-29 13:45:10.123456'}, the form {@code psqlodbc} renders a bound timestamp in) parsed to nothing,
   * and the resulting {@link java.time.format.DateTimeParseException} was swallowed by the blanket handler at the end
   * of {@link Type#convert} which answered {@code null}. The insert reported success while the column was emptied.
   * <p>
   * Two guarantees are asserted here: the space-separated fractional form parses, and an unparseable datetime string
   * fails loudly instead of degrading to {@code null}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SpaceSeparatedFractionalDateTimeTest extends TestHelper {

    @Test
    void spaceSeparatedLiteralWithFractionIsStored() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8090");
      type.createProperty("ts", Type.DATETIME_MICROS);

      final LocalDateTime expected = LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000);

      database.transaction(() -> {
        for (final String literal : new String[] { "2024-02-29T13:45:10.123456", "2024-02-29 13:45:10.123456" }) {
          final ResultSet rs = database.command("sql", "INSERT INTO Ev8090 SET label = ?, ts = ?", literal, literal);
          assertThat(rs.hasNext()).isTrue();
          assertThat(rs.next().<Object>getProperty("ts")).as("literal '%s'", literal).isEqualTo(expected);
        }
      });

      database.transaction(() -> {
        final ResultSet rs = database.query("sql", "SELECT ts FROM Ev8090");
        int rows = 0;
        while (rs.hasNext()) {
          assertThat(rs.next().<Object>getProperty("ts")).isEqualTo(expected);
          ++rows;
        }
        assertThat(rows).isEqualTo(2);
      });
    }

    @Test
    void spaceSeparatedLiteralWithoutFractionKeepsWorking() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8090NoFraction");
      type.createProperty("ts", Type.DATETIME_MICROS);

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Ev8090NoFraction").set("ts", "2024-02-29 13:45:10").save();
        assertThat(doc.get("ts")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10));
      });
    }

    @Test
    void fractionIsAcceptedOnEveryDateTimePrecision() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8090Precision");
      type.createProperty("millis", Type.DATETIME);
      type.createProperty("micros", Type.DATETIME_MICROS);
      type.createProperty("nanos", Type.DATETIME_NANOS);
      type.createProperty("seconds", Type.DATETIME_SECOND);

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Ev8090Precision")
            .set("millis", "2024-02-29 13:45:10.123")
            .set("micros", "2024-02-29 13:45:10.123456")
            .set("nanos", "2024-02-29 13:45:10.123456789")
            .set("seconds", "2024-02-29 13:45:10")
            .save();

        assertThat(doc.get("millis")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_000_000));
        assertThat(doc.get("micros")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));
        assertThat(doc.get("nanos")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_789));
        assertThat(doc.get("seconds")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10));
      });
    }

    @Test
    void unparseableDateTimeFailsInsteadOfStoringNull() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8090Bad");
      type.createProperty("ts", Type.DATETIME_MICROS);

      database.transaction(() -> {
        assertThatThrownBy(() -> database.newDocument("Ev8090Bad").set("ts", "not-a-timestamp").save())//
            .isInstanceOf(IllegalArgumentException.class)//
            .hasMessageContaining("ts")//
            .rootCause().hasMessageContaining("not-a-timestamp");

        // Nothing must have been written: the failure has to leave the type empty rather than holding a NULL row.
        assertThat(database.countType("Ev8090Bad", false)).isZero();
      });

      // The same refusal reaches a SQL client instead of an INSERT that reports success.
      database.transaction(() -> assertThatThrownBy(
          () -> database.command("sql", "INSERT INTO Ev8090Bad SET ts = ?", "not-a-timestamp")).isInstanceOf(
          RuntimeException.class));
    }

    /**
     * The Zulu form keeps its wall-clock, unchanged by issue #8090: that is what Cypher's {@code datetime()} round-trip
     * depends on (issue #4125). An offset-bearing literal names an instant and is stored as its UTC wall clock, like the
     * same instant typed as an {@code OffsetDateTime} (issue #10489).
     */
    @Test
    void zuluKeepsItsWallClockAndAnOffsetIsConvertedToUtc() {
      final DocumentType type = database.getSchema().createDocumentType("Ev8090Zone");
      type.createProperty("ts", Type.DATETIME_MICROS);

      final LocalDateTime wallClock = LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000);
      final LocalDateTime utcWallClock = LocalDateTime.of(2024, 2, 29, 12, 45, 10, 123_456_000);

      database.transaction(() -> {
        final MutableDocument zulu = database.newDocument("Ev8090Zone").set("ts", "2024-02-29T13:45:10.123456Z").save();
        assertThat(zulu.<LocalDateTime>get("ts")).isEqualTo(wallClock);

        for (final String literal : new String[] { "2024-02-29T13:45:10.123456+01:00", "2024-02-29 13:45:10.123456+01" }) {
          final MutableDocument doc = database.newDocument("Ev8090Zone").set("ts", literal).save();
          assertThat(doc.<LocalDateTime>get("ts")).as("literal '%s'", literal).isEqualTo(utcWallClock);
        }
      });
    }

    /**
     * The same literal must reach the same instant through the bulk path that bypasses {@code Type.convert}.
     */
    @Test
    void dateUtilsSharedPathAcceptsTheSameForms() {
      final long micros = DateUtils.dateTimeToTimestamp(database, "2024-02-29 13:45:10.123456", ChronoUnit.MICROS);
      final long isoMicros = DateUtils.dateTimeToTimestamp(database, "2024-02-29T13:45:10.123456", ChronoUnit.MICROS);
      assertThat(micros).isEqualTo(isoMicros);
    }

    /**
     * Without a database in scope the chain used to guess the format from the string's LENGTH, so anything that did not
     * happen to be 10, 19 or 23 characters long fell through to {@code null}. One formatter now covers the whole family.
     */
    @Test
    void sharedPathWithoutDatabaseAcceptsTheWholeFamily() {
      assertThat(DateUtils.parseDateTime(null, "2024-02-29 13:45:10.123456")).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));
      assertThat(DateUtils.parseDateTime(null, "2024-02-29 13:45:10")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10));
      assertThat(DateUtils.parseDateTime(null, "2024-02-29 13:45")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45));
      assertThat(DateUtils.parseDateTime(null, "2024-02-29")).isEqualTo(LocalDateTime.of(2024, 2, 29, 0, 0));
      assertThat(DateUtils.parseDateTime(null, "2024-02-29T13:45:10.123456789")).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_789));

      assertThatThrownBy(() -> DateUtils.parseDateTime(null, "29/02/2024 13:45:10")).isInstanceOf(
          DateTimeParseException.class);
    }

    /**
     * PostgreSQL renders a {@code timestamptz} offset in three widths and the ODBC driver passes whichever the server
     * sent straight back; all three have to denote the same instant.
     */
    @Test
    void everyOffsetWidthDenotesTheSameInstant() {
      // An offset is stored as the UTC wall clock of its instant (issues #10489, #10498), so all three widths have to land on one instant.
      final LocalDateTime expected = OffsetDateTime.parse("2024-02-29T13:45:10.123456+01:00")
          .atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();

      for (final String offset : new String[] { "+01", "+0100", "+01:00" })
        assertThat(DateUtils.parseDateTime(database, "2024-02-29 13:45:10.123456" + offset)).as("offset '%s'", offset)
            .isEqualTo(expected);

      // The wall-clock-preserving variant reads the same three literals without moving them.
      for (final String offset : new String[] { "+01", "+0100", "+01:00" })
        assertThat(DateUtils.parseDateTimeKeepingWallClock(database, "2024-02-29 13:45:10.123456" + offset))//
            .as("offset '%s'", offset).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));
    }

    /**
     * The schema's own patterns keep priority: the SQL-timestamp formatter is a widening fallback, never a reinterpreter.
     */
    @Test
    void schemaDateTimeFormatStillWins() {
      final String original = database.getSchema().getDateTimeFormat();
      database.getSchema().setDateTimeFormat("dd/MM/yyyy HH:mm:ss");
      try {
        assertThat(DateUtils.parseDateTime(database, "29/02/2024 13:45:10")).isEqualTo(
            LocalDateTime.of(2024, 2, 29, 13, 45, 10));
        // ...and the SQL-timestamp spelling is still accepted alongside it.
        assertThat(DateUtils.parseDateTime(database, "2024-02-29 13:45:10.123456")).isEqualTo(
            LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));
      } finally {
        database.getSchema().setDateTimeFormat(original);
      }
    }

    /**
     * The read-side SQL surfaces guessed the pattern from the string's length the same way the write side did, so they
     * answered an empty result for the very literal the write side dropped.
     */
    @Test
    void sqlDateFunctionAndAsDateTimeReadTheSameLiteral() {
      database.transaction(() -> {
        assertThat(database.query("sql", "SELECT date('2024-02-29 13:45:10.123456') AS d").next().<Object>getProperty("d"))//
            .isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));

        assertThat(
            database.query("sql", "SELECT '2024-02-29 13:45:10.123456'.asDatetime() AS d").next().<Object>getProperty("d"))//
            .isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));

        // An explicit format still wins over the shared chain.
        assertThat(database.query("sql", "SELECT date('29/02/2024', 'dd/MM/yyyy') AS d").next().<Object>getProperty("d"))//
            .isEqualTo(LocalDateTime.of(2024, 2, 29, 0, 0));

        // Both read-side surfaces keep their documented answer for a value they cannot read at all: null, not an
        // error. Only the WRITE path refuses - that asymmetry is the whole point of the split.
        assertThat(database.query("sql", "SELECT date('not-a-timestamp') AS d").next().<Object>getProperty("d")).isNull();
        assertThat(
            database.query("sql", "SELECT 'not-a-timestamp'.asDatetime() AS d").next().<Object>getProperty("d")).isNull();
      });
    }

    /**
     * Making {@code Type.convert} strict must not reach the paths that merely COERCE values they did not write.
     * An index on a schemaless property sees whatever the records hold, so one row of an entirely different shape
     * has to index under a null key rather than abort the build - for EVERY index family, not just the LSM one
     * (the hash index carries its own copy of the key conversion; it was missed in the first cut of this fix).
     */
    @Test
    void aHeterogeneousRowDoesNotFailCreateIndex() {
      for (final Schema.INDEX_TYPE indexType : new Schema.INDEX_TYPE[] { Schema.INDEX_TYPE.LSM_TREE,
          Schema.INDEX_TYPE.HASH }) {
        final String typeName = "Het8090" + indexType.name();
        database.getSchema().createDocumentType(typeName);

        database.transaction(() -> {
          database.newDocument(typeName).set("ts", "2024-02-29 13:45:10.123456").save();
          database.newDocument(typeName).set("ts", "not a date").save();
        });

        // The property type is only settled now, by the index: the rows above were written while it had none.
        database.getSchema().getType(typeName).createProperty("ts", Type.DATETIME_MICROS);
        database.getSchema().buildTypeIndex(typeName, new String[] { "ts" }).withType(indexType).withUnique(false)
            .create();

        // ...and an ordinary write of an unindexable value afterwards is refused by the WRITE path, loudly, rather
        // than indexing a null key: that is the half of the split that must stay strict.
        database.transaction(() -> assertThatThrownBy(
            () -> database.newDocument(typeName).set("ts", "still not a date").save()).isInstanceOf(
            IllegalArgumentException.class));

        assertThat(database.countType(typeName, false)).as("index type %s", indexType).isEqualTo(2);
      }
    }

    /**
     * ...and the mercy the index key conversion shows stops at a date it cannot read. A value of a shape the key type
     * cannot take at all - a non-numeric string on a LONG key - raised {@link NumberFormatException} through the build
     * before this fix and still has to, rather than being silently indexed under a null key. That is the difference
     * between {@code convertIndexKeyOrNull()} and the blanket {@code convertOrNull()}, and it is the reason the index
     * sites do not simply use the latter.
     */
    @Test
    void aNonDateMismatchStillFailsTheIndexBuild() {
      for (final Schema.INDEX_TYPE indexType : new Schema.INDEX_TYPE[] { Schema.INDEX_TYPE.LSM_TREE,
          Schema.INDEX_TYPE.HASH }) {
        final String typeName = "Num8090" + indexType.name();
        database.getSchema().createDocumentType(typeName);

        database.transaction(() -> {
          database.newDocument(typeName).set("n", 42L).save();
          database.newDocument(typeName).set("n", "not a number").save();
        });

        database.getSchema().getType(typeName).createProperty("n", Type.LONG);

        assertThatThrownBy(() -> database.getSchema().buildTypeIndex(typeName, new String[] { "n" }).withType(indexType)
            .withUnique(false).create())//
            .as("index type %s", indexType)//
            .rootCause().isInstanceOf(NumberFormatException.class);
      }

      // The policy itself, directly: an unreadable date is a null key, every other refusal still escapes.
      assertThat(Type.convertIndexKeyOrNull(database, "not a date", LocalDateTime.class)).isNull();
      assertThatThrownBy(() -> Type.convertIndexKeyOrNull(database, "not a number", Long.class))//
          .isInstanceOf(NumberFormatException.class);
    }

    /**
     * Routing {@code date()}'s no-format path through the shared chain must not lose the argument CHECKING that path
     * used to get for free: a length-matched string went through {@code formatterFor()}, so an unknown time zone id
     * was reported as a client error (issue #6388) rather than ignored. An id that cannot exist is a mistake in the
     * call whether or not the branch has anything to apply it to.
     */
    @Test
    void dateStillRejectsAnUnknownTimeZoneWithNoFormatNamed() {
      database.transaction(() -> {
        assertThatThrownBy(() -> database.query("sql",
            "SELECT date('2024-02-29 13:45:10.123456', { timezone: 'Nowhere/Atall' }) AS d").next())//
            .isInstanceOf(IllegalArgumentException.class)//
            .rootCause().hasMessageContaining("Nowhere/Atall");

        // A zone that does exist is accepted and, on this path, changes nothing about what is read - the chain reads
        // no zone out of the text, which is exactly what the branch's comment claims.
        assertThat(database.query("sql", "SELECT date('2024-02-29 13:45:10.123456', { timezone: 'Asia/Tokyo' }) AS d")
            .next().<Object>getProperty("d"))//
            .isEqualTo(database.query("sql", "SELECT date('2024-02-29 13:45:10.123456') AS d").next()
                .<Object>getProperty("d"));
      });
    }

    /**
     * A MIN/MAX bound on a precision-bearing datetime property is checked against the value, rather than reported as a
     * violation whatever the value is. {@code DATETIME_MICROS} and its siblings had no arm in the validator's switch,
     * so they fell to the {@code default} one - which reports a violation UNCONDITIONALLY. Setting a bound on the very
     * type this issue was reported on therefore failed every subsequent write to that property.
     */
    @Test
    void aBoundOnAPrecisionDatetimePropertyIsActuallyCompared() {
      for (final Type type : new Type[] { Type.DATETIME_SECOND, Type.DATETIME_MICROS, Type.DATETIME_NANOS }) {
        final String typeName = "Bound8090" + type.name();
        database.getSchema().createDocumentType(typeName).createProperty("ts", type)//
            .setMin("2024-01-01 00:00:00").setMax("2030-01-01 00:00:00");

        database.transaction(() -> {
          // Inside the bounds: this used to be refused too, which is what made the property unwritable.
          database.newDocument(typeName).set("ts", "2024-02-29 13:45:10.123456").save();

          // ...and outside them it is still refused, so the bound is being read rather than ignored.
          assertThatThrownBy(() -> database.newDocument(typeName).set("ts", "2031-06-01 00:00:00").save())//
              .as("type %s", type)//
              .isInstanceOf(ValidationException.class);
        });

        assertThat(database.countType(typeName, false)).as("type %s", type).isEqualTo(1);
      }
    }

    /**
     * The {@code Instant} branch of {@code Type.convert} had no {@code String} case at all, so with
     * {@code arcadedb.dateTimeImplementation=java.time.Instant} a datetime literal stayed in the record as the raw
     * {@link String} it arrived as. It now goes through the same shared chain as every other datetime target.
     */
    @Test
    void instantTargetReadsTheSameLiterals() {
      final Instant expected = LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000)
          .atZone(database.getSchema().getZoneId()).toInstant();

      for (final String literal : new String[] { "2024-02-29 13:45:10.123456", "2024-02-29T13:45:10.123456" })
        assertThat(Type.convert(database, literal, Instant.class)).as("literal '%s'", literal).isEqualTo(expected);

      assertThatThrownBy(() -> Type.convert(database, "not-a-timestamp", Instant.class))//
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The grammar is exactly what its Javadoc says, in both directions: a fractional second only qualifies a second
     * that is actually there, and an offset only offsets a time that is actually there.
     */
    @Test
    void theGrammarAcceptsNothingBeyondWhatItDocuments() {
      assertThatThrownBy(() -> DateUtils.parseDateTime(null, "2024-02-29 13:45.123456"))//
          .as("a fraction with no second to qualify")
          .isInstanceOf(DateTimeParseException.class);

      assertThatThrownBy(() -> DateUtils.parseDateTime(null, "2024-02-29+01:00"))//
          .as("an offset with no time to offset")
          .isInstanceOf(DateTimeParseException.class);

      // The shapes either of those could be mistaken for are still read.
      assertThat(DateUtils.parseDateTime(null, "2024-02-29")).isEqualTo(LocalDateTime.of(2024, 2, 29, 0, 0));
      assertThat(DateUtils.parseDateTime(null, "2024-02-29 13:45")).isEqualTo(LocalDateTime.of(2024, 2, 29, 13, 45));
    }

    /**
     * Every READ-side surface answers {@code null} for a value it cannot express, rather than aborting the query at
     * the first row that does not fit. Making {@code Type.convert} strict must not leak past the write path.
     */
    @Test
    void readSideSurfacesAnswerNullRatherThanFailing() {
      database.getSchema().createDocumentType("Read8090");
      database.transaction(() -> {
        database.newDocument("Read8090").set("v", "2024-02-29 13:45:10.123456").save();
        database.newDocument("Read8090").set("v", "not a date").save();
      });

      database.transaction(() -> {
        // convert() and asDate()/asDatetime() must each yield one row with a value and one with null, not an error.
        for (final String projection : new String[] { "v.convert('datetime')", "v.asDatetime()", "v.asDate()" }) {
          int nulls = 0, values = 0;
          final ResultSet rs = database.query("sql", "SELECT " + projection + " AS d FROM Read8090");
          while (rs.hasNext()) {
            if (rs.next().getProperty("d") == null)
              ++nulls;
            else
              ++values;
          }
          assertThat(nulls).as("%s nulls", projection).isEqualTo(1);
          assertThat(values).as("%s values", projection).isEqualTo(1);
        }
      });
    }

    /**
     * The typed date/time accessors READ a value the record already holds, so one they cannot express keeps
     * answering null: the value is not lost, it is simply not that shape. Only a WRITE refuses.
     */
    @Test
    void typedDateAccessorsStayLenient() {
      database.getSchema().createDocumentType("Acc8090");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Acc8090").set("v", "not a date").save();

        assertThat(doc.getDate("v")).isNull();
        assertThat(doc.getCalendar("v")).isNull();
        assertThat(doc.getLocalDate("v")).isNull();
        assertThat(doc.getLocalDateTime("v")).isNull();
        assertThat(doc.getZonedDateTime("v")).isNull();
        assertThat(doc.getInstant("v")).isNull();

        // ...and the value itself was never lost, which is why answering null here costs nothing.
        assertThat(doc.getString("v")).isEqualTo("not a date");
      });
    }

    /**
     * For a {@code ZonedDateTime} target the zone is the whole point of the type, so an offset the input carries is
     * preserved rather than dropped - and the same moment denotes the same instant whichever separator it arrives
     * with. Before issue #8090 every string reaching this branch answered null, because the only patterns it tried
     * carry no zone at all.
     */
    @Test
    void zonedDateTimeTargetPreservesAnOffsetWhicheverSeparator() {
      final Instant expected = OffsetDateTime.parse("2024-02-29T13:45:10.123456+01:00").toInstant();

      for (final String literal : new String[] { "2024-02-29T13:45:10.123456+01:00", "2024-02-29 13:45:10.123456+01:00",
          "2024-02-29 13:45:10.123456+01" }) {
        final Object converted = Type.convert(database, literal, ZonedDateTime.class);
        assertThat(converted).as("literal '%s'", literal).isInstanceOf(ZonedDateTime.class);
        assertThat(((ZonedDateTime) converted).toInstant()).as("literal '%s'", literal).isEqualTo(expected);
      }

      // With no offset to preserve there is nothing to keep, so the wall-clock is anchored to the database's zone.
      assertThat(Type.convert(database, "2024-02-29 13:45:10.123456", ZonedDateTime.class)).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000).atZone(database.getSchema().getZoneId()));

      assertThatThrownBy(() -> Type.convert(database, "not-a-timestamp", ZonedDateTime.class))//
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The third policy: a caller materializing a value it did not write, with no schema in scope to read date
     * patterns from - the remote client - keeps what it was handed rather than nulling or refusing it. A value the
     * server formatted with a custom pattern is not readable client-side, and losing it there would be its own
     * silent data loss.
     */
    @Test
    void convertOrKeepHandsBackWhatItCannotRead() {
      // No Database, so no schema patterns: this is exactly the remote client's situation.
      assertThat(Type.convertOrKeep(null, "01 01 0001 BC", LocalDate.class)).isEqualTo("01 01 0001 BC");
      assertThat(Type.convertOrKeep(null, "not a date", LocalDateTime.class)).isEqualTo("not a date");

      // ...and it still converts whatever it CAN read, so keeping is the fallback, not the behaviour.
      assertThat(Type.convertOrKeep(null, "2024-02-29 13:45:10.123456", LocalDateTime.class)).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000));

      // The sibling policies on the same input: no value, and refuse.
      assertThat(Type.convertOrNull(null, "01 01 0001 BC", LocalDate.class)).isNull();
      assertThatThrownBy(() -> Type.convert(null, "01 01 0001 BC", LocalDate.class))//
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Every target that IS an instant keeps the offset the value carries, from whichever format read it - including a
     * schema pattern that captures one, which resolved through {@code LocalDateTime} and threw the offset away before
     * any caller could see it. Re-anchoring what was left to the database's zone landed on a different moment than
     * the value named.
     */
    @Test
    void anOffsetCapturedByASchemaPatternReachesEveryInstantTarget() {
      final String original = database.getSchema().getDateTimeFormat();
      database.getSchema().setDateTimeFormat("yyyy-MM-dd HH:mm:ss XXX");
      try {
        final String literal = "2024-02-29 13:45:10 +01:00";
        final Instant expected = OffsetDateTime.parse("2024-02-29T13:45:10+01:00").toInstant();

        assertThat(((ZonedDateTime) Type.convert(database, literal, ZonedDateTime.class)).toInstant()).isEqualTo(expected);
        assertThat(Type.convert(database, literal, Instant.class)).isEqualTo(expected);
        assertThat(((Date) Type.convert(database, literal, Date.class)).toInstant()).isEqualTo(expected);

        // The LocalDateTime target has no zone to keep, so it stores the UTC wall clock of that instant (issue #10489).
        assertThat(Type.convert(database, literal, LocalDateTime.class)).isEqualTo(
            LocalDateTime.of(2024, 2, 29, 12, 45, 10));
      } finally {
        database.getSchema().setDateTimeFormat(original);
      }
    }

    /**
     * With no database in scope there is no zone to rebase onto, so an offset-bearing value must be read as the
     * instant it names rather than have its offset dropped and the remainder anchored to the JVM's zone.
     */
    @Test
    void anOffsetSurvivesWithNoDatabaseInScope() {
      final Instant expected = OffsetDateTime.parse("2024-02-29T13:45:10.123456+01:00").toInstant();

      assertThat(DateUtils.parseZonedDateTime(null, "2024-02-29T13:45:10.123456+01:00").toInstant()).isEqualTo(expected);
      assertThat(DateUtils.parseZonedDateTime(null, "2024-02-29 13:45:10.123456+01:00").toInstant()).isEqualTo(expected);
      assertThat(Type.convert(null, "2024-02-29T13:45:10.123456+01:00", Instant.class)).isEqualTo(expected);
    }

    /**
     * A {@code DATE} property refuses an unreadable string exactly as a {@code DATETIME_MICROS} one does. Both target
     * classes behind it changed error path in this fix - {@code LocalDate}'s no-database branch used to fall off the
     * end and answer the original String, and {@code convertToDate} used to guess by length and give up - so the
     * refusal is pinned for each rather than assumed to follow from the datetime case.
     */
    @Test
    void aDatePropertyAlsoRefusesAnUnreadableString() {
      database.getSchema().createDocumentType("Ev8090Date").createProperty("d", Type.DATE);

      database.transaction(() -> {
        assertThatThrownBy(() -> database.newDocument("Ev8090Date").set("d", "not-a-date").save())//
            .isInstanceOf(IllegalArgumentException.class)//
            .rootCause().hasMessageContaining("not-a-date");

        assertThat(database.countType("Ev8090Date", false)).isZero();

        // ...and it still reads the spellings it should, including the one this issue is about.
        assertThat(database.newDocument("Ev8090Date").set("d", "2024-02-29 13:45:10.123456").get("d")).isEqualTo(
            LocalDate.of(2024, 2, 29));
      });

      // Both target classes directly, since the property type only exercises whichever one is configured.
      assertThatThrownBy(() -> Type.convert(database, "not-a-date", LocalDate.class))//
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> Type.convert(database, "not-a-date", Date.class))//
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> Type.convert(null, "not-a-date", LocalDate.class))//
          .as("no database in scope: used to answer the original String, now refuses")
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The {@code LocalDate} target reads the schema's patterns through the SAME chain every other datetime target uses,
     * rather than through a second copy of "try dateTimeFormat, then dateFormat" open-coded in its own branch. Two
     * copies of that chain drifting apart is half of what issue #8090 was, so this pins that there is only one left:
     * both schema patterns, the SQL-timestamp spelling and plain ISO all have to answer through it, with a custom
     * dateTimeFormat in force.
     */
    @Test
    void aLocalDateReadsTheSchemaPatternsThroughTheSharedChain() {
      final String originalDateTime = database.getSchema().getDateTimeFormat();
      final String originalDate = database.getSchema().getDateFormat();
      database.getSchema().setDateTimeFormat("dd/MM/yyyy HH:mm:ss");
      database.getSchema().setDateFormat("dd/MM/yyyy");
      try {
        // The schema's dateTimeFormat, which the branch used to reach through its own LocalDate.parse call...
        assertThat(Type.convert(database, "29/02/2024 13:45:10", LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));
        // ...its dateFormat, which was the second copy's second attempt...
        assertThat(Type.convert(database, "29/02/2024", LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));
        // ...and the built-in shapes behind both, the SQL-timestamp spelling this issue is about included.
        assertThat(Type.convert(database, "2024-02-29 13:45:10.123456", LocalDate.class)).isEqualTo(
            LocalDate.of(2024, 2, 29));
        assertThat(Type.convert(database, "2024-02-29T13:45:10", LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));
        assertThat(Type.convert(database, "2024-02-29", LocalDate.class)).isEqualTo(LocalDate.of(2024, 2, 29));

        // An offset is read as a wall clock here: a LocalDate has no zone, so the day written is the day meant and
        // nothing may shift it across midnight.
        assertThat(Type.convert(database, "2024-02-29T23:30:00+05:00", LocalDate.class)).isEqualTo(
            LocalDate.of(2024, 2, 29));

        // A custom dateTimeFormat in force does not make an unreadable value readable.
        assertThatThrownBy(() -> Type.convert(database, "not-a-date", LocalDate.class))//
            .isInstanceOf(IllegalArgumentException.class);
      } finally {
        database.getSchema().setDateTimeFormat(originalDateTime);
        database.getSchema().setDateFormat(originalDate);
      }
    }

    /**
     * Reading a literal answers what writing it would have stored, for an offset-bearing one too. The read-side
     * surfaces reached the shared chain through its REBASING overload while {@code Type.convert} used the
     * wall-clock-preserving one, so the same string had two answers depending on which entry point saw it - and
     * offset-bearing literals are exactly what these functions became newly able to read in this fix.
     */
    @Test
    void theReadSideAnswersWhatTheWriteSideWouldStore() {
      final String literal = "2026-01-01T00:00:00+01:00";
      // the UTC wall clock of the instant the literal names (issue #10489)
      final LocalDateTime expected = LocalDateTime.of(2025, 12, 31, 23, 0);

      // The write path, and the SQL surfaces that read the same string, must not disagree.
      assertThat(Type.convert(database, literal, LocalDateTime.class)).as("write path").isEqualTo(expected);

      database.transaction(() -> {
        for (final String projection : new String[] { "'" + literal + "'.asDatetime()", "date('" + literal + "')" })
          assertThat(database.query("sql", "SELECT " + projection + " AS d").next().<Object>getProperty("d"))//
              .as(projection).isEqualTo(expected);

        assertThat(database.query("sql", "SELECT '" + literal + "'.asDate() AS d").next().<Object>getProperty("d"))//
            .isEqualTo(LocalDate.of(2026, 1, 1));

        // convert('datetime') is NOT in that list on purpose: DATETIME's default Java type is java.util.Date, an
        // INSTANT, so it keeps the offset rather than the wall-clock - the same split this fix draws everywhere
        // else. Pinned here so the difference reads as the policy it is rather than as the bug just fixed.
        assertThat(database.query("sql", "SELECT '" + literal + "'.convert('datetime') AS d").next()
            .<Object>getProperty("d")).isEqualTo(Date.from(OffsetDateTime.parse(literal).toInstant()));
      });
    }

    /**
     * The same contract change the {@code LocalDate} branch got, for its two siblings: with no database in scope these
     * used to fall off the end and answer the ORIGINAL String when the string-length guess matched nothing, and now
     * refuse. {@code Type.convert} is public, so this is pinned for each rather than left to be discovered.
     */
    @Test
    void everyNoDatabaseStringBranchRefusesRatherThanPassingTheValueThrough() {
      for (final Class<?> target : new Class<?>[] { LocalDateTime.class, ZonedDateTime.class, LocalDate.class,
          Instant.class, Date.class })
        assertThatThrownBy(() -> Type.convert(null, "not-a-timestamp", target))//
            .as("target %s", target.getSimpleName())//
            .isInstanceOf(IllegalArgumentException.class);

      // The lenient policies are what a caller reaches for when it wants the old shape back.
      assertThat(Type.convertOrNull(null, "not-a-timestamp", LocalDateTime.class)).isNull();
      assertThat(Type.convertOrKeep(null, "not-a-timestamp", LocalDateTime.class)).isEqualTo("not-a-timestamp");
    }

    /**
     * {@code convertOrKeep} promises the ORIGINAL value back, so it has to answer one even where {@code convert}
     * gave up through its blanket handler rather than by throwing - otherwise a value that reached the remote client
     * intact would be discarded there, which is this issue's own failure moved to the client.
     */
    @Test
    void convertOrKeepNeverAnswersNullForANonNullValue() {
      // A shape no branch can convert and none throws for: the blanket handler answers null inside convert().
      assertThat(Type.convertOrKeep(null, List.of("x"), Integer.class)).isEqualTo(List.of("x"));
      assertThat(Type.convertOrKeep(null, "not-a-timestamp", LocalDateTime.class)).isEqualTo("not-a-timestamp");

      // null in is still null out: there is no original to keep.
      assertThat(Type.convertOrKeep(null, null, LocalDateTime.class)).isNull();

      // ...but the leniency stops at date parsing. A value of a shape no branch can take at all failed the read
      // before this issue and still does: having no schema to read DATES with says nothing about those.
      assertThatThrownBy(() -> Type.convertOrKeep(null, new Object(), Date.class))//
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> Type.convertOrKeep(null, "not-a-number", Integer.class))//
          .isInstanceOf(NumberFormatException.class);
    }

    /**
     * The MIN/MAX date constraint was the one conversion site the audit had not reached. It is a write-time check, so
     * it keeps the strict conversion - but a bound nothing can read now reports the schema layer's own
     * ValidationException naming which side failed, where it used to convert to null and then NPE on the comparison.
     */
    @Test
    void anUnreadableDateBoundIsAValidationErrorRatherThanAnNPE() {
      // Since #9172 (#9026) such a bound is refused when it is declared, instead of being accepted and then failing every
      // write; the write-time ValidationException stays for a bound a schema written before that still carries.
      final DocumentType type = database.getSchema().createDocumentType("Ev8090Bound");
      final Property bounded = type.createProperty("d", Type.DATETIME);
      assertThatThrownBy(() -> bounded.setMax("not-a-date"))//
          .isInstanceOf(IllegalArgumentException.class)//
          .hasMessageContaining("Maximum")//
          .hasMessageContaining("not-a-date");
      assertThat(bounded.getMax()).isNull();

      // A readable bound still validates normally, in the SQL-timestamp spelling this issue added.
      final DocumentType ok = database.getSchema().createDocumentType("Ev8090BoundOk");
      ok.createProperty("d", Type.DATETIME).setMax("2024-02-29 13:45:10");

      database.transaction(() -> {
        database.newDocument("Ev8090BoundOk").set("d", "2024-02-28 10:00:00").save();
        assertThatThrownBy(() -> database.newDocument("Ev8090BoundOk").set("d", "2024-03-01 10:00:00").save())//
            .isInstanceOf(ValidationException.class);
      });
    }

    /**
     * {@code java.util.Date} is the other target class fed by the same literal shape.
     */
    @Test
    void dateTargetAcceptsTheSpaceSeparatedFraction() {
      final Object converted = Type.convert(database, "2024-02-29 13:45:10.123456", Date.class);
      assertThat(converted).isInstanceOf(Date.class);
      assertThat(((Date) converted).getTime()).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_000_000).atZone(database.getSchema().getZoneId()).toInstant()
              .toEpochMilli());
    }
  }

  /**
   * Regression for #8993: canonical number key.
   *
   * Issue #8993: one number is one key over a STRING key type, whatever type it was written with. A declared STRING property keeps
   * answering a lookup by the text it holds.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CanonicalNumberKeyTest extends TestHelper {

    @Test
    void numbersOfEveryTypeShareOneSpelling() {
      assertThat(key(3.0d)).isEqualTo("3");
      assertThat(key(3.0f)).isEqualTo("3");
      assertThat(key(new BigDecimal("3.00"))).isEqualTo("3");
      assertThat(key(-0.0d)).isEqualTo("0");
      assertThat(key(0.1f)).isEqualTo("0.1");
      assertThat(key(2.5d)).isEqualTo("2.5");
      assertThat(key(1.0E10d)).isEqualTo("10000000000");
      assertThat(key(Double.NaN)).isEqualTo("NaN");
      assertThat(key(Double.POSITIVE_INFINITY)).isEqualTo("Infinity");
      assertThat(key(new BigDecimal("1E+999999"))).isEqualTo("1E+999999");
    }

    @Test
    void declaredStringPropertyIsStillFoundByItsText() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE S").close();
        database.command("sql", "CREATE PROPERTY S.s STRING").close();
        database.command("sql", "CREATE INDEX ON S (s) NOTUNIQUE").close();
        database.command("sql", "INSERT INTO S SET s = '3.0'").close();
      });
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM S WHERE s = '3.0'")) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM S WHERE s = ?", 3.0d)) {
        assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
      }
    }

    private static Object key(final Object number) {
      return Type.canonicalNumberKey(number);
    }
  }

  /**
   * Regression for #9028: scalar into typed list.
   *
   * A scalar written to a LIST OF &lt;number&gt; property is wrapped as one element and converted like the elements of a list are (#9028).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @SuppressWarnings("unchecked")
  @Nested
  class ScalarIntoTypedListTest extends TestHelper {

    @Test
    void sqlScalarIsConvertedToTheDeclaredElementType() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.anything LIST");
      database.command("sql", "CREATE PROPERTY T.longs LIST OF LONG");
      database.command("sql", "CREATE PROPERTY T.doubles LIST OF DOUBLE");

      database.transaction(() -> {
        final Result r = database.command("sql", "INSERT INTO T SET anything = 5, longs = 5, doubles = 5").next();
        assertThat((List<Object>) r.getProperty("anything")).containsExactly(5);
        assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
        assertThat((List<Object>) r.getProperty("doubles")).containsExactly(5.0D);
      });
      database.transaction(() -> {
        final Result r = database.command("sql", "INSERT INTO T SET longs = [5], doubles = 5.0").next();
        assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
        assertThat((List<Object>) r.getProperty("doubles")).containsExactly(5.0D);
      });
    }

    @Test
    void cypherCreateAndSetAgreeOnScalarIntoTypedList() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE PROPERTY V.ints LIST OF INTEGER");
      database.command("sql", "CREATE PROPERTY V.longs LIST OF LONG");

      database.transaction(() -> database.command("opencypher", "CREATE (:V {id: 0})").close());
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:V {id: 1, ints: 5, longs: 5})").close();
        database.command("opencypher", "MATCH (n:V {id: 0}) SET n.ints = 5, n.longs = 5").close();
      });

      for (final int id : new int[] { 0, 1 }) {
        final Result r = database.query("sql", "SELECT ints, longs FROM V WHERE id = " + id).next();
        assertThat((List<Object>) r.getProperty("ints")).containsExactly(5);
        assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
      }
    }

    @Test
    void primitiveArrayIsConvertedElementByElement() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.longs LIST OF LONG");
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("P");
        doc.set("longs", new int[] { 1, 2 });
        doc.save();
        assertThat((List<Object>) doc.get("longs")).containsExactly(1L, 2L);
      });
    }

    @Test
    void scalarIntoListOfString() {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE PROPERTY S.strs LIST OF STRING");
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("S");
        doc.set("strs", 7);
        doc.save();
        assertThat((List<Object>) doc.get("strs")).containsExactly("7");
      });
    }

    @Test
    void scalarIntoListOfDate() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.days LIST OF DATE");
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("D");
        doc.set("days", LocalDate.of(2026, 10, 3));
        doc.save();
        final List<Object> days = (List<Object>) doc.get("days");
        assertThat(days).hasSize(1);
        assertThat(String.valueOf(days.get(0))).contains("2026");
      });
    }

    @Test
    void unconvertibleTextIntoListOfIntegerIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE B");
      database.command("sql", "CREATE PROPERTY B.ints LIST OF INTEGER");
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO B SET ints = 'abc'")))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ints");
    }

    @Test
    void outOfRangeScalarIsRefusedNotClamped() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.command("sql", "CREATE PROPERTY R.longs LIST OF LONG");
      assertThatThrownBy(() -> database.transaction(() -> {
        final var doc = database.newDocument("R");
        doc.set("longs", new BigDecimal("9223372036854775808"));
        doc.save();
      })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("longs");
    }

    @Test
    void outOfRangeListElementIsRefusedNotClamped() {
      database.command("sql", "CREATE DOCUMENT TYPE E");
      database.command("sql", "CREATE PROPERTY E.longs LIST OF LONG");
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument("E");
        doc.set("longs", List.of(new BigDecimal("9223372036854775808")));
        doc.save();
      })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("longs");
    }

    @Test
    void longAboveIntegerRangeIsRefusedInListOfInteger() {
      database.command("sql", "CREATE DOCUMENT TYPE G");
      database.command("sql", "CREATE PROPERTY G.ints LIST OF INTEGER");
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument("G");
        doc.set("ints", Integer.MAX_VALUE + 1L);
        doc.save();
      })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ints");
    }

    @Test
    void mapOfLongConvertsAndRefusesLikeAList() {
      database.command("sql", "CREATE DOCUMENT TYPE M");
      database.command("sql", "CREATE PROPERTY M.values MAP OF LONG");
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("M");
        doc.set("values", java.util.Map.of("a", 5));
        doc.save();
        assertThat(((java.util.Map<String, Object>) doc.get("values")).get("a")).isEqualTo(5L);
      });
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument doc = database.newDocument("M");
        doc.set("values", java.util.Map.of("a", new BigDecimal("9223372036854775808")));
        doc.save();
      })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("values");
    }
  }

  /**
   * Regression for #9110: declared type conversion.
   *
   * Issue #9110 (consolidates #9014, #9015, #9041, #9027, #9024): a value the declared property type cannot hold must be
   * refused with an error naming the property, never stored as NULL, as 0, as the wrong type or as a wrapped number.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DeclaredTypeConversionTest extends TestHelper {

    private void assertRefused(final String typeName, final String property, final Object value) {
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(typeName).set(property, value).save()))
          .as("%s <- %s", property, value == null ? null : value.getClass().getSimpleName() + " " + value)
          .isInstanceOf(IllegalArgumentException.class);
    }

    private Object roundTrip(final String typeName, final String property, final Object value) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).save().getIdentity());
      final Document d = database.lookupByRID(rid[0], true).asDocument();
      return d.get(property);
    }

    @Test
    void inconvertibleValueIsNeverStoredAsNullInANumericProperty() {
      final DocumentType type = database.getSchema().createDocumentType("Num9110");
      final String[] props = { "b", "s", "i", "l", "f", "d" };
      final Type[] types = { Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE };
      for (int i = 0; i < props.length; i++)
        type.createProperty(props[i], types[i]);

      final Object[] bad = { true, new ArrayList<>(List.of(1, 2)), new HashMap<>(Map.of("a", 1)), LocalDate.of(2026, 10, 3) };
      for (final String p : props)
        for (final Object v : bad) {
          // a date written to a LONG is the epoch milliseconds, as documented for DATE -> LONG
          if (p.equals("l") && v instanceof LocalDate)
            continue;
          assertRefused("Num9110", p, v);
        }
    }

    @Test
    void errorNamesTheProperty() {
      database.getSchema().createDocumentType("Named9110").createProperty("age", Type.INTEGER);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Named9110").set("age", true).save()))
          .hasStackTraceContaining("age");
    }

    @Test
    void sqlAndCypherRefuseInconvertibleValues() {
      database.getSchema().createVertexType("T9110").createProperty("integer", Type.INTEGER);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T9110 SET integer = true").close()))
          .isInstanceOf(RuntimeException.class);
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T9110 SET integer = [1, 2]").close()))
          .isInstanceOf(RuntimeException.class);
      assertThatThrownBy(
          () -> database.transaction(() -> database.command("opencypher", "CREATE (:T9110 {integer: true})").close()))
          .isInstanceOf(RuntimeException.class);
      assertThatThrownBy(
          () -> database.transaction(() -> database.command("opencypher", "CREATE (:T9110 {integer: [1, 2]})").close()))
          .isInstanceOf(RuntimeException.class);
      assertThat(database.countType("T9110", true)).isZero();
    }

    @Test
    void emptyStringIsRefusedForEveryNumericType() {
      final DocumentType type = database.getSchema().createDocumentType("Empty9110");
      final String[] props = { "b", "s", "i", "l", "f", "d", "dec" };
      final Type[] types = { Type.BYTE, Type.SHORT, Type.INTEGER, Type.LONG, Type.FLOAT, Type.DOUBLE, Type.DECIMAL };
      for (int i = 0; i < props.length; i++)
        type.createProperty(props[i], types[i]);
      for (final String p : props)
        assertRefused("Empty9110", p, "");
    }

    @Test
    void longRefusesValuesOutsideTheSixtyFourBitRange() {
      database.getSchema().createDocumentType("Long9110").createProperty("l", Type.LONG);
      final Object[] bad = { new BigInteger("12345678901234567890"), new BigDecimal("1E+20"), 1.0e19d, 1.0e19f, Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY, -1.0e19d, 9.223372036854775807E18d };
      for (final Object v : bad)
        assertRefused("Long9110", "l", v);

      assertThat(roundTrip("Long9110", "l", Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
      assertThat(roundTrip("Long9110", "l", Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
      assertThat(roundTrip("Long9110", "l", new BigInteger("9223372036854775807"))).isEqualTo(Long.MAX_VALUE);
      assertThat(roundTrip("Long9110", "l", -9.223372036854775808E18d)).isEqualTo(Long.MIN_VALUE);
      assertThat(roundTrip("Long9110", "l", 42.9d)).isEqualTo(42L);
    }

    @Test
    void numberToBooleanIsFalseOnlyForZero() {
      database.getSchema().createDocumentType("Bool9110").createProperty("flag", Type.BOOLEAN);
      assertThat((Boolean) roundTrip("Bool9110", "flag", 0)).isFalse();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 0.0d)).isFalse();
      assertThat((Boolean) roundTrip("Bool9110", "flag", BigDecimal.ZERO)).isFalse();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 1)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 2)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 0.5d)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", -0.5d)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 4294967296L)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", 1e10d)).isTrue();
      assertThat((Boolean) roundTrip("Bool9110", "flag", "TRUE")).isTrue();

      assertRefused("Bool9110", "flag", new ArrayList<>(List.of(1, 2)));
      assertRefused("Bool9110", "flag", LocalDate.of(2026, 10, 3));
    }

    @Test
    void booleanLookupByFractionMatchesTheTrueRecords() {
      database.getSchema().createDocumentType("BoolQ9110").createProperty("flag", Type.BOOLEAN);
      database.transaction(() -> {
        database.newDocument("BoolQ9110").set("flag", true).save();
        database.newDocument("BoolQ9110").set("flag", false).save();
      });
      assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 0.5").next().<Long>getProperty("c"))
          .isEqualTo(1L);
      assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 4294967296").next().<Long>getProperty("c"))
          .isEqualTo(1L);
      assertThat(database.query("sql", "SELECT count(*) AS c FROM BoolQ9110 WHERE flag = 0").next().<Long>getProperty("c"))
          .isEqualTo(1L);
    }

    @Test
    void typesWithoutACaseForTheValueRefuseItInsteadOfKeepingIt() {
      final DocumentType type = database.getSchema().createDocumentType("Keep9110");
      type.createProperty("dec", Type.DECIMAL);
      type.createProperty("dt", Type.DATETIME);
      type.createProperty("dtm", Type.DATETIME_MICROS);
      type.createProperty("bin", Type.BINARY);
      type.createProperty("link", Type.LINK);
      type.createProperty("str", Type.STRING);

      assertRefused("Keep9110", "dec", true);
      assertRefused("Keep9110", "dt", "");
      assertRefused("Keep9110", "dt", true);
      assertRefused("Keep9110", "dtm", new HashMap<>(Map.of("a", 1)));
      assertRefused("Keep9110", "bin", "abc");
      assertRefused("Keep9110", "link", "abc");
      assertRefused("Keep9110", "link", new ArrayList<>(List.of(1, 2)));
      assertRefused("Keep9110", "str", new byte[] { 1, 2, 3 });
      assertRefused("Keep9110", "str", new int[] { 1, 2, 3 });
    }

    @Test
    void byteArrayIsRefusedByEveryDeclaredTypeThatCannotHoldIt() {
      final DocumentType type = database.getSchema().createDocumentType("Bytes9110");
      type.createProperty("i", Type.INTEGER);
      type.createProperty("flag", Type.BOOLEAN);
      type.createProperty("dec", Type.DECIMAL);
      type.createProperty("dt", Type.DATETIME);
      for (final String p : new String[] { "i", "flag", "dec", "dt" })
        assertRefused("Bytes9110", p, new byte[] { 1, 2, 3 });
    }

    @Test
    void digitOnlyStringIsRefusedForEveryDateTarget() {
      final DocumentType type = database.getSchema().createDocumentType("Epoch9110");
      type.createProperty("dt", Type.DATETIME);
      type.createProperty("dts", Type.DATETIME_SECOND);
      type.createProperty("dtm", Type.DATETIME_MICROS);
      // as likely a compact date (yyyyMMdd, a year) as an epoch count: guessing would store a wrong instant
      for (final String p : new String[] { "dt", "dts", "dtm" })
        for (final String v : new String[] { "1791000000000", "20261003", "2026", "" })
          assertRefused("Epoch9110", p, v);
      // a number is still an epoch count
      assertThat(roundTrip("Epoch9110", "dt", 1791000000000L)).isEqualTo(LocalDateTime.of(2026, 10, 3, 4, 0));
    }

    @Test
    void crossJavaTimeShapesConvertToTheRightMoment() {
      final OffsetDateTime offset = OffsetDateTime.parse("2026-10-03T10:15:30+02:00");
      final Instant moment = Instant.parse("2026-10-03T08:15:30Z");
      assertThat(Type.convert(database, offset, Instant.class)).isEqualTo(moment);
      assertThat(Type.convert(database, offset, ZonedDateTime.class)).isEqualTo(offset.toZonedDateTime());
      assertThat(Type.convert(database, offset, LocalDate.class)).isEqualTo(LocalDate.of(2026, 10, 3));
      assertThat(Type.convert(database, ZonedDateTime.parse("2026-10-03T23:30:00-05:00"), LocalDate.class))
          .isEqualTo(LocalDate.of(2026, 10, 4));
      assertThat(Type.convert(database, LocalDateTime.of(2026, 10, 3, 10, 0), Instant.class))
          .isEqualTo(Instant.parse("2026-10-03T10:00:00Z"));
      assertThat(Type.convert(database, LocalDateTime.of(2026, 10, 3, 10, 0), ZonedDateTime.class))
          .isEqualTo(ZonedDateTime.of(2026, 10, 3, 10, 0, 0, 0, ZoneOffset.UTC));
      assertThat(Type.convert(database, LocalDate.of(2026, 10, 3), Instant.class)).isEqualTo(Instant.parse("2026-10-03T00:00:00Z"));
      assertThat(Type.convert(database, LocalDate.of(2026, 10, 3), LocalDateTime.class))
          .isEqualTo(LocalDateTime.of(2026, 10, 3, 0, 0));
      assertThat(Type.convert(database, LocalDate.of(2026, 10, 3), ZonedDateTime.class))
          .isEqualTo(ZonedDateTime.of(2026, 10, 3, 0, 0, 0, 0, ZoneOffset.UTC));

      final DocumentType type = database.getSchema().createDocumentType("Time9110");
      type.createProperty("dt", Type.DATETIME);
      type.createProperty("d", Type.DATE);
      assertThat(roundTrip("Time9110", "dt", offset)).isEqualTo(LocalDateTime.of(2026, 10, 3, 8, 15, 30));
      assertThat(roundTrip("Time9110", "d", offset)).isEqualTo(LocalDate.of(2026, 10, 3));
    }

    @Test
    void outOfRangeLongIsOnlyRefusedForADeclaredProperty() {
      // no property: the plain conversion, a getter or an index lookup by a literal, keeps clamping
      assertThat(Type.convert(database, 1e30d, Long.class)).isEqualTo(Long.MAX_VALUE);
      assertThat(Type.convertOrKeep(database, 1e30d, Long.class)).isEqualTo(Long.MAX_VALUE);
      assertThat(Type.convertIndexKeyOrNull(database, 1e30d, Long.class)).isEqualTo(Long.MAX_VALUE);
      database.getSchema().createDocumentType("LongProp9110").createProperty("l", Type.LONG);
      assertRefused("LongProp9110", "l", 1e30d);
    }

    @Test
    void floatAndDoubleRefuseAFiniteValueThatOverflows() {
      final DocumentType type = database.getSchema().createDocumentType("Ovf9110");
      type.createProperty("f", Type.FLOAT);
      type.createProperty("d", Type.DOUBLE);
      assertRefused("Ovf9110", "f", 1e40d);
      assertRefused("Ovf9110", "f", new BigDecimal("1E+400"));
      assertRefused("Ovf9110", "d", new BigDecimal("1E+400"));
      assertThat(roundTrip("Ovf9110", "f", Float.POSITIVE_INFINITY)).isEqualTo(Float.POSITIVE_INFINITY);
      assertThat(roundTrip("Ovf9110", "d", Double.POSITIVE_INFINITY)).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(roundTrip("Ovf9110", "f", 1.5d)).isEqualTo(1.5f);
    }

    @Test
    void nanAndDigitStringsStayLenientWithoutAProperty() {
      assertThat(Type.convert(database, Double.NaN, Boolean.class)).isEqualTo(false);
      assertThat(Type.convert(database, "20261003", LocalDateTime.class)).isEqualTo("20261003");
    }

    @Test
    void linkListOfInvalidRidStringsIsRefusedAndLenientWrappersKeepTheOriginal() {
      database.getSchema().createDocumentType("Link9110").createProperty("link", Type.LINK);
      assertRefused("Link9110", "link", new ArrayList<>(List.of("not-a-rid", "#1:2")));
      final List<Object> list = new ArrayList<>(List.of(1, 2));
      assertThat(Type.convertOrKeep(database, true, BigDecimal.class)).isEqualTo(true);
      assertThat(Type.convertIndexKeyOrNull(database, list, Integer.class)).isNull();
    }

    @Test
    void convertOrKeepWithAPropertyKeepsTheOriginalForContentRefusals() {
      final DocumentType type = database.getSchema().createDocumentType("Remote9110");
      type.createProperty("i", Type.INTEGER);
      type.createProperty("l", Type.LONG);
      type.createProperty("f", Type.FLOAT);
      type.createProperty("flag", Type.BOOLEAN);
      // the remote client reads records it did not write: a column that cannot be converted must not fail the read
      assertThat(Type.convertOrKeep(database, "", Integer.class, type.getProperty("i"))).isEqualTo("");
      assertThat(Type.convertOrKeep(database, 1e30d, Long.class, type.getProperty("l"))).isEqualTo(1e30d);
      assertThat(Type.convertOrKeep(database, 1e40d, Float.class, type.getProperty("f"))).isEqualTo(1e40d);
      assertThat(Type.convertOrKeep(database, Double.NaN, Boolean.class, type.getProperty("flag"))).isEqualTo(Double.NaN);
      // while a write of the same values is refused
      assertThatThrownBy(() -> Type.convert(database, "", Integer.class, type.getProperty("i")))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void digitStringIsKeptByTheRemoteReadAndRefusedByAWrite() {
      final DocumentType type = database.getSchema().createDocumentType("LongDigits9110");
      type.createProperty("dt", Type.DATETIME);
      final String digits = "123456789012345678901234567890";
      assertRefused("LongDigits9110", "dt", digits);
      assertThat(Type.convertOrKeep(database, digits, LocalDateTime.class, type.getProperty("dt"))).isEqualTo(digits);
    }

    @Test
    void singleElementListStillUnwrapsForALinkProperty() {
      database.getSchema().createDocumentType("LinkOne9110").createProperty("link", Type.LINK);
      final RID[] target = new RID[1];
      database.transaction(() -> target[0] = database.newDocument("LinkOne9110").save().getIdentity());
      assertThat(roundTrip("LinkOne9110", "link", new ArrayList<>(List.of(target[0])))).isEqualTo(target[0]);
    }

    @Test
    void nanToLongKeepsFailingWithoutAProperty() {
      assertThatThrownBy(() -> Type.convert(database, Double.NaN, Long.class)).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> Type.convertOrKeep(database, Double.NaN, Long.class)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dateKeepsReadingADigitOnlyStringAsAnEpochCount() {
      final DocumentType type = database.getSchema().createDocumentType("DateDigits9110");
      type.createProperty("d", Type.DATE);
      // DATE has always read a digit-only string as an epoch count (days), unlike the DATETIME targets
      assertThat(roundTrip("DateDigits9110", "d", "20")).isEqualTo(LocalDate.of(1970, 1, 21));
    }

    @Test
    void aListPropertyKeepsAcceptingNonLinkMembers() {
      database.getSchema().createDocumentType("ListMix9110").createProperty("l", Type.LIST);
      assertThat(roundTrip("ListMix9110", "l", new ArrayList<>(List.of("a", 1, true)))).isEqualTo(List.of("a", 1, true));
    }

    @Test
    void nullMemberOfALinkListIsRefusedWithAClearMessage() {
      database.getSchema().createDocumentType("LinkNull9110").createProperty("link", Type.LINK);
      final List<Object> list = new ArrayList<>();
      list.add(null);
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("LinkNull9110").set("link", list).save()))
          .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("link");
    }

    @Test
    void emptyCollectionIsRefusedByALinkProperty() {
      database.getSchema().createDocumentType("LinkEmpty9110").createProperty("link", Type.LINK);
      assertRefused("LinkEmpty9110", "link", new ArrayList<>());
    }

    @Test
    void indexKeyConversionAnswersNullForAnInconvertibleShape() {
      assertThat(Type.convertIndexKeyOrNull(database, new ArrayList<>(List.of(1, 2)), Integer.class)).isNull();
    }

    @Test
    void byteArrayIsRefusedByAListProperty() {
      database.getSchema().createDocumentType("BytesList9110").createProperty("l", Type.LIST);
      assertRefused("BytesList9110", "l", new byte[] { 1, 2 });
      assertThat(roundTrip("BytesList9110", "l", new ArrayList<>(List.of(1, 2)))).isEqualTo(List.of(1, 2));
    }

    @Test
    void nanIsRefusedByABooleanProperty() {
      database.getSchema().createDocumentType("Nan9110").createProperty("flag", Type.BOOLEAN);
      assertRefused("Nan9110", "flag", Double.NaN);
    }

    @Test
    void gettersOnSchemalessDataKeepTheirLenientBehavior() {
      database.getSchema().createDocumentType("Loose9110");
      database.transaction(() -> {
        database.newDocument("Loose9110").set("a", new ArrayList<>(List.of(1, 2))).set("b", "").set("c", true).save();
      });
      final Document d = database.query("sql", "SELECT FROM Loose9110").next().getElement().get();
      assertThat(d.getInteger("a")).isNull();
      assertThat(d.getInteger("b")).isEqualTo(0);
      assertThat(d.getInteger("c")).isNull();
    }

    @Test
    void indexedIntegerLookupByBooleanDoesNotThrow() {
      final DocumentType type = database.getSchema().createDocumentType("Look9110");
      type.createProperty("i", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "i");
      database.transaction(() -> database.newDocument("Look9110").set("i", 1).save());
      assertThat(database.query("sql", "SELECT FROM Look9110 WHERE i = true").hasNext()).isFalse();
    }

    @Test
    void validValuesStillConvert() {
      final DocumentType type = database.getSchema().createDocumentType("Ok9110");
      type.createProperty("dec", Type.DECIMAL);
      type.createProperty("bin", Type.BINARY);
      type.createProperty("str", Type.STRING);
      type.createProperty("link", Type.LINK);
      type.createProperty("i", Type.INTEGER);

      assertThat(roundTrip("Ok9110", "dec", "1.5")).isEqualTo(new BigDecimal("1.5"));
      assertThat(roundTrip("Ok9110", "dec", 2)).isEqualTo(new BigDecimal("2"));
      assertThat((byte[]) roundTrip("Ok9110", "bin", new byte[] { 1, 2 })).containsExactly(1, 2);
      assertThat(roundTrip("Ok9110", "str", 12)).isEqualTo("12");
      assertThat(roundTrip("Ok9110", "i", "12")).isEqualTo(12);
      assertThat(roundTrip("Ok9110", "i", 12L)).isEqualTo(12);
    }

    @Test
    void indexedAndPlainPropertiesAgree() {
      final DocumentType type = database.getSchema().createDocumentType("Idx9110");
      type.createProperty("flag", Type.BOOLEAN);
      type.createProperty("dec", Type.DECIMAL);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "flag");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "dec");
      assertRefused("Idx9110", "flag", new ArrayList<>(List.of(1, 2)));
      assertRefused("Idx9110", "dec", true);
      assertThat(database.countType("Idx9110", true)).isZero();
    }

    @Test
    void convertOrKeepStillKeepsTheOriginalForTheRemoteClient() {
      final List<Integer> list = new ArrayList<>(List.of(1, 2));
      assertThat(Type.convertOrKeep(database, list, Integer.class)).isSameAs(list);
      assertThat(Type.convertOrKeep(database, true, Integer.class)).isEqualTo(true);
      assertThat(Type.convertOrNull(database, true, Integer.class)).isNull();
    }
  }

  /**
   * Regression for #9146: undeclared big integer.
   *
   * Issue #9146: a {@link BigInteger} written to an undeclared property must never lose digits. Inside the long range it
   * is narrowed to an Integer/Long with the value intact, outside of it is kept exactly as a DECIMAL instead of being
   * silently rounded to a DOUBLE.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UndeclaredBigIntegerTest extends TestHelper {

    private Document write(final String typeName, final BigInteger value) {
      database.getSchema().createDocumentType(typeName);
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(typeName).set("big", value).save().getIdentity());
      return database.lookupByRID(rid[0], true).asDocument();
    }

    @Test
    void bigIntegerInsideLongRangeKeepsItsValue() {
      assertThat(((Number) write("In1", BigInteger.ONE).get("big")).longValue()).isEqualTo(1L);
      assertThat(write("In2", new BigInteger("9007199254740993")).get("big")).isEqualTo(9007199254740993L);
      assertThat(write("In3", BigInteger.valueOf(Long.MAX_VALUE)).get("big")).isEqualTo(Long.MAX_VALUE);
      assertThat(write("In4", BigInteger.valueOf(Long.MIN_VALUE)).get("big")).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void bigIntegerOutsideLongRangeIsNotRoundedToDouble() {
      final String[] values = { "9223372036854775808", "12345678901234567890", "-9223372036854775809",
          "123456789012345678901234567890123456789012345678901234567890" };
      for (int i = 0; i < values.length; i++) {
        final BigInteger expected = new BigInteger(values[i]);
        final Object read = write("Out" + i, expected).get("big");
        assertThat(read).as(values[i]).isInstanceOf(BigDecimal.class);
        assertThat(((BigDecimal) read).toBigIntegerExact()).as(values[i]).isEqualTo(expected);
      }
    }

    @Test
    void sqlTypeReportsDecimalForOutOfRangeBigInteger() {
      write("Sql1", new BigInteger("12345678901234567890"));
      assertThat(database.query("sql", "SELECT big.type() AS t FROM Sql1").next().<String>getProperty("t")).isEqualTo("DECIMAL");
    }
  }

  /**
   * Regression for #9976: date implementation change keeps constraints.
   *
   * Issue #9976: a REGEXP on a DATE or DATETIME property is matched against the text of the class the database holds the value
   * as, which {@code arcadedb.dateImplementation} and {@code arcadedb.dateTimeImplementation} decide for the whole database.
   * Changing them re-checked nothing, so a pattern accepted for one class matched no value of the next and every later write of
   * the property failed. The change is now refused naming the property, and so is a change of {@code arcadedb.dateFormat} or
   * {@code arcadedb.dateTimeFormat} that would leave a MIN or MAX of a date property unreadable, which every write reads
   * through them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class DateImplementationChangeKeepsConstraintsTest extends TestHelper {

    @Test
    void theReproductionOfTheIssueIsRefusedAndLeavesTheSettingAsItWas() {
      database.command("sql", "CREATE VERTEX TYPE R1");
      database.command("sql", "CREATE PROPERTY R1.d DATE (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-01-15','yyyy-MM-dd')").close());

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R1.d")
          .hasMessageContaining("'[0-9]{4}-[0-9]{2}-[0-9]{2}'").hasMessageContaining("held as Date");

      assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
      database.transaction(() -> database.command("sql", "INSERT INTO R1 SET d = date('2024-02-20','yyyy-MM-dd')").close());

      // once the pattern is gone, the change goes through
      database.command("sql", "ALTER PROPERTY R1.d REGEXP null");
      database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
      assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
    }

    @Test
    void aPatternSomeValueOfTheNewClassMatchesDoesNotBlockTheChange() {
      database.command("sql", "CREATE VERTEX TYPE R2");
      database.command("sql", "CREATE PROPERTY R2.d DATE (regexp '.*20[0-9]{2}.*')");
      database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Date'");
      assertThat(serializer().getDateImplementation()).isEqualTo(Date.class);
      database.transaction(() -> database.command("sql", "INSERT INTO R2 SET d = date('2024-02-20','yyyy-MM-dd')").close());
    }

    @Test
    void theDatetimeImplementationIsCheckedForTheTypesItDecides() {
      database.command("sql", "CREATE VERTEX TYPE R3");
      database.command("sql", "CREATE PROPERTY R3.t DATETIME (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");
      // a microsecond datetime stays a LocalDateTime when DATETIME becomes a java.util.Date, so its pattern does not block
      database.command("sql", "CREATE PROPERTY R3.micros DATETIME_MICROS (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}T.*')");

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` 'java.util.Date'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R3.t").hasMessageNotContaining("R3.micros");
      assertThat(serializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);

      database.command("sql", "ALTER PROPERTY R3.t REGEXP null");
      database.command("sql", "ALTER DATABASE `arcadedb.dateTimeImplementation` 'java.util.Date'");
      assertThat(serializer().getDateTimeImplementation()).isEqualTo(Date.class);
    }

    @Test
    void aCalendarHeldDateIsCheckedToo() {
      database.command("sql", "CREATE VERTEX TYPE R4");
      database.command("sql", "CREATE PROPERTY R4.d DATE (regexp '[0-9]{4}-.*')");
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateImplementation` 'java.util.Calendar'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R4.d").hasMessageContaining("held as");
      assertThat(serializer().getDateImplementation()).isEqualTo(LocalDate.class);
    }

    @Test
    void aDateFormatChangeThatLeavesABoundUnreadableIsRefused() {
      database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd/MM/yyyy'");
      database.command("sql", "CREATE VERTEX TYPE R5");
      database.command("sql", "CREATE PROPERTY R5.d DATE (min '15/01/2024')");
      database.transaction(() -> database.command("sql", "INSERT INTO R5 SET d = '20/01/2024'").close());

      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy-MM-dd'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("R5.d")
          .hasMessageContaining("MIN '15/01/2024'");
      assertThat(database.getSchema().getDateFormat()).isEqualTo("dd/MM/yyyy");
      database.transaction(() -> database.command("sql", "INSERT INTO R5 SET d = '21/01/2024'").close());

      // a bound written in a form every format reads does not block it
      database.command("sql", "ALTER PROPERTY R5.d MIN '2024-01-15'");
      database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy-MM-dd'");
      assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MM-dd");
    }

    @Test
    void aSchemaWhoseFormatAndBoundsAlreadyDisagreeStillOpens() throws IOException {
      database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd/MM/yyyy'");
      database.command("sql", "CREATE VERTEX TYPE R6");
      database.command("sql", "CREATE PROPERTY R6.d DATE (min '15/01/2024')");
      database.close();

      // the format is changed behind the check's back: the load assigns it, it does not go through the setter that refuses
      final File schemaFile = new File(getDatabasePath(), "schema.json");
      final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
      root.getJSONObject("settings").put("dateFormat", "yyyy-MM-dd");
      Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

      database = factory.open();
      assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MM-dd");
      assertThat(database.getSchema().getType("R6").getProperty("d").getMin()).isEqualTo("15/01/2024");
      // the remedy works, and the setter refuses nothing a bound it cannot read either predates
      database.command("sql", "ALTER PROPERTY R6.d MIN '2024-01-15'");
      database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'yyyy/MM/dd'");
    }

    private BinarySerializer serializer() {
      return ((DatabaseInternal) database).getSerializer();
    }
  }
}
