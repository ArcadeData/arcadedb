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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue8090SpaceSeparatedFractionalDateTimeTest extends TestHelper {

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
   * An offset-bearing literal keeps its wall-clock on this path, unchanged by issue #8090: that is what Cypher's
   * {@code datetime()} round-trip depends on (issue #4125). Only the accepted formats were widened.
   */
  @Test
  void offsetAndZuluFormsKeepTheirWallClock() {
    final DocumentType type = database.getSchema().createDocumentType("Ev8090Zone");
    type.createProperty("ts", Type.DATETIME_MICROS);

    final LocalDateTime wallClock = LocalDateTime.of(2024, 2, 29, 13, 45, 10, 123_456_000);

    database.transaction(() -> {
      for (final String literal : new String[] { "2024-02-29T13:45:10.123456Z", "2024-02-29T13:45:10.123456+01:00",
          "2024-02-29 13:45:10.123456+01" }) {
        final MutableDocument doc = database.newDocument("Ev8090Zone").set("ts", literal).save();
        assertThat(doc.<LocalDateTime>get("ts")).as("literal '%s'", literal).isEqualTo(wallClock);
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
    // The bulk path rebases an offset onto the database's zone, so all three widths have to land on one instant.
    final LocalDateTime expected = OffsetDateTime.parse("2024-02-29T13:45:10.123456+01:00")
        .atZoneSameInstant(database.getSchema().getZoneId()).toLocalDateTime();

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

      // The LocalDateTime target still drops it, on purpose: it has no zone to keep (issue #4125).
      assertThat(Type.convert(database, literal, LocalDateTime.class)).isEqualTo(
          LocalDateTime.of(2024, 2, 29, 13, 45, 10));
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
   * Reading a literal answers what writing it would have stored, for an offset-bearing one too. The read-side
   * surfaces reached the shared chain through its REBASING overload while {@code Type.convert} used the
   * wall-clock-preserving one, so the same string had two answers depending on which entry point saw it - and
   * offset-bearing literals are exactly what these functions became newly able to read in this fix.
   */
  @Test
  void theReadSideAnswersWhatTheWriteSideWouldStore() {
    final String literal = "2026-01-01T00:00:00+01:00";
    final LocalDateTime expected = LocalDateTime.of(2026, 1, 1, 0, 0);

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
  }

  /**
   * The MIN/MAX date constraint was the one conversion site the audit had not reached. It is a write-time check, so
   * it keeps the strict conversion - but a bound nothing can read now reports the schema layer's own
   * ValidationException naming which side failed, where it used to convert to null and then NPE on the comparison.
   */
  @Test
  void anUnreadableDateBoundIsAValidationErrorRatherThanAnNPE() {
    final DocumentType type = database.getSchema().createDocumentType("Ev8090Bound");
    type.createProperty("d", Type.DATETIME).setMax("not-a-date");

    database.transaction(() -> assertThatThrownBy(
        () -> database.newDocument("Ev8090Bound").set("d", "2024-02-29 13:45:10").save())//
        .isInstanceOf(ValidationException.class)//
        .hasMessageContaining("max")//
        .hasMessageContaining("not-a-date"));

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
