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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;

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

      // date() keeps its documented answer for a value it cannot read at all.
      assertThat(database.query("sql", "SELECT date('not-a-timestamp') AS d").next().<Object>getProperty("d")).isNull();
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
