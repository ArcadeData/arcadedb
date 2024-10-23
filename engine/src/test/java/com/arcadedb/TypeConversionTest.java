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
package com.arcadedb;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.NanoClock;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TypeConversionTest extends TestHelper {
  @Override
  public void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("ConversionTest");

      type.createProperty("string", Type.STRING);
      type.createProperty("int", Type.INTEGER);
      type.createProperty("long", Type.LONG);
      type.createProperty("float", Type.FLOAT);
      type.createProperty("double", Type.DOUBLE);
      type.createProperty("decimal", Type.DECIMAL);
      type.createProperty("date", Type.DATE);
      type.createProperty("datetime_second", Type.DATETIME_SECOND);
      type.createProperty("datetime_millis", Type.DATETIME);
      type.createProperty("datetime_micros", Type.DATETIME_MICROS);
      type.createProperty("datetime_nanos", Type.DATETIME_NANOS);
    });
  }

  @Test
  public void testNoConversion() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();
      final Instant instant = new NanoClock().instant();
      final LocalDate localDate = LocalDate.now();
      final LocalDateTime localDateTime = LocalDateTime.now();

      doc.set("string", "test");
      doc.set("int", 33);
      doc.set("long", 33L);
      doc.set("float", 33.33f);
      doc.set("double", 33.33d);
      doc.set("decimal", new BigDecimal("33.33"));
      doc.set("date", now);
      doc.set("instant", instant); // USE NANOS
      doc.set("datetime_second", now);
      doc.set("datetime_millis", now);
      doc.set("datetime_micros", now);
      doc.set("datetime_nanos", now);
      doc.set("localDate", localDate); // SCHEMALESS
      doc.set("localDateTime", localDateTime); // SCHEMALESS

      assertThat(doc.get("int")).isEqualTo(33);
      assertThat(doc.get("long")).isEqualTo(33L);
      assertThat(doc.get("float")).isEqualTo(33.33f);
      assertThat(doc.get("double")).isEqualTo(33.33d);
      assertThat(doc.get("decimal")).isEqualTo(new BigDecimal("33.33"));
      assertThat(doc.get("date")).isEqualTo(now);

      assertThat(((LocalDateTime) doc.get("datetime_second")).getNano()).isEqualTo(0);
      assertThat(doc.get("datetime_millis")).isEqualTo(now);
      assertThat(DateUtils.getPrecision(doc.getLocalDateTime("datetime_millis").getNano())).isEqualTo(ChronoUnit.MILLIS);
      assertThat(DateUtils.getPrecision(((LocalDateTime) doc.get("datetime_micros")).getNano())).isEqualTo(ChronoUnit.MILLIS);
      assertThat(DateUtils.getPrecision(((LocalDateTime) doc.get("datetime_nanos")).getNano())).isEqualTo(ChronoUnit.MILLIS);

      assertThat(doc.get("localDate")).isEqualTo(localDate);
      assertThat(doc.get("localDateTime")).isEqualTo(localDateTime);
    });
  }

  @Test
  public void testConversionDecimals() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("decimal", "33.33");
      assertThat(doc.get("decimal")).isEqualTo(new BigDecimal("33.33"));

      doc.set("decimal", 33.33f);
      assertThat(doc.get("decimal")).isEqualTo(new BigDecimal("33.33"));

      doc.set("decimal", 33.33d);
      assertThat(doc.get("decimal")).isEqualTo(new BigDecimal("33.33"));

      doc.save();

      String property = database.query("sql", "select decimal.format('%.1f') as d from " + doc.getIdentity()).nextIfAvailable()
          .getProperty("d");
      assertThat(property).isEqualTo(String.format("%.1f", 33.3F));
      property = database.query("sql", "select decimal.format('%.2f') as d from " + doc.getIdentity()).nextIfAvailable()
          .getProperty("d");
      assertThat(property).isEqualTo(String.format("%.2f", 33.33F));

      doc.delete();
    });
  }

  @Test
  public void testConversionDates() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("date", now.getTime());
      doc.set("datetime_millis", now.getTime());
      assertThat(doc.get("date")).isEqualTo(now);
      assertThat(doc.get("datetime_millis")).isEqualTo(now);

      doc.set("date", "" + now.getTime());
      doc.set("datetime_millis", "" + now.getTime());
      assertThat(doc.get("date")).isEqualTo(now);
      assertThat(doc.get("datetime_millis")).isEqualTo(now);

      final SimpleDateFormat df = new SimpleDateFormat(database.getSchema().getDateTimeFormat());

      doc.set("date", df.format(now));
      doc.set("datetime_millis", df.format(now));
      assertThat(df.format(doc.get("date"))).isEqualTo(df.format(now));
      assertThat(df.format(doc.get("datetime_millis"))).isEqualTo(df.format(now));

      final LocalDate localDate = LocalDate.now();
      final LocalDateTime localDateTime = LocalDateTime.now();
      doc.set("date", localDate);
      doc.set("datetime_nanos", localDateTime);
      assertThat(doc.getLocalDate("date")).isEqualTo(localDate);

      assertThat(doc.getLocalDateTime("datetime_nanos")).isEqualTo(
          localDateTime.truncatedTo(DateUtils.getPrecision(localDateTime.getNano())));

      assertThat(doc.getCalendar("datetime_nanos").getTime().getTime())
          .isEqualTo(TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC),
              TimeUnit.SECONDS) + localDateTime.getLong(ChronoField.MILLI_OF_SECOND));
    });
  }

  @Test
  public void testDateAndDateTimeSettingsAreSavedInDatabase() {
    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.time.LocalDate`");

    assertThat(((DatabaseInternal) database).getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);
    assertThat(((DatabaseInternal) database).getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);

    database.close();

    database = factory.open();

    assertThat(((DatabaseInternal) database).getSerializer().getDateTimeImplementation()).isEqualTo(LocalDateTime.class);
    assertThat(((DatabaseInternal) database).getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);

    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.util.Date`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.util.Date`");

    assertThat(((DatabaseInternal) database).getSerializer().getDateTimeImplementation()).isEqualTo(Date.class);
    assertThat(((DatabaseInternal) database).getSerializer().getDateImplementation()).isEqualTo(Date.class);
  }

  @Test
  public void testLocalDateTime() throws ClassNotFoundException {
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);

    final LocalDateTime localDateTime = LocalDateTime.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");
      database.transaction(() -> {
        // TEST SECOND PRECISION
        doc.set("datetime_second", localDateTime.truncatedTo(ChronoUnit.SECONDS));
        doc.save();
      });

      doc.reload();
      assertThat(doc.get("datetime_second")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.SECONDS));

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", localDateTime);
        doc.save();
      });

      doc.reload();
      assertThat(doc.get("datetime_millis")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.MILLIS));

      // TEST MICROSECONDS PRECISION
      database.transaction(() -> {
        doc.set("datetime_micros", localDateTime);
        doc.save();
      });

      doc.reload();
      assertThat(doc.get("datetime_micros")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.MICROS));

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", localDateTime);
        doc.save();
      });
      doc.reload();
      assertThat(doc.get("datetime_nanos")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.NANOS));

      assertThat(database.query("sql", "select datetime_second.asLong() as long from ConversionTest")
          .nextIfAvailable()
          .<Long>getProperty("long"))
          .isNotNull();
      assertThat(database.query("sql", "select datetime_millis.asLong() as long from ConversionTest")
          .nextIfAvailable()
          .<Long>getProperty("long"))
          .isNotNull();
      assertThat(database.query("sql", "select datetime_micros.asLong() as long from ConversionTest")
          .nextIfAvailable()
          .<Long>getProperty("long"))
          .isNotNull();
      assertThat(database.query("sql", "select datetime_nanos.asLong() as long from ConversionTest")
          .nextIfAvailable()
          .<Long>getProperty("long"))
          .isNotNull();

      assertThat(doc.getDate("datetime_millis").getTime()).isEqualTo(
          TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(
              ChronoField.MILLI_OF_SECOND));

      assertThat(doc.getLocalDateTime("datetime_nanos")).isEqualTo(localDateTime);

    } finally {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testSQL() throws ClassNotFoundException {
    final LocalDateTime localDateTime = LocalDateTime.now();

    database.command("sql", "alter database dateTimeImplementation `java.time.LocalDateTime`");
    try {
      database.begin();
      ResultSet result = database.command("sql", "insert into ConversionTest set datetime_second = ?", localDateTime);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().toElement().get("datetime_second")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.SECONDS));

      result = database.command("sql", "insert into ConversionTest set datetime_millis = ?", localDateTime);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().toElement().get("datetime_millis")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.MILLIS));

      result = database.command("sql", "insert into ConversionTest set datetime_micros = ?", localDateTime);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().toElement().get("datetime_micros")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.MICROS));

      result = database.command("sql", "insert into ConversionTest set datetime_nanos = ?", localDateTime);
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().toElement().get("datetime_nanos")).isEqualTo(localDateTime.truncatedTo(ChronoUnit.NANOS));

      database.commit();
    } finally {
      database.command("sql", "alter database dateTimeImplementation `java.util.Date`");
    }
  }

  @Test
  public void testCalendar() throws ClassNotFoundException {
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Calendar.class);

    final Calendar calendar = Calendar.getInstance();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        doc.set("datetime_millis", calendar);
        doc.save();
      });

      doc.reload();
      assertThat(doc.get("datetime_millis")).isEqualTo(calendar);
      assertThat(doc.getCalendar("datetime_millis")).isEqualTo(calendar);

    } finally {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testLocalDate() throws ClassNotFoundException {
    ((DatabaseInternal) database).getSerializer().setDateImplementation(LocalDate.class);

    final LocalDate localDate = LocalDate.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        doc.set("date", localDate);
        doc.save();
      });

      doc.reload();
      assertThat(doc.get("date")).isEqualTo(localDate);
      assertThat(localDate.isEqual(doc.getLocalDate("date"))).isTrue();

    } finally {
      ((DatabaseInternal) database).getSerializer().setDateImplementation(Date.class);
    }
  }

  @Test
  public void testZonedDateTime() throws ClassNotFoundException {
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);

    final ZonedDateTime zonedDateTime = ZonedDateTime.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        // TEST SECOND PRECISION
        doc.set("datetime_second", zonedDateTime.truncatedTo(ChronoUnit.SECONDS));
        doc.save();
      });

      doc.reload();
      assertThat(
          zonedDateTime.truncatedTo(ChronoUnit.SECONDS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_second"))).isTrue();

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", zonedDateTime.truncatedTo(ChronoUnit.MILLIS));
        doc.save();
      });

      doc.reload();
      assertThat(
          zonedDateTime.truncatedTo(ChronoUnit.MILLIS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_millis"))).isTrue();

      if (!System.getProperty("os.name").startsWith("Windows")) {
        // NOTE: ON WINDOWS MICROSECONDS ARE NOT HANDLED CORRECTLY

        // TEST MICROSECONDS PRECISION
        database.transaction(() -> {
          doc.set("datetime_micros", zonedDateTime);
          doc.save();
        });

        doc.reload();
        assertThat(
            zonedDateTime.truncatedTo(ChronoUnit.MICROS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_micros"))).isTrue();
      }

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", zonedDateTime);
        doc.save();
      });
      doc.reload();
      assertThat(doc.getZonedDateTime("datetime_nanos")).isEqualTo(zonedDateTime.truncatedTo(ChronoUnit.NANOS));
      assertThat(doc.getZonedDateTime("datetime_nanos")).isEqualTo(zonedDateTime);
    } finally {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testInstant() throws ClassNotFoundException {
    ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Instant.class);

    final Instant instant = Instant.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");
      database.transaction(() -> {
        // TEST SECOND PRECISION
        doc.set("datetime_second", instant.truncatedTo(ChronoUnit.SECONDS));
        doc.save();
      });

      doc.reload();
      assertThat(doc.getInstant("datetime_second")).isEqualTo(instant.truncatedTo(ChronoUnit.SECONDS));

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", instant);
        doc.save();
      });

      doc.reload();
      assertThat(doc.getInstant("datetime_millis")).isEqualTo(instant.truncatedTo(ChronoUnit.MILLIS));

      // TEST MICROSECONDS PRECISION
      database.transaction(() -> {
        doc.set("datetime_micros", instant);
        doc.save();
      });

      doc.reload();
      if (!System.getProperty("os.name").startsWith("Windows"))
        // NOTE: ON WINDOWS MICROSECONDS ARE NOT HANDLED CORRECTLY
        assertThat(doc.getInstant("datetime_micros")).isEqualTo(instant.truncatedTo(ChronoUnit.MICROS));

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", instant);
        doc.save();
      });
      doc.reload();
      assertThat(doc.getInstant("datetime_nanos")).isEqualTo(instant.truncatedTo(ChronoUnit.NANOS));
      assertThat(doc.getInstant("datetime_nanos")).isEqualTo(instant);

    } finally {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testConversion() {
    assertThat(DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.SECONDS)).isEqualTo(10);
    assertThat(DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.MILLIS)).isEqualTo(10_000);
    assertThat(DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.MICROS)).isEqualTo(10_000_000);
    assertThat(DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.NANOS)).isEqualTo(10_000_000_000L);

    assertThat(DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.SECONDS)).isEqualTo(10);
    assertThat(DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.MILLIS)).isEqualTo(10_000);
    assertThat(DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.MICROS)).isEqualTo(10_000_000);
    assertThat(DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.NANOS)).isEqualTo(10_000_000_000L);

    assertThat(DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.SECONDS)).isEqualTo(10);
    assertThat(DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.MILLIS)).isEqualTo(10_000);
    assertThat(DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.MICROS)).isEqualTo(10_000_000);
    assertThat(DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.NANOS)).isEqualTo(10_000_000_000L);

    assertThat(DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.SECONDS)).isEqualTo(10);
    assertThat(DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.MILLIS)).isEqualTo(10_000);
    assertThat(DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.MICROS)).isEqualTo(10_000_000);
    assertThat(DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.NANOS)).isEqualTo(10_000_000_000L);
  }

  @Test
  public void testSQLMath() {
    if (System.getProperty("os.name").startsWith("Windows"))
      // NOTE: ON WINDOWS MICROSECONDS ARE NOT HANDLED CORRECTLY
      return;

    database.command("sql", "alter database dateTimeImplementation `java.time.LocalDateTime`");
    try {
      database.begin();
      final LocalDateTime date1 = LocalDateTime.now();
      ResultSet resultSet = database.command("sql", "insert into ConversionTest set datetime_micros = ?", date1);
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().toElement().get("datetime_micros")).isEqualTo(date1.truncatedTo(ChronoUnit.MICROS));

      final LocalDateTime date2 = LocalDateTime.now().plusSeconds(1);
      resultSet = database.command("sql", "insert into ConversionTest set datetime_micros = ?", date2);
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().toElement().get("datetime_micros")).isEqualTo(date2.truncatedTo(ChronoUnit.MICROS));

      resultSet = database.command("sql", "select from ConversionTest where datetime_micros between ? and ?", date1, date2);
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();
      assertThat(resultSet.hasNext()).isFalse();

      try {
        Thread.sleep(1001);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      resultSet = database.command("sql", "select sysdate() - datetime_micros as diff from ConversionTest");

      assertThat(resultSet.hasNext()).isTrue();
      Result result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isFalse();

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isFalse();

      assertThat(resultSet.hasNext()).isFalse();

      resultSet = database.command("sql",
          """
              select sysdate() - datetime_micros as diff
              from ConversionTest
              where sysdate() - datetime_micros < duration(100000000000, 'nanosecond')
              """);
      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isFalse();

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isFalse();

      assertThat(resultSet.hasNext()).isFalse();

      resultSet = database.command("sql",
          """
              select datetime_micros - sysdate() as diff
              from ConversionTest
              where abs( datetime_micros - sysdate() ) < duration(100000000000, 'nanosecond')
              """);

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isFalse();

      resultSet = database.command("sql",
          """
              select datetime_micros - date(?, 'yyyy-MM-dd HH:mm:ss.SSS') as diff
              from ConversionTest
              where abs( datetime_micros - sysdate() ) < duration(100000000000, 'nanosecond')
              """,
          DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now()));

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isFalse();

      resultSet = database.command("sql",
          """
              select datetime_micros - date(?, 'yyyy-MM-dd HH:mm:ss.SSS') as diff
              from ConversionTest
              where abs( datetime_micros - sysdate() ) < duration(3, "second")
              """,
          DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now()));

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isTrue();
      result = resultSet.next();
      assertThat(((Duration) result.getProperty("diff")).isNegative()).as("Returned " + result.getProperty("diff")).isTrue();

      assertThat(resultSet.hasNext()).isFalse();

      database.commit();
    } finally {
      database.command("sql", "alter database dateTimeImplementation `java.util.Date`");
    }
  }

}
