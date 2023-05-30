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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.text.*;
import java.time.*;
import java.time.chrono.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;

public class DateTest extends TestHelper {
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
      doc.set("long", 33l);
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

      Assertions.assertEquals(33, doc.get("int"));
      Assertions.assertEquals(33l, doc.get("long"));
      Assertions.assertEquals(33.33f, doc.get("float"));
      Assertions.assertEquals(33.33d, doc.get("double"));
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));
      Assertions.assertEquals(now, doc.get("date"));

      Assertions.assertEquals(0, ((LocalDateTime) doc.get("datetime_second")).getNano());
      Assertions.assertEquals(now, doc.get("datetime_millis"));
      Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(doc.getLocalDateTime("datetime_millis").getNano()));
      Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(((LocalDateTime) doc.get("datetime_micros")).getNano()));
      Assertions.assertEquals(ChronoUnit.MILLIS, DateUtils.getPrecision(((LocalDateTime) doc.get("datetime_nanos")).getNano()));

      Assertions.assertEquals(localDate, doc.get("localDate"));
      Assertions.assertEquals(localDateTime, doc.get("localDateTime"));
    });
  }

  @Test
  public void testConversionDecimals() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("decimal", "33.33");
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));

      doc.set("decimal", 33.33f);
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));

      doc.set("decimal", 33.33d);
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));
    });
  }

  @Test
  public void testConversionDates() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();

      doc.set("date", now.getTime());
      doc.set("datetime_millis", now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime_millis"));

      doc.set("date", "" + now.getTime());
      doc.set("datetime_millis", "" + now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime_millis"));

      final SimpleDateFormat df = new SimpleDateFormat(database.getSchema().getDateTimeFormat());

      doc.set("date", df.format(now));
      doc.set("datetime_millis", df.format(now));
      Assertions.assertEquals(df.format(now), df.format(doc.get("date")));
      Assertions.assertEquals(df.format(now), df.format(doc.get("datetime_millis")));

      final LocalDate localDate = LocalDate.now();
      final LocalDateTime localDateTime = LocalDateTime.now();
      doc.set("date", localDate);
      doc.set("datetime_nanos", localDateTime);
      Assertions.assertEquals(localDate, doc.getLocalDate("date"));
      Assertions.assertEquals(localDateTime.truncatedTo(DateUtils.getPrecision(localDateTime.getNano())), doc.getLocalDateTime("datetime_nanos"));

      Assertions.assertEquals(
          TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(ChronoField.MILLI_OF_SECOND),
          doc.getCalendar("datetime_nanos").getTime().getTime());
    });
  }

  @Test
  public void testDateAndDateTimeSettingsAreSavedInDatabase() {
    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.time.LocalDate`");

    Assertions.assertEquals(LocalDateTime.class, ((DatabaseInternal) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(LocalDate.class, ((DatabaseInternal) database).getSerializer().getDateImplementation());

    database.close();

    database = factory.open();

    Assertions.assertEquals(LocalDateTime.class, ((DatabaseInternal) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(LocalDate.class, ((DatabaseInternal) database).getSerializer().getDateImplementation());

    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.util.Date`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.util.Date`");

    Assertions.assertEquals(Date.class, ((DatabaseInternal) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(Date.class, ((DatabaseInternal) database).getSerializer().getDateImplementation());
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
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.SECONDS), doc.get("datetime_second"));

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", localDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MILLIS), doc.get("datetime_millis"));

      // TEST MICROSECONDS PRECISION
      database.transaction(() -> {
        doc.set("datetime_micros", localDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), doc.get("datetime_micros"));

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", localDateTime);
        doc.save();
      });
      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), doc.get("datetime_nanos"));

      Assertions.assertNotNull(database.query("sql", "select datetime_second.asLong() as long from ConversionTest").nextIfAvailable().getProperty("long"));
      Assertions.assertNotNull(database.query("sql", "select datetime_millis.asLong() as long from ConversionTest").nextIfAvailable().getProperty("long"));
      Assertions.assertNotNull(database.query("sql", "select datetime_micros.asLong() as long from ConversionTest").nextIfAvailable().getProperty("long"));
      Assertions.assertNotNull(database.query("sql", "select datetime_nanos.asLong() as long from ConversionTest").nextIfAvailable().getProperty("long"));

      Assertions.assertEquals(
          TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(ChronoField.MILLI_OF_SECOND),
          doc.getDate("datetime_millis").getTime());

      Assertions.assertTrue(localDateTime.isEqual(doc.getLocalDateTime("datetime_nanos")));

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
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.SECONDS), result.next().toElement().get("datetime_second"));

      result = database.command("sql", "insert into ConversionTest set datetime_millis = ?", localDateTime);
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MILLIS), result.next().toElement().get("datetime_millis"));

      result = database.command("sql", "insert into ConversionTest set datetime_micros = ?", localDateTime);
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), result.next().toElement().get("datetime_micros"));

      result = database.command("sql", "insert into ConversionTest set datetime_nanos = ?", localDateTime);
      Assertions.assertTrue(result.hasNext());
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), result.next().toElement().get("datetime_nanos"));

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
      Assertions.assertEquals(calendar, doc.get("datetime_millis"));
      Assertions.assertEquals(calendar, doc.getCalendar("datetime_millis"));

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
      Assertions.assertEquals(localDate, doc.get("date"));
      Assertions.assertTrue(localDate.isEqual(doc.getLocalDate("date")));

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
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.SECONDS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_second")));

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", zonedDateTime.truncatedTo(ChronoUnit.MILLIS));
        doc.save();
      });

      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.MILLIS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_millis")));

      // TEST MICROSECONDS PRECISION
      database.transaction(() -> {
        doc.set("datetime_micros", zonedDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.MICROS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_micros")));

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", zonedDateTime);
        doc.save();
      });
      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.NANOS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime_nanos")));
      Assertions.assertTrue(zonedDateTime.isEqual(doc.getZonedDateTime("datetime_nanos")));

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
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.SECONDS), doc.get("datetime_second"));

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        doc.set("datetime_millis", instant);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.MILLIS), doc.get("datetime_millis"));

      // TEST MICROSECONDS PRECISION
      database.transaction(() -> {
        doc.set("datetime_micros", instant);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.MICROS), doc.get("datetime_micros"));

      // TEST NANOSECOND PRECISION
      database.transaction(() -> {
        doc.set("datetime_nanos", instant);
        doc.save();
      });
      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.NANOS), doc.get("datetime_nanos"));
      Assertions.assertEquals(instant, doc.getInstant("datetime_nanos"));

    } finally {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testConversion() {
    Assertions.assertEquals(10, DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.SECONDS));
    Assertions.assertEquals(10_000, DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.MILLIS));
    Assertions.assertEquals(10_000_000, DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.MICROS));
    Assertions.assertEquals(10_000_000_000L, DateUtils.convertTimestamp(10_000_000_000L, ChronoUnit.NANOS, ChronoUnit.NANOS));

    Assertions.assertEquals(10, DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.SECONDS));
    Assertions.assertEquals(10_000, DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.MILLIS));
    Assertions.assertEquals(10_000_000, DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.MICROS));
    Assertions.assertEquals(10_000_000_000L, DateUtils.convertTimestamp(10_000_000, ChronoUnit.MICROS, ChronoUnit.NANOS));

    Assertions.assertEquals(10, DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.SECONDS));
    Assertions.assertEquals(10_000, DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.MILLIS));
    Assertions.assertEquals(10_000_000, DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.MICROS));
    Assertions.assertEquals(10_000_000_000L, DateUtils.convertTimestamp(10_000, ChronoUnit.MILLIS, ChronoUnit.NANOS));

    Assertions.assertEquals(10, DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.SECONDS));
    Assertions.assertEquals(10_000, DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.MILLIS));
    Assertions.assertEquals(10_000_000, DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.MICROS));
    Assertions.assertEquals(10_000_000_000L, DateUtils.convertTimestamp(10, ChronoUnit.SECONDS, ChronoUnit.NANOS));
  }

  @Test
  public void testSQLMath() {
    database.command("sql", "alter database dateTimeImplementation `java.time.LocalDateTime`");
    try {
      database.begin();
      final LocalDateTime date1 = LocalDateTime.now();
      ResultSet resultSet = database.command("sql", "insert into ConversionTest set datetime_micros = ?", date1);
      Assertions.assertTrue(resultSet.hasNext());
      Assertions.assertEquals(date1.truncatedTo(ChronoUnit.MICROS), resultSet.next().toElement().get("datetime_micros"));

      final LocalDateTime date2 = LocalDateTime.now().plusSeconds(1);
      resultSet = database.command("sql", "insert into ConversionTest set datetime_micros = ?", date2);
      Assertions.assertTrue(resultSet.hasNext());
      Assertions.assertEquals(date2.truncatedTo(ChronoUnit.MICROS), resultSet.next().toElement().get("datetime_micros"));

      resultSet = database.command("sql", "select from ConversionTest where datetime_micros between ? and ?", date1, date2);
      Assertions.assertTrue(resultSet.hasNext());
      resultSet.next();
      Assertions.assertTrue(resultSet.hasNext());
      resultSet.next();
      Assertions.assertFalse(resultSet.hasNext());

      try {
        Thread.sleep(1001);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      resultSet = database.command("sql", "select sysdate() - datetime_micros as diff from ConversionTest");

      Assertions.assertTrue(resultSet.hasNext());
      Result result = resultSet.next();
      Assertions.assertFalse(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertFalse(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertFalse(resultSet.hasNext());

      resultSet = database.command("sql",
          "select sysdate() - datetime_micros as diff from ConversionTest where sysdate() - datetime_micros < duration(100000000000, 'nanosecond')");
      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertFalse(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertFalse(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertFalse(resultSet.hasNext());

      resultSet = database.command("sql",
          "select datetime_micros - sysdate() as diff from ConversionTest where abs( datetime_micros - sysdate() ) < duration(100000000000, 'nanosecond')");

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertFalse(resultSet.hasNext());

      resultSet = database.command("sql",
          "select datetime_micros - date(?, 'yyyy-MM-dd HH:mm:ss.SSS') as diff from ConversionTest where abs( datetime_micros - sysdate() ) < duration(100000000000, 'nanosecond')",
          DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now()));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertFalse(resultSet.hasNext());

      resultSet = database.command("sql",
          "select datetime_micros - date(?, 'yyyy-MM-dd HH:mm:ss.SSS') as diff from ConversionTest where abs( datetime_micros - sysdate() ) < duration(3, \"second\")",
          DateUtils.getFormatter("yyyy-MM-dd HH:mm:ss.SSS").format(LocalDateTime.now()));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertTrue(resultSet.hasNext());
      result = resultSet.next();
      Assertions.assertTrue(((Duration) result.getProperty("diff")).isNegative(), "Returned " + result.getProperty("diff"));

      Assertions.assertFalse(resultSet.hasNext());

      database.commit();
    } finally {
      database.command("sql", "alter database dateTimeImplementation `java.util.Date`");
    }
  }

}
