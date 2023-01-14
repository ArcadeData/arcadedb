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

import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.text.*;
import java.time.*;
import java.time.chrono.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.*;

public class DocumentTest extends TestHelper {
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
      type.createProperty("datetime", Type.DATETIME);
    });
  }

  @Test
  public void testNoConversion() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");

      final Date now = new Date();
      final LocalDate localDate = LocalDate.now();
      final LocalDateTime localDateTime = LocalDateTime.now();

      doc.set("string", "test");
      doc.set("int", 33);
      doc.set("long", 33l);
      doc.set("float", 33.33f);
      doc.set("double", 33.33d);
      doc.set("decimal", new BigDecimal("33.33"));
      doc.set("date", now);
      doc.set("datetime", now);
      doc.set("localDate", localDate);
      doc.set("localDateTime", localDateTime);

      Assertions.assertEquals(33, doc.get("int"));
      Assertions.assertEquals(33l, doc.get("long"));
      Assertions.assertEquals(33.33f, doc.get("float"));
      Assertions.assertEquals(33.33d, doc.get("double"));
      Assertions.assertEquals(new BigDecimal("33.33"), doc.get("decimal"));
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));
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
      doc.set("datetime", now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));

      doc.set("date", "" + now.getTime());
      doc.set("datetime", "" + now.getTime());
      Assertions.assertEquals(now, doc.get("date"));
      Assertions.assertEquals(now, doc.get("datetime"));

      final SimpleDateFormat df = new SimpleDateFormat(database.getSchema().getDateTimeFormat());

      doc.set("date", df.format(now));
      doc.set("datetime", df.format(now));
      Assertions.assertEquals(df.format(now), df.format(doc.get("date")));
      Assertions.assertEquals(df.format(now), df.format(doc.get("datetime")));

      final LocalDate localDate = LocalDate.now();
      final LocalDateTime localDateTime = LocalDateTime.now();
      doc.set("date", localDate);
      doc.set("datetime", localDateTime);
      Assertions.assertEquals(localDate, doc.getLocalDate("date"));
      Assertions.assertEquals(localDateTime.truncatedTo(DateUtils.getPrecision(localDateTime.getNano())), doc.getLocalDateTime("datetime"));

      Assertions.assertEquals(
          TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(ChronoField.MILLI_OF_SECOND),
          doc.getCalendar("datetime").getTime().getTime());
    });
  }

  @Test
  public void testDateAndDateTimeSettingsAreSavedInDatabase() {
    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.time.LocalDate`");
    database.command("sql", "alter database `arcadedb.dateTimePrecision` nanosecond");

    Assertions.assertEquals(java.time.LocalDateTime.class, ((EmbeddedDatabase) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(java.time.LocalDate.class, ((EmbeddedDatabase) database).getSerializer().getDateImplementation());
    Assertions.assertEquals(ChronoUnit.NANOS, ((EmbeddedDatabase) database).getSerializer().getDateTimePrecision());

    database.close();

    database = factory.open();

    Assertions.assertEquals(java.time.LocalDateTime.class, ((EmbeddedDatabase) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(java.time.LocalDate.class, ((EmbeddedDatabase) database).getSerializer().getDateImplementation());
    Assertions.assertEquals(ChronoUnit.NANOS, ((EmbeddedDatabase) database).getSerializer().getDateTimePrecision());

    database.command("sql", "alter database `arcadedb.dateTimeImplementation` `java.util.Date`");
    database.command("sql", "alter database `arcadedb.dateImplementation` `java.util.Date`");
    database.command("sql", "alter database `arcadedb.dateTimePrecision` millisecond");

    Assertions.assertEquals(java.util.Date.class, ((EmbeddedDatabase) database).getSerializer().getDateTimeImplementation());
    Assertions.assertEquals(java.util.Date.class, ((EmbeddedDatabase) database).getSerializer().getDateImplementation());
    Assertions.assertEquals(ChronoUnit.MILLIS, ((EmbeddedDatabase) database).getSerializer().getDateTimePrecision());
  }

  @Test
  public void testDateAndDateTimeSettingsAreSavedInProperties() {
    database.command("sql", "create document type LogEvent");
    database.command("sql", "create property LogEvent.date datetime (precision 'nanosecond')");

    Assertions.assertEquals("nanosecond", database.getSchema().getType("LogEvent").getProperty("date").getPrecision());
    Assertions.assertEquals(ChronoUnit.NANOS, database.getSchema().getType("LogEvent").getProperty("date").getDateTimePrecision());

    final LocalDateTime now = LocalDateTime.now();

    database.transaction(() -> {
      MutableDocument doc = database.newDocument("LogEvent").set("date", now).save();
      Assertions.assertEquals(now.truncatedTo(ChronoUnit.NANOS), doc.getLocalDateTime("date"));
    });

    database.transaction(() -> {
      database.command("sql", "update LogEvent set date = date.precision('microsecond')");
      database.command("sql", "alter property LogEvent.date precision 'microsecond'");
    });

    Assertions.assertEquals("microsecond", database.getSchema().getType("LogEvent").getProperty("date").getPrecision());
    Assertions.assertEquals(ChronoUnit.MICROS, database.getSchema().getType("LogEvent").getProperty("date").getDateTimePrecision());

    Assertions.assertEquals(now.truncatedTo(ChronoUnit.MICROS),
        database.iterateType("LogEvent", false).next().getRecord().asDocument().getLocalDateTime("date"));

    database.close();
    database = factory.open();

    database.transaction(() -> {
      database.command("sql", "update LogEvent set date = date.precision('millisecond')");
      database.command("sql", "alter property LogEvent.date precision 'millisecond'");
    });

    Assertions.assertEquals("millisecond", database.getSchema().getType("LogEvent").getProperty("date").getPrecision());
    Assertions.assertEquals(ChronoUnit.MILLIS, database.getSchema().getType("LogEvent").getProperty("date").getDateTimePrecision());

    Assertions.assertEquals(now.truncatedTo(ChronoUnit.MILLIS),
        database.iterateType("LogEvent", false).next().getRecord().asDocument().getLocalDateTime("date"));

    database.close();
    database = factory.open();

    Assertions.assertEquals("millisecond", database.getSchema().getType("LogEvent").getProperty("date").getPrecision());
    Assertions.assertEquals(ChronoUnit.MILLIS, database.getSchema().getType("LogEvent").getProperty("date").getDateTimePrecision());
  }

  @Test
  public void testLocalDateTime() throws ClassNotFoundException {
    ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);

    final LocalDateTime localDateTime = LocalDateTime.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
        doc.set("datetime", localDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MILLIS), doc.get("datetime"));

      // TEST MICROSECONDS PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("microsecond");
      database.transaction(() -> {
        doc.set("datetime", localDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.MICROS), doc.get("datetime"));

      // TEST NANOSECOND PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("nanosecond");
      database.transaction(() -> {
        doc.set("datetime", localDateTime);
        doc.save();
      });
      doc.reload();
      Assertions.assertEquals(localDateTime.truncatedTo(ChronoUnit.NANOS), doc.get("datetime"));

      Assertions.assertEquals(
          TimeUnit.MILLISECONDS.convert(localDateTime.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) + localDateTime.getLong(ChronoField.MILLI_OF_SECOND),
          doc.getDate("datetime").getTime());

      Assertions.assertTrue(localDateTime.isEqual(doc.getLocalDateTime("datetime")));

    } finally {
      ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Date.class);
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
    }
  }

  @Test
  public void testCalendar() throws ClassNotFoundException {
    ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Calendar.class);

    final Calendar calendar = Calendar.getInstance();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        doc.set("datetime", calendar);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(calendar, doc.get("datetime"));
      Assertions.assertEquals(calendar, doc.getCalendar("datetime"));

    } finally {
      ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Date.class);
    }
  }

  @Test
  public void testLocalDate() throws ClassNotFoundException {
    ((EmbeddedDatabase) database).getSerializer().setDateImplementation(LocalDate.class);

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
      ((EmbeddedDatabase) database).getSerializer().setDateImplementation(Date.class);
    }
  }

  @Test
  public void testZonedDateTime() throws ClassNotFoundException {
    ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(ZonedDateTime.class);

    final ZonedDateTime zonedDateTime = ZonedDateTime.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
        doc.set("datetime", zonedDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.MILLIS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime")));

      // TEST MICROSECONDS PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("microsecond");
      database.transaction(() -> {
        doc.set("datetime", zonedDateTime);
        doc.save();
      });

      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.MICROS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime")));

      // TEST NANOSECOND PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("nanosecond");
      database.transaction(() -> {
        doc.set("datetime", zonedDateTime);
        doc.save();
      });
      doc.reload();
      Assertions.assertTrue(zonedDateTime.truncatedTo(ChronoUnit.NANOS).isEqual((ChronoZonedDateTime<?>) doc.get("datetime")));
      Assertions.assertTrue(zonedDateTime.isEqual(doc.getZonedDateTime("datetime")));

    } finally {
      ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Date.class);
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
    }
  }

  @Test
  public void testInstant() throws ClassNotFoundException {
    ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Instant.class);

    final Instant instant = Instant.now();

    try {
      final MutableDocument doc = database.newDocument("ConversionTest");

      database.transaction(() -> {
        // TEST MILLISECONDS PRECISION
        ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
        doc.set("datetime", instant);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.MILLIS), doc.get("datetime"));

      // TEST MICROSECONDS PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("microsecond");
      database.transaction(() -> {
        doc.set("datetime", instant);
        doc.save();
      });

      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.MICROS), doc.get("datetime"));

      // TEST NANOSECOND PRECISION
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("nanosecond");
      database.transaction(() -> {
        doc.set("datetime", instant);
        doc.save();
      });
      doc.reload();
      Assertions.assertEquals(instant.truncatedTo(ChronoUnit.NANOS), doc.get("datetime"));
      Assertions.assertEquals(instant, doc.getInstant("datetime"));

    } finally {
      ((EmbeddedDatabase) database).getSerializer().setDateTimeImplementation(Date.class);
      ((EmbeddedDatabase) database).getSerializer().setDateTimePrecision("millisecond");
    }
  }

  @Test
  public void testDetached() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("ConversionTest");
      doc.set("name", "Tim");
      final EmbeddedDocument embeddedObj = (EmbeddedDocument) doc.newEmbeddedDocument("ConversionTest", "embeddedObj").set("embeddedObj", true);

      final List<EmbeddedDocument> embeddedList = new ArrayList<>();
      doc.set("embeddedList", embeddedList);
      doc.newEmbeddedDocument("ConversionTest", "embeddedList").set("embeddedList", true);

      final Map<String, EmbeddedDocument> embeddedMap = new HashMap<>();
      doc.set("embeddedMap", embeddedMap);
      doc.newEmbeddedDocument("ConversionTest", "embeddedMap", "first").set("embeddedMap", true);

      final DetachedDocument detached = doc.detach();

      Assertions.assertEquals("Tim", detached.getString("name"));
      Assertions.assertEquals(embeddedObj, detached.get("embeddedObj"));
      Assertions.assertEquals(embeddedList, detached.get("embeddedList"));
      Assertions.assertEquals(embeddedMap, detached.get("embeddedMap"));
      Assertions.assertNull(detached.getString("lastname"));

      final Set<String> props = detached.getPropertyNames();
      Assertions.assertEquals(4, props.size());
      Assertions.assertTrue(props.contains("name"));
      Assertions.assertTrue(props.contains("embeddedObj"));
      Assertions.assertTrue(props.contains("embeddedList"));
      Assertions.assertTrue(props.contains("embeddedMap"));

      final Map<String, Object> map = detached.toMap();
      Assertions.assertEquals(6, map.size());

      Assertions.assertEquals("Tim", map.get("name"));
      Assertions.assertEquals(embeddedObj, map.get("embeddedObj"));
      Assertions.assertTrue(((DetachedDocument) map.get("embeddedObj")).getBoolean("embeddedObj"));
      Assertions.assertEquals(embeddedList, map.get("embeddedList"));
      Assertions.assertTrue(((List<DetachedDocument>) map.get("embeddedList")).get(0).getBoolean("embeddedList"));
      Assertions.assertEquals(embeddedMap, map.get("embeddedMap"));
      Assertions.assertTrue(((Map<String, DetachedDocument>) map.get("embeddedMap")).get("first").getBoolean("embeddedMap"));

      Assertions.assertEquals("Tim", detached.toJSON().get("name"));

      detached.toString();

      try {
        detached.modify();
        Assertions.fail("modify");
      } catch (final UnsupportedOperationException e) {
      }

      detached.reload();

      try {
        detached.setBuffer(null);
        Assertions.fail("setBuffer");
      } catch (final UnsupportedOperationException e) {
      }

      Assertions.assertNull(detached.getString("name"));
      Assertions.assertNull(detached.getString("lastname"));
    });
  }
}
