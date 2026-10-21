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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10489: the same instant, {@code 2020-01-02T03:04:05+02:00}, was stored as {@code 03:04:05} from a
 * String (an HTTP JSON value, a SQL literal or parameter) and as {@code 01:04:05} from an {@link OffsetDateTime} or a
 * {@link ZonedDateTime} (a Bolt DateTime with an offset, a Java value), so HTTP and Bolt wrote different DATETIME values for
 * one instant. A string now names the instant it carries and stores it as the UTC wall clock the typed values do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10489DateTimeOffsetStringTest extends TestHelper {

  private static final String           INSTANT = "2020-01-02T03:04:05+02:00";
  private static final LocalDateTime    UTC     = LocalDateTime.of(2020, 1, 2, 1, 4, 5);

  @Test
  void everyWayOfWritingTheSameInstantStoresTheSameValue() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.t DATETIME");

    database.transaction(() -> {
      database.newDocument("D").set("k", "java String").set("t", INSTANT).save();
      database.newDocument("D").set("k", "java OffsetDateTime").set("t", OffsetDateTime.parse(INSTANT)).save();
      database.newDocument("D").set("k", "java ZonedDateTime")
          .set("t", ZonedDateTime.parse("2020-01-02T03:04:05+02:00[Europe/Amsterdam]")).save();
      database.command("sql", "INSERT INTO D SET k = 'sql literal', t = '" + INSTANT + "'").close();
      database.command("sql", "INSERT INTO D SET k = 'sql String param', t = :t", Map.of("t", INSTANT)).close();
      database.command("sql", "INSERT INTO D SET k = 'sql OffsetDateTime param', t = :t", Map.of("t", OffsetDateTime.parse(INSTANT)))
          .close();
    });

    final Map<String, Object> stored = new LinkedHashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT k, t FROM D")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        stored.put(row.getProperty("k"), row.getProperty("t"));
      }
    }

    assertThat(stored).hasSize(6);
    stored.forEach((k, v) -> assertThat(v).as(k).isEqualTo(UTC));
  }

  @Test
  void aRegionSuffixAndTheSpaceSeparatedSpellingNameTheSameInstant() {
    final DocumentType type = database.getSchema().createDocumentType("Spellings");
    type.createProperty("t", Type.DATETIME);

    database.transaction(() -> {
      for (final String literal : new String[] { "2020-07-02T03:04:05+02:00[Europe/Amsterdam]", "2020-07-02T03:04:05+02:00",
          "2020-07-02 03:04:05+02:00", "2020-07-02 03:04:05+02", "2020-07-02 03:04:05+0200" }) {
        final LocalDateTime stored = database.newDocument("Spellings").set("t", literal).save().getLocalDateTime("t");
        assertThat(stored).as(literal).isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
      }
    });
  }

  /** A negative offset moves the wall clock forward, and a named zone applies the offset it has on that date (DST). */
  @Test
  void negativeOffsetsAndNamedZonesAcrossDstNameTheirInstant() {
    final DocumentType type = database.getSchema().createDocumentType("Negative");
    type.createProperty("t", Type.DATETIME);

    database.transaction(() -> {
      assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05-05:00").save().getLocalDateTime("t"))
          .isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
      // Amsterdam is +01:00 in January and +02:00 in July
      assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05+01:00[Europe/Amsterdam]").save()
          .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 1, 2, 2, 4, 5));
      assertThat(database.newDocument("Negative").set("t", "2020-07-02T03:04:05+02:00[Europe/Amsterdam]").save()
          .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 7, 2, 1, 4, 5));
      // New York is -05:00 in January and -04:00 in July
      assertThat(database.newDocument("Negative").set("t", "2020-01-02T03:04:05-05:00[America/New_York]").save()
          .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
      assertThat(database.newDocument("Negative").set("t", "2020-07-02T03:04:05-04:00[America/New_York]").save()
          .getLocalDateTime("t")).isEqualTo(LocalDateTime.of(2020, 7, 2, 7, 4, 5));
    });

    // and what the read side answers for the same strings is what the write side stored
    assertThat(database.query("sql", "SELECT '2020-01-02T03:04:05-05:00'.asDatetime() AS d").next().<Object>getProperty("d"))
        .isEqualTo(LocalDateTime.of(2020, 1, 2, 8, 4, 5));
    assertThat(database.query("sql", "SELECT date('2020-07-02T03:04:05-04:00[America/New_York]') AS d").next().<Object>getProperty("d"))
        .isEqualTo(LocalDateTime.of(2020, 7, 2, 7, 4, 5));
  }

  @Test
  void anOffsetNamingUtcAndAnInputWithoutOffsetKeepTheirWallClock() {
    final DocumentType type = database.getSchema().createDocumentType("Wall");
    type.createProperty("t", Type.DATETIME);

    final LocalDateTime wallClock = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
    database.transaction(() -> {
      // the Z that Cypher's datetime() renders (issue #4125), an explicit zero offset and no offset at all
      for (final String literal : new String[] { "2020-01-02T03:04:05Z", "2020-01-02T03:04:05+00:00", "2020-01-02T03:04:05",
          "2020-01-02 03:04:05" })
        assertThat(database.newDocument("Wall").set("t", literal).save().getLocalDateTime("t")).as(literal).isEqualTo(wallClock);
    });
  }

  @Test
  void theRuleDoesNotDependOnTheDatabaseZone() {
    final ZoneId original = database.getSchema().getZoneId();
    database.getSchema().setZoneId(ZoneId.of("America/Los_Angeles"));
    try {
      final DocumentType type = database.getSchema().createDocumentType("Zoned");
      type.createProperty("t", Type.DATETIME);

      database.transaction(() -> {
        assertThat(database.newDocument("Zoned").set("t", INSTANT).save().getLocalDateTime("t")).isEqualTo(UTC);
        assertThat(database.newDocument("Zoned").set("t", OffsetDateTime.parse(INSTANT)).save().getLocalDateTime("t"))
            .isEqualTo(UTC);
        assertThat(database.newDocument("Zoned").set("t", "2020-01-02T03:04:05Z").save().getLocalDateTime("t"))
            .isEqualTo(LocalDateTime.of(2020, 1, 2, 3, 4, 5));
      });
    } finally {
      database.getSchema().setZoneId(original);
    }
  }

  @Test
  void theReadSideAnswersWhatTheWriteSideStores() {
    assertThat(Type.convert(database, INSTANT, LocalDateTime.class)).isEqualTo(UTC);
    assertThat(database.query("sql", "SELECT '" + INSTANT + "'.asDatetime() AS d").next().<Object>getProperty("d")).isEqualTo(UTC);
    assertThat(database.query("sql", "SELECT date('" + INSTANT + "') AS d").next().<Object>getProperty("d")).isEqualTo(UTC);
  }

  @Test
  void aDateStillReadsTheDayWritten() {
    // a LocalDate has no zone: the day written is the day meant, and nothing may shift it across midnight
    assertThat(Type.convert(database, "2020-01-02T23:30:00-05:00", LocalDate.class)).isEqualTo(LocalDate.of(2020, 1, 2));
  }
}
