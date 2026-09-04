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
package com.arcadedb.utility;

import com.arcadedb.TestHelper;
import com.arcadedb.function.date.DateFormat;
import com.arcadedb.function.text.FormatFunction;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7144, the follow-up to #7112: pinning {@link DateUtils#getFormatter(String)} to {@code Locale.ENGLISH} only
 * covered the values that reach {@code DateUtils.format/parse}. The same schema patterns were still fed straight to
 * {@code DateTimeFormatter.ofPattern()} - which binds {@code Locale.getDefault(Locale.Category.FORMAT)} - from
 * {@code parseIsoDateTime}, from {@link Type}'s String conversions, from {@link JSONObject}'s date formats and from
 * the SQL/Cypher date functions. Two nodes with different JVM locales then disagreed on the textual form of a date,
 * and a value written by one failed to parse on the other.
 * <p>
 * Every pattern used here carries a textual field ({@code MMMM}, {@code EEEE}) and is unique to this class, so it is
 * built - and cached - for the first time under the Italian locale installed below. An unpinned formatter would
 * render {@code marzo}/{@code mercoledì} and fail to parse the English form.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7144SchemaDatePatternLocaleTest extends TestHelper {
  private static final String  DATE_PATTERN     = "dd MMMM yyyy";
  private static final String  DATETIME_PATTERN = "dd MMMM yyyy HH:mm:ss";
  private static final String  ENGLISH_DATE     = "04 March 2026";
  private static final String  ENGLISH_DATETIME = "04 March 2026 10:20:30";
  private              Locale  originalLocale;

  @BeforeEach
  void installHostileLocale() {
    originalLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale.setDefault(Locale.Category.FORMAT, Locale.ITALIAN);
  }

  @AfterEach
  void restoreLocale() {
    Locale.setDefault(Locale.Category.FORMAT, originalLocale);
  }

  @Override
  protected void beginTest() {
    database.getSchema().setDateFormat(DATE_PATTERN);
    database.getSchema().setDateTimeFormat(DATETIME_PATTERN);
  }

  /** The premise: an unpinned formatter really would answer in Italian under this locale. */
  @Test
  void localeIsActuallyHostile() {
    assertThat(java.time.format.DateTimeFormatter.ofPattern(DATE_PATTERN).format(LocalDate.of(2026, 3, 4)))
        .as("otherwise every assertion below would pass with or without the fix")
        .isNotEqualTo(ENGLISH_DATE);
  }

  @Test
  void typeConvertsStringToTemporalWithSchemaPatternInEnglish() {
    assertThat(Type.convert(database, ENGLISH_DATE, LocalDate.class)).isEqualTo(LocalDate.of(2026, 3, 4));
    assertThat(Type.convert(database, ENGLISH_DATETIME, LocalDateTime.class))
        .isEqualTo(LocalDateTime.of(2026, 3, 4, 10, 20, 30));
    // java.util.Date goes through the SimpleDateFormat twin of the same chain
    assertThat(Type.convert(database, ENGLISH_DATETIME, Date.class)).isNotNull();
  }

  @Test
  void jsonSerializesTheSchemaPatternInEnglish() {
    final JSONObject json = new JSONObject().setDateFormat(DATE_PATTERN).setDateTimeFormat(DATETIME_PATTERN);
    json.put("when", LocalDateTime.of(2026, 3, 4, 10, 20, 30));
    assertThat(json.getString("when")).isEqualTo(ENGLISH_DATETIME);
  }

  @Test
  void sqlFormatFunctionsRenderInEnglish() {
    assertThat(new FormatFunction().execute(new Object[] { LocalDateTime.of(2026, 3, 4, 10, 20, 30), DATETIME_PATTERN }, null))
        .isEqualTo(ENGLISH_DATETIME);
    // date.format() renders in the machine's own time zone, so only the textual field is asserted here
    assertThat(new DateFormat().execute(new Object[] { 1772622030000L, "ms", DATE_PATTERN }, null).toString())
        .contains("March");
  }

  /** End to end: a record written with a textual schema pattern reads back the same under a foreign locale. */
  @Test
  void recordRoundTripsThroughTheSchemaPattern() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Issue7144").createProperty("occurredAt", Type.DATETIME);
      // The String goes through Type.convert with the schema's own textual pattern
      database.newDocument("Issue7144").set("occurredAt", ENGLISH_DATETIME).save();
    });

    database.transaction(() -> {
      final Object readBack = database.query("sql", "select occurredAt from Issue7144").nextIfAvailable()
          .getProperty("occurredAt");
      assertThat(readBack).isEqualTo(LocalDateTime.of(2026, 3, 4, 10, 20, 30));
    });
  }
}
