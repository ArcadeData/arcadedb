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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8253
 * <p>
 * {@code DATETIME_SECOND} stores epoch SECONDS. {@code DateUtils.dateTime()} fed that value straight into
 * {@code new Date(millis)} / {@code Calendar.setTimeInMillis(millis)}, both of which always expect MILLIS, so the
 * value came back 1000 times too small under {@code java.util.Date}/{@code java.util.Calendar} - silently, no
 * exception and no corrupt-property warning.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8253DatetimeSecondPrecisionTest extends TestHelper {
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
