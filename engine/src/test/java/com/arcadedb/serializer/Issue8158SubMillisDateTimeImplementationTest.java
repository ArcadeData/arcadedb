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
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8158
 * <p>
 * With {@code arcadedb.dateTimeImplementation} set to {@code java.util.Date} or {@code java.util.Calendar}, a
 * {@code DATETIME_MICROS}/{@code DATETIME_NANOS} column was written successfully (the write side converts those
 * columns to {@link LocalDateTime} regardless of the setting) but every read threw inside
 * {@code DateUtils.dateTime()}, was swallowed by the corrupt-property recovery arm and surfaced as {@code null}. The
 * read side now materialises a sub-millisecond column as {@link LocalDateTime} under a class that cannot carry it,
 * while {@code DATETIME} and {@code DATETIME_SECOND} keep honouring the setting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8158SubMillisDateTimeImplementationTest extends TestHelper {
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
