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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.serializer.BinarySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10910: under {@code arcadedb.dateImplementation=java.util.Date} (or Calendar, or LocalDateTime) a number written into a
 * DATE property was read as epoch milliseconds, while the default LocalDate implementation reads it as epoch days. The remote
 * client encodes a DATE as epoch days (#4601), so a date written through it was stored as 1970-01-01.
 */
class Issue10910DateNumberAsDaysTest extends TestHelper {
  private static final LocalDate DAY        = LocalDate.of(2024, 1, 15);
  private static final long      DAY_MILLIS = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

  @AfterEach
  void restoreDefault() {
    serializer().setDateImplementation(LocalDate.class);
  }

  @Test
  void javaUtilDateReadsANumberAsDays() {
    serializer().setDateImplementation(Date.class);
    createType();

    assertThat(((Date) written(DAY.toEpochDay())).getTime()).isEqualTo(DAY_MILLIS);
    assertThat(((Date) written(String.valueOf(DAY.toEpochDay()))).getTime()).isEqualTo(DAY_MILLIS);
    assertThat(((Date) written((int) DAY.toEpochDay())).getTime()).isEqualTo(DAY_MILLIS);
    // the millis of a UTC midnight are that day, as for the LocalDate implementation (#10865)
    assertThat(((Date) written(DAY_MILLIS)).getTime()).isEqualTo(DAY_MILLIS);
    assertThat(((Date) written(0L)).getTime()).isZero();
    assertThat(((Date) written(-1L)).getTime()).isEqualTo(-86_400_000L);
  }

  @Test
  void javaUtilDateRefusesADayCountThatOverflowsMillis() {
    serializer().setDateImplementation(Date.class);
    createType();

    assertThatThrownBy(() -> written(200_000_000_000L)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void calendarAndListRefuseADayCountThatOverflowsMillis() {
    serializer().setDateImplementation(Calendar.class);
    createType();
    database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));

    assertThatThrownBy(() -> written(200_000_000_000L)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> inTx(() -> database.newDocument("D").set("ds", List.of(200_000_000_000L)).get("ds"))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void aNegativeDayCountIsAPreEpochDay() {
    serializer().setDateImplementation(Date.class);
    createType();

    assertThat(((Date) written(-10L)).getTime()).isEqualTo(-10 * 86_400_000L);
  }

  @Test
  void aSmallNonMidnightMillisValueIsADayCount() {
    serializer().setDateImplementation(Date.class);
    createType();

    // the ambiguity window, as for the LocalDate implementation: a number below ~1.07e11 is days
    assertThat(((Date) written(1_000_000L)).getTime()).isEqualTo(1_000_000L * 86_400_000L);
  }

  @Test
  void calendarReadsANumberAsDays() {
    serializer().setDateImplementation(Calendar.class);
    createType();

    assertThat(((Calendar) written(DAY.toEpochDay())).getTimeInMillis()).isEqualTo(DAY_MILLIS);
  }

  @Test
  void localDateTimeReadsANumberAsDays() {
    serializer().setDateImplementation(LocalDateTime.class);
    createType();

    assertThat(written(DAY.toEpochDay())).isEqualTo(DAY.atStartOfDay());
    assertThat(written(DAY_MILLIS)).isEqualTo(DAY.atStartOfDay());
  }

  @Test
  void aNumberThatIsNoDayCountKeepsBeingReadAsMillis() {
    serializer().setDateImplementation(Date.class);
    createType();

    // a "now" in milliseconds (not a UTC midnight) was accepted as milliseconds before and still is
    assertThat(((Date) written(1_705_312_345_678L)).getTime()).isEqualTo(1_705_312_345_678L);
  }

  @Test
  void localDateKeepsReadingANumberAsDays() {
    createType();

    assertThat(written(DAY.toEpochDay())).isEqualTo(DAY);
    assertThat(written(DAY_MILLIS)).isEqualTo(DAY);
  }

  @Test
  void aDatetimeStillReadsANumberAsMillis() {
    serializer().setDateImplementation(Date.class);
    serializer().setDateTimeImplementation(Date.class);
    createType();

    final Object value = inTx(() -> database.newDocument("D").set("t", 1_705_276_800_000L).get("t"));
    assertThat(((Date) value).getTime()).isEqualTo(1_705_276_800_000L);
  }

  @Test
  void listOfDateReadsANumberAsDays() {
    serializer().setDateImplementation(Date.class);
    createType();
    database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));

    final Object value = inTx(() -> database.newDocument("D").set("ds", List.of(DAY.toEpochDay())).get("ds"));
    assertThat(((List<?>) value).get(0)).isEqualTo(new Date(DAY_MILLIS));
  }

  @Test
  void sqlInsertOfADayCountThroughTheStatement() {
    serializer().setDateImplementation(Date.class);
    createType();

    database.transaction(() -> database.command("sql", "INSERT INTO D SET d = ?", DAY.toEpochDay()));
    final Object stored = database.query("sql", "SELECT d FROM D").next().getProperty("d");
    assertThat(((Date) stored).getTime()).isEqualTo(DAY_MILLIS);
  }

  private void createType() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("D");
      type.createProperty("d", Type.DATE);
      type.createProperty("t", Type.DATETIME);
    });
  }

  private Object written(final Object value) {
    return inTx(() -> {
      final MutableDocument doc = database.newDocument("D").set("d", value);
      return doc.get("d");
    });
  }

  private Object inTx(final Supplier<Object> supplier) {
    final Object[] result = new Object[1];
    database.transaction(() -> result[0] = supplier.get());
    return result[0];
  }

  private BinarySerializer serializer() {
    return ((DatabaseInternal) database).getSerializer();
  }
}
