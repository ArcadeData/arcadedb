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
import com.arcadedb.database.RID;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11025: a number written into a DATE property was read differently by the four classes a DATE can be held as. Under
 * {@code java.util.Date} and {@code Calendar} a number between ~1.07e11 and ~3.65e11 was a day count too large for a millisecond
 * {@code long} and was refused, while the default {@code LocalDate} stored it as a day hundreds of millions of years away; and a
 * "now" in milliseconds was stored by {@code java.util.Date} / {@code Calendar} / {@code LocalDateTime} and refused by
 * {@code LocalDate}. For one DATE property and one number the day must not depend on {@code arcadedb.dateImplementation}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11025DateNumberImplementationsAgreeTest extends TestHelper {
  private static final List<Class<?>> IMPLEMENTATIONS = List.of(LocalDate.class, Date.class, Calendar.class, LocalDateTime.class);

  private static final long MAX_DAY_COUNT = Long.MAX_VALUE / DateUtils.MS_IN_A_DAY;
  private static final long NOW_MILLIS    = 1_705_312_345_678L;

  // the day-count window, the millis window, both edges of each and the band of the issue
  private static final long[] NUMBERS = { 0L, 1L, -1L, 19_737L, 1_000_000L, 1_705_276_800_000L, NOW_MILLIS, -NOW_MILLIS,
      MAX_DAY_COUNT, MAX_DAY_COUNT + 1, -MAX_DAY_COUNT, -MAX_DAY_COUNT - 1, 110_000_000_000L, 150_000_000_000L, 200_000_000_000L,
      LocalDate.MAX.toEpochDay(), LocalDate.MAX.toEpochDay() + 1, Long.MAX_VALUE / 2, Long.MAX_VALUE, -(Long.MAX_VALUE / 2) };

  @AfterEach
  void restoreDefault() {
    serializer().setDateImplementation(LocalDate.class);
  }

  @Test
  void everyImplementationHoldsTheSameDayForTheSameNumber() {
    createType();
    for (final long number : NUMBERS) {
      final Map<Class<?>, Long> days = new LinkedHashMap<>();
      for (final Class<?> implementation : IMPLEMENTATIONS) {
        serializer().setDateImplementation(implementation);
        final Object written = inTx(() -> database.newDocument("D").set("d", number).get("d"));
        assertThat(written).as("%s under %s", number, implementation.getSimpleName()).isInstanceOf(implementation);
        days.put(implementation, DateUtils.dateToEpochDays(written));
      }
      assertThat(days.values().stream().distinct().count()).as("days of %s by implementation: %s", number, days).isEqualTo(1L);
    }
  }

  @Test
  void everyImplementationStoresAndReadsBackTheSameDay() {
    createType();
    for (final long number : NUMBERS) {
      final List<Long> days = new ArrayList<>();
      for (final Class<?> implementation : IMPLEMENTATIONS) {
        serializer().setDateImplementation(implementation);
        final RID[] rid = new RID[1];
        database.transaction(() -> rid[0] = database.newDocument("D").set("d", number).save().getIdentity());
        days.add(DateUtils.dateToEpochDays(database.lookupByRID(rid[0], true).asDocument().get("d")));
      }
      assertThat(days.stream().distinct().count()).as("stored days of %s: %s", number, days).isEqualTo(1L);
    }
  }

  @Test
  void aNumberPastTheLargestDayCountIsTheMillisecondsItIs() {
    createType();
    for (final Class<?> implementation : IMPLEMENTATIONS) {
      serializer().setDateImplementation(implementation);
      // the band of the issue: 1976-05-03 as milliseconds, refused by java.util.Date before, a day of the year 547 million
      // under LocalDate
      assertThat(dayOf(200_000_000_000L)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(1976, 5, 3));
      // a "now" in milliseconds, refused by LocalDate before
      assertThat(dayOf(NOW_MILLIS)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(2024, 1, 15));
      // the largest day count is still a day count
      assertThat(dayOf(MAX_DAY_COUNT).toEpochDay()).as(implementation.getSimpleName()).isEqualTo(MAX_DAY_COUNT);
      // a small number is a day count, as it always was
      assertThat(dayOf(19_737L)).as(implementation.getSimpleName()).isEqualTo(LocalDate.of(2024, 1, 15));
    }
  }

  @Test
  void aListOfDateAgreesWithTheProperty() {
    createType();
    database.transaction(() -> database.getSchema().getType("D").createProperty("ds", Type.LIST, "DATE"));
    for (final Class<?> implementation : IMPLEMENTATIONS) {
      serializer().setDateImplementation(implementation);
      final Object list = inTx(() -> database.newDocument("D").set("ds", List.of(200_000_000_000L, NOW_MILLIS, 19_737L)).get("ds"));
      assertThat(((List<?>) list).stream().map(DateUtils::dateToEpochDays).map(LocalDate::ofEpochDay).toList())
          .as(implementation.getSimpleName())
          .containsExactly(LocalDate.of(1976, 5, 3), LocalDate.of(2024, 1, 15), LocalDate.of(2024, 1, 15));
    }
  }

  @Test
  void aNumberWhoseMillisecondsAreNoDayOfAnImplementationIsRefusedByAll() {
    createType();
    // Long.MIN_VALUE as milliseconds is a day before the first one a java.util.Date can hold: every implementation refuses it,
    // so none stores what another refuses
    for (final Class<?> implementation : IMPLEMENTATIONS) {
      serializer().setDateImplementation(implementation);
      assertThatThrownBy(() -> inTx(() -> database.newDocument("D").set("d", Long.MIN_VALUE).get("d"))).as(
          implementation.getSimpleName()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void theReadingOfANumberAtTheEdgesOfTheDayCountWindow() {
    assertThat(DateUtils.dateNumberToEpochDays(MAX_DAY_COUNT)).isEqualTo(MAX_DAY_COUNT);
    assertThat(DateUtils.dateNumberToEpochDays(-MAX_DAY_COUNT)).isEqualTo(-MAX_DAY_COUNT);
    assertThat(DateUtils.dateNumberToEpochDays(MAX_DAY_COUNT + 1)).isEqualTo(Math.floorDiv(MAX_DAY_COUNT + 1, DateUtils.MS_IN_A_DAY));
    assertThat(DateUtils.dateNumberToEpochDays(-MAX_DAY_COUNT - 1)).isEqualTo(Math.floorDiv(-MAX_DAY_COUNT - 1, DateUtils.MS_IN_A_DAY));
    assertThat(DateUtils.dateNumberToEpochDays(Long.MAX_VALUE)).isEqualTo(MAX_DAY_COUNT);
    // every answer is a day a java.util.Date can hold as milliseconds
    for (final long number : NUMBERS)
      assertThat(Math.abs(DateUtils.dateNumberToEpochDays(number))).as("%s", number).isLessThanOrEqualTo(MAX_DAY_COUNT);
    assertThatThrownBy(() -> DateUtils.dateNumberToEpochDays(Long.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aComparisonKeepsReadingANumberAsDays() {
    // no property behind the conversion: a number is a count of days, so a number never equals a date (#8913)
    assertThat(Type.convert(database, 19_737L, LocalDate.class)).isEqualTo(LocalDate.of(2024, 1, 15));
    assertThat(Type.convert(database, 200_000_000L, LocalDate.class)).isEqualTo(LocalDate.ofEpochDay(200_000_000L));
  }

  private LocalDate dayOf(final long number) {
    return LocalDate.ofEpochDay(DateUtils.dateToEpochDays(inTx(() -> database.newDocument("D").set("d", number).get("d"))));
  }

  private void createType() {
    database.transaction(() -> database.getSchema().createDocumentType("D").createProperty("d", Type.DATE));
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
