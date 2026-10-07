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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9324: with {@code arcadedb.dateImplementation=java.time.LocalDateTime} a DATE (a count of days) was read as if it
 * were milliseconds, so every DATE answered a 1970 timestamp and could not be written back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9324DateLocalDateTimeTest extends TestHelper {
  private static final LocalDate DAY = LocalDate.of(2024, 2, 29);

  @Test
  void dateReadsAsMidnightOfThatDay() {
    assertThat(DateUtils.date(database, DAY.toEpochDay(), LocalDateTime.class)).isEqualTo(DAY.atStartOfDay());
    assertThat(DateUtils.date(database, LocalDate.of(1960, 5, 1).toEpochDay(), LocalDateTime.class))
        .isEqualTo(LocalDate.of(1960, 5, 1).atStartOfDay());
  }

  @Test
  void storedDateRoundTripsUnderLocalDateTimeImplementation() {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Object previous = serializer.getDateImplementation();
    try {
      serializer.setDateImplementation(LocalDateTime.class);
      database.transaction(() -> {
        database.getSchema().createDocumentType("T").createProperty("D", Type.DATE);
        database.newDocument("T").set("D", DAY).save();
      });

      database.transaction(() -> {
        final MutableDocument doc = database.iterateType("T", true).next().asDocument().modify();
        final Object read = doc.get("D");
        assertThat(read).isEqualTo(DAY.atStartOfDay());
        // read-modify-write must not fail
        doc.set("D", read).save();
        assertThat(doc.get("D")).isEqualTo(DAY.atStartOfDay());
      });
    } finally {
      serializer.setDateImplementation(previous);
    }
  }

  @Test
  void numberToLocalDateTimeStaysOnEpochMillis() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ms").createProperty("dt", Type.DATETIME);
      database.newDocument("Ms").set("dt", 1791000000000L).save();
    });
    final Object read = database.iterateType("Ms", true).next().asDocument().get("dt");
    assertThat(read).isEqualTo(LocalDateTime.of(2026, 10, 3, 4, 0));
  }

  @Test
  void dateColumnTruncatesToDaysUnderDatetimeImplementations() {
    database.getSchema().createDocumentType("Dy").createProperty("d", Type.DATE);
    for (final Class<?> implementation : new Class<?>[] { LocalDateTime.class, java.time.Instant.class, java.time.ZonedDateTime.class }) {
      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Object previous = serializer.getDateImplementation();
      try {
        serializer.setDateImplementation(implementation);
        database.transaction(() -> database.newDocument("Dy").set("d", "2024-02-29T13:45:10").save());
      } catch (final IllegalArgumentException e) {
        // a string is a separate conversion path; only the typed values below must succeed
      } finally {
        serializer.setDateImplementation(previous);
      }
    }
    database.transaction(() -> database.newDocument("Dy").set("d", LocalDateTime.of(2024, 2, 29, 13, 45, 10)).save());
    assertThat(database.iterateType("Dy", true).next().asDocument().get("d")).isEqualTo(DAY);
  }
}
