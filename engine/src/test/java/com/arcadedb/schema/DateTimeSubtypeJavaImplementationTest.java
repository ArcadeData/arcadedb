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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Type#getJavaImplementation} answers, for every DATETIME precision, the class the deserializer reads the value
 * back as: the configured datetime implementation, except that a sub-millisecond column stays a {@code LocalDateTime}
 * under one that stops at the millisecond (issue #8158). The write path keeps a value in that class in memory, so a record
 * must read back as the same class it was written as.
 */
class DateTimeSubtypeJavaImplementationTest extends TestHelper {

  private static final String[] PRECISIONS = { "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS", "DATETIME_NANOS" };

  @Test
  void everyPrecisionAnswersTheClassItIsReadBackAs() {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    for (final Class<?> implementation : new Class<?>[] { LocalDateTime.class, ZonedDateTime.class, Instant.class, Date.class,
        Calendar.class }) {
      serializer.setDateTimeImplementation(implementation);
      final boolean millisecondBound = implementation == Date.class || implementation == Calendar.class;

      assertThat(Type.DATETIME.getJavaImplementation(database)).as("%s DATETIME", implementation).isEqualTo(implementation);
      assertThat(Type.DATETIME_SECOND.getJavaImplementation(database)).as("%s SECOND", implementation).isEqualTo(implementation);
      for (final Type subMillis : new Type[] { Type.DATETIME_MICROS, Type.DATETIME_NANOS })
        assertThat(subMillis.getJavaImplementation(database)).as("%s %s", implementation, subMillis)
            .isEqualTo(millisecondBound ? LocalDateTime.class : implementation);

      for (final String precision : PRECISIONS) {
        final String typeName = "T_" + implementation.getSimpleName() + "_" + precision;
        database.getSchema().createDocumentType(typeName).createProperty("t", Type.getTypeByName(precision));
        final RID[] rid = new RID[1];
        database.transaction(
            () -> rid[0] = database.newDocument(typeName).set("t", Instant.ofEpochSecond(1600000000L, 123456789)).save()
                .getIdentity());
        // a fresh transaction, so the record is deserialized rather than served from the writing transaction's cache
        final Object[] written = new Object[1];
        database.transaction(() -> written[0] = database.lookupByRID(rid[0], true).asDocument().get("t"));
        assertThat(written[0]).as("%s %s read back", implementation, precision)
            .isInstanceOf(Type.getTypeByName(precision).getJavaImplementation(database));
      }
    }
  }
}
