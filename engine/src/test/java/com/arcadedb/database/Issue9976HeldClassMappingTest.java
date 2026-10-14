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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinarySerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9976: a change of {@code arcadedb.dateImplementation} or {@code arcadedb.dateTimeImplementation} checks the patterns
 * of exactly the types whose held class it changes, through {@link DocumentValidator#heldClassUnder}. That mapping has to
 * agree with {@link Type#getJavaImplementation} on every type: a type added later and left out would have its patterns
 * silently skipped by the check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9976HeldClassMappingTest extends TestHelper {

  @Test
  void theMappingAgreesWithTheSerializerOnEveryType() {
    final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
    final Class<?> previousDate = serializer.getDateImplementation();
    final Class<?> previousDateTime = serializer.getDateTimeImplementation();
    try {
      for (final Class<?> dateImplementation : List.of(LocalDate.class, Date.class, Calendar.class, LocalDateTime.class))
        for (final Class<?> dateTimeImplementation : List.of(LocalDateTime.class, Date.class, Calendar.class, Instant.class,
            ZonedDateTime.class)) {
          serializer.setDateImplementation(dateImplementation);
          serializer.setDateTimeImplementation(dateTimeImplementation);
          for (final Type type : Type.values()) {
            final Class<?> mapped = DocumentValidator.heldClassUnder(type, dateImplementation, dateTimeImplementation);
            if (mapped != null)
              assertThat(mapped).as("%s under %s / %s", type, dateImplementation.getSimpleName(),
                  dateTimeImplementation.getSimpleName()).isEqualTo(type.getJavaImplementation(database));
            else
              // a type the mapping leaves out is one the two settings do not move
              assertThat(type.getJavaImplementation(database)).as("%s, left out of the mapping", type)
                  .isEqualTo(type.getJavaImplementation(null));
          }
        }
    } finally {
      serializer.setDateImplementation(previousDate);
      serializer.setDateTimeImplementation(previousDateTime);
    }
  }
}
