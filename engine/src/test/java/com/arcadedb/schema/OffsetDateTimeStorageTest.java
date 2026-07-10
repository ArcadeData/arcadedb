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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.utility.DateUtils;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a {@link OffsetDateTime} value must be storable in a schemaless property. The type
 * system previously did not register {@code OffsetDateTime} (only {@code Date}/{@code LocalDateTime}/
 * {@code ZonedDateTime}/{@code Instant}), so {@code getTypeByValue} returned {@code null} and the value
 * was silently dropped on write - which broke native datetime writes over Bolt (the Neo4j driver packs a
 * fixed-offset datetime as an offset {@code DateTime} struct, decoded to {@code OffsetDateTime}).
 */
class OffsetDateTimeStorageTest {

  @Test
  void offsetDateTimeResolvesToDatetimeType() {
    assertThat(Type.getTypeByValue(OffsetDateTime.now(ZoneOffset.UTC))).isEqualTo(Type.DATETIME);
    assertThat(DateUtils.isDate(OffsetDateTime.now(ZoneOffset.UTC))).isTrue();
  }

  @Test
  void offsetDateTimePersistsInSchemalessProperty() {
    final String path = "./target/testoffsetdt_" + System.nanoTime();
    final Database database = new DatabaseFactory(path).create();
    try {
      final OffsetDateTime value = OffsetDateTime.of(2024, 1, 15, 10, 30, 45, 0, ZoneOffset.ofHours(2));

      database.getSchema().getOrCreateDocumentType("Ev");
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("Ev");
        doc.set("id", 1);
        doc.set("ts", value);
        doc.save();
      });

      final var result = database.query("sql", "SELECT ts FROM Ev WHERE id = 1");
      assertThat(result.hasNext()).isTrue();
      final Object stored = result.next().getProperty("ts");
      // Previously null (dropped); now stored as a datetime preserving the instant.
      assertThat(stored).isNotNull();
      assertThat(DateUtils.dateTimeToTimestamp(stored, ChronoUnit.MILLIS))
          .isEqualTo(value.toInstant().toEpochMilli());
    } finally {
      database.drop();
    }
  }
}
