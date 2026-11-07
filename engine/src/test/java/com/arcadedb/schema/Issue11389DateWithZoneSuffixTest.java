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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11389: pgjdbc sends a {@code java.sql.Date} as untyped text with a zone suffix ({@code '2024-05-06 +01'}). PostgreSQL
 * reads it as a date and ignores the zone; the engine refused it for a DATE property and matched nothing in a predicate.
 */
class Issue11389DateWithZoneSuffixTest extends TestHelper {

  @Test
  void insertOfDateWithZoneSuffixKeepsTheDay() {
    database.getSchema().createDocumentType("D11389").createProperty("d", Type.DATE);
    for (final String text : new String[] { "2024-05-06 +01", "2024-05-06 +01:00", "2024-05-06 -08", "2024-05-06 Z" })
      database.transaction(() -> database.newDocument("D11389").set("d", text).save());

    try (final ResultSet rs = database.query("sql", "SELECT FROM D11389")) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat((Object) rs.next().getProperty("d")).isEqualTo(LocalDate.of(2024, 5, 6));
        count++;
      }
      assertThat(count).isEqualTo(4);
    }
  }

  @Test
  void predicatesWithZoneSuffixMatchTheDay() {
    database.getSchema().createDocumentType("W11389").createProperty("d", Type.DATE);
    database.transaction(() -> database.command("sql", "INSERT INTO W11389 SET d = '2024-05-06'"));

    assertCount("d = ?", 1);
    assertCount("d >= ?", 1);
    assertCount("d <= ?", 1);
    assertCount("d < ?", 0);
    assertCount("d > ?", 0);
  }

  private void assertCount(final String where, final int expected) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM W11389 WHERE " + where, "2024-05-06 +01")) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).as(where).isEqualTo(expected);
    }
  }
}
