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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9237: {@code p IS NULL} answered through a UNIQUE or UNIQUE_HASH index with NULL_STRATEGY INDEX returned one of the
 * records whose p is null, because the point lookup of a unique index stops at the first entry of a key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9237UniqueIsNullTest extends TestHelper {

  private static final String[][] TYPES = { { "Scan", null }, { "NotUniqueIdx", "NOTUNIQUE" }, { "UniqueIdx", "UNIQUE" },
      { "UniqueHashIdx", "UNIQUE_HASH" }, { "NotUniqueHashIdx", "NOTUNIQUE_HASH" } };

  @Override
  public void beginTest() {
    for (final String[] t : TYPES) {
      database.command("sql", "CREATE VERTEX TYPE " + t[0]);
      database.command("sql", "CREATE PROPERTY " + t[0] + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t[0] + ".p INTEGER");
      if (t[1] != null)
        database.command("sql", "CREATE INDEX ON " + t[0] + " (p) " + t[1] + " NULL_STRATEGY INDEX");
      database.transaction(() -> database.newVertex(t[0]).set("id", 1, "p", 1).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 2, "p", null).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 3).save());
      database.transaction(() -> database.newVertex(t[0]).set("id", 4, "p", null).save());
    }
  }

  private List<Long> ids(final String query) {
    final List<Long> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").longValue());
    }
    Collections.sort(ids);
    return ids;
  }

  @Test
  void isNullReturnsEveryRecordWhoseKeyIsNull() {
    for (final String[] t : TYPES) {
      assertThat(ids("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L);
      assertThat(ids("SELECT id FROM " + t[0] + " WHERE p IS NULL AND id > 0")).as(t[0]).containsExactly(2L, 3L, 4L);
      assertThat(ids("SELECT count(*) AS id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3L);
    }
  }

  @Test
  void isNullAfterDeletingOneOfTheNullRecords() {
    for (final String[] t : TYPES) {
      database.transaction(() -> database.command("sql", "DELETE FROM " + t[0] + " WHERE id = 2"));
      assertThat(ids("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(3L, 4L);
    }
  }

  @Test
  void isNullAfterAReopen() {
    reopenDatabase();
    for (final String[] t : TYPES)
      assertThat(ids("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L);
  }

  @Test
  void isNullInsideTheTransactionThatWritesTheNulls() {
    for (final String[] t : TYPES) {
      database.transaction(() -> {
        database.newVertex(t[0]).set("id", 5, "p", null).save();
        assertThat(ids("SELECT id FROM " + t[0] + " WHERE p IS NULL")).as(t[0]).containsExactly(2L, 3L, 4L, 5L);
      });
    }
  }
}
