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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #8814 (Halloween problem): an UPDATE that moves an indexed property forward, with a WHERE range on that same
 * property, walked the index range it was changing and met its own new keys ahead of the cursor, updating the same
 * records again and again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8814UpdateHalloweenTest extends TestHelper {
  private static final int N = 2000;

  private void load() {
    database.transaction(() -> {
      final var type = database.getSchema().createVertexType("V");
      type.createProperty("a", Type.LONG);
      type.createProperty("b", Type.LONG);
      database.getSchema().buildTypeIndex("V", new String[] { "a" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < N; i++)
        database.newVertex("V").set("a", (long) i, "b", 0L).save();
    });
  }

  private void assertEachMovedOnce(final String where, final long expectedUpdated) {
    database.begin();
    final long returned;
    try (final ResultSet rs = database.command("sql", "UPDATE V SET a = a + 10, b = b + 1 WHERE " + where)) {
      returned = rs.next().<Number>getProperty("count").longValue();
    }
    database.commit();
    assertThat(returned).isEqualTo(expectedUpdated);

    long once = 0;
    long more = 0;
    try (final ResultSet rs = database.query("sql", "SELECT b FROM V")) {
      while (rs.hasNext()) {
        final long b = rs.next().<Number>getProperty("b").longValue();
        if (b == 1)
          once++;
        else if (b > 1)
          more++;
      }
    }
    assertThat(more).isZero();
    assertThat(once).isEqualTo(expectedUpdated);
  }

  @ParameterizedTest
  @ValueSource(strings = { "a < 1001", "a >= 0 AND a <= 1000", "a BETWEEN 0 AND 1000" })
  void rangeSpellings(final String where) {
    load();
    assertEachMovedOnce(where, 1001);
  }

  @Test
  void limitStillStopsEarly() {
    final String where = "a >= 0 AND a <= 1000";
    load();
    database.begin();
    try (final ResultSet rs = database.command("sql", "UPDATE V SET a = a + 10, b = b + 1 WHERE " + where + " LIMIT 5")) {
      assertThat(rs.next().<Number>getProperty("count").longValue()).isEqualTo(5);
    }
    database.commit();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE b > 0")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(5);
    }
  }
}
