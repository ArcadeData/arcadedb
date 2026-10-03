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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8976: the count(*) and min()/max() shortcuts that answer a whole-type aggregate without reading records
 * dropped the arithmetic or the modifier around the aggregate.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8976HardwiredAggregateProjectionTest extends TestHelper {

  @BeforeEach
  void load() {
    for (final String type : new String[] { "Plain", "Indexed" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".i INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
    }
    database.command("sql", "CREATE INDEX ON Indexed (s) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "Plain", "Indexed" }) {
        database.newDocument(type).set("i", 10, "s", "abc").save();
        database.newDocument(type).set("i", 20, "s", "z").save();
        database.newDocument(type).set("s", "hello").save();
      }
    });
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }

  @Test
  void countStarWithArithmetic() {
    assertThat(((Number) single("SELECT count(*) + 1 AS v FROM Plain")).longValue()).isEqualTo(4L);
    assertThat(((Number) single("SELECT count(*) * 10 AS v FROM Plain")).longValue()).isEqualTo(30L);
    assertThat(((Number) single("SELECT count(*) + 1 AS v FROM Indexed")).longValue()).isEqualTo(4L);
    assertThat(((Number) single("SELECT count(*) AS v FROM Plain")).longValue()).isEqualTo(3L);
  }

  @Test
  void minMaxWithModifierOnIndexedProperty() {
    for (final String type : new String[] { "Plain", "Indexed" }) {
      assertThat(((Number) single("SELECT max(s.length()) AS v FROM " + type)).intValue()).isEqualTo(5);
      assertThat(((Number) single("SELECT min(s.length()) AS v FROM " + type)).intValue()).isEqualTo(1);
      assertThat(single("SELECT max(s.toUpperCase()) AS v FROM " + type)).isEqualTo("Z");
    }
    assertThat(single("SELECT max(s) AS v FROM Indexed")).isEqualTo("z");
    assertThat(single("SELECT min(s) AS v FROM Indexed")).isEqualTo("abc");
  }
}
