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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8975: SQL min() and max() compared strings by UTF-16 code unit, so a supplementary character (an emoji)
 * sorted below U+FF5A, disagreeing with ORDER BY, the index and openCypher, which compare by code point.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8975StringMinMaxCodePointTest extends TestHelper {
  private static final String FULLWIDTH_Z = "ｚ";
  private static final String EMOJI       = "😀";

  private void load() {
    database.command("sql", "CREATE VERTEX TYPE S");
    database.command("sql", "CREATE PROPERTY S.s STRING");
    database.transaction(() -> {
      database.newVertex("S").set("s", FULLWIDTH_Z).save();
      database.newVertex("S").set("s", EMOJI).save();
    });
  }

  private String single(final String sql, final String name) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty(name);
    }
  }

  private void assertCodePointOrder() {
    assertThat(single("SELECT max(s) AS v FROM S", "v")).isEqualTo(EMOJI);
    assertThat(single("SELECT min(s) AS v FROM S", "v")).isEqualTo(FULLWIDTH_Z);
    assertThat(single("SELECT min(s) AS mn, max(s) AS v FROM S", "v")).isEqualTo(EMOJI);
    assertThat(single("SELECT min(s) AS v, max(s) AS mx FROM S", "v")).isEqualTo(FULLWIDTH_Z);
    assertThat(single("SELECT s AS v FROM S ORDER BY s DESC LIMIT 1", "v")).isEqualTo(EMOJI);
  }

  @Test
  void minMaxWithoutIndex() {
    load();
    assertCodePointOrder();
  }

  @Test
  void minMaxWithIndex() {
    load();
    database.command("sql", "CREATE INDEX ON S (s) NOTUNIQUE");
    assertCodePointOrder();
  }

  @Test
  void openCypherAgrees() {
    load();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:S) RETURN min(n.s) AS mn, max(n.s) AS mx")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("mn")).isEqualTo(FULLWIDTH_Z);
      assertThat(r.<String>getProperty("mx")).isEqualTo(EMOJI);
    }
  }
}
