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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9045 (comment between ORDER and BY / GROUP and BY) and #9052 (hexadecimal integer literals).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9045And9052SqlLexerTest extends TestHelper {
  @Test
  void commentsBetweenOrderAndBy() {
    database.getSchema().createDocumentType("T9045");
    database.transaction(() -> {
      for (int i = 3; i >= 1; i--)
        database.command("sql", "INSERT INTO T9045 SET id = " + i + ", g = " + (i % 2)).close();
    });

    for (final String order : new String[] { "ORDER BY", "ORDER /* c */ BY", "ORDER/**/BY", "ORDER -- c\nBY", "ORDER -- c\r\nBY", "order \n\t BY" }) {
      final List<Integer> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT id FROM T9045 " + order + " id")) {
        while (rs.hasNext())
          ids.add(rs.next().<Integer>getProperty("id"));
      }
      assertThat(ids).as(order).containsExactly(1, 2, 3);
    }

    for (final String group : new String[] { "GROUP BY", "GROUP /* c */ BY", "GROUP/**/BY", "GROUP -- c\nBY" }) {
      try (final ResultSet rs = database.query("sql", "SELECT g, count(*) AS c FROM T9045 " + group + " g")) {
        int n = 0;
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
        assertThat(n).as(group).isEqualTo(2);
      }
    }
  }

  @Test
  void commentContainingTokensBetweenOrderAndByDoesNotSwallowTheQuery() {
    database.getSchema().createDocumentType("T9045b");
    database.transaction(() -> database.command("sql", "INSERT INTO T9045b SET id = 1").close());

    try (final ResultSet rs = database.query("sql", "SELECT id FROM T9045b ORDER /* a */ /* b */ BY id")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
    // a token between two comments is not part of the keyword
    assertThatThrownBy(() -> database.query("sql", "SELECT id FROM T9045b ORDER /* a */ x /* b */ BY id").close())
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void hexadecimalOverflowIsAParseError() {
    assertThatThrownBy(() -> database.query("sql", "SELECT 0xFFFFFFFFFFFFFFFFL AS r").close()).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void hexadecimalVariantsAndPositions() {
    assertThat(scalar("SELECT 0xffl AS r")).isEqualTo(255L);
    assertThat(scalar("SELECT -0x10 AS r")).isEqualTo(-16);
    database.getSchema().createDocumentType("T9052");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("sql", "INSERT INTO T9052 SET id = " + i).close();
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM T9052 ORDER BY id LIMIT 0x10 SKIP 0xA")) {
      int n = 0;
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
      assertThat(n).isEqualTo(10);
    }
    for (final String bad : new String[] { "SELECT 0x AS r", "SELECT 0xG AS r" })
      assertThatThrownBy(() -> database.query("sql", bad).close()).as(bad).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void hexadecimalLiterals() {
    assertThat(scalar("SELECT 0xFF AS r")).isEqualTo(255);
    assertThat(scalar("SELECT 0XFFL AS r")).isEqualTo(255L);
    assertThat(scalar("SELECT 0x10 + 1 AS r")).isEqualTo(17);
    assertThat(scalar("SELECT 0xFFFFFFFF AS r")).isEqualTo(4294967295L);
    assertThat(scalar("SELECT 0 AS r")).isEqualTo(0);
    assertThat(scalar("SELECT 255 AS r")).isEqualTo(255);
  }

  private Object scalar(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("r");
    }
  }
}
