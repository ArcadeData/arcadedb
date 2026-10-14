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
 * Issue #10005: {@code LIKE '%'} did not match a stored empty string, because {@code QueryHelper.likeUntil} returned false
 * whenever exactly one of the value and the pattern was empty, before the pattern was looked at.
 */
class Issue10005LikeEmptyStringTest extends TestHelper {
  @Test
  void anEmptyValueMatchesAPatternOfOnlyPercentSigns() {
    assertThat(QueryHelper.like("", "%", 1000)).isTrue();
    assertThat(QueryHelper.like("", "%%", 1000)).isTrue();
    assertThat(QueryHelper.like("", "", 1000)).isTrue();
  }

  @Test
  void anEmptyValueDoesNotMatchAPatternThatNeedsACharacter() {
    assertThat(QueryHelper.like("", "_", 1000)).isFalse();
    assertThat(QueryHelper.like("", "a", 1000)).isFalse();
    assertThat(QueryHelper.like("", "%a", 1000)).isFalse();
    assertThat(QueryHelper.like("", "a%", 1000)).isFalse();
    assertThat(QueryHelper.like("", "%a%", 1000)).isFalse();
  }

  @Test
  void anEmptyPatternStillMatchesOnlyAnEmptyValue() {
    assertThat(QueryHelper.like("a", "", 1000)).isFalse();
    assertThat(QueryHelper.like(" ", "", 1000)).isFalse();
  }

  @Test
  void aNullOnEitherSideDoesNotMatch() {
    assertThat(QueryHelper.like(null, "%", 1000)).isFalse();
    assertThat(QueryHelper.like("", null, 1000)).isFalse();
  }

  @Test
  void likePercentKeepsTheEmptyStringInACountAndAGroup() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.transaction(() -> {
      for (final String v : new String[] { "", "a", "a", "" })
        database.command("sql", "INSERT INTO T SET s = ?", v);
      database.command("sql", "INSERT INTO T SET x = 1");
    });

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM T WHERE s LIKE '%'")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(4L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT s, count(*) AS n FROM T WHERE s LIKE '%' GROUP BY s ORDER BY s")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM T WHERE s LIKE ''")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(2L);
    }
  }
}
