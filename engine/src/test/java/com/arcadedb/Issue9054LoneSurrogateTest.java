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
package com.arcadedb;

import com.arcadedb.database.Binary;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/9054">issue #9054</a>: a String holding a lone UTF-16
 * surrogate was silently stored as '?', so another string came back and a UNIQUE index saw it equal to "?".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9054LoneSurrogateTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "CREATE PROPERTY S.s STRING");
      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.command("sql", "CREATE PROPERTY U.s STRING");
      database.command("sql", "CREATE INDEX ON U (s) UNIQUE");
    });
  }

  @Test
  void loneSurrogatesAreRefusedOnWrite() {
    for (final String value : new String[] { "\uD83D", "\uDE00", "a\uD83Db", "\uDE00\uD83D" })
      assertThatThrownBy(() -> database.transaction(() -> save("S", value))).isInstanceOf(ValidationException.class)
          .hasMessageContaining("lone UTF-16 surrogate").satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM S")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
  }

  @Test
  void wellFormedAndQuestionMarkStringsRoundTrip() {
    final String[] values = { "abc", "😀", "?", "a?b??", "😀?😀", "" };
    database.transaction(() -> {
      for (final String v : values)
        save("S", v);
    });
    try (final ResultSet rs = database.query("sql", "SELECT s FROM S")) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat(values).contains(rs.next().<String>getProperty("s"));
        count++;
      }
      assertThat(count).isEqualTo(values.length);
    }
  }

  @Test
  void uniqueIndexDoesNotCollideLoneSurrogateWithQuestionMark() {
    database.transaction(() -> save("U", "?"));
    assertThatThrownBy(() -> database.transaction(() -> save("U", "\uD83D"))).isInstanceOf(ValidationException.class);
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM U")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
  }

  @Test
  void binaryPutStringRefusesLoneSurrogate() {
    final Binary binary = new Binary();
    assertThatThrownBy(() -> binary.putString("\uD83D")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> binary.putString(0, "x\uDE00")).isInstanceOf(ValidationException.class);
    binary.putString("?");
    binary.position(0);
    assertThat(binary.getString()).isEqualTo("?");
  }

  private void save(final String type, final String value) {
    final MutableDocument d = database.newDocument(type);
    d.set("s", value);
    d.save();
  }
}
