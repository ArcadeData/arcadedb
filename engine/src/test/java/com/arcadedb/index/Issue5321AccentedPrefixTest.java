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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5321: partial-prefix lookup on a composite NOTUNIQUE index returns rows
 * of OTHER keys when the first key component contains some accented (multi-byte UTF-8) characters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5321AccentedPrefixTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE ReproN7");
      database.command("sql", "CREATE PROPERTY ReproN7.w STRING");
      database.command("sql", "CREATE PROPERTY ReproN7.l STRING");
      database.command("sql", "CREATE PROPERTY ReproN7.t STRING");
      database.command("sql", "CREATE INDEX ON ReproN7 (w, l, t) NOTUNIQUE");

      final String[] words = { "cioccolato", "cinema", "cioè", "ciondolo", "citare", "così", "è", "perché", "casa", "cane" };
      for (final String w : words) {
        database.command("sql", "INSERT INTO ReproN7 SET w = ?, l = 'it', t = 'A'", w);
        database.command("sql", "INSERT INTO ReproN7 SET w = ?, l = 'it', t = 'B'", w);
      }
    });
  }

  private long countPartial(final String w) {
    return database.query("sql", "SELECT w FROM ReproN7 WHERE w = ? AND l = 'it'", w).stream().count();
  }

  @Test
  void fullKeyEqualityAlwaysCorrect() {
    database.transaction(() -> {
      assertThat(database.query("sql", "SELECT w FROM ReproN7 WHERE w = 'cioè' AND l = 'it' AND t = 'A'").stream().count())
          .isEqualTo(1);
      assertThat(database.query("sql", "SELECT w FROM ReproN7 WHERE w = 'è' AND l = 'it' AND t = 'A'").stream().count())
          .isEqualTo(1);
    });
  }

  @Test
  void partialPrefixEqualityAsciiCorrect() {
    database.transaction(() -> {
      assertThat(countPartial("casa")).isEqualTo(2);
      assertThat(countPartial("cinema")).isEqualTo(2);
    });
  }

  @Test
  void partialPrefixEqualityAccentedCorrect() {
    database.transaction(() -> {
      assertThat(countPartial("così")).as("così").isEqualTo(2);
      assertThat(countPartial("perché")).as("perché").isEqualTo(2);
      assertThat(countPartial("cioè")).as("cioè").isEqualTo(2);
      assertThat(countPartial("è")).as("è").isEqualTo(2);
    });
  }
}
