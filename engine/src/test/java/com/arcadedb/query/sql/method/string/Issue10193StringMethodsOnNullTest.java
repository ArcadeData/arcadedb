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
package com.arcadedb.query.sql.method.string;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10193: on a NULL value {@code s.replace('a', 'b')} answered {@code 'a'} (its first argument) and {@code s.length()}
 * answered 0, so an UPDATE wrote a value into rows that had none. Both are NULL now, as SQL's REPLACE() and LENGTH() are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10193StringMethodsOnNullTest extends TestHelper {

  @Test
  void updateWithReplaceAndLengthOnNullKeepsNull() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.id LONG");
    database.command("sql", "CREATE PROPERTY T.c INTEGER");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET id = 1"));

    database.transaction(() -> database.command("sql", "UPDATE T SET s = s.replace('a', 'b')"));
    database.transaction(() -> database.command("sql", "UPDATE T SET c = s.length()"));

    try (final ResultSet rs = database.query("sql", "SELECT s, c FROM T WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("s")).isNull();
      assertThat(row.<Object>getProperty("c")).isNull();
    }
  }

  @Test
  void aNullArgumentAnswersNull() {
    try (final ResultSet rs = database.query("sql",
        "SELECT 'abc'.replace(null, 'b') AS r1, 'abc'.replace('a', null) AS r2, 'abc'.replace('a', 'b') AS r3, "
            + "'abc'.length() AS l1, ''.length() AS l2")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("r1")).isNull();
      assertThat(row.<Object>getProperty("r2")).isNull();
      assertThat(row.<String>getProperty("r3")).isEqualTo("bbc");
      assertThat(row.<Integer>getProperty("l1")).isEqualTo(3);
      assertThat(row.<Integer>getProperty("l2")).isZero();
    }
  }

  @Test
  void methodsOnAMissingPropertyAreNull() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.transaction(() -> database.command("sql", "INSERT INTO U SET id = 1"));

    try (final ResultSet rs = database.query("sql", "SELECT missing.replace('a', 'b') AS r, missing.length() AS l FROM U")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("r")).isNull();
      assertThat(row.<Object>getProperty("l")).isNull();
    }
  }
}
