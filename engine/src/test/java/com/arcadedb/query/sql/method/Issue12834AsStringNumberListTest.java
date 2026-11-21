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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12834: {@code asString()} on a list of numbers rendered it through the float vector formatter, so a list of integers,
 * longs or doubles came back as different numbers ({@code [1234567890123456].asString()} answered {@code [1.234568E15]}).
 */
class Issue12834AsStringNumberListTest extends TestHelper {
  @Test
  void aListOfLongsKeepsEveryDigit() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.ids LIST OF LONG");
    database.transaction(() -> database.command("sql", "INSERT INTO D SET ids = [1234567890123456, 9007199254740993]"));

    assertThat(value("SELECT ids.asString() AS v FROM D")).isEqualTo("[1234567890123456, 9007199254740993]");
  }

  @Test
  void literalListsKeepTheirElementTypes() {
    assertThat(value("SELECT [16777217, 2].asString() AS v")).isEqualTo("[16777217, 2]");
    assertThat(value("SELECT [1, 2].asString() AS v")).isEqualTo("[1, 2]");
    assertThat(value("SELECT [0.1234567890123, 2.5].asString() AS v")).isEqualTo("[0.1234567890123, 2.5]");
    assertThat(value("SELECT [1, null].asString() AS v")).isEqualTo("[1, null]");
  }

  @Test
  void aStoredPrimitiveArrayOfLongsKeepsEveryDigit() {
    database.command("sql", "CREATE DOCUMENT TYPE E");
    database.command("sql", "CREATE PROPERTY E.ids ARRAY_OF_LONGS");
    database.transaction(() -> database.command("sql", "INSERT INTO E SET ids = [1234567890123456, 2]"));

    assertThat(value("SELECT ids.asString() AS v FROM E")).isEqualTo("[1234567890123456, 2]");
  }

  /** The vector use of the method is unchanged: floats, and any list with an explicit vector format. */
  @Test
  void vectorsAreStillFormatted() {
    database.command("sql", "CREATE DOCUMENT TYPE Emb");
    database.command("sql", "CREATE PROPERTY Emb.v ARRAY_OF_FLOATS");
    database.transaction(() -> database.command("sql", "INSERT INTO Emb SET v = [0.5, 0.25, 0.75]"));

    assertThat(value("SELECT v.asString() AS v FROM Emb")).isEqualTo("[0.5, 0.25, 0.75]");
    assertThat(value("SELECT v.asString('MATLAB') AS v FROM Emb")).isEqualTo("[0.5 0.25 0.75]");
    assertThat(value("SELECT [1.0, 2.0, 3.0].asString() AS v")).isEqualTo("[1.0, 2.0, 3.0]");
    assertThat(value("SELECT [1.0, 2.0, 3.0].asString('MATLAB') AS v")).isEqualTo("[1.0 2.0 3.0]");
    assertThat(value("SELECT [1, 2, 3].asString('NUMPY') AS v")).isEqualTo("1.0, 2.0, 3.0");
  }

  private Object value(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
