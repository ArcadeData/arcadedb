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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7773: {@code Result.toJSON()} short-circuited to the backing record whenever the row
 * {@code isElement()}, but {@code Projection#calculateSingle()} builds exactly such a row for {@code SELECT *} plus
 * an extra projection item - it sets the element (for the {@code *}) AND writes the explicit items into the row's
 * own content, so the two disagree. The element short-circuit therefore answered the record instead of the row: a
 * computed alias was dropped, an alias colliding with a real column answered the stored value instead of the
 * computed one, and an excluded column ({@code !col}) leaked back in - even though {@code JsonSerializer
 * .serializeResult()} (which drives the HTTP/remote path) and {@code getPropertyNames()}/{@code getProperty()} were
 * already correct in all three cases.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7773ResultToJSONProjectedRowTest {

  @Test
  void starPlusComputedAliasIsNotDropped() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Alias", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, d = 500");

      try (final ResultSet rs = db.query("sql", "SELECT *, n+1 AS z FROM T")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).contains("z");
        assertThat(row.<Integer>getProperty("z")).isEqualTo(2);

        final JSONObject json = row.toJSON();
        assertThat(json.has("z")).as("computed alias must not be dropped from toJSON()").isTrue();
        assertThat(json.getInt("z")).isEqualTo(2);
        assertThat(json.getInt("n")).isEqualTo(1);
        assertThat(json.getInt("d")).isEqualTo(500);
      }
    });
  }

  @Test
  void starPlusAliasCollidingWithARealColumnAnswersTheComputedValue() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Collide", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, d = 500");

      try (final ResultSet rs = db.query("sql", "SELECT *, n+1 AS d FROM T")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("d")).isEqualTo(2);

        final JSONObject json = row.toJSON();
        assertThat(json.getInt("d")).as("the computed alias must win over the stored column of the same name")
            .isEqualTo(2);
      }
    });
  }

  @Test
  void starMinusAnExcludedColumnIsNotLeakedBackIn() throws Exception {
    TestHelper.executeInNewDatabase("issue7773Exclude", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1, secret = 'hunter2'");

      try (final ResultSet rs = db.query("sql", "SELECT *, !secret FROM T")) {
        final Result row = rs.next();
        assertThat(row.getPropertyNames()).doesNotContain("secret");

        final JSONObject json = row.toJSON();
        assertThat(json.has("secret")).as("excluded column must not leak back into toJSON()").isFalse();
        assertThat(json.getInt("n")).isEqualTo(1);
      }
    });
  }

  @Test
  void plainSelectStarIsUnchangedAndStillCarriesRidAndType() throws Exception {
    TestHelper.executeInNewDatabase("issue7773PlainStar", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1");

      try (final ResultSet rs = db.query("sql", "SELECT * FROM T")) {
        final Result row = rs.next();
        final JSONObject json = row.toJSON();
        assertThat(json.getInt("n")).isEqualTo(1);
        assertThat(json.has("@rid")).isTrue();
        assertThat(json.has("@type")).isTrue();
      }
    });
  }

  @Test
  void asJSONSqlMethodAgreesWithResultToJSON() throws Exception {
    TestHelper.executeInNewDatabase("issue7773AsJson", (db) -> {
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET n = 1");

      try (final ResultSet rs = db.query("sql",
          "SELECT $a.asJSON() AS j FROM T LET $a = (SELECT *, n+1 AS z FROM T)")) {
        final Object j = rs.next().getProperty("j");
        // asJSON() on a Result dispatches through Result.toJSON() (SQLMethodAsJSON), so it must now include z too.
        assertThat(j.toString()).contains("\"z\"");
      }
    });
  }
}
