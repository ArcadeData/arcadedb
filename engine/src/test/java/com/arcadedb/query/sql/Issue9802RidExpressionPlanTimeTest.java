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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9802: {@code WHERE @rid = {"@rid": <expr>}} is planned with the statement's own context, so a string or a
 * parameter inside the expression form resolves exactly like the same value written as a literal or inside
 * {@code IN [...]}, instead of failing at plan time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9802RidExpressionPlanTimeTest extends TestHelper {
  private RID first;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D1");
      first = database.newDocument("D1").set("n", 1).save().getIdentity();
      database.newDocument("D1").set("n", 2).save();
    });
  }

  @Test
  void everySpellingOfTheSameRidSelectsTheSameRecord() {
    assertSelectsFirst("SELECT FROM D1 WHERE @rid = " + first);
    assertSelectsFirst("SELECT FROM D1 WHERE @rid = ?", first.toString());
    assertSelectsFirst("SELECT FROM D1 WHERE @rid IN [{\"@rid\": \"" + first + "\"}]");
    assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": \"" + first + "\"}");
    assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": ?}", first.toString());
  }

  @Test
  void aParameterInsideTheExpressionFormIsReResolvedOnEveryExecution() {
    // The plan is cached by statement text: a second execution with another binding must not reuse the first RID
    assertSelectsFirst("SELECT FROM D1 WHERE @rid = {\"@rid\": ?}", first.toString());
    try (final ResultSet rs = database.query("sql", "SELECT FROM D1 WHERE @rid = {\"@rid\": ?}",
        new RID(first.getBucketId(), first.getPosition() + 1).toString())) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Integer) rs.next().getProperty("n")).isEqualTo(2);
    }
  }

  @Test
  void aPositionBeyondIntRangeResolves() {
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM D1 WHERE @rid = {\"@rid\": \"#" + first.getBucketId() + ":3000000000\"}")) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT {\"@rid\": \"#1:3000000000\"} AS r")) {
      assertThat(rs.next().<Object>getProperty("r").toString()).isEqualTo("#1:3000000000");
    }
  }

  @Test
  void aRidTargetOnAnUnknownBucketIsNoMatchUnderABucketFilter() {
    final String bucketName = database.getSchema().getBucketById(first.getBucketId()).getName();
    try (final ResultSet rs = database.query("sql",
        "SELECT FROM bucket:" + bucketName + " WHERE @rid = #" + first.getBucketId() + ":" + first.getPosition())) {
      assertThat(rs.hasNext()).isTrue();
    }
    // A RID target in a bucket the schema does not know: no row, instead of a NullPointerException
    try (final ResultSet rs = database.query("sql", "SELECT FROM [#9999:0] WHERE @rid IS NOT NULL")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void deleteAndUpdateResolveTheExpressionForm() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE D1 SET touched = true WHERE @rid = {\"@rid\": ?}",
          first.toString())) {
        assertThat((Long) rs.next().getProperty("count")).isEqualTo(1L);
      }
    });
    database.transaction(() -> assertThat(first.asDocument().getBoolean("touched")).isTrue());

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM D1 WHERE @rid = {\"@rid\": \"" + first + "\"}")) {
        assertThat((Long) rs.next().getProperty("count")).isEqualTo(1L);
      }
    });
    database.transaction(() -> assertThat(database.countType("D1", false)).isEqualTo(1L));
  }

  private void assertSelectsFirst(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      assertThat(rs.hasNext()).as(sql).isTrue();
      assertThat(rs.next().getIdentity().get()).as(sql).isEqualTo(first);
      assertThat(rs.hasNext()).as(sql).isFalse();
    }
  }
}
