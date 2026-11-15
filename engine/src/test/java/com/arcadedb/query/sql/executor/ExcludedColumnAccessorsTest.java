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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11628: a row of {@code SELECT *, !name} keeps the backing record, and every accessor of the
 * row that can fall through to that record must still answer that the excluded column is not there, so the accessors
 * agree with {@link Result#getPropertyNames()}.
 */
class ExcludedColumnAccessorsTest extends TestHelper {

  private static final Object ABSENT = new Object();

  @BeforeEach
  void createData() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Target");
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.secret STRING");
      database.command("sql", "CREATE PROPERTY P.link LINK");
      database.command("sql", "INSERT INTO Target SET v = 1");
      database.command("sql", "INSERT INTO P SET name = 'a', secret = 's', link = (SELECT FROM Target)");
    });
  }

  private void withRow(final String query, final Consumer<Result> check) {
    try (final ResultSet rs = database.query("sql", query)) {
      check.accept(rs.next());
    }
  }

  @Test
  void hasPropertyAnswersFalseForAnExcludedColumn() {
    withRow("SELECT *, !secret FROM P", row -> {
      assertThat(row.getPropertyNames()).doesNotContain("secret");
      assertThat(row.hasProperty("secret")).isFalse();
      assertThat(row.hasProperty("name")).isTrue();
      assertThat(row.<Object>getProperty("secret")).isNull();
    });
  }

  @Test
  void hasPropertyAnswersFalseForAnExcludedRecordAttribute() {
    withRow("SELECT *, !@rid FROM P", row -> {
      assertThat(row.getPropertyNames()).doesNotContain("@rid");
      assertThat(row.hasProperty("@rid")).isFalse();
    });
  }

  @Test
  void elementBackedGettersHonourTheExclusion() {
    withRow("SELECT *, !secret, !link FROM P", row -> {
      assertThat(row.<Object>getProperty("secret", "default")).isEqualTo("default");
      assertThat(row.getPropertyIfPresent("secret", ABSENT)).isSameAs(ABSENT);
      assertThat(row.getElementProperty("link")).isNull();
      assertThat(row.getPropertyType("secret")).isNull();
      // the columns the projection kept still answer
      assertThat(row.<Object>getProperty("name", "default")).isEqualTo("a");
      assertThat(row.getPropertyIfPresent("name", ABSENT)).isEqualTo("a");
    });

    // control: without the exclusion the same accessors reach the record
    withRow("SELECT * FROM P", row -> {
      assertThat(row.getElementProperty("link")).isNotNull();
      assertThat(row.getPropertyType("secret")).isEqualTo(Type.STRING);
      assertThat(row.hasProperty("secret")).isTrue();
    });
  }

  @Test
  void mapsLeaveTheExcludedColumnOut() {
    withRow("SELECT *, !secret FROM P", row -> {
      assertThat(row.toMap()).doesNotContainKey("secret").containsEntry("name", "a");
      assertThat(row.toPropertyMap(false)).doesNotContainKey("secret").containsEntry("name", "a");
    });
  }

  /**
   * Every column and attribute excluded leaves the row's own store empty, so the accessors that read the record whenever
   * the store is empty reached it: the excluded values came straight back.
   */
  @Test
  void anEmptyProjectionDoesNotFallBackToTheRecord() {
    withRow("SELECT *, !name, !secret, !link, !@rid, !@type FROM P", row -> {
      assertThat(row.getPropertyNames()).isEmpty();
      assertThat(row.<Object>getProperty("secret")).isNull();
      assertThat(row.<Object>getProperty("name")).isNull();
      assertThat(row.hasProperty("name")).isFalse();
      assertThat(row.getPropertyIfPresent("name", ABSENT)).isSameAs(ABSENT);
      assertThat(row.toMap()).doesNotContainKeys("name", "secret", "link");
      assertThat(row.toPropertyMap(false)).isEmpty();
      assertThat(row.toJSON().has("secret")).isFalse();
    });
  }

  /**
   * The same empty row, under DISTINCT: it is a reshaped row, so two records project to the same (empty) row and
   * collapse, instead of being told apart by the identity the projection dropped.
   */
  @Test
  void distinctComparesAnEmptyProjectionByValue() {
    database.transaction(() -> database.command("sql", "INSERT INTO P SET name = 'b', secret = 't'"));
    try (final ResultSet rs = database.query("sql", "SELECT DISTINCT *, !name, !secret, !link, !@rid, !@type FROM P")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void anOuterQueryDoesNotSeeTheExcludedColumnAsDefined() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM (SELECT *, !secret FROM P) WHERE secret IS DEFINED")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM (SELECT *, !secret FROM P) WHERE name IS DEFINED")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
  }

  /**
   * A projection over a {@code SELECT *, !secret} row keeps the same full record as its element, so the exclusion has to
   * travel with it, or every element-backed accessor answers the excluded column (and attribute) again.
   */
  @Test
  void anOuterProjectionKeepsTheInnerExclusion() {
    try (final ResultSet rs = database.query("sql", "SELECT *, 1 AS one FROM (SELECT *, !secret, !@rid FROM P)")) {
      final Result row = rs.next();
      assertThat(row.<Integer>getProperty("one")).isEqualTo(1);
      assertThat(row.<String>getProperty("name")).isEqualTo("a");
      assertThat(row.getPropertyNames()).doesNotContain("secret", "@rid");
      assertThat(row.hasProperty("secret")).isFalse();
      assertThat(row.<Object>getProperty("secret", ABSENT)).isSameAs(ABSENT);
      assertThat(row.getPropertyIfPresent("secret", ABSENT)).isSameAs(ABSENT);
      assertThat(row.toMap()).doesNotContainKeys("secret", "@rid");
      assertThat(row.toJSON().has("secret")).isFalse();
      assertThat(row.toJSON().has("@rid")).isFalse();
    }
    try (final ResultSet rs = database.query("sql", "SELECT *, !name FROM (SELECT *, !secret FROM P)")) {
      final Result row = rs.next();
      assertThat(row.hasProperty("secret")).isFalse();
      assertThat(row.hasProperty("name")).isFalse();
      assertThat(row.hasProperty("@rid")).isTrue();
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c FROM (SELECT *, 1 AS one FROM (SELECT *, !secret FROM P)) WHERE secret IS DEFINED")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }
  }
}
