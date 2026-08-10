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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for #6033: {@code BETWEEN} on a case-insensitive ({@code COLLATE CI}) indexed column already
 * uses the index for the bare {@code field BETWEEN a AND b} shape (fixed by #6018), but the equally common
 * {@code field.toLowerCase() BETWEEN a AND b} shape - the CI-index analogue of the {@code field.toLowerCase() =
 * 'value'} pattern that {@link com.arcadedb.query.sql.parser.BinaryCondition} already recognizes - fell through to
 * a full bucket scan because {@code BetweenCondition.isIndexAware()} had no equivalent branch.
 *
 * <p>Note: ArcadeDB SQL has no bare {@code LOWER(field)} function (only the {@code field.toLowerCase()} method
 * call) - the issue's repro uses that syntax, which does not parse. The underlying planner defect is real and is
 * reproduced here with the syntax ArcadeDB actually supports.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6033BetweenToLowerCaseCiIndexTest {
  private static final String DB_PATH = "target/databases/Issue6033BetweenToLowerCaseCiIndexTest";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private String plan(final String query, final Object... params) {
    return database.query("sql", query, params).getExecutionPlan().get().prettyPrint(0, 3);
  }

  @Test
  void betweenOnLowerCaseWrappedCiIndexedColumnUsesTheIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");

      database.command("sql", "INSERT INTO Product SET name = 'Apple'");
      database.command("sql", "INSERT INTO Product SET name = 'BANANA'");
      database.command("sql", "INSERT INTO Product SET name = 'cherry'");
      database.command("sql", "INSERT INTO Product SET name = 'Watermelon'");
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
      assertThat(planString).contains("FETCH FROM INDEX");
      assertThat(planString).doesNotContain("SCAN WITH FILTER");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      // case-insensitively, "apple" and "banana" fall in ['a'..'c'], "cherry" and "watermelon" do not
      assertThat(names).containsExactlyInAnyOrder("Apple", "BANANA");
    });
  }

  @Test
  void betweenOnLowerCaseWrappedCiIndexedColumnMatchesNonIndexedEquivalent() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE PROPERTY Product.code STRING"); // not indexed, used as the control

      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");

      database.command("sql", "INSERT INTO Product SET name = 'Apple', code = 'Apple'");
      database.command("sql", "INSERT INTO Product SET name = 'BANANA', code = 'BANANA'");
      database.command("sql", "INSERT INTO Product SET name = 'cherry', code = 'cherry'");
      database.command("sql", "INSERT INTO Product SET name = 'Watermelon', code = 'Watermelon'");
    });

    database.transaction(() -> {
      final List<String> indexed = new ArrayList<>();
      final ResultSet rsIndexed = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
      while (rsIndexed.hasNext())
        indexed.add(rsIndexed.next().getProperty("name"));

      final List<String> nonIndexed = new ArrayList<>();
      final ResultSet rsNonIndexed = database.query("sql", "SELECT code AS name FROM Product WHERE code.toLowerCase() BETWEEN 'a' AND 'c'");
      while (rsNonIndexed.hasNext())
        nonIndexed.add(rsNonIndexed.next().getProperty("name"));

      indexed.sort(String::compareTo);
      nonIndexed.sort(String::compareTo);
      assertThat(indexed).isEqualTo(nonIndexed);
    });
  }

  @Test
  void betweenOnLowerCaseWrappedFieldWithoutCiIndexFallsBackToScan() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      // plain index, no COLLATE CI: the CI-lowercase branch must not kick in
      database.command("sql", "CREATE INDEX ON Product (name) NOTUNIQUE");

      database.command("sql", "INSERT INTO Product SET name = 'Apple'");
      database.command("sql", "INSERT INTO Product SET name = 'BANANA'");
      database.command("sql", "INSERT INTO Product SET name = 'cherry'");
    });

    database.transaction(() -> {
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
      assertThat(planString).contains("SCAN WITH FILTER");
      assertThat(planString).doesNotContain("FETCH FROM INDEX");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() BETWEEN 'a' AND 'c'");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      assertThat(names).containsExactlyInAnyOrder("Apple", "BANANA");
    });
  }

  @Test
  void betweenOnChainedModifierAfterToLowerCaseOnCiIndexFallsBackToScan() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");

      database.command("sql", "INSERT INTO Product SET name = ' Apple '");
      database.command("sql", "INSERT INTO Product SET name = ' BANANA '");
      database.command("sql", "INSERT INTO Product SET name = ' cherry '");
    });

    database.transaction(() -> {
      // an extra modifier after toLowerCase() breaks the "field.toLowerCase()" pattern isFieldWithLowerCaseMethod()
      // matches, so this must still fall back to a full scan rather than (incorrectly) using the CI index
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase().trim() BETWEEN 'a' AND 'c'");
      assertThat(planString).contains("SCAN WITH FILTER");
      assertThat(planString).doesNotContain("FETCH FROM INDEX");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase().trim() BETWEEN 'a' AND 'c'");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      assertThat(names).containsExactlyInAnyOrder(" Apple ", " BANANA ");
    });
  }

  @Test
  void plainLowerFunctionSyntaxFromTheIssueIsNotSupportedByArcadeDbSql() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) NOTUNIQUE");
      database.command("sql", "INSERT INTO Product SET name = 'Apple'");
    });

    database.transaction(() -> assertThat(
        catchThrowable(() -> database.query("sql", "SELECT * FROM Product WHERE LOWER(name) BETWEEN 'a' AND 'c'").hasNext()))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Unknown function name 'LOWER'"));
  }
}
