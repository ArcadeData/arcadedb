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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Queries on {@code COLLATE CI} indexes: {@code field.toLowerCase()} with BETWEEN / IN / mixed-case operands, and plain
 * ranges, equality and IN that must answer what a scan answers. Consolidated from the per-issue classes for #6033,
 * #6037, #8560 and #8932 (the fixtures of {@link CaseInsensitiveIndexTest} use a Product type that the #6033 and #6037
 * tests create themselves, hence a separate class); each test keeps its issue number in its {@code @DisplayName}.
 */
class CaseInsensitiveIndexQueryTest extends TestHelper {

  @BeforeEach
  void fill() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.name STRING");
      database.command("sql", "CREATE INDEX ON P (name COLLATE ci) NOTUNIQUE");
      for (final String n : List.of("John", "MARY", "anne", "Bob"))
        database.command("sql", "INSERT INTO P SET name = ?", n);
    });
  }

  private String plan(final String query, final Object... params) {
    return database.query("sql", query, params).getExecutionPlan().get().prettyPrint(0, 3);
  }

  private List<String> names(final String query) {
    return names(query, Map.of());
  }

  private List<String> names(final String query, final Map<String, Object> params) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query, params)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.add(r.getProperty("name"));
      }
    }
    return result;
  }

  /*
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

  @DisplayName("#6033 between on lower case wrapped ci indexed column uses the index")
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

  @DisplayName("#6033 between on lower case wrapped ci indexed column matches non indexed equivalent")
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

  @DisplayName("#6033 between on lower case wrapped field without ci index falls back to scan")
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

  @DisplayName("#6033 between on chained modifier after to lower case on ci index falls back to scan")
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

  @DisplayName("#6033 plain lower function syntax from the issue is not supported by arcade db sql")
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

  /*
   * Regression test for #6037: {@code field.toLowerCase() = 'value'} and {@code field.toLowerCase() BETWEEN a AND b}
   * already use a {@code COLLATE CI} index (via {@link com.arcadedb.query.sql.parser.BinaryCondition} and, since
   * #6036, {@link com.arcadedb.query.sql.parser.BetweenCondition}), but the equally common
   * {@code field.toLowerCase() IN [...]} shape fell through to a full bucket scan because
   * {@link com.arcadedb.query.sql.parser.InCondition#isIndexAware} had no equivalent branch.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#6037 in on lower case wrapped ci indexed column uses the index")
  @Test
  void inOnLowerCaseWrappedCiIndexedColumnUsesTheIndex() {
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
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase() IN ['apple', 'banana']");
      assertThat(planString).contains("FETCH FROM INDEX");
      assertThat(planString).doesNotContain("SCAN WITH FILTER");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() IN ['apple', 'banana']");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      assertThat(names).containsExactlyInAnyOrder("Apple", "BANANA");
    });
  }

  @DisplayName("#6037 in on lower case wrapped ci indexed column matches non indexed equivalent")
  @Test
  void inOnLowerCaseWrappedCiIndexedColumnMatchesNonIndexedEquivalent() {
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
      final ResultSet rsIndexed = database.query("sql",
          "SELECT name FROM Product WHERE name.toLowerCase() IN ['apple', 'cherry']");
      while (rsIndexed.hasNext())
        indexed.add(rsIndexed.next().getProperty("name"));

      final List<String> nonIndexed = new ArrayList<>();
      final ResultSet rsNonIndexed = database.query("sql",
          "SELECT code AS name FROM Product WHERE code.toLowerCase() IN ['apple', 'cherry']");
      while (rsNonIndexed.hasNext())
        nonIndexed.add(rsNonIndexed.next().getProperty("name"));

      indexed.sort(String::compareTo);
      nonIndexed.sort(String::compareTo);
      assertThat(indexed).isEqualTo(nonIndexed);
    });
  }

  @DisplayName("#6037 in on lower case wrapped field without ci index falls back to scan")
  @Test
  void inOnLowerCaseWrappedFieldWithoutCiIndexFallsBackToScan() {
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
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase() IN ['apple', 'banana']");
      assertThat(planString).contains("SCAN WITH FILTER");
      assertThat(planString).doesNotContain("FETCH FROM INDEX");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() IN ['apple', 'banana']");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      assertThat(names).containsExactlyInAnyOrder("Apple", "BANANA");
    });
  }

  @DisplayName("#6037 in on chained modifier after to lower case on ci index falls back to scan")
  @Test
  void inOnChainedModifierAfterToLowerCaseOnCiIndexFallsBackToScan() {
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
      final String planString = plan("EXPLAIN SELECT name FROM Product WHERE name.toLowerCase().trim() IN ['apple', 'banana']");
      assertThat(planString).contains("SCAN WITH FILTER");
      assertThat(planString).doesNotContain("FETCH FROM INDEX");

      final List<String> names = new ArrayList<>();
      final ResultSet rs = database.query("sql",
          "SELECT name FROM Product WHERE name.toLowerCase().trim() IN ['apple', 'banana']");
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));

      assertThat(names).containsExactlyInAnyOrder(" Apple ", " BANANA ");
    });
  }

  /*
   * {@code field.toLowerCase() <op> X} on a {@code COLLATE ci} index must answer what the same predicate answers without
   * the index, also when X is not lower case: the index lower-cases X behind the user's back, so the condition has to be
   * re-checked on what it returns (issue #8560).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#8560 lower case literals still use the index")
  @Test
  void lowerCaseLiteralsStillUseTheIndex() {
    for (final String query : List.of("SELECT name FROM P WHERE name.toLowerCase() = 'john'",
        "SELECT name FROM P WHERE name.toLowerCase() IN ['john','mary']",
        "SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'a' AND 'c'",
        "SELECT name FROM P WHERE name.toLowerCase() >= 'a'"))
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3)).as(query).contains("FETCH FROM INDEX");
  }

  @DisplayName("#8560 another property spelling is not bound to the index")
  @Test
  void anotherPropertySpellingIsNotBoundToTheIndex() {
    // Property names are case sensitive: NAME is not the indexed property name
    assertThat(names("SELECT name FROM P WHERE NAME.toLowerCase() = 'john'")).isEmpty();
  }

  @DisplayName("#8560 unsatisfiable equality returns nothing")
  @Test
  void unsatisfiableEqualityReturnsNothing() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'JOHN'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = :n", Map.of("n", "JOHN"))).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'john'")).containsExactly("John");
  }

  @DisplayName("#8560 unsatisfiable in returns nothing")
  @Test
  void unsatisfiableInReturnsNothing() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['JOHN','MARY']")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['john','mary']")).containsExactlyInAnyOrder("John", "MARY");
  }

  @DisplayName("#8560 between bounds are not lower cased")
  @Test
  void betweenBoundsAreNotLowerCased() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'A' AND 'C'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'a' AND 'c'")).containsExactlyInAnyOrder("anne", "Bob");
  }

  @DisplayName("#8560 mixed case range bound keeps every matching record")
  @Test
  void mixedCaseRangeBoundKeepsEveryMatchingRecord() {
    // "anne" and "Bob" lower-case above 'C' (97 > 67): the index must not narrow the bound to 'c' and lose them
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'C'")).containsExactlyInAnyOrder("John", "MARY", "anne", "Bob");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() < 'C'")).isEmpty();
  }

  @DisplayName("#8560 composite ci index range on the second field keeps every matching record")
  @Test
  void compositeCiIndexRangeOnTheSecondFieldKeepsEveryMatchingRecord() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Q");
      database.command("sql", "CREATE PROPERTY Q.k STRING");
      database.command("sql", "CREATE PROPERTY Q.name STRING");
      database.command("sql", "CREATE INDEX ON Q (k, name COLLATE ci) NOTUNIQUE");
      for (final String n : List.of("John", "MARY", "anne", "Bob"))
        database.command("sql", "INSERT INTO Q SET k = 'x', name = ?", n);
    });
    assertThat(names("SELECT name FROM Q WHERE k = 'x' AND name.toLowerCase() >= 'C'")).containsExactlyInAnyOrder("John", "MARY", "anne", "Bob");
    assertThat(names("SELECT name FROM Q WHERE k = 'x' AND name.toLowerCase() >= 'a' AND name.toLowerCase() < 'C'")).isEmpty();
    assertThat(names("SELECT name FROM Q WHERE k = 'x' AND name.toLowerCase() >= 'a' AND name.toLowerCase() < 'c'")).containsExactlyInAnyOrder("anne", "Bob");
  }

  @DisplayName("#8560 parameterized range answers what the scan answers")
  @Test
  void parameterizedRangeAnswersWhatTheScanAnswers() {
    // A bound parameter cannot be judged at plan time, so the range is evaluated without the index: same rows either way
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= :p", Map.of("p", "C"))).containsExactlyInAnyOrder("John", "MARY", "anne", "Bob");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= :p", Map.of("p", "c"))).containsExactlyInAnyOrder("John", "MARY");
  }

  @DisplayName("#8560 range operators agree with the subquery form")
  @Test
  void rangeOperatorsAgreeWithTheSubqueryForm() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'A' AND name.toLowerCase() < 'C'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() < 'c'")).containsExactlyInAnyOrder("anne", "Bob");
  }

  /*
   * A range written on the plain property ({@code name >= 'C'}) must answer the same with a {@code COLLATE ci} index
   * reachable as behind a subquery that hides it: the index lower-cases the bound and its keys, which is not the case
   * sensitive comparison the predicate asks for (issue #8932). Equality and IN keep the documented case-insensitive
   * lookup of the index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#8932 plain ranges agree with the scan")
  @Test
  void plainRangesAgreeWithTheScan() {
    for (final String predicate : List.of("name >= 'C'", "name > 'C'", "name <= 'C'", "name < 'c'", "name >= 'c'", "name BETWEEN 'A' AND 'C'",
        "name BETWEEN 'a' AND 'c'", "name >= 'A' AND name < 'C'", "name >= 'a' AND name < 'k'")) {
      final List<String> indexed = names("SELECT name FROM P WHERE " + predicate);
      final List<String> scanned = names("SELECT name FROM (SELECT FROM P) WHERE " + predicate);
      assertThat(indexed).as(predicate).containsExactlyInAnyOrderElementsOf(scanned);
    }
    assertThat(names("SELECT name FROM P WHERE name >= 'C'")).containsExactlyInAnyOrder("John", "MARY", "anne");
    assertThat(names("SELECT name FROM P WHERE name BETWEEN 'A' AND 'C'")).containsExactly("Bob");
  }

  @DisplayName("#8932 plain equality and in agree with the scan")
  @Test
  void plainEqualityAndInAgreeWithTheScan() {
    // The index folds its keys, but the plain predicate is case sensitive: with or without the index it answers the same (issue #9403)
    for (final String predicate : List.of("name = 'JOHN'", "name = 'John'", "name IN ['JOHN', 'mary']", "name IN ['John', 'MARY', 'zed']")) {
      final List<String> indexed = names("SELECT name FROM P WHERE " + predicate);
      final List<String> scanned = names("SELECT name FROM (SELECT FROM P) WHERE " + predicate);
      assertThat(indexed).as(predicate).containsExactlyInAnyOrderElementsOf(scanned);
    }
    assertThat(names("SELECT name FROM P WHERE name = 'JOHN'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name = 'John'")).containsExactly("John");
    assertThat(names("SELECT name FROM P WHERE name IN ['John', 'MARY', 'zed']")).containsExactlyInAnyOrder("John", "MARY");
  }

  @DisplayName("#8932 to lower case equality and in keep the case insensitive lookup")
  @Test
  void toLowerCaseEqualityAndInKeepTheCaseInsensitiveLookup() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'john'")).containsExactly("John");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['john', 'mary']")).containsExactlyInAnyOrder("John", "MARY");
  }
}
