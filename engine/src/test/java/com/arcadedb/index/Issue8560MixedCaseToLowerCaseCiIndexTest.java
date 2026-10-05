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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code field.toLowerCase() <op> X} on a {@code COLLATE ci} index must answer what the same predicate answers without
 * the index, also when X is not lower case: the index lower-cases X behind the user's back, so the condition has to be
 * re-checked on what it returns (issue #8560).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8560MixedCaseToLowerCaseCiIndexTest extends TestHelper {

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

  @Test
  void lowerCaseLiteralsStillUseTheIndex() {
    for (final String query : List.of("SELECT name FROM P WHERE name.toLowerCase() = 'john'",
        "SELECT name FROM P WHERE name.toLowerCase() IN ['john','mary']",
        "SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'a' AND 'c'",
        "SELECT name FROM P WHERE name.toLowerCase() >= 'a'"))
      assertThat(database.query("sql", "EXPLAIN " + query).getExecutionPlan().get().prettyPrint(0, 3)).as(query).contains("FETCH FROM INDEX");
  }

  @Test
  void anotherPropertySpellingIsNotBoundToTheIndex() {
    // Property names are case sensitive: NAME is not the indexed property name
    assertThat(names("SELECT name FROM P WHERE NAME.toLowerCase() = 'john'")).isEmpty();
  }

  @Test
  void unsatisfiableEqualityReturnsNothing() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'JOHN'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = :n", Map.of("n", "JOHN"))).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'john'")).containsExactly("John");
  }

  @Test
  void unsatisfiableInReturnsNothing() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['JOHN','MARY']")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['john','mary']")).containsExactlyInAnyOrder("John", "MARY");
  }

  @Test
  void betweenBoundsAreNotLowerCased() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'A' AND 'C'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() BETWEEN 'a' AND 'c'")).containsExactlyInAnyOrder("anne", "Bob");
  }

  @Test
  void mixedCaseRangeBoundKeepsEveryMatchingRecord() {
    // "anne" and "Bob" lower-case above 'C' (97 > 67): the index must not narrow the bound to 'c' and lose them
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'C'")).containsExactlyInAnyOrder("John", "MARY", "anne", "Bob");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() < 'C'")).isEmpty();
  }

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

  @Test
  void parameterizedRangeAnswersWhatTheScanAnswers() {
    // A bound parameter cannot be judged at plan time, so the range is evaluated without the index: same rows either way
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= :p", Map.of("p", "C"))).containsExactlyInAnyOrder("John", "MARY", "anne", "Bob");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= :p", Map.of("p", "c"))).containsExactlyInAnyOrder("John", "MARY");
  }

  @Test
  void rangeOperatorsAgreeWithTheSubqueryForm() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'A' AND name.toLowerCase() < 'C'")).isEmpty();
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() < 'c'")).containsExactlyInAnyOrder("anne", "Bob");
  }
}
