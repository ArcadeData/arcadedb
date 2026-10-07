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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A range written on the plain property ({@code name >= 'C'}) must answer the same with a {@code COLLATE ci} index
 * reachable as behind a subquery that hides it: the index lower-cases the bound and its keys, which is not the case
 * sensitive comparison the predicate asks for (issue #8932). Equality and IN keep the documented case-insensitive
 * lookup of the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8932PlainRangeOnCiIndexTest extends TestHelper {

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
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.add(r.getProperty("name"));
      }
    }
    return result;
  }

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

  @Test
  void toLowerCaseEqualityAndInKeepTheCaseInsensitiveLookup() {
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() = 'john'")).containsExactly("John");
    assertThat(names("SELECT name FROM P WHERE name.toLowerCase() IN ['john', 'mary']")).containsExactlyInAnyOrder("John", "MARY");
  }

  @Test
  void toLowerCaseRangesStillUseTheIndex() {
    assertThat(database.query("sql", "EXPLAIN SELECT name FROM P WHERE name.toLowerCase() >= 'a'").getExecutionPlan().get().prettyPrint(0, 3))
        .contains("FETCH FROM INDEX");
  }
}
