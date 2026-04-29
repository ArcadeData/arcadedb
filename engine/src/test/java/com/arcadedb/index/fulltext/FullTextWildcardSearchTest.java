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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the bug reported in GitHub issue #4030: "SEARCH_FIELDS" and "SEARCH_INDEX" discard wildcards.
 * Wildcards `*`, `?`, fuzzy `~` and regex `/.../` should yield matching results, not be silently stripped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextWildcardSearchTest extends TestHelper {

  private void seed() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.txt STRING");
      database.command("sql", "CREATE INDEX ON Doc (txt) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET txt = 'Hello World'");
      database.command("sql", "INSERT INTO Doc SET txt = 'Help me'");
      database.command("sql", "INSERT INTO Doc SET txt = 'Heaven and earth'");
    });
  }

  private Set<String> queryAsSet(final String sql) {
    final Set<String> matches = new HashSet<>();
    final ResultSet rs = database.query("sql", sql);
    while (rs.hasNext()) {
      final Result r = rs.next();
      matches.add(r.getProperty("txt").toString());
    }
    return matches;
  }

  @Test
  void searchFieldsSuffixWildcardLowerCase() {
    seed();
    database.transaction(() -> {
      // 'hel*' should match documents that contain a token starting with "hel"
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hel*') = true");

      assertThat(titles).containsExactlyInAnyOrder("Hello World", "Help me");
    });
  }

  @Test
  void searchFieldsSuffixWildcardMixedCase() {
    seed();
    database.transaction(() -> {
      // Wildcard query matching is case-insensitive (analyzer lowercases the index)
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'Hel*') = true");

      assertThat(titles).containsExactlyInAnyOrder("Hello World", "Help me");
    });
  }

  @Test
  void searchFieldsSuffixWildcardNarrowPrefix() {
    seed();
    database.transaction(() -> {
      // 'hell*' should match only "hello" (since "help" doesn't start with "hell")
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hell*') = true");

      assertThat(titles).containsExactly("Hello World");
    });
  }

  @Test
  void searchFieldsSingleCharWildcard() {
    seed();
    database.transaction(() -> {
      // 'hell?' should match a token of exactly 5 characters: "hello", "help" should not match (only 4 chars)
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hell?') = true");

      assertThat(titles).containsExactly("Hello World");
    });
  }

  @Test
  void searchFieldsFuzzyWildcard() {
    seed();
    database.transaction(() -> {
      // Fuzzy: 'helo~' should match "hello" with edit distance <= 2
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'helo~') = true");

      assertThat(titles).contains("Hello World");
    });
  }

  @Test
  void searchIndexSuffixWildcard() {
    seed();
    database.transaction(() -> {
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]','hel*') = true");

      assertThat(titles).containsExactlyInAnyOrder("Hello World", "Help me");
    });
  }

  @Test
  void searchIndexSingleCharWildcard() {
    seed();
    database.transaction(() -> {
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]','hell?') = true");

      assertThat(titles).containsExactly("Hello World");
    });
  }

  @Test
  void searchIndexFuzzyWildcard() {
    seed();
    database.transaction(() -> {
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]','helo~') = true");

      assertThat(titles).contains("Hello World");
    });
  }

  /**
   * Regression test mirroring the exact reproduction in <a href="https://github.com/ArcadeData/arcadedb/issues/4030">#4030</a>:
   * with a single document {@code 'Hello World'}, all wildcard / fuzzy variants of {@code Hell} must match.
   */
  @Test
  void issue4030Repro() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.txt STRING");
      database.command("sql", "CREATE INDEX ON Doc (txt) FULL_TEXT");
      database.command("sql", "INSERT INTO Doc SET txt = 'Hello World'");
    });

    database.transaction(() -> {
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'Hell*') = true")).containsExactly("Hello World");
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'Hell?') = true")).containsExactly("Hello World");
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'Hell~') = true")).containsExactly("Hello World");
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hell*') = true")).containsExactly("Hello World");
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hell?') = true")).containsExactly("Hello World");
      assertThat(queryAsSet("SELECT FROM Doc WHERE SEARCH_FIELDS(['txt'],'hell~') = true")).containsExactly("Hello World");
    });
  }

  @Test
  void searchIndexLeadingWildcardWhenAllowed() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.txt STRING");
      // Allow leading wildcard
      database.command("sql", "CREATE INDEX ON Doc (txt) FULL_TEXT METADATA { 'allowLeadingWildcard': true }");
      database.command("sql", "INSERT INTO Doc SET txt = 'Hello World'");
      database.command("sql", "INSERT INTO Doc SET txt = 'jello good'");
      database.command("sql", "INSERT INTO Doc SET txt = 'Heaven and earth'");
    });

    database.transaction(() -> {
      // '*ello' should match "hello" and "jello"
      final Set<String> titles = queryAsSet(
          "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[txt]','*ello') = true");

      assertThat(titles).containsExactlyInAnyOrder("Hello World", "jello good");
    });
  }
}
