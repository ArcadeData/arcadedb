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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two {@code CONTAINSTEXT} conditions on the SAME property used to reach the full-text index only once: the
 * planner claimed at most one condition per indexed property, so the second stayed in the residual filter and
 * was evaluated by {@link com.arcadedb.query.sql.parser.ContainsTextCondition#evaluate} - a case-sensitive
 * {@code String.contains} - instead of the index's analyzer-token, case-insensitive matching. Same operator,
 * same query, two different meanings decided only by which condition happened to be claimed first (issue #6427).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6427ContainsTextSamePropertyTwiceTest extends TestHelper {

  @Test
  void twoConditionsOnASinglePropertyIndexAreBothPushedToTheIndex() {
    final String type = "Article6427";
    createArticles(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

    database.transaction(() -> {
      assertThat(explain("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'concurrency'"))
          .contains("FETCH FROM INDEX " + type + "[title]");

      // The index is analyzer-token and case-insensitive: a residual String.contains would answer this empty,
      // because the stored word is 'concurrency' and the row filter comparison used to be case-sensitive.
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'CONCURRENCY'"))
          .containsExactly("e");

      // Both conditions still have to hold: a second condition matching nothing empties the result.
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'zzz'")).isEmpty();

      // Sanity: the two conditions really are ANDed together, not just the first one answered.
      assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java'"))
          .containsExactlyInAnyOrder("a", "c", "e");
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'concurrency'"))
          .containsExactly("e");
    });
  }

  @Test
  void threeConditionsOnTheSamePropertyAreAllIntersected() {
    final String type = "Article6427c";
    createArticles(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

    database.transaction(() -> {
      assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT "
          + "'concurrency' AND title CONTAINSTEXT 'threads'")).isEmpty();

      database.command("sql",
          "INSERT INTO " + type + " SET id = 'f', title = 'java concurrency threads', content = 'more'");
    });

    database.transaction(() -> assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' "
        + "AND title CONTAINSTEXT 'concurrency' AND title CONTAINSTEXT 'threads'")).containsExactly("f"));
  }

  @Test
  void twoConditionsOnTheSamePropertyOfAMultiPropertyIndexAreBothPushedToTheIndex() {
    final String type = "Article6427m";
    createArticles(type, "CREATE INDEX ON " + type + " (title, content) FULL_TEXT");

    database.transaction(() -> {
      assertThat(explain(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'concurrency'"))
          .contains("FETCH FROM INDEX " + type + "[title,content]");

      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'CONCURRENCY'"))
          .containsExactly("e");
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'zzz'")).isEmpty();

      // A word that only lives in 'content' must not satisfy a second condition on 'title', even though the
      // multi-property index also stores it unqualified.
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'databases'"))
          .isEmpty();
    });
  }

  @Test
  void aRepeatedConditionOnOnePropertyCombinesWithAConditionOnAnotherProperty() {
    final String type = "Article6427m2";
    createArticles(type, "CREATE INDEX ON " + type + " (title, content) FULL_TEXT");

    database.transaction(() -> assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' "
        + "AND title CONTAINSTEXT 'concurrency' AND content CONTAINSTEXT 'threads'")).containsExactly("e"));
  }

  @Test
  void aNonFulltextConditionAlongsideARepeatedConditionStillFilters() {
    final String type = "Article6427nf";
    createArticles(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

    database.transaction(() -> {
      assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' "
          + "AND title CONTAINSTEXT 'concurrency' AND id = 'e'")).containsExactly("e");
      assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' "
          + "AND title CONTAINSTEXT 'concurrency' AND id = 'a'")).isEmpty();
    });
  }

  /**
   * A {@code null}-valued condition makes the whole block unsatisfiable (issue #6414) even when it is the SECOND
   * condition on a property whose slot already accumulated a value from the first. The null check in
   * {@code FetchFromIndexStep.processFullTextBlock} runs before the slot-accumulation logic, so a null cannot be
   * silently folded into (or lost behind) an already-partial {@code List} slot.
   */
  @Test
  void aNullValuedRepeatedConditionOnTheSamePropertyMatchesNothing() {
    final String type = "Article6427null";
    createArticles(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

    final Map<String, Object> noValue = new HashMap<>();
    noValue.put("missing", null);

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT :missing", noValue))
          .isEmpty();
      // Order does not matter: the null-valued condition short-circuits whether it is claimed first or second.
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT :missing AND title CONTAINSTEXT 'java'", noValue))
          .isEmpty();
      // Sanity: without the null-valued condition, the same two-condition-on-one-property query still matches.
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'concurrency'"))
          .containsExactly("e");
    });
  }

  @Test
  void theSameHoldsForABM25Index() {
    final String type = "Article6427bm";
    createArticles(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'CONCURRENCY'"))
          .containsExactly("e");
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'zzz'")).isEmpty();
    });
  }

  private void createArticles(final String typeName, final String indexCommand) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
      database.command("sql", "CREATE PROPERTY " + typeName + ".title STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".content STRING");
      database.command("sql", indexCommand);

      database.command("sql", "INSERT INTO " + typeName + " SET id = 'a', title = 'java', content = 'databases'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'b', title = 'python', content = 'java bindings'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'c', title = 'JAVA rocks', content = 'other'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'd', title = 'cooking notes', content = 'pasta'");
      database.command("sql",
          "INSERT INTO " + typeName + " SET id = 'e', title = 'java concurrency', content = 'threads'");
    });
  }

  private Set<String> idsMatching(final String query) {
    return idsMatching(query, Map.of());
  }

  private Set<String> idsMatching(final String query, final Map<String, Object> parameters) {
    final Set<String> ids = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query, parameters)) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().toJSON().toString();
    }
  }
}
