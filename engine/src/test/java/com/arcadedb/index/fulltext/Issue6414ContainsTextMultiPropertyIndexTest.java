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
 * A {@code CONTAINSTEXT} on one property of a MULTI-property full-text index was not pushed down onto the index, and the
 * scan it fell back to evaluates the operator as {@code String.contains}: the same operator over the same data answered
 * differently depending only on how the index happened to be declared. Pushed down on BOTH properties it was worse than
 * that - the lookup read the first condition and the planner dropped the second from the filter as well, so a query
 * whose second condition matched nothing returned every row (issue #6414, item 2).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6414ContainsTextMultiPropertyIndexTest extends TestHelper {

  @Test
  void aConditionOnOnePropertyOfAMultiPropertyIndexUsesTheIndex() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

    database.transaction(() -> {
      assertThat(explain("SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java'"))
          .contains("FETCH FROM INDEX Article6414[title,content]");
      // The second property alone is just as good a key: a full-text key is one query string per property, not a
      // composite key whose used prefix has to start at the first one.
      assertThat(explain("SELECT id FROM Article6414 WHERE content CONTAINSTEXT 'java'"))
          .contains("FETCH FROM INDEX Article6414[title,content]");
    });
  }

  /**
   * The heart of the item: the answer must not depend on the index's arity. Every query below is run against the same
   * rows indexed once as {@code (title, content)} and once as {@code (title)}, and the two must agree.
   */
  @Test
  void multiAndSinglePropertyIndexesAnswerTheSameQuestionTheSameWay() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
    createArticles("CREATE INDEX ON Single6414 (title) FULL_TEXT", "Single6414");

    database.transaction(() -> {
      for (final String literal : new String[] { "java", "JAVA", "ava", "rocks", "databases" }) {
        final Set<String> multi = ids("Article6414", "title", literal);
        final Set<String> single = ids("Single6414", "title", literal);
        assertThat(multi).as("literal '%s'", literal).isEqualTo(single);
      }
    });
  }

  /**
   * The property named in the condition is the property searched: a token that only the OTHER indexed property carries
   * is not a match, even though the index stores it unprefixed as well.
   */
  @Test
  void aConditionIsRestrictedToThePropertyItNames() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

    database.transaction(() -> {
      assertThat(ids("Article6414", "title", "databases")).isEmpty();
      assertThat(ids("Article6414", "content", "databases")).containsExactly("a");
      // Every whitespace-separated word is bound to the named property, not just the first. Qualifying only the head
      // would leave 'databases' matching any property and bring row 'a' back through its content.
      assertThat(ids("Article6414", "title", "zzz databases")).isEmpty();
      assertThat(ids("Article6414", "title", "zzz java")).containsExactlyInAnyOrder("a", "c");
    });
  }

  @Test
  void twoConditionsOnTheSameIndexAreAConjunction() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

    database.transaction(() -> {
      // Before the fix this returned every row in the type: the lookup honoured 'java' and the planner had already
      // removed 'zzz' from the filter it would otherwise have been checked by.
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'zzz'")).isEmpty();
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'databases'"))
          .containsExactly("a");
    });
  }

  /**
   * A condition the index cannot serve stays in the filter that runs over the fetched rows: claiming the
   * {@code CONTAINSTEXT} must not swallow whatever else the {@code AND} carried.
   */
  @Test
  void conditionsTheIndexDoesNotServeStillFilter() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

    database.transaction(() -> {
      assertThat(idsMatching("SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND id = 'c'"))
          .containsExactly("c");
      assertThat(idsMatching("SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND id = 'b'")).isEmpty();
    });
  }

  /**
   * A {@code null} value cannot match anything - {@link com.arcadedb.query.sql.parser.ContainsTextCondition#evaluate}
   * answers false for it off the index, and a single-property index answers nothing. On the positional key the two
   * readings share one slot, since {@code null} is also how the key says "this property is not constrained", so the
   * condition has to fail the whole block rather than quietly disappear from it.
   */
  @Test
  void aNullValuedConditionMatchesNothingRatherThanBeingDropped() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
    createArticles("CREATE INDEX ON Single6414 (title) FULL_TEXT", "Single6414");

    final Map<String, Object> noValue = new HashMap<>();
    noValue.put("missing", null);

    database.transaction(() -> {
      // Was 'a': the unsatisfiable condition landed in the slot that means "unconstrained" and was never checked.
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT :missing", noValue))
          .isEmpty();
      assertThat(idsMatching("SELECT id FROM Article6414 WHERE content CONTAINSTEXT :missing", noValue)).isEmpty();
      // The same question of a single-property index, which always answered nothing, and of a property no index covers.
      assertThat(idsMatching("SELECT id FROM Single6414 WHERE title CONTAINSTEXT :missing", noValue)).isEmpty();
      assertThat(idsMatching("SELECT id FROM Article6414 WHERE id CONTAINSTEXT :missing", noValue)).isEmpty();
    });
  }

  /**
   * Pins what a SECOND condition on an already-claimed property does now that issue #6427 is fixed: the planner claims
   * every {@code CONTAINSTEXT} condition on an index property, not just the first, so both reach the index instead of
   * one being answered by the residual {@code String.contains}.
   */
  @Test
  void aSecondConditionOnTheSamePropertyIsAlsoAnsweredByTheIndex() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
    database.transaction(() -> database.command("sql",
        "INSERT INTO Article6414 SET id = 'e', title = 'java concurrency', content = 'threads'"));

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'concurrency'"))
          .containsExactly("e");
      // A second condition that matches nothing still empties the result.
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'zzz'")).isEmpty();
      // Issue #6427: the index is case-insensitive, so the two conditions now agree about the same word regardless of
      // case - this used to answer empty, when the second condition was still evaluated by the residual, case-sensitive
      // String.contains.
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'CONCURRENCY'"))
          .containsExactly("e");
    });
  }

  /**
   * The BM25 similarity reaches the index through a different scoring path (type-wide corpus statistics), which has to
   * read the same positional key.
   */
  @Test
  void theSameHoldsForABM25Index() {
    createArticles("CREATE INDEX ON Article6414 (title, content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

    database.transaction(() -> {
      assertThat(ids("Article6414", "title", "java")).containsExactlyInAnyOrder("a", "c");
      assertThat(ids("Article6414", "title", "databases")).isEmpty();
      assertThat(ids("Article6414", "content", "databases")).containsExactly("a");
      assertThat(idsMatching(
          "SELECT id FROM Article6414 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'zzz'")).isEmpty();
    });
  }

  /**
   * A property indexed {@code BY ITEM} is named {@code "obj.hd by item"} by the index and {@code obj.hd} by every query
   * that mentions it, and {@link LSMTreeFullTextIndex#put} prefixes its postings with the FORMER. So the per-property key
   * only reaches them if the qualifier written as the base name resolves back to the stored spelling: without that, this
   * shape answered nothing at all, and before the positional key it answered its first condition and dropped the second.
   */
  @Test
  void aNestedByItemPropertyIsSearchedByTheNameTheQueryWrites() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Pair6414");
      database.command("sql", "CREATE PROPERTY Pair6414.hd STRING");
      database.command("sql", "CREATE PROPERTY Pair6414.tl STRING");
      database.command("sql", "CREATE DOCUMENT TYPE Doc6414");
      database.command("sql", "CREATE PROPERTY Doc6414.obj LIST OF Pair6414");
      database.command("sql", "CREATE INDEX combo6414 ON Doc6414 (`obj.hd` BY ITEM, `obj.tl` BY ITEM) FULL_TEXT");

      database.command("sql", "INSERT INTO Doc6414 SET id = 'x', obj = "
          + "[{\"@type\": \"Pair6414\", \"hd\": \"alpha\", \"tl\": \"beta\"}]");
      database.command("sql", "INSERT INTO Doc6414 SET id = 'y', obj = "
          + "[{\"@type\": \"Pair6414\", \"hd\": \"gamma\", \"tl\": \"delta\"}]");
    });

    database.transaction(() -> {
      assertThat(explain("SELECT id FROM Doc6414 WHERE `obj.hd` CONTAINSTEXT 'alpha'")).contains("FETCH FROM INDEX");
      assertThat(idsMatching("SELECT id FROM Doc6414 WHERE `obj.hd` CONTAINSTEXT 'alpha'")).containsExactly("x");
      // The token lives in the OTHER indexed property, so the condition naming this one does not match it.
      assertThat(idsMatching("SELECT id FROM Doc6414 WHERE `obj.hd` CONTAINSTEXT 'beta'")).isEmpty();
      assertThat(idsMatching(
          "SELECT id FROM Doc6414 WHERE `obj.hd` CONTAINSTEXT 'alpha' AND `obj.tl` CONTAINSTEXT 'beta'"))
          .containsExactly("x");
      assertThat(idsMatching(
          "SELECT id FROM Doc6414 WHERE `obj.hd` CONTAINSTEXT 'alpha' AND `obj.tl` CONTAINSTEXT 'zzz'")).isEmpty();
    });
  }

  private void createArticles(final String indexCommand) {
    createArticles(indexCommand, "Article6414");
  }

  private void createArticles(final String indexCommand, final String typeName) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
      database.command("sql", "CREATE PROPERTY " + typeName + ".title STRING");
      database.command("sql", "CREATE PROPERTY " + typeName + ".content STRING");
      database.command("sql", indexCommand);

      database.command("sql", "INSERT INTO " + typeName + " SET id = 'a', title = 'java', content = 'databases'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'b', title = 'python', content = 'java bindings'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'c', title = 'JAVA rocks', content = 'other'");
      database.command("sql", "INSERT INTO " + typeName + " SET id = 'd', title = 'cooking notes', content = 'pasta'");
    });
  }

  private Set<String> ids(final String typeName, final String property, final String literal) {
    return idsMatching("SELECT id FROM " + typeName + " WHERE " + property + " CONTAINSTEXT '" + literal + "'");
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
