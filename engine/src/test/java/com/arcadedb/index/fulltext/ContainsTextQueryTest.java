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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CONTAINSTEXT} over FULL_TEXT indexes: literals carrying a colon, multi-property indexes, the same property constrained more
 * than once, and the shared corpus scan of a BM25 conjunction (issues #6382, #6414, #6427, #6436).
 */
class ContainsTextQueryTest extends TestHelper {
  // ===== Issue #6382 =====
  // {@code CONTAINSTEXT} is literal text to find, not a query language, but the direct full-text lookup split every
  // whitespace-separated part on the first {@code ':'} and looked the remainder up as a field-qualified key. A
  // single-property index never stores a qualified key, so any literal carrying a colon - a time, a timestamp, a
  // {@code ns:name} - matched nothing at all (issue #6382).

  @Test
  @DisplayName("#6382 single property index matches aliteral containing acolon")
  void singlePropertyIndexMatchesALiteralContainingAColon() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Note6382");
      database.command("sql", "CREATE PROPERTY Note6382.label STRING");
      database.command("sql", "CREATE INDEX ON Note6382 (label) FULL_TEXT");

      database.command("sql", "INSERT INTO Note6382 SET id = 'a', label = 'foo:bar happens here'");
      database.command("sql", "INSERT INTO Note6382 SET id = 'b', label = 'unrelated text'");
      database.command("sql", "INSERT INTO Note6382 SET id = 'c', label = 'meeting at 3:30 today'");
    });

    database.transaction(() -> {
      assertThat(noteIdsContaining("foo:bar")).containsExactly("a");
      assertThat(noteIdsContaining("3:30")).containsExactly("c");
    });
  }

  /**
   * The Lucene-backed executor treats the sole indexed property named as a qualifier as no qualifier at all
   * ({@code FullTextQueryExecutor.isUnqualified}); the direct path has to agree.
   */
  @Test
  @DisplayName("#6382 single property index accepts its own property as aqualifier")
  void singlePropertyIndexAcceptsItsOwnPropertyAsAQualifier() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Qualified6382");
      database.command("sql", "CREATE PROPERTY Qualified6382.label STRING");
      database.command("sql", "CREATE INDEX ON Qualified6382 (label) FULL_TEXT");

      database.command("sql", "INSERT INTO Qualified6382 SET id = 'a', label = 'java programming'");
      database.command("sql", "INSERT INTO Qualified6382 SET id = 'b', label = 'python scripting'");
    });

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT id FROM Qualified6382 WHERE label CONTAINSTEXT 'label:java'")) {
        assertThat(collect(rs)).containsExactly("a");
      }
    });
  }

  /**
   * A real field qualifier on a multi-property index keeps working, and a prefix that is NOT an indexed property is
   * literal text: it matches the document that really carries it, not nothing at all. Exercised through the index
   * lookup directly: it is the raw qualifier handling that is under test here, below the positional per-property key
   * the planner now builds for a multi-property index (issue #6414).
   */
  @Test
  @DisplayName("#6382 multi property index keeps field qualifiers and treats unknown ones as literals")
  void multiPropertyIndexKeepsFieldQualifiersAndTreatsUnknownOnesAsLiterals() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article6382");
      database.command("sql", "CREATE PROPERTY Article6382.title STRING");
      database.command("sql", "CREATE PROPERTY Article6382.content STRING");
      database.command("sql", "CREATE INDEX ON Article6382 (title, content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article6382 SET id = 'a', title = 'java', content = 'databases'");
      database.command("sql", "INSERT INTO Article6382 SET id = 'b', title = 'python', content = 'java bindings'");
      database.command("sql", "INSERT INTO Article6382 SET id = 'c', title = 'release notes', content = 'see jira:1234 for details'");
    });

    database.transaction(() -> {
      // A real qualifier still narrows to the property it names.
      assertThat(idsFromIndex("Article6382", "title:java")).containsExactly("a");
      assertThat(idsFromIndex("Article6382", "content:java")).containsExactly("b");
      // 'jira' is not an indexed property, so the whole part stays literal text and finds the document that
      // actually contains it. Before the fix it was looked up as a qualified key nothing ever stored.
      assertThat(idsFromIndex("Article6382", "jira:1234")).containsExactly("c");
    });
  }

  private Set<String> idsFromIndex(final String typeName, final String literal) {
    final Set<String> ids = new HashSet<>();
    for (final Index index : database.getSchema().getType(typeName).getAllIndexes(true)) {
      final IndexCursor cursor = ((RangeIndex) index).get(new Object[] { literal });
      while (cursor.hasNext())
        ids.add(cursor.next().asDocument().getString("id"));
    }
    return ids;
  }

  private Set<String> noteIdsContaining(final String literal) {
    try (final ResultSet rs = database.query("sql", "SELECT id FROM Note6382 WHERE label CONTAINSTEXT ?", literal)) {
      return collect(rs);
    }
  }

  private static Set<String> collect(final ResultSet rs) {
    final Set<String> ids = new HashSet<>();
    while (rs.hasNext())
      ids.add(rs.next().getProperty("id"));
    return ids;
  }

  // ===== Issue #6414 =====
  // A {@code CONTAINSTEXT} on one property of a MULTI-property full-text index was not pushed down onto the index, and the
  // scan it fell back to evaluates the operator as {@code String.contains}: the same operator over the same data answered
  // differently depending only on how the index happened to be declared. Pushed down on BOTH properties it was worse than
  // that - the lookup read the first condition and the planner dropped the second from the filter as well, so a query
  // whose second condition matched nothing returned every row (issue #6414, item 2).

  @Test
  @DisplayName("#6414 a condition on one property of amulti property index uses the index")
  void aConditionOnOnePropertyOfAMultiPropertyIndexUsesTheIndex() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

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
  @DisplayName("#6414 multi and single property indexes answer the same question the same way")
  void multiAndSinglePropertyIndexesAnswerTheSameQuestionTheSameWay() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
    createArticles6414("CREATE INDEX ON Single6414 (title) FULL_TEXT", "Single6414");

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
  @DisplayName("#6414 a condition is restricted to the property it names")
  void aConditionIsRestrictedToThePropertyItNames() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

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
  @DisplayName("#6414 two conditions on the same index are aconjunction")
  void twoConditionsOnTheSameIndexAreAConjunction() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

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
  @DisplayName("#6414 conditions the index does not serve still filter")
  void conditionsTheIndexDoesNotServeStillFilter() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");

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
  @DisplayName("#6414 a null valued condition matches nothing rather than being dropped")
  void aNullValuedConditionMatchesNothingRatherThanBeingDropped() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
    createArticles6414("CREATE INDEX ON Single6414 (title) FULL_TEXT", "Single6414");

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
  @DisplayName("#6414 a second condition on the same property is also answered by the index")
  void aSecondConditionOnTheSamePropertyIsAlsoAnsweredByTheIndex() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT");
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
  @DisplayName("#6414 the same holds for a BM25 index")
  void theSameHoldsForABM25Index() {
    createArticles6414("CREATE INDEX ON Article6414 (title, content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

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
  @DisplayName("#6414 a nested by item property is searched by the name the query writes")
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

  private void createArticles6414(final String indexCommand) {
    createArticles6414(indexCommand, "Article6414");
  }

  private void createArticles6414(final String indexCommand, final String typeName) {
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

  // ===== Issue #6427 =====
  // Two {@code CONTAINSTEXT} conditions on the SAME property used to reach the full-text index only once: the
  // planner claimed at most one condition per indexed property, so the second stayed in the residual filter and
  // was evaluated by {@link com.arcadedb.query.sql.parser.ContainsTextCondition#evaluate} - a case-sensitive
  // {@code String.contains} - instead of the index's analyzer-token, case-insensitive matching. Same operator,
  // same query, two different meanings decided only by which condition happened to be claimed first (issue #6427).

  @Test
  @DisplayName("#6427 two conditions on asingle property index are both pushed to the index")
  void twoConditionsOnASinglePropertyIndexAreBothPushedToTheIndex() {
    final String type = "Article6427";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

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
  @DisplayName("#6427 three conditions on the same property are all intersected")
  void threeConditionsOnTheSamePropertyAreAllIntersected() {
    final String type = "Article6427c";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

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
  @DisplayName("#6427 two conditions on the same property of amulti property index are both pushed to the index")
  void twoConditionsOnTheSamePropertyOfAMultiPropertyIndexAreBothPushedToTheIndex() {
    final String type = "Article6427m";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title, content) FULL_TEXT");

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
  @DisplayName("#6427 a repeated condition on one property combines with acondition on another property")
  void aRepeatedConditionOnOnePropertyCombinesWithAConditionOnAnotherProperty() {
    final String type = "Article6427m2";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title, content) FULL_TEXT");

    database.transaction(() -> assertThat(idsMatching("SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' "
        + "AND title CONTAINSTEXT 'concurrency' AND content CONTAINSTEXT 'threads'")).containsExactly("e"));
  }

  @Test
  @DisplayName("#6427 a non fulltext condition alongside arepeated condition still filters")
  void aNonFulltextConditionAlongsideARepeatedConditionStillFilters() {
    final String type = "Article6427nf";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

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
  @DisplayName("#6427 a null valued repeated condition on the same property matches nothing")
  void aNullValuedRepeatedConditionOnTheSamePropertyMatchesNothing() {
    final String type = "Article6427null";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT");

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
  @DisplayName("#6427 the same holds for a BM25 index with repeated conditions")
  void theSameHoldsForABM25IndexWithRepeatedConditions() {
    final String type = "Article6427bm";
    createArticles6427(type, "CREATE INDEX ON " + type + " (title) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'CONCURRENCY'"))
          .containsExactly("e");
      assertThat(idsMatching(
          "SELECT id FROM " + type + " WHERE title CONTAINSTEXT 'java' AND title CONTAINSTEXT 'zzz'")).isEmpty();
    });
  }

  private void createArticles6427(final String typeName, final String indexCommand) {
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

  // ===== Issue #6436 =====
  // A BM25 CONTAINSTEXT conjunction over several properties of a multi-property full-text index answers each constrained
  // property as one type-wide scoring pass, intersected. Before the fix, EVERY one of those passes independently scanned
  // document frequencies for the whole corpus - the same, corpus-wide statistics recomputed once per constrained property
  // instead of once for the whole query (issue #6436). This pins two things: the document-frequency scan now runs exactly
  // once per query regardless of how many properties are constrained, and the scored/intersected results are unchanged.

  @Test
  @DisplayName("#6436 a three property conjunction scans document frequencies once")
  void aThreePropertyConjunctionScansDocumentFrequenciesOnce() {
    createArticles6436();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'backend'"))
          .containsExactly("a");

      // One shared corpus scan for the whole conjunction, not one per constrained property.
      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  @Test
  @DisplayName("#6436 a two property conjunction scans document frequencies once")
  void aTwoPropertyConjunctionScansDocumentFrequenciesOnce() {
    createArticles6436();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching("SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm'"))
          .containsExactlyInAnyOrder("a", "c");

      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  @Test
  @DisplayName("#6436 a single property condition still scans document frequencies once")
  void aSinglePropertyConditionStillScansDocumentFrequenciesOnce() {
    createArticles6436();

    database.transaction(() -> {
      FullTextSearch.resetDocumentFrequencyScanInvocationsForTesting();

      assertThat(idsMatching("SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java'"))
          .containsExactlyInAnyOrder("a", "c");

      assertThat(FullTextSearch.getDocumentFrequencyScanInvocationsForTesting()).isEqualTo(1);
    });
  }

  /**
   * The scan-sharing refactor must not change what a conjunction answers: same intersected, scored result as the
   * per-property path it replaces.
   */
  @Test
  @DisplayName("#6436 the conjunction result is unaffected by how many properties are constrained")
  void theConjunctionResultIsUnaffectedByHowManyPropertiesAreConstrained() {
    createArticles6436();

    database.transaction(() -> {
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'backend'"))
          .containsExactly("a");
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'java' AND content CONTAINSTEXT 'jvm' "
              + "AND tags CONTAINSTEXT 'zzz'"))
          .isEmpty();
      assertThat(idsMatching(
          "SELECT id FROM Article6436 WHERE title CONTAINSTEXT 'python' AND content CONTAINSTEXT 'scripting'"))
          .containsExactly("b");
    });
  }

  private void createArticles6436() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article6436");
      database.command("sql", "CREATE PROPERTY Article6436.title STRING");
      database.command("sql", "CREATE PROPERTY Article6436.content STRING");
      database.command("sql", "CREATE PROPERTY Article6436.tags STRING");
      database.command("sql",
          "CREATE INDEX ON Article6436 (title, content, tags) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");

      database.command("sql",
          "INSERT INTO Article6436 SET id = 'a', title = 'java concurrency', content = 'jvm internals', "
              + "tags = 'backend performance'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'b', title = 'python basics', content = 'scripting tips', "
              + "tags = 'frontend tooling'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'c', title = 'JAVA rocks', content = 'jvm tuning', tags = 'frontend'");
      database.command("sql",
          "INSERT INTO Article6436 SET id = 'd', title = 'cooking notes', content = 'pasta recipes', tags = 'food'");
    });
  }
}
