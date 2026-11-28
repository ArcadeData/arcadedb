/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How large a full-text query may be: the groups of a query nest to a bounded depth and it holds a bounded number of clauses, and
 * the parentheses of its text nest to a bounded depth before it is parsed. A query within the bounds answers what it always did, on
 * every search surface; one past them is refused as a query that does not parse.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextQueryNestingTest extends TestHelper {
  private static final int GROUP_LIMIT       = FullTextQueryParsers.MAX_GROUP_DEPTH;
  private static final int PARENTHESES_LIMIT = FullTextQueryParsers.MAX_PARENTHESES_DEPTH;
  private static final int CLAUSE_LIMIT      = FullTextQueryParsers.MAX_CLAUSES;

  private static final String[] WORDS = { "alpha", "beta", "gamma", "delta" };

  /** Every combination of the four words once, as the records 1 to 15 (the bits of k), and a record of its own word as 16. */
  private static final String[] TEXTS = new String[17];

  static {
    for (int k = 1; k < 16; k++) {
      final StringBuilder text = new StringBuilder();
      for (int bit = 0; bit < WORDS.length; bit++)
        if ((k & (1 << bit)) != 0)
          text.append(text.isEmpty() ? "" : " ").append(WORDS[bit]);
      TEXTS[k] = text.toString();
    }
    TEXTS[16] = "omega";
  }

  /** The types searched, each with a full-text index on {@code t}: BM25 over 3 buckets, BM25 over 1 bucket, CLASSIC over 3 buckets. */
  private static final List<String> TYPES = List.of("Multi", "Single", "Classic");

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      createType("Multi", 3, "");
      createType("Single", 1, "");
      createType("Classic", 3, " METADATA {\"similarity\": \"CLASSIC\"}");
      database.command("sql", "CREATE DOCUMENT TYPE Pair BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Pair.k INTEGER");
      database.command("sql", "CREATE PROPERTY Pair.t STRING");
      database.command("sql", "CREATE PROPERTY Pair.u STRING");
      database.command("sql", "CREATE INDEX ON Pair (t, u) FULL_TEXT");
      for (final String type : TYPES)
        for (int k = 1; k < TEXTS.length; k++)
          database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, TEXTS[k]);
      for (int k = 1; k < TEXTS.length; k++)
        database.command("sql", "INSERT INTO Pair SET k = ?, t = ?, u = 'filler'", k, TEXTS[k]);
    });
  }

  @Test
  void groupsNestedWithinTheLimitAnswerTheirMatches() {
    for (int depth : new int[] { 1, 2, 5, GROUP_LIMIT - 1 }) {
      final Set<String> words = new TreeSet<>(List.of("omega"));
      for (int i = 0; i < depth; i++)
        words.add(WORDS[i % WORDS.length]);

      for (final String type : TYPES) {
        assertThat(search(type, orChain(depth))).as("%s: %d groups of alternatives", type, depth).isEqualTo(holdingAny(words));
        assertThat(search(type, requiredChain(depth))).as("%s: %d groups of requirements", type, depth).isEqualTo(holding("alpha"));
      }
    }
  }

  @Test
  void aGroupOfOnlyProhibitedClausesNestedWithinTheLimitAnswersTheComplementOfItsMatches() {
    for (int levels = 0; levels < GROUP_LIMIT; levels++)
      for (final String type : TYPES)
        assertThat(search(type, negationChain(levels))).as("%s: %d prohibited groups around -alpha", type, levels)
            .isEqualTo(levels % 2 == 0 ? matching(k -> !has(k, "alpha")) : holding("alpha"));
  }

  @Test
  void prohibitedGroupsBesideOtherProhibitedClausesAnswerTheirComplement() {
    for (final String type : TYPES) {
      assertThat(search(type, "-beta -(-alpha -gamma)")).as(type)
          .isEqualTo(matching(k -> !has(k, "beta") && (has(k, "alpha") || has(k, "gamma"))));
      assertThat(search(type, "-(-alpha -gamma) -beta")).as(type)
          .isEqualTo(matching(k -> !has(k, "beta") && (has(k, "alpha") || has(k, "gamma"))));
      assertThat(search(type, "-delta -(-beta -(-alpha -gamma))")).as(type)
          .isEqualTo(matching(k -> !has(k, "delta") && (has(k, "beta") || !(has(k, "alpha") || has(k, "gamma")))));
      // two groups hold the matches one after the other: the complement of the union of the complements
      assertThat(search(type, "-(-alpha) -(-beta)")).as(type).isEqualTo(matching(k -> has(k, "alpha") && has(k, "beta")));
      assertThat(search(type, "gamma -(-alpha) -(-beta)")).as(type).isEqualTo(holding("alpha", "beta", "gamma"));
      assertThat(search(type, "alpha -(-beta)")).as(type).isEqualTo(holding("alpha", "beta"));
    }
  }

  @Test
  void groupsNestedBeyondTheLimitAreRefused() {
    final List<String> queries = List.of(orChain(GROUP_LIMIT + 1), orChain(10 * GROUP_LIMIT), requiredChain(GROUP_LIMIT + 1),
        requiredChain(10 * GROUP_LIMIT), negationChain(GROUP_LIMIT), negationChain(10 * GROUP_LIMIT),
        "alpha " + "(beta ".repeat(GROUP_LIMIT + 1) + "gamma" + ")".repeat(GROUP_LIMIT + 1));

    for (final String query : queries) {
      for (final String type : TYPES)
        assertRefused(() -> search(type, query));
      assertRefused(() -> searchFields(query));
      assertRefused(() -> search("Multi", query, "ORDER BY $score DESC LIMIT 3"));
      assertRefused(() -> run(null, query));
      assertRefused(() -> run(List.of("t"), query));
    }
  }

  @Test
  void aBoostedGroupIsALevelOfNesting() {
    for (final String type : TYPES)
      assertThat(search(type, boostChain(GROUP_LIMIT))).as(type).isEqualTo(holding("alpha"));

    for (final String type : TYPES)
      assertRefused(() -> search(type, boostChain(GROUP_LIMIT + 1)));
    assertRefused(() -> searchFields(boostChain(GROUP_LIMIT + 1)));
  }

  @Test
  void aRestrictedSearchCountsTheOrOfItsFieldsAsALevel() {
    assertThat(searchFields(orChain(GROUP_LIMIT - 1))).isEqualTo(holdingAny(Set.of("alpha", "beta", "gamma", "delta", "omega")));
    assertThat(searchFields(negationChain(GROUP_LIMIT - 2))).isEqualTo(matching(k -> !has(k, "alpha")));
    assertRefused(() -> searchFields(orChain(GROUP_LIMIT)));
    assertRefused(() -> searchFields(negationChain(GROUP_LIMIT - 1)));
    assertRefused(() -> run(List.of("t"), orChain(GROUP_LIMIT)));
    assertThatCode(() -> run(List.of("t"), orChain(GROUP_LIMIT - 1))).doesNotThrowAnyException();
  }

  @Test
  void parenthesesNestedBeyondTheLimitAreRefusedBeforeTheyAreParsed() {
    // a group of one clause is the clause, so these nest no group at all and only the parentheses of the text count
    for (final String type : TYPES)
      assertThat(search(type, "(".repeat(PARENTHESES_LIMIT) + "alpha" + ")".repeat(PARENTHESES_LIMIT))).as(type).isEqualTo(holding("alpha"));

    final int deep = 200_000;
    final List<String> queries = List.of("(".repeat(PARENTHESES_LIMIT + 1) + "alpha" + ")".repeat(PARENTHESES_LIMIT + 1),
        "(".repeat(deep) + "alpha" + ")".repeat(deep), "(".repeat(deep), "-(".repeat(deep) + "alpha", "(alpha^2 ".repeat(deep),
        // the parentheses inside a quoted phrase are no group and the ones outside it are
        "(\"))\" ".repeat(deep) + "alpha");
    for (final String query : queries) {
      for (final String type : TYPES)
        assertRefused(() -> search(type, query));
      assertRefused(() -> searchFields(query));
      assertRefused(() -> run(null, query));
    }
  }

  @Test
  void parenthesesThatAreNoGroupAreNotCounted() {
    final int many = 5 * PARENTHESES_LIMIT;
    for (final String type : TYPES) {
      // groups that follow one another nest nothing
      assertThat(search(type, "(alpha) ".repeat(many))).as(type).isEqualTo(holding("alpha"));
      assertThat(search(type, "(alpha) AND (beta) ".repeat(many))).as(type).isEqualTo(holding("alpha", "beta"));
      // an escaped parenthesis is a character of the word, and one inside a phrase, a regexp or a range is no parenthesis
      assertThat(search(type, "\\(".repeat(many) + "alpha")).as(type).isEqualTo(holding("alpha"));
      assertThat(search(type, "alpha OR \"" + "(".repeat(many) + "\"")).as(type).isEqualTo(holding("alpha"));
      assertThat(search(type, "alpha OR /" + "(".repeat(many) + "a" + ")".repeat(many) + "/")).as(type).isEqualTo(holding("alpha"));
      assertThat(search(type, "alpha OR [a" + "(".repeat(many) + " TO z]")).as(type).isEqualTo(holding("alpha"));
    }
  }

  @Test
  void aQueryHoldingMoreClausesThanTheLimitIsRefused() {
    final String group = "(" + "alpha ".repeat(1000) + ") ";
    final int groups = CLAUSE_LIMIT / 1000 + 1;
    assertThat(groups * 1001).isGreaterThan(CLAUSE_LIMIT);

    for (final String type : TYPES) {
      assertThat(search(type, group.repeat(2))).as(type).isEqualTo(holding("alpha"));
      assertRefused(() -> search(type, group.repeat(groups)));
    }
    assertRefused(() -> run(null, group.repeat(groups)));
  }

  @Test
  void aRefusalNamesWhatIsTooLargeAndNotTheQuery() {
    final String query = negationChain(GROUP_LIMIT + 5);
    assertThatThrownBy(() -> run(null, query)).isInstanceOf(FullTextQueryParseException.class)
        .hasMessageContaining(String.valueOf(GROUP_LIMIT)).hasMessageNotContaining(query);
    final String parentheses = "(".repeat(PARENTHESES_LIMIT + 1);
    assertThatThrownBy(() -> run(null, parentheses)).isInstanceOf(FullTextQueryParseException.class)
        .hasMessageContaining(String.valueOf(PARENTHESES_LIMIT)).hasMessageNotContaining(parentheses);
  }

  @Test
  void aQueryThatDoesNotParseIsStillRefusedByTheParser() {
    for (final String query : new String[] { "\"alpha" + "(".repeat(2 * PARENTHESES_LIMIT), "alpha AND (", "\"alpha" })
      assertThatThrownBy(() -> run(null, query)).as("query of %d characters", query.length())
          .isInstanceOf(FullTextQueryParseException.class);
  }

  private void createType(final String type, final int buckets, final String metadata) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (t) FULL_TEXT" + metadata);
  }

  /** {@code alpha OR (beta OR (gamma OR (... OR omega)))}: the groups nest {@code depth} levels. */
  private static String orChain(final int depth) {
    String query = "omega";
    for (int i = depth - 1; i >= 0; i--)
      query = WORDS[i % WORDS.length] + " OR (" + query + ")";
    return query;
  }

  /** {@code +alpha +(+alpha +(... +(alpha)))}: the groups nest {@code depth} levels. */
  private static String requiredChain(final int depth) {
    String query = "alpha";
    for (int i = 0; i < depth; i++)
      query = "+alpha +(" + query + ")";
    return query;
  }

  /** {@code -(-(-alpha))}: {@code levels} groups of only prohibited clauses around a prohibited alpha. */
  private static String negationChain(final int levels) {
    return "-(".repeat(levels) + "-alpha" + ")".repeat(levels);
  }

  /** {@code ((alpha)^2)^2}: {@code depth} boosts around alpha. */
  private static String boostChain(final int depth) {
    return "(".repeat(depth) + "alpha" + ")^2".repeat(depth);
  }

  private static boolean has(final int k, final String word) {
    return TEXTS[k].contains(word);
  }

  private static Set<Integer> matching(final Predicate<Integer> predicate) {
    final Set<Integer> keys = new TreeSet<>();
    for (int k = 1; k < TEXTS.length; k++)
      if (predicate.test(k))
        keys.add(k);
    return keys;
  }

  private static Set<Integer> holding(final String... words) {
    return matching(k -> {
      for (final String word : words)
        if (!has(k, word))
          return false;
      return true;
    });
  }

  private static Set<Integer> holdingAny(final Set<String> words) {
    return matching(k -> words.stream().anyMatch(word -> has(k, word)));
  }

  private Set<Integer> search(final String type, final String query) {
    return search(type, query, "");
  }

  private Set<Integer> search(final String type, final String query, final String tail) {
    return keys("SELECT k FROM " + type + " WHERE SEARCH_INDEX('" + type + "[t]', :q) = true " + tail, query);
  }

  /** A search restricted to {@code t}, one of the two properties of the index of {@code Pair}: each clause is expanded over it. */
  private Set<Integer> searchFields(final String query) {
    return keys("SELECT k FROM Pair WHERE SEARCH_FIELDS(['t'], :q) = true", query);
  }

  private Set<Integer> keys(final String sql, final String query) {
    final Set<Integer> keys = new TreeSet<>();
    try (final ResultSet rs = database.query("sql", sql, Map.of("q", query))) {
      while (rs.hasNext())
        keys.add(rs.next().<Integer>getProperty("k"));
    }
    return keys;
  }

  /** The executor on the one bucket of {@code Single}, without any of the layers of a statement above it. */
  private void run(final List<String> restrictedFields, final String query) {
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Single[t]");
    final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
    final IndexCursor cursor = new FullTextQueryExecutor(ftIndex, WorkGuard.forCommandDeadline(null), restrictedFields, null).search(query,
        -1);
    final List<Object> rows = new ArrayList<>();
    while (cursor.hasNext())
      rows.add(cursor.next());
  }

  private static void assertRefused(final Runnable search) {
    assertThatThrownBy(search::run).satisfies(thrown -> {
      Throwable cause = thrown;
      while (cause != null && !(cause instanceof FullTextQueryParseException))
        cause = cause.getCause() == cause ? null : cause.getCause();
      assertThat(cause).as("a query that does not parse: %s", thrown).isNotNull();
    });
  }
}
