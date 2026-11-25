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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.management.ThreadMXBean;
import org.apache.lucene.search.FuzzyQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The cost of a fuzzy full-text clause is a function of the keys it reads and of the edits it allows, never of how long the term
 * of the clause is, and the work guard of the statement is consulted in proportion to the characters measured.
 * <p>
 * The timings bound a few seconds against a run that takes much longer when the cost is not bounded (see
 * {@link StallAwareStopwatch#assertGaveUpWithin}): they are tripwires, and the {@link Timeout} of each test is a hang detector.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextFuzzyMatchingCostTest extends TestHelper {
  private static final String KEYWORD_ANALYZER = "org.apache.lucene.analysis.core.KeywordAnalyzer";

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aLongTermCostsNothingPerKeyRead() {
    createType("Words", null);
    final int keys = 3000;
    database.transaction(() -> {
      for (int i = 0; i < keys; i += 10) {
        final StringBuilder text = new StringBuilder();
        for (int j = 0; j < 10; j++)
          text.append("word").append(String.format("%05d", i + j)).append(' ');
        insert("Words", i, text.toString());
      }
    });

    final int termLength = 1_000_000;
    final String longTerm = "a".repeat(termLength);

    // What the statement allocates, not how long it takes: a conversion of the term for every key read would allocate the code
    // points of the term (4 bytes each) as many times as there are keys: 12 GB here, against some 150 MB for the statement when the
    // code points are worked out once for each walk (the parser, the analyzer and the clause all hold their copies of the term)
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeTrue(threads.isThreadAllocatedMemorySupported() && threads.isThreadAllocatedMemoryEnabled());
    final long thread = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(thread);
    final Map<Integer, Float> matches = search("Words", longTerm + "~2");
    final long allocated = threads.getThreadAllocatedBytes(thread) - before;

    assertThat(allocated).as("%d bytes allocated for a term of %d characters over %d keys", allocated, termLength, keys)
        .isLessThan(400L * termLength);
    assertThat(matches).isEmpty();
    // the clause still finds what it is meant to find, in the same type
    assertThat(search("Words", "word00007")).containsOnlyKeys(0);
    assertThat(search("Words", "word00007~1")).containsKeys(0, 10, 1000);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void longStoredTokensAreMeasuredWithinTheEditBand() {
    createType("Tokens", KEYWORD_ANALYZER);
    final int half = 4_000;
    final String term = "a".repeat(half) + "bc" + "a".repeat(half - 2);
    final String oneSubstitution = "a".repeat(half) + "bd" + "a".repeat(half - 2);
    final String oneSwap = "a".repeat(half) + "cb" + "a".repeat(half - 2);
    final String oneDeletion = "a".repeat(half) + "b" + "a".repeat(half - 2);
    final String twoEdits = replaceAt(replaceAt(term, 100, 'z'), 5_000, 'z');
    final String threeEdits = replaceAt(replaceAt(replaceAt(term, 100, 'z'), 3_000, 'z'), 7_000, 'z');

    database.transaction(() -> {
      insert("Tokens", 1, term);
      insert("Tokens", 2, oneSubstitution);
      insert("Tokens", 3, oneSwap);
      insert("Tokens", 4, oneDeletion);
      insert("Tokens", 5, twoEdits);
      insert("Tokens", 6, threeEdits);
      // distinct long tokens of the same length, each several edits away from the term
      for (int i = 0; i < 120; i++) {
        String other = term;
        for (int j = 0; j < 6; j++)
          other = replaceAt(other, (i * 37 + j * 1_009) % (2 * half), (char) ('d' + (i + j) % 20));
        insert("Tokens", 100 + i, other);
      }
    });

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final Map<Integer, Float> matches = search("Tokens", term + "~2");
    stopwatch.assertGaveUpWithin(5_000, "a band of the edits allowed around the diagonal from a full matrix of the lengths for every key");

    assertThat(matches).containsOnlyKeys(1, 2, 3, 4, 5);
    assertThat(search("Tokens", term + "~1")).containsOnlyKeys(1, 2, 3, 4);
    assertThat(search("Tokens", term + "~0")).containsOnlyKeys(1);
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void theWorkGuardIsConsultedInProportionToTheCharactersMeasured() {
    createType("Guarded", KEYWORD_ANALYZER);
    final int length = 8_000;
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        insert("Guarded", i, ("" + (char) ('a' + i % 20)).repeat(length - 1) + (char) ('a' + i / 20));
    });

    final List<LSMTreeFullTextIndex> buckets = bucketIndexes("Guarded");
    final String query = "a".repeat(length) + "~2";
    final long expired = System.currentTimeMillis() - 10_000;

    // no key matches, and fewer keys are read than the 1024 between two checks of the guard: only the characters measured can reach it
    assertThatThrownBy(() -> FullTextQueryExecutor.selectFuzzyExpansions(buckets, query, WorkGuard.forDeadline("the test", expired), null))
        .isInstanceOf(TimeoutException.class);

    // the same walk with no deadline completes, and expands the clause to the keys one edit from the term and to the term itself
    final Map<FuzzyQuery, Set<String>> expansions = FullTextQueryExecutor.selectFuzzyExpansions(buckets, query,
        WorkGuard.forDeadline("the test", Long.MAX_VALUE), null);
    assertThat(expansions).hasSize(1);
    assertThat(expansions.values().iterator().next()).hasSize(3);
  }

  @Test
  void theBoundedDistanceAgreesWithTheFullMatrix() {
    final Random random = new Random(12_345);
    final int[] alphabet = { 'a', 'b', 'c', 0x20AC, 0x1F600, 0x1F601 };
    for (int round = 0; round < 20_000; round++) {
      final String token = randomText(random, alphabet, random.nextInt(9));
      final String term = random.nextInt(3) == 0 ? mutate(random, alphabet, token) : randomText(random, alphabet, random.nextInt(9));
      for (int maxEdits = 0; maxEdits <= 3; maxEdits++)
        for (final boolean transpositions : new boolean[] { true, false }) {
          final int expected = Math.min(fullMatrix(token, term, transpositions), maxEdits + 1);
          assertThat(FullTextQueryExecutor.fuzzyDistance(token, term, transpositions, maxEdits))
              .as("'%s' to '%s', max %d, transpositions %s", token, term, maxEdits, transpositions).isEqualTo(expected);
        }
    }
  }

  @Test
  void theBoundedDistanceAgreesWithTheFullMatrixOnLongTexts() {
    final Random random = new Random(777);
    final int[] alphabet = { 'a', 'b' };
    for (int round = 0; round < 300; round++) {
      final String token = randomText(random, alphabet, 40 + random.nextInt(60));
      final String term = mutate(random, alphabet, token);
      for (int maxEdits = 1; maxEdits <= 3; maxEdits++)
        for (final boolean transpositions : new boolean[] { true, false })
          assertThat(FullTextQueryExecutor.fuzzyDistance(token, term, transpositions, maxEdits))
              .as("'%s' to '%s', max %d, transpositions %s", token, term, maxEdits, transpositions)
              .isEqualTo(Math.min(fullMatrix(token, term, transpositions), maxEdits + 1));
    }
  }

  @Test
  void theDistanceOfTermsOfAnyLengthIsAnsweredFromTheirLengthsWhenTheyDiffer() {
    final int[] huge = FullTextQueryExecutor.codePointsOf("a".repeat(2_000_000));
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    for (int i = 0; i < 2_000; i++)
      assertThat(FullTextQueryExecutor.fuzzyDistance("word" + i, huge, true, 2)).isEqualTo(3);
    stopwatch.assertGaveUpWithin(5_000, "a conversion of the long term for every token from the one made by the caller");
  }

  private void createType(final String type, final String analyzer) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 2");
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
      database.command("sql",
          "CREATE INDEX ON " + type + " (t) FULL_TEXT" + (analyzer != null ? " METADATA {\"analyzer\": \"" + analyzer + "\"}" : ""));
    });
  }

  private void insert(final String type, final int k, final String text) {
    database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, text);
  }

  private List<LSMTreeFullTextIndex> bucketIndexes(final String type) {
    final TypeIndex typeIndex = database.getSchema().getType(type).getIndexesByProperties("t").getFirst();
    final List<LSMTreeFullTextIndex> buckets = new ArrayList<>();
    for (final Index index : typeIndex.getIndexesOnBuckets())
      buckets.add((LSMTreeFullTextIndex) index);
    return buckets;
  }

  private Map<Integer, Float> search(final String type, final String query) {
    final ResultSet rs = database.query("sql", "SELECT k, $score AS s FROM " + type + " WHERE SEARCH_INDEX('" + type + "[t]', :q) = true",
        Map.of("q", query));
    final Map<Integer, Float> found = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      found.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
    }
    return found;
  }

  private static String replaceAt(final String text, final int index, final char replacement) {
    return text.substring(0, index) + replacement + text.substring(index + 1);
  }

  private static String randomText(final Random random, final int[] alphabet, final int length) {
    final StringBuilder text = new StringBuilder();
    for (int i = 0; i < length; i++)
      text.appendCodePoint(alphabet[random.nextInt(alphabet.length)]);
    return text.toString();
  }

  /** A text a few random edits away from {@code text}: a substitution, a deletion, an insertion or a swap of two neighbours. */
  private static String mutate(final Random random, final int[] alphabet, final String text) {
    final List<Integer> points = new ArrayList<>(text.codePoints().boxed().toList());
    final int edits = random.nextInt(4);
    for (int i = 0; i < edits; i++) {
      final int kind = random.nextInt(4);
      if (kind == 0 && !points.isEmpty())
        points.set(random.nextInt(points.size()), alphabet[random.nextInt(alphabet.length)]);
      else if (kind == 1 && !points.isEmpty())
        points.remove(random.nextInt(points.size()));
      else if (kind == 2)
        points.add(random.nextInt(points.size() + 1), alphabet[random.nextInt(alphabet.length)]);
      else if (points.size() > 1) {
        final int at = random.nextInt(points.size() - 1);
        final Integer swapped = points.get(at);
        points.set(at, points.get(at + 1));
        points.set(at + 1, swapped);
      }
    }
    final StringBuilder mutated = new StringBuilder();
    for (final int point : points)
      mutated.appendCodePoint(point);
    return mutated.toString();
  }

  /** The full dynamic programming matrix, the optimal string alignment distance: the reference the bounded one must agree with. */
  private static int fullMatrix(final String a, final String b, final boolean transpositions) {
    final int[] s = a.codePoints().toArray();
    final int[] t = b.codePoints().toArray();
    final int[][] d = new int[s.length + 1][t.length + 1];
    for (int i = 0; i <= s.length; i++)
      d[i][0] = i;
    for (int j = 0; j <= t.length; j++)
      d[0][j] = j;
    for (int i = 1; i <= s.length; i++)
      for (int j = 1; j <= t.length; j++) {
        int value = Math.min(Math.min(d[i - 1][j], d[i][j - 1]) + 1, d[i - 1][j - 1] + (s[i - 1] == t[j - 1] ? 0 : 1));
        if (transpositions && i > 1 && j > 1 && s[i - 1] == t[j - 2] && s[i - 2] == t[j - 1])
          value = Math.min(value, d[i - 2][j - 2] + 1);
        d[i][j] = value;
      }
    return d[s.length][t.length];
  }
}
