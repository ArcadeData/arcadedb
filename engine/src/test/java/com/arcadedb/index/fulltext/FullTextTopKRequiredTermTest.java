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
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A keyword search scoped to a tenant, {@code (group:"g1") AND (x y z)}, is answered by {@link FullTextTopKSearch} with the
 * required term probed and the disjunction walked with MaxScore. The answer must stay that of the general path: the same
 * documents, in the same order, with the same scores.
 */
class FullTextTopKRequiredTermTest extends TestHelper {
  private static final int VOCABULARY = 300;

  @Test
  void requiredTermInFrontOfADisjunctionEqualsTheGeneralPath() {
    for (final int buckets : new int[] { 1, 4 }) {
      final String type = "Doc" + buckets;
      create(type, buckets, 3_000, 5 + buckets);
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, type + "[body,grp]");
      final Random random = new Random(buckets);
      for (int q = 0; q < 60; q++) {
        final String keywords = words(random, 1 + random.nextInt(12));
        final String group = "g" + random.nextInt(4);
        final String[] shapes = { "(grp:\"" + group + "\") AND (" + keywords + ")", "grp:" + group + " AND (" + keywords + ")",
            "(grp:\"" + group + "\") AND (" + keywords + ") AND (grp:\"g" + random.nextInt(4) + "\")" };
        for (final String query : shapes)
          for (final int limit : new int[] { 1, 10, 100, -1 })
            assertSame(index, query, limit, new HashSet<>(Arrays.asList(keywords.split(" "))).size() >= 2);
      }
    }
  }

  @Test
  void aGroupNoDocumentHoldsAnswersNothing() {
    create("DocNone", 2, 500, 9);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "DocNone[body,grp]");
    final Map<RID, Float> top = FullTextSearch.search(index, "(grp:\"nope\") AND (" + word(1) + " " + word(2) + ")", 10,
        WorkGuard.forCommandDeadline(null), null, null);
    assertThat(top).isEmpty();
  }

  @Test
  void theShapeIsRecognised() {
    create("DocShape", 1, 50, 3);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "DocShape[body,grp]");
    final LSMTreeFullTextIndex bucket = FullTextSearch.getBucketIndexes(index).getFirst();
    final FullTextQueryExecutor.TermShape shape = new FullTextQueryExecutor(bucket, WorkGuard.forCommandDeadline(null), null,
        null).termShape("(grp:\"g1\") AND (" + word(1) + " " + word(2) + ")");
    assertThat(shape).isNotNull();
    assertThat(shape.required()).hasSize(1);
    assertThat(shape.tokens()).hasSize(3);
    // a negation or a phrase in front of the disjunction keeps the general path
    assertThat(new FullTextQueryExecutor(bucket, WorkGuard.forCommandDeadline(null), null, null).termShape(
        "-grp:g1 AND (" + word(1) + " " + word(2) + ")")).isNull();
    // one distinct keyword is a conjunction of terms, repeated or not: `(x x)` keeps the general path as `(x)` does
    assertThat(new FullTextQueryExecutor(bucket, WorkGuard.forCommandDeadline(null), null, null).termShape(
        "(grp:\"g1\") AND (" + word(1) + ")")).isNull();
    assertThat(new FullTextQueryExecutor(bucket, WorkGuard.forCommandDeadline(null), null, null).termShape(
        "(grp:\"g1\") AND (" + word(1) + " " + word(1) + ")")).isNull();
  }

  @Test
  void aRepeatedKeywordAnswersAsTheGeneralPath() {
    create("DocRepeat", 4, 1_000, 11);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "DocRepeat[body,grp]");
    for (final int limit : new int[] { 1, 10, 100, -1 }) {
      assertSame(index, "(grp:\"g1\") AND (" + word(0) + " " + word(0) + ")", limit, false);
      assertSame(index, "(grp:\"g1\") AND (" + word(0) + " " + word(0) + " " + word(1) + ")", limit, true);
    }
  }

  /**
   * Two distinct required tokens that both match: every one of them is probed and adds its largest contribution to the MaxScore
   * bound, and a document lacking either is no match.
   */
  @Test
  void twoDistinctRequiredTermsEqualTheGeneralPath() {
    final String type = "DocTwoRequired";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 3");
      database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".grp STRING");
      database.command("sql", "CREATE INDEX ON " + type + " (body, grp) FULL_TEXT");
    });
    final Random random = new Random(42);
    final String[] keywords = { "kwone", "kwtwo", "kwthree" };
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final StringBuilder body = new StringBuilder(words(random, 2 + random.nextInt(20)));
        // the required tokens with varying frequencies, so their contributions differ from document to document
        for (int r = i % 2 == 0 ? 1 + random.nextInt(3) : 0; r > 0; --r)
          body.append(" reqone");
        for (int r = i % 3 == 0 ? 1 + random.nextInt(3) : 0; r > 0; --r)
          body.append(" reqtwo");
        for (int k = random.nextInt(4); k > 0; --k)
          body.append(' ').append(keywords[random.nextInt(keywords.length)]);
        database.newDocument(type).set("body", body.toString()).set("grp", "g" + (i % 4)).save();
      }
    });
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, type + "[body,grp]");

    final String bothRequired = "reqone AND reqtwo AND (kwone kwtwo kwthree)";
    final FullTextQueryExecutor.TermShape shape = new FullTextQueryExecutor(FullTextSearch.getBucketIndexes(index).getFirst(),
        WorkGuard.forCommandDeadline(null), null, null).termShape(bothRequired);
    assertThat(shape).isNotNull();
    assertThat(shape.required()).containsExactlyInAnyOrder("reqone", "reqtwo");

    for (final String query : new String[] { bothRequired, "(grp:\"g0\") AND reqone AND (kwone kwtwo)",
        "reqtwo AND reqone AND (kwthree^2 kwone)" }) {
      assertThat(FullTextSearch.searchMatchingFirst(FullTextSearch.getBucketIndexes(index), query, -1,
          WorkGuard.forCommandDeadline(null), null, null)).as("matches of %s", query).isNotEmpty();
      for (final int limit : new int[] { 1, 5, 50, -1 })
        assertSame(index, query, limit, true);
    }
  }

  private void assertSame(final TypeIndex index, final String query, final int limit, final boolean expectTopK) {
    final Map<RID, Float> top = FullTextSearch.search(index, query, limit, WorkGuard.forCommandDeadline(null), null, null);
    final Map<RID, Float> general = FullTextSearch.searchMatchingFirst(FullTextSearch.getBucketIndexes(index), query, limit,
        WorkGuard.forCommandDeadline(null), null, null);
    if (limit < 1) {
      assertThat(top).as(query).isEqualTo(general);
      return;
    }
    final List<Map.Entry<RID, Float>> expected = ranked(general, limit);
    assertThat(ranked(top, limit)).as("best %d of %s", limit, query).isEqualTo(expected);
    // The top-k path keeps one best `limit` across buckets. A query with one distinct keyword is a conjunction of terms, which
    // the general path answers (each bucket's best matches), so there only the ranking is compared.
    final boolean topK = new FullTextQueryExecutor(FullTextSearch.getBucketIndexes(index).getFirst(),
        WorkGuard.forCommandDeadline(null), null, null).termShape(query) != null;
    assertThat(topK).as("top-k path taken for %s", query).isEqualTo(expectTopK);
    if (topK)
      assertThat(top.size()).as("kept for %s", query).isEqualTo(expected.size());
    else
      assertThat(top.size()).as("kept for %s", query).isGreaterThanOrEqualTo(expected.size());
  }

  private static List<Map.Entry<RID, Float>> ranked(final Map<RID, Float> scores, final int limit) {
    final List<Map.Entry<RID, Float>> entries = new ArrayList<>();
    for (final Map.Entry<RID, Float> e : scores.entrySet())
      entries.add(Map.entry(new RID(e.getKey().getBucketId(), e.getKey().getPosition()), e.getValue()));
    entries.sort(Comparator.<Map.Entry<RID, Float>, Float>comparing(Map.Entry::getValue).reversed().thenComparing(Map.Entry::getKey));
    return entries.subList(0, Math.min(limit, entries.size()));
  }

  private void create(final String type, final int buckets, final int documents, final long seed) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".grp STRING");
      database.command("sql", "CREATE INDEX ON " + type + " (body, grp) FULL_TEXT");
    });
    final Random random = new Random(seed);
    for (int from = 0; from < documents; from += 500) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(documents, start + 500); i++)
          database.newDocument(type).set("body", words(random, 4 + random.nextInt(40))).set("grp", "g" + random.nextInt(4)).save();
      });
    }
  }

  private static String word(final int i) {
    return "w" + i + "x";
  }

  private static String words(final Random random, final int count) {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < count; i++)
      b.append(word(Math.min(VOCABULARY - 1, (int) Math.floor(Math.pow(VOCABULARY + 1, random.nextDouble())) - 1))).append(' ');
    return b.toString().trim();
  }
}
