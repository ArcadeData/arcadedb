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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DefaultDataEncryption;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.crypto.KeyGenerator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12458: a BM25 search of a disjunction of terms with a limit read and scored every match before it kept the best k. It is
 * now answered from primitive posting arrays read once, with MaxScore pruning against one threshold shared by every bucket
 * ({@link FullTextTopKSearch}). The answer must stay exactly that of the general path: the same documents, in the same order,
 * with the same scores to the last bit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12458BM25TopKTest extends TestHelper {
  private static final int      VOCABULARY = 400;
  private static final String[] WORDS      = new String[VOCABULARY];

  static {
    final String[] syllables = { "ka", "lo", "mi", "ren", "tor", "vel", "zen", "dal", "fen", "gri", "por", "sul" };
    for (int i = 0; i < VOCABULARY; i++)
      WORDS[i] = syllables[i % 12] + syllables[(i / 12) % 12] + syllables[(i / 144) % 12] + "x";
  }

  @Override
  protected void beginTest() {
    // this test compacts the indexes itself, so the engine must not hold a compaction reservation of its own
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
  }

  @Test
  void topKEqualsTheGeneralPathAcrossBuckets() {
    createCorpus("Doc", 8, 3_000, 11);
    assertSameAnswers("Doc[body]", null, 21);
  }

  @Test
  void topKEqualsTheGeneralPathOnOneBucket() {
    createCorpus("Doc", 1, 1_500, 12);
    assertSameAnswers("Doc[body]", null, 22);
  }

  @Test
  void postingsAreReadUnderDataEncryption() throws Exception {
    // with data encryption the count of an index entry's values is stored encrypted, the RIDs are not
    final KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(256);
    database.setDataEncryption(DefaultDataEncryption.useDefaults(keyGenerator.generateKey()));
    createCorpus("Doc", 4, 800, 28);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
    assertThat(FullTextSearch.search(index, WORDS[0] + " " + WORDS[1], -1)).isNotEmpty();
    assertSameAnswers("Doc[body]", null, 29);
  }

  @Test
  void deletionsAndUpdatesInLaterTransactionsAreApplied() {
    createCorpus("Doc", 4, 2_000, 13);
    final Random random = new Random(14);
    // removals and re-additions in separate transactions, so the index holds deletion markers in front of older postings
    for (int round = 0; round < 5; round++) {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Doc")) {
          while (rs.hasNext()) {
            final Result row = rs.next();
            final int pick = random.nextInt(10);
            if (pick == 0)
              row.getRecord().get().delete();
            else if (pick == 1)
              row.getRecord().get().asDocument().modify().set("body", randomBody(random)).save();
          }
        }
      });
    }
    assertSameAnswers("Doc[body]", null, 23);

    // a deleted document is never answered
    final List<RID> deleted = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc LIMIT 50")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          deleted.add(row.getIdentity().get());
          row.getRecord().get().delete();
        }
      }
    });
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
    for (int q = 0; q < 20; q++) {
      final Map<RID, Float> all = FullTextSearch.search(index, query(new Random(q), 5), -1);
      assertThat(all.keySet()).doesNotContainAnyElementsOf(deleted);
    }
  }

  @Test
  void compactedAndMutablePostingsTogether() {
    createCorpus("Doc", 4, 2_000, 15);
    compactAll("Doc[body]");
    // newer postings and deletion markers on the mutable pages, older ones in the compacted series
    final Random random = new Random(16);
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        insert("Doc", "n" + i, randomBody(random));
    });
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id LIKE 'd1%'")) {
        while (rs.hasNext())
          rs.next().getRecord().get().delete();
      }
    });
    assertSameAnswers("Doc[body]", null, 24);
  }

  @Test
  void uncommittedChangesOfTheTransactionAreSeen() {
    createCorpus("Doc", 4, 1_000, 17);
    database.transaction(() -> {
      final String rare = WORDS[VOCABULARY - 1];
      insert("Doc", "fresh", rare + " " + rare + " " + rare);
      final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
      final Map<RID, Float> top = FullTextSearch.search(index, rare + " " + WORDS[3], 5);
      final List<RID> general = ranked(general(index, rare + " " + WORDS[3], 5, null), 5);
      assertThat(ranked(top, 5)).isEqualTo(general);
      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE id = 'fresh'")) {
        assertThat(top).containsKey(rs.next().getIdentity().get());
      }
      assertSameAnswers("Doc[body]", null, 25);
    });
  }

  @Test
  void boostsAndFieldRestrictionsKeepTheirScores() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Article.id STRING");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");
    });
    final Random random = new Random(18);
    for (int batch = 0; batch < 4; batch++)
      database.transaction(() -> {
        for (int i = 0; i < 250; i++) {
          final MutableDocument doc = database.newDocument("Article");
          doc.set("id", "a" + i);
          doc.set("title", randomBody(random).substring(0, 30));
          doc.set("body", randomBody(random));
          doc.save();
        }
      });

    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Article[title,body]");
    final String[] queries = { WORDS[2] + "^3 " + WORDS[5], "title:" + WORDS[1] + " body:" + WORDS[4] + "^2 " + WORDS[9],
        "(" + WORDS[1] + " " + WORDS[7] + ") " + WORDS[30] };
    for (final String q : queries) {
      assertThat(FullTextTopKSearch.search(FullTextSearch.getBucketIndexes(index), q, 10, WorkGuard.forCommandDeadline(null), null,
          null)).as("'%s' is a disjunction of terms", q).isNotNull();
      for (final int limit : new int[] { 1, 10, -1 })
        assertSameAnswer(index, q, limit, null);
      for (final int limit : new int[] { 3, -1 })
        assertSameAnswer(index, q, limit, List.of("title"));
    }
  }

  @Test
  void otherQueryShapesKeepTheGeneralPath() {
    createCorpus("Doc", 2, 200, 19);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
    final String a = WORDS[1];
    final String b = WORDS[2];
    for (final String q : new String[] { "+" + a + " " + b, a + " AND " + b, "\"" + a + " " + b + "\"", a.substring(0, 3) + "*",
        "-" + a + " " + b, a + "~1", "/" + a + "/" }) {
      assertThat(FullTextTopKSearch.search(FullTextSearch.getBucketIndexes(index), q, 10, WorkGuard.forCommandDeadline(null), null,
          null)).as("'%s' is not a disjunction of terms", q).isNull();
    }
  }

  @Test
  void aLimitBeyondTheMatchesAllocatesForTheMatches() {
    createCorpus("Doc", 2, 300, 30);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
    final String q = WORDS[0] + " " + WORDS[1];
    // the heap grows with what it keeps, not to the limit a caller asks
    assertThat(FullTextSearch.search(index, q, Integer.MAX_VALUE)).isEqualTo(FullTextSearch.search(index, q, -1));
  }

  @Test
  void theSharedThresholdPrunesTheMatches() {
    createCorpus("Doc", 8, 4_000, 20);
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, "Doc[body]");
    // many common words: most documents match, and only a few can be among the best ten
    final StringBuilder q = new StringBuilder();
    for (int i = 0; i < 30; i++)
      q.append(WORDS[i]).append(' ');
    final int matches = FullTextSearch.search(index, q.toString(), -1).size();

    final long[] exactScores = new long[1];
    final Map<RID, Float> top = FullTextTopKSearch.search(FullTextSearch.getBucketIndexes(index), q.toString(), 10,
        WorkGuard.forCommandDeadline(null), null, null, exactScores);
    final long scored = exactScores[0];

    assertThat(top).hasSize(10);
    assertThat(ranked(top, 10)).isEqualTo(ranked(general(index, q.toString(), 10, null), 10));
    assertThat(matches).isGreaterThan(3_000);
    assertThat(scored).as("documents scored exactly for the best 10 of %d matches", matches).isLessThan(matches / 2);
  }

  @Test
  void sqlTopKIsThePrefixOfTheUnboundedOrder() {
    createCorpus("Doc", 8, 2_000, 26);
    final Random random = new Random(27);
    for (int i = 0; i < 20; i++) {
      final String q = query(random, 1 + random.nextInt(12));
      final List<String> limited = ids("SELECT id, $score FROM Doc WHERE SEARCH_INDEX('Doc[body]', ?) = true ORDER BY $score DESC, @rid "
          + "LIMIT 7", q);
      final List<String> unbounded = ids("SELECT id, $score FROM Doc WHERE SEARCH_INDEX('Doc[body]', ?) = true ORDER BY $score DESC, @rid",
          q);
      assertThat(limited).isEqualTo(unbounded.subList(0, Math.min(7, unbounded.size())));
    }
  }

  private List<String> ids(final String sql, final String q) {
    final List<String> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, q)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        ids.add(row.getProperty("id") + "=" + row.getProperty("$score"));
      }
    }
    return ids;
  }

  private void assertSameAnswers(final String indexName, final List<String> restrictedFields, final long seed) {
    final TypeIndex index = FullTextSearch.resolveFullTextIndex(database, indexName);
    final Random random = new Random(seed);
    for (int q = 0; q < 40; q++) {
      final String query = query(random, 1 + random.nextInt(25));
      for (final int limit : new int[] { 1, 10, 50, -1 })
        assertSameAnswer(index, query, limit, restrictedFields);
    }
  }

  private void assertSameAnswer(final TypeIndex index, final String query, final int limit, final List<String> restrictedFields) {
    final Map<RID, Float> top = FullTextSearch.search(index, query, limit, WorkGuard.forCommandDeadline(null), restrictedFields, null);
    final Map<RID, Float> general = general(index, query, limit, restrictedFields);
    if (limit < 1) {
      assertThat(top).as("every match of '%s'", query).isEqualTo(general);
      return;
    }
    // the general path keeps each bucket's best `limit`; their best `limit` overall is the answer
    final List<Map.Entry<RID, Float>> expected = rankedEntries(general, limit);
    final List<Map.Entry<RID, Float>> actual = rankedEntries(top, limit);
    assertThat(top.size()).as("matches kept for '%s' limit %d", query, limit).isEqualTo(expected.size());
    assertThat(actual).as("best %d of '%s'", limit, query).isEqualTo(expected);
  }

  private static Map<RID, Float> general(final TypeIndex index, final String query, final int limit,
      final List<String> restrictedFields) {
    return FullTextSearch.searchMatchingFirst(FullTextSearch.getBucketIndexes(index), query, limit, WorkGuard.forCommandDeadline(null),
        restrictedFields, null);
  }

  private static List<RID> ranked(final Map<RID, Float> scores, final int limit) {
    final List<RID> rids = new ArrayList<>();
    for (final Map.Entry<RID, Float> e : rankedEntries(scores, limit))
      rids.add(e.getKey());
    return rids;
  }

  private static List<Map.Entry<RID, Float>> rankedEntries(final Map<RID, Float> scores, final int limit) {
    final List<Map.Entry<RID, Float>> entries = new ArrayList<>();
    for (final Map.Entry<RID, Float> e : scores.entrySet())
      entries.add(Map.entry(new RID(e.getKey().getBucketId(), e.getKey().getPosition()), e.getValue()));
    entries.sort(Comparator.<Map.Entry<RID, Float>, Float>comparing(Map.Entry::getValue).reversed()
        .thenComparing(Map.Entry::getKey));
    return entries.subList(0, Math.min(limit, entries.size()));
  }

  private void createCorpus(final String type, final int buckets, final int documents, final long seed) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY " + type + ".id STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
      database.command("sql", "CREATE INDEX ON " + type + " (body) FULL_TEXT");
    });
    final Random random = new Random(seed);
    for (int batch = 0; batch < documents; batch += 500) {
      final int from = batch;
      database.transaction(() -> {
        for (int i = from; i < Math.min(documents, from + 500); i++)
          insert(type, "d" + i, randomBody(random));
      });
    }
  }

  private void insert(final String type, final String id, final String body) {
    final MutableDocument doc = database.newDocument(type);
    doc.set("id", id);
    doc.set("body", body);
    doc.save();
  }

  /** A body drawn from a Zipf vocabulary, so that a few words are in most documents and most words in a few. */
  private static String randomBody(final Random random) {
    final int length = 5 + random.nextInt(60);
    final StringBuilder body = new StringBuilder();
    for (int i = 0; i < length; i++)
      body.append(WORDS[zipf(random)]).append(' ');
    return body.toString();
  }

  private static int zipf(final Random random) {
    return Math.min(VOCABULARY - 1, (int) Math.floor(Math.pow(VOCABULARY + 1, random.nextDouble())) - 1);
  }

  private static String query(final Random random, final int words) {
    final String[] picked = new String[words];
    for (int i = 0; i < words; i++)
      picked[i] = WORDS[random.nextInt(VOCABULARY)];
    return String.join(" ", Arrays.asList(picked));
  }

  private void compactAll(final String indexName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      try {
        assertThat(((IndexInternal) bucketIndex).scheduleCompaction()).isTrue();
        ((IndexInternal) bucketIndex).compact();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
