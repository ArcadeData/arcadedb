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
package com.arcadedb.function.sql.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextRecordScorer;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11683: {@code SEARCH_INDEX} evaluated on the rows of a small target, such as a list of RIDs, computed every match of
 * the query in the whole type on the first row, so that the next rows could be read from it. Each row is now scored from its own
 * record, with the analyzer of the index and the statistics the complete search uses, and gets exactly the match and the score
 * the complete search gives it.
 */
class Issue11683SearchIndexSmallTargetTest extends TestHelper {
  private static final String[] WORDS      = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta" };
  // how often each word is drawn: alpha in most documents, zeta in about a quarter
  private static final int[]    WEIGHTS    = { 30, 16, 12, 8, 6, 3 };
  private static final int      DOCUMENTS  = 4_000;
  private static final int      SAMPLE     = 16;
  private static final String[] ISSUE_WORDS = { "capital", "loan", "risk", "liquidity", "basel", "credit", "deposit", "rate",
      "policy", "audit", "report", "fee" };

  /** Queries every row of which is decided on its record. */
  private static final List<String> PER_RECORD = List.of(//
      "alpha", "zeta", "alpha beta", "alpha beta gamma delta", "zeta epsilon delta gamma beta alpha", "+alpha +beta",
      "+alpha -gamma", "+zeta -alpha", "\"alpha beta\"", "\"beta zeta\"", "alpha^3 beta gamma^0.5", "+delta (alpha zeta)",
      "alpha (+beta -zeta)", "alpha missingword", "content:alpha zeta");

  /** Queries the record alone does not decide: the complete search answers them, as before. */
  private static final List<String> COMPLETE = List.of("alph*", "zet?", "alpha -beta", "-alpha", "alpah~", "missingword");

  @ParameterizedTest
  @ValueSource(ints = { 1, 4 })
  void aRidListTargetGetsTheMatchesAndScoresOfTheCompleteSearch(final int buckets) {
    createCorpus("Doc", buckets, false);
    final List<RID> sample = sample("Doc");

    for (final String query : PER_RECORD) {
      final Map<RID, Float> complete = completeAnswer("SEARCH_INDEX('Doc[content]', :q)", "Doc", query);
      final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
      final Map<RID, Float> rows = targetAnswer("SEARCH_INDEX('Doc[content]', :q)", sample, query);
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).as(query).isEqualTo(before);
      assertThat(rows).as(query).isEqualTo(restrictedTo(complete, sample));
    }
    // at least one of the sampled rows matches and one does not, for the common and the rare term alike
    assertThat(targetAnswer("SEARCH_INDEX('Doc[content]', :q)", sample, "zeta")).isNotEmpty().hasSizeLessThan(SAMPLE);

    for (final String query : COMPLETE) {
      final Map<RID, Float> complete = completeAnswer("SEARCH_INDEX('Doc[content]', :q)", "Doc", query);
      final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
      final Map<RID, Float> rows = targetAnswer("SEARCH_INDEX('Doc[content]', :q)", sample, query);
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).as(query).isEqualTo(before + 1);
      assertThat(rows).as(query).isEqualTo(restrictedTo(complete, sample));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 4 })
  void aMultiPropertyIndexAndSearchFieldsScoreTheRowsAsTheCompleteSearch(final int buckets) {
    createCorpus("Art", buckets, true);
    final List<RID> sample = sample("Art");

    for (final String query : List.of("alpha", "title:alpha", "title:alpha body:beta gamma", "body:\"alpha zeta\"",
        "+title:beta -body:gamma", "zeta title:zeta^2 body:delta")) {
      final Map<RID, Float> complete = completeAnswer("SEARCH_INDEX('Art[title,body]', :q)", "Art", query);
      final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
      assertThat(targetAnswer("SEARCH_INDEX('Art[title,body]', :q)", sample, query)).as(query)
          .isEqualTo(restrictedTo(complete, sample));
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).as(query).isEqualTo(before);
    }

    for (final String fields : List.of("['title']", "['body']", "['title', 'body']")) {
      for (final String query : List.of("alpha beta", "gamma delta", "+gamma -alpha")) {
        final String condition = "SEARCH_FIELDS(" + fields + ", :q)";
        final Map<RID, Float> complete = completeAnswer(condition, "Art", query);
        final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
        assertThat(targetAnswer(condition, sample, query)).as(fields + " " + query).isEqualTo(restrictedTo(complete, sample));
        assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).as(fields + " " + query).isEqualTo(before);
      }
    }
  }

  /** Every record of the type, scored on its own, against the complete search: the same matches, and scores equal to the bit. */
  @ParameterizedTest
  @ValueSource(ints = { 1, 4 })
  void everyRecordScoredOnItsOwnEqualsTheCompleteSearch(final int buckets) {
    createCorpus("Doc", buckets, false);
    createCorpus("Art", buckets, true);
    final TypeIndex content = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");
    final TypeIndex titleAndBody = (TypeIndex) database.getSchema().getIndexByName("Art[title,body]");
    final List<Document> docs = documents("Doc");
    final List<Document> arts = documents("Art");

    for (final String query : PER_RECORD)
      assertScoredOnItsOwn(content, query, null, docs);
    for (final String query : List.of("alpha", "title:alpha", "title:alpha body:beta gamma", "body:\"alpha zeta\"",
        "+title:beta -body:gamma", "zeta title:zeta^2 body:delta", "zeta epsilon delta gamma beta alpha"))
      assertScoredOnItsOwn(titleAndBody, query, null, arts);
    for (final List<String> fields : List.of(List.of("title"), List.of("body"), List.of("title", "body")))
      for (final String query : List.of("alpha beta", "zeta", "+gamma -alpha", "\"beta zeta\" delta^2"))
        assertScoredOnItsOwn(titleAndBody, query, fields, arts);

    for (final String query : List.of("alph*", "zet?", "alpha -beta", "-alpha", "alpah~", "/al.*/"))
      assertThat(FullTextRecordScorer.prepare(content, query, null, WorkGuard.forCommandDeadline(null))).as(query).isNull();
  }

  private static void assertScoredOnItsOwn(final TypeIndex index, final String query, final List<String> fields,
      final List<Document> documents) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(null);
    final Map<RID, Float> complete = FullTextSearch.search(index, query, -1, guard, fields, null);
    final FullTextRecordScorer scorer = FullTextRecordScorer.prepare(index, query, fields, guard);
    assertThat(scorer).as(query).isNotNull();
    final Map<RID, Float> own = new HashMap<>();
    final long[] analyzed = new long[1];
    for (final Document document : documents) {
      assertThat(scorer.covers(document.getIdentity())).isTrue();
      final float score = scorer.score(document, analyzed);
      assertThat(score).as(query).isNotNaN();
      if (score != FullTextRecordScorer.NO_MATCH)
        own.put(document.getIdentity(), score);
    }
    assertThat(complete).as(query).isNotEmpty();
    assertThat(own).as(fields + " " + query).isEqualTo(complete);
  }

  private List<Document> documents(final String type) {
    final List<Document> documents = new ArrayList<>();
    database.iterateType(type, true).forEachRemaining(record -> documents.add(record.asDocument()));
    return documents;
  }

  /**
   * The shape of the issue: 100 RIDs ranked by BM25 for a term most of the type holds. No row needs the complete search; a target
   * as large as the type still gets it, once, and the same answer.
   */
  @Test
  void aSmallTargetDoesNotComputeEveryMatchOfTheType() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Chunk BUCKETS 8");
      database.command("sql", "CREATE PROPERTY Chunk.text STRING");
      database.command("sql", "CREATE INDEX ON Chunk (text) FULL_TEXT");
      final Random random = new Random(11683);
      for (int i = 0; i < 10_000; i++) {
        final StringBuilder text = new StringBuilder();
        for (int w = 0; w < 12; w++)
          text.append(ISSUE_WORDS[random.nextInt(ISSUE_WORDS.length)]).append(' ');
        database.newDocument("Chunk").set("text", text.toString()).save();
      }
    });
    final List<RID> all = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Chunk")) {
      rs.stream().forEach(r -> all.add(r.getProperty("rid")));
    }
    final List<RID> first100 = all.subList(0, 100);
    final String condition = "SEARCH_INDEX('Chunk[text]', :q)";
    final Map<RID, Float> complete = completeAnswer(condition, "Chunk", "capital");

    long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
    final List<Result> ranked = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT @rid AS rid, $score AS score FROM " + ridList(first100) + " WHERE " + condition
            + " = true ORDER BY $score DESC LIMIT 10", Map.of("q", "capital"))) {
      rs.stream().forEach(ranked::add);
    }
    assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).isEqualTo(before);
    assertThat(ranked).hasSize(10);
    for (final Result row : ranked)
      assertThat(row.<Float>getProperty("score")).isEqualTo(complete.get(row.<RID>getProperty("rid")));

    // every chunk as the target: past a quarter of the term's postings the complete search answers the remaining rows
    before = FullTextFunctionSearch.getCompleteSearchesForTesting();
    assertThat(targetAnswer(condition, all, "capital")).isEqualTo(complete);
    assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).isEqualTo(before + 1);
  }

  /** A transaction's own changes to the index are part of what the index answers inside it: the complete search answers. */
  @Test
  void pendingChangesToTheIndexAreLeftToTheCompleteSearch() {
    createCorpus("Doc", 4, false);
    final List<RID> sample = sample("Doc");
    database.begin();
    try {
      sample.getFirst().asDocument().modify().set("content", "zeta zeta alpha").save();
      final Map<RID, Float> complete = completeAnswer("SEARCH_INDEX('Doc[content]', :q)", "Doc", "zeta alpha");
      final long before = FullTextFunctionSearch.getCompleteSearchesForTesting();
      final Map<RID, Float> rows = targetAnswer("SEARCH_INDEX('Doc[content]', :q)", sample, "zeta alpha");
      assertThat(FullTextFunctionSearch.getCompleteSearchesForTesting()).isEqualTo(before + 1);
      assertThat(rows).isEqualTo(restrictedTo(complete, sample)).containsKey(sample.getFirst());
    } finally {
      database.rollback();
    }
  }

  private void createCorpus(final String type, final int buckets, final boolean twoProperties) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
      if (twoProperties) {
        database.command("sql", "CREATE PROPERTY " + type + ".title STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
        database.command("sql", "CREATE INDEX ON " + type + " (title, body) FULL_TEXT");
      } else {
        database.command("sql", "CREATE PROPERTY " + type + ".content STRING");
        database.command("sql", "CREATE INDEX ON " + type + " (content) FULL_TEXT");
      }
      final Random random = new Random(buckets * 31L + (twoProperties ? 7 : 0));
      for (int i = 0; i < DOCUMENTS; i++) {
        // lengths from 1 to 30 tokens and repeated words, so term frequencies and document lengths vary
        if (twoProperties)
          database.newDocument(type).set("title", text(random, 1 + random.nextInt(12))).set("body", text(random, 1 + random.nextInt(30)))
              .save();
        else
          database.newDocument(type).set("content", text(random, 1 + random.nextInt(30))).save();
      }
    });
  }

  private static String text(final Random random, final int length) {
    int total = 0;
    for (final int weight : WEIGHTS)
      total += weight;
    final StringBuilder text = new StringBuilder();
    for (int i = 0; i < length; i++) {
      int draw = random.nextInt(total);
      int word = 0;
      while (draw >= WEIGHTS[word])
        draw -= WEIGHTS[word++];
      text.append(WORDS[word]).append(' ');
    }
    return text.toString();
  }

  /** Records spread over the whole type and every bucket of it. */
  private List<RID> sample(final String type) {
    final List<RID> all = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + type)) {
      rs.stream().forEach(r -> all.add(r.getProperty("rid")));
    }
    final List<RID> sample = new ArrayList<>(SAMPLE);
    for (int i = 0; i < SAMPLE; i++)
      sample.add(all.get(i * all.size() / SAMPLE + i));
    return sample;
  }

  /** The indexed fetch over the type: every match with the score the complete search gives it. */
  private Map<RID, Float> completeAnswer(final String condition, final String type, final String query) {
    return answer("SELECT @rid AS rid, $score AS score FROM " + type + " WHERE " + condition + " = true", query);
  }

  /** The condition evaluated on each row of a RID list. */
  private Map<RID, Float> targetAnswer(final String condition, final List<RID> rids, final String query) {
    return answer("SELECT @rid AS rid, $score AS score FROM " + ridList(rids) + " WHERE " + condition + " = true", query);
  }

  private Map<RID, Float> answer(final String sql, final String query) {
    final Map<RID, Float> scores = new HashMap<>();
    try (final ResultSet rs = database.query("sql", sql, Map.of("q", query))) {
      rs.stream().forEach(r -> scores.put(r.getProperty("rid"), r.getProperty("score")));
    }
    return scores;
  }

  private static String ridList(final List<RID> rids) {
    final StringBuilder list = new StringBuilder("[");
    for (final RID rid : rids) {
      if (list.length() > 1)
        list.append(", ");
      list.append(rid);
    }
    return list.append(']').toString();
  }

  private static Map<RID, Float> restrictedTo(final Map<RID, Float> complete, final List<RID> rids) {
    final Map<RID, Float> restricted = new HashMap<>();
    for (final RID rid : rids)
      if (complete.containsKey(rid))
        restricted.put(rid, complete.get(rid));
    return restricted;
  }
}
