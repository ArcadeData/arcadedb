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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12474: a prohibited clause beside optional ones ({@code foo -bar}) answers the records matching the optional clauses less
 * the prohibited ones, as Lucene does, in every bucket: a bucket where no optional clause matched answered all its records but the
 * prohibited ones. A prohibited clause excludes from the group it is written in, not from the whole query, so
 * {@code (foo -bar) baz} answers a record holding {@code bar baz}, as Lucene does. The expected answers are the ones Lucene gives
 * on the same documents with the same analyzer and parser.
 * <p>
 * Every query is run through each path the search takes: a BM25 index over several buckets (bucket-local matching, type-wide
 * scoring), a BM25 index over one bucket, a CLASSIC index over several buckets, and the per-record evaluation of a RID list.
 */
class Issue12474FullTextProhibitedClauseTest extends TestHelper {
  // Inserted in this order into 4 buckets taken in turn: every record holding foo lands in the first bucket, so the 3 others hold
  // none
  private static final String[] TEXTS = { "foo", "bar", "baz", "qux", "foo bar", "bar baz", "baz qux", "other0", "foo baz", "other1",
      "other2", "other3", "foo bar baz", "other4", "other5", "other6", "other7" };

  /** The types, each with a full-text index on {@code t}: BM25 over 4 buckets, BM25 over 1 bucket, CLASSIC over 4 buckets. */
  private static final List<String> TYPES = List.of("Multi", "Single", "Classic");

  /** Queries with a prohibited clause, answered by Lucene as by ArcadeDB. */
  private static final List<String> QUERIES = List.of(//
      "foo -bar",//
      "nomatch -bar",//
      "foo -nomatch",//
      "(foo -bar) baz",//
      "baz (foo -bar)",//
      "foo baz -bar",//
      "+foo -bar",//
      "+foo +baz -bar",//
      "foo AND NOT bar",//
      "foo OR baz NOT bar",//
      "(foo -bar) (baz -qux)",//
      "+(foo -bar) +baz",//
      "foo -(bar -baz)",//
      "foo -(bar baz)",//
      "foo* -bar",//
      "fo~1 -bar",//
      "\"foo bar\" -baz",//
      "other* -other3",//
      "qux -(baz -foo)");

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      createType("Multi", 4, "");
      createType("Single", 1, "");
      createType("Classic", 4, " METADATA {\"similarity\": \"CLASSIC\"}");
      for (final String type : TYPES)
        for (int k = 0; k < TEXTS.length; k++)
          database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, TEXTS[k]);
    });
  }

  @Test
  void aProhibitedClauseBesideOptionalOnesAnswersOnlyTheirMatches() {
    // the case of the issue: 'foo -bar' answered every record of a bucket holding no foo, less bar. Buckets holding no foo exist
    for (final String type : List.of("Multi", "Classic"))
      assertThat(database.query("sql", "SELECT @rid AS rid FROM " + type + " WHERE t LIKE '%foo%'").stream()
          .map(r -> r.<RID>getProperty("rid").getBucketId()).collect(Collectors.toSet())).as(type).hasSize(1);
    for (final String type : TYPES) {
      assertThat(search(type, "foo -bar").keySet()).as(type).containsExactlyInAnyOrder(0, 8);
      assertThat(search(type, "nomatch -bar").keySet()).as(type).isEmpty();
    }
  }

  @Test
  void everyPathAnswersAsLucene() throws Exception {
    final Map<String, Set<Integer>> lucene = lucene(QUERIES);
    for (final String query : QUERIES)
      for (final String type : TYPES)
        assertThat(search(type, query).keySet()).as(type + ": " + query).isEqualTo(lucene.get(query));
  }

  @Test
  void aProhibitedClauseExcludesFromItsOwnGroupOnly() {
    // bar baz (5) and foo bar baz (12) match baz: the -bar of the other group does not exclude them
    for (final String type : TYPES)
      assertThat(search(type, "(foo -bar) baz").keySet()).as(type).containsExactlyInAnyOrder(0, 2, 5, 6, 8, 12);
  }

  @Test
  void thePerRecordEvaluationAgreesWithTheIndex() {
    for (final String type : List.of("Multi", "Single")) {
      final String rids = database.query("sql", "SELECT @rid AS rid FROM " + type).stream()
          .map(r -> r.getProperty("rid").toString()).collect(Collectors.joining(", ", "[", "]"));
      for (final String query : QUERIES)
        assertThat(searchIndex(rids, type + "[t]", query)).as(type + ": " + query).isEqualTo(search(type, query));
    }
  }

  /**
   * A query made only of prohibited clauses answers every record but their matches, where Lucene answers nothing: an ArcadeDB
   * extension, kept, and a group made only of prohibited clauses is that complement too.
   */
  @Test
  void aPurelyProhibitedQueryStillAnswersTheComplement() {
    final Set<Integer> all = new HashSet<>();
    for (int k = 0; k < TEXTS.length; k++)
      all.add(k);
    final Set<Integer> withoutBar = new HashSet<>(all);
    withoutBar.removeAll(Set.of(1, 4, 5, 12));
    for (final String type : TYPES) {
      assertThat(search(type, "-bar").keySet()).as(type).isEqualTo(withoutBar);
      assertThat(search(type, "-nomatch").keySet()).as(type).isEqualTo(all);
      // a group of prohibited clauses, required beside a positive clause: foo less bar
      assertThat(search(type, "+foo +(-bar)").keySet()).as(type).containsExactlyInAnyOrder(0, 8);
      // a prohibited group of prohibited clauses is a double negation: foo and bar
      assertThat(search(type, "foo -(-bar)").keySet()).as(type).containsExactlyInAnyOrder(4, 12);
    }
  }

  private void createType(final String type, final int buckets, final String metadata) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (t) FULL_TEXT" + metadata);
  }

  private Map<Integer, Float> search(final String type, final String query) {
    return searchIndex(type, type + "[t]", query);
  }

  private Map<Integer, Float> searchIndex(final String target, final String index, final String query) {
    final Map<Integer, Float> scores = new HashMap<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT k, $score AS s FROM " + target + " WHERE SEARCH_INDEX('" + index + "', :q) = true", Map.of("q", query))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        scores.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
      }
    }
    return scores;
  }

  /** What Lucene answers for each query, on the same texts, with the same analyzer and the same parser. */
  private static Map<String, Set<Integer>> lucene(final List<String> queries) throws Exception {
    final Map<String, Set<Integer>> answers = new HashMap<>();
    try (final ByteBuffersDirectory directory = new ByteBuffersDirectory(); final StandardAnalyzer analyzer = new StandardAnalyzer()) {
      try (final IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
        for (int k = 0; k < TEXTS.length; k++) {
          final Document document = new Document();
          document.add(new StoredField("k", k));
          document.add(new TextField("t", TEXTS[k], Field.Store.NO));
          writer.addDocument(document);
        }
      }
      try (final DirectoryReader reader = DirectoryReader.open(directory)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final StoredFields storedFields = searcher.storedFields();
        for (final String query : queries) {
          final Set<Integer> keys = new HashSet<>();
          for (final ScoreDoc hit : searcher.search(new QueryParser("t", analyzer).parse(query), 1000).scoreDocs)
            keys.add(storedFields.document(hit.doc).getField("k").numericValue().intValue());
          answers.put(query, keys);
        }
      }
    }
    return answers;
  }
}
