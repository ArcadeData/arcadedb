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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12462: a fuzzy term expands to at most {@code maxExpansions} indexed terms (50 by default), the best ones as Lucene's
 * FuzzyQuery ranks them, across the whole type rather than per bucket. The expected answers come from Lucene itself, run on the
 * same documents with the same analyzer.
 */
class Issue12462FuzzyMaxExpansionsTest extends TestHelper {

  /** More terms within one or two edits of "abcd" than a fuzzy term may expand to, of several lengths, so the ranking matters. */
  private static final List<String> TOKENS = tokens();

  private static final List<String> QUERIES = List.of("abcd~1", "abcd~2", "abcd~", "abc~1", "abcde~1", "bacd~1", "abcd~1 OR zzzz",
      "common -abcd~1", "+common +abcd~1", "abcd~1^3 abce~1", "abcd~1 -abce~1");

  /**
   * {@link #QUERIES} but the optional clause beside a prohibited one: with five expansions some bucket holds no match for it, and
   * such a bucket answers its whole content (#12474, a defect of the boolean evaluation, not of the fuzzy expansion).
   */
  private static final List<String> QUERIES_WITHOUT_SHOULD_AND_MUST_NOT = QUERIES.stream()
      .filter(query -> !query.equals("abcd~1 -abce~1")).toList();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      createType("Doc", "BUCKETS 4", "");
      createType("Classic", "BUCKETS 4", " METADATA {\"similarity\": \"CLASSIC\"}");
      createType("Single", "BUCKETS 1", "");
      createType("Few", "BUCKETS 4", " METADATA {\"fuzzyMaxExpansions\": 5}");

      database.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 4");
      database.command("sql", "CREATE PROPERTY Article.k INTEGER");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");
      for (int k = 0; k < TOKENS.size(); k++)
        database.command("sql", "INSERT INTO Article SET k = ?, title = ?, body = ?", k, TOKENS.get(k),
            TOKENS.get(TOKENS.size() - 1 - k));
    });
  }

  @Test
  void theCapBinds() {
    // the corpus holds far more terms within one edit of "abcd" than a fuzzy term may expand to
    final long withinOneEdit = TOKENS.stream().filter(t -> FullTextQueryExecutor.fuzzyDistance(t, "abcd", true, 1) <= 1).count();
    assertThat(withinOneEdit).isGreaterThan(3L * FullTextIndexMetadata.DEFAULT_FUZZY_MAX_EXPANSIONS);
    assertThat(search("Doc", "Doc[t]", "abcd~1")).hasSize(FullTextIndexMetadata.DEFAULT_FUZZY_MAX_EXPANSIONS);
  }

  @Test
  void aBM25TypeOfSeveralBucketsMatchesWhatLuceneMatches() throws Exception {
    assertAgreesWithLucene("Doc", FuzzyQuery.defaultMaxExpansions, QUERIES);
  }

  @Test
  void aClassicTypeOfSeveralBucketsMatchesWhatLuceneMatches() throws Exception {
    assertAgreesWithLucene("Classic", FuzzyQuery.defaultMaxExpansions, QUERIES);
  }

  @Test
  void aTypeOfOneBucketMatchesWhatLuceneMatches() throws Exception {
    assertAgreesWithLucene("Single", FuzzyQuery.defaultMaxExpansions, QUERIES);
  }

  @Test
  void theIndexSetsItsOwnMaxExpansions() throws Exception {
    assertAgreesWithLucene("Few", 5, QUERIES_WITHOUT_SHOULD_AND_MUST_NOT);
    assertThat(search("Few", "Few[t]", "abcd~1")).hasSize(5);
  }

  @Test
  void aFieldQualifiedFuzzyTermMatchesWhatLuceneMatches() throws Exception {
    try (final ByteBuffersDirectory directory = luceneIndex(true)) {
      for (final String query : List.of("title:abcd~1", "body:abcd~2", "title:abcd~1 OR body:abcd~1"))
        assertThat(search("Article", "Article[title,body]", query)).as(query)
            .isEqualTo(lucene(directory, query, FuzzyQuery.defaultMaxExpansions));
    }
  }

  /**
   * The index orders its keys by UTF-8 bytes, which is code point order: a character of the basic plane above the surrogates
   * (fullwidth c, U+FF43) sorts before a supplementary one (mathematical bold c, U+1D41C), the reverse of their UTF-16 order. The
   * walk over the chosen expansions must be bounded in the index's order, or it starts past a chosen key or stops before one.
   */
  @Test
  void aFuzzyTermReachesExpansionsWhoseUtf16OrderDiffersFromTheIndexOrder() throws Exception {
    final List<String> wide = List.of("ab\uFF43", "ab\uD835\uDC1C", "ab\uFF43z");
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Wide BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Wide.k INTEGER");
      database.command("sql", "CREATE PROPERTY Wide.t STRING");
      database.command("sql", "CREATE INDEX ON Wide (t) FULL_TEXT");
      for (int k = 0; k < wide.size(); k++)
        database.command("sql", "INSERT INTO Wide SET k = ?, t = ?", k, wide.get(k));
    });
    try (final ByteBuffersDirectory directory = new ByteBuffersDirectory()) {
      try (final IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
        for (int k = 0; k < wide.size(); k++) {
          final Document document = new Document();
          document.add(new StoredField("k", k));
          document.add(new TextField("t", wide.get(k), Field.Store.NO));
          writer.addDocument(document);
        }
      }
      for (final String query : List.of("abc~1", "abc~2", "ab\uFF43~1", "ab\uD835\uDC1C~1")) {
        final Set<Integer> expected = lucene(directory, query, FuzzyQuery.defaultMaxExpansions);
        assertThat(expected).as(query).hasSizeGreaterThan(1);
        assertThat(search("Wide", "Wide[t]", query)).as(query).isEqualTo(expected);
      }
    }
  }

  @Test
  void theExplanationShowsTheChosenExpansionsOnly() {
    final JSONObject explain = FullTextSearch.explainScoring((TypeIndex) database.getSchema().getIndexByName("Doc[t]"), "abcd~1");
    assertThat(explain.getJSONArray("terms").length()).isEqualTo(FullTextIndexMetadata.DEFAULT_FUZZY_MAX_EXPANSIONS);
    assertThat(explain.has("termsTruncated")).isFalse();
  }

  @Test
  void theMaxExpansionsSurvivesAReopen() {
    reopenDatabase();
    assertThat(fullTextMetadata("Few[t]").getFuzzyMaxExpansions()).isEqualTo(5);
    assertThat(search("Few", "Few[t]", "abcd~1")).hasSize(5);
    assertThat(fullTextMetadata("Doc[t]").getFuzzyMaxExpansions()).isEqualTo(FullTextIndexMetadata.DEFAULT_FUZZY_MAX_EXPANSIONS);
  }

  private FullTextIndexMetadata fullTextMetadata(final String indexName) {
    return ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[0])
        .getFullTextMetadata();
  }

  @Test
  void aMaxExpansionsThatIsNotPositiveIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Bad");
    database.command("sql", "CREATE PROPERTY Bad.t STRING");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Bad (t) FULL_TEXT METADATA {\"fuzzyMaxExpansions\": 0}"))
        .hasMessageContaining("fuzzyMaxExpansions");
    assertThatThrownBy(() -> new FullTextIndexMetadata("Bad", new String[] { "t" }, -1).setFuzzyMaxExpansions(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theBoostIsTheOneLuceneRanksExpansionsBy() {
    assertThat(FullTextQueryExecutor.fuzzyBoost("abcd", 0, 4)).isEqualTo(1.0f);
    assertThat(FullTextQueryExecutor.fuzzyBoost("abce", 1, 4)).isEqualTo(0.75f);
    // an edit in a shorter term costs more
    assertThat(FullTextQueryExecutor.fuzzyBoost("abc", 1, 4)).isEqualTo(1.0f - 1.0f / 3.0f);
    assertThat(FullTextQueryExecutor.fuzzyBoost("abcde", 1, 4)).isEqualTo(0.75f);
  }

  @Test
  void termsOfTheSameBoostAreRankedInCodePointOrder() {
    assertThat(FullTextQueryExecutor.compareCodePoints("abc", "abd")).isNegative();
    assertThat(FullTextQueryExecutor.compareCodePoints("abc", "abcd")).isNegative();
    assertThat(FullTextQueryExecutor.compareCodePoints("abc", "abc")).isZero();
    // a supplementary character comes after every character of the basic plane, as in UTF-8, unlike in UTF-16 order
    assertThat(FullTextQueryExecutor.compareCodePoints("a😀", "a�")).isPositive();
    assertThat("a😀".compareTo("a�")).isNegative();
  }

  private void assertAgreesWithLucene(final String type, final int maxExpansions, final List<String> queries) throws Exception {
    try (final ByteBuffersDirectory directory = luceneIndex(false)) {
      for (final String query : queries)
        assertThat(search(type, type + "[t]", query)).as(type + ": " + query).isEqualTo(lucene(directory, query, maxExpansions));
    }
  }

  private void createType(final String type, final String buckets, final String metadata) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " " + buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (t) FULL_TEXT" + metadata);
    for (int k = 0; k < TOKENS.size(); k++)
      database.command("sql", "INSERT INTO " + type + " SET k = ?, t = ?", k, TOKENS.get(k) + " common");
  }

  private Set<Integer> search(final String type, final String index, final String query) {
    final Set<Integer> keys = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM " + type + " WHERE SEARCH_INDEX('" + index + "', :q) = true",
        Map.of("q", query))) {
      while (rs.hasNext())
        keys.add(rs.next().<Integer>getProperty("k"));
    }
    return keys;
  }

  private static ByteBuffersDirectory luceneIndex(final boolean articles) throws Exception {
    final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try (final IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (int k = 0; k < TOKENS.size(); k++) {
        final Document document = new Document();
        document.add(new StoredField("k", k));
        if (articles) {
          document.add(new TextField("title", TOKENS.get(k), Field.Store.NO));
          document.add(new TextField("body", TOKENS.get(TOKENS.size() - 1 - k), Field.Store.NO));
        } else
          document.add(new TextField("t", TOKENS.get(k) + " common", Field.Store.NO));
        writer.addDocument(document);
      }
    }
    return directory;
  }

  private static Set<Integer> lucene(final ByteBuffersDirectory directory, final String queryText, final int maxExpansions)
      throws Exception {
    final Analyzer analyzer = new StandardAnalyzer();
    final QueryParser parser = new QueryParser("t", analyzer) {
      @Override
      protected Query newFuzzyQuery(final Term term, final float minimumSimilarity, final int prefixLength) {
        final String text = term.text();
        return new FuzzyQuery(term, FuzzyQuery.floatToEdits(minimumSimilarity, text.codePointCount(0, text.length())),
            prefixLength, maxExpansions, FuzzyQuery.defaultTranspositions);
      }
    };
    final Set<Integer> keys = new HashSet<>();
    try (final DirectoryReader reader = DirectoryReader.open(directory)) {
      final IndexSearcher searcher = new IndexSearcher(reader);
      final StoredFields stored = searcher.storedFields();
      for (final ScoreDoc hit : searcher.search(parser.parse(queryText), 100_000).scoreDocs)
        keys.add(stored.document(hit.doc).getField("k").numericValue().intValue());
    }
    return keys;
  }

  private static List<String> tokens() {
    final Set<String> tokens = new LinkedHashSet<>();
    final String base = "abcd";
    tokens.add(base);
    for (int i = 0; i < base.length(); i++)
      for (char c = 'a'; c <= 'z'; c++) {
        // one substitution, one insertion, one deletion
        tokens.add(base.substring(0, i) + c + base.substring(i + 1));
        tokens.add(base.substring(0, i) + c + base.substring(i));
        tokens.add(base.substring(0, i) + base.substring(i + 1));
      }
    for (char c = 'a'; c <= 'z'; c++)
      tokens.add(base + c);
    // two edits
    for (char c = 'a'; c <= 'z'; c++) {
      tokens.add("zz" + c + "d");
      tokens.add("a" + c + "c" + c);
    }
    tokens.add("zzzz");
    // a term far from every query, so a prohibited clause leaves something
    tokens.add("unrelated");
    return new ArrayList<>(tokens);
  }
}
