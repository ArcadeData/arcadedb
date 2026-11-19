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
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
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
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12505: Lucene's FuzzyQuery keeps at most {@link IndexSearcher#getMaxClauseCount()} (1024 by default) expansions whatever
 * {@code maxExpansions} it is given, so a {@code fuzzyMaxExpansions} above that is refused when an index is defined, and one
 * already persisted above it still opens and behaves as the cap, as Lucene does.
 */
class Issue12505FuzzyMaxExpansionsUpperBoundTest extends TestHelper {

  private static final int ABOVE_THE_CAP = 100_000;

  @Test
  void theCapIsLucenesDefaultMaxClauseCount() {
    assertThat(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS).isEqualTo(IndexSearcher.getMaxClauseCount());
    assertThat(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS).isEqualTo(1024);
  }

  @Test
  void sqlCreateIndexRefusesAValueAboveTheCap() {
    createType("Bad");
    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX ON Bad (t) FULL_TEXT METADATA {\"fuzzyMaxExpansions\": " + ABOVE_THE_CAP + "}"))
        .hasMessageContaining("fuzzyMaxExpansions").hasMessageContaining("1024");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON Bad (t) FULL_TEXT METADATA {\"fuzzyMaxExpansions\": 1025}"))
        .hasMessageContaining("fuzzyMaxExpansions");
    assertThat(database.getSchema().existsIndex("Bad[t]")).isFalse();

    database.command("sql", "CREATE INDEX ON Bad (t) FULL_TEXT METADATA {\"fuzzyMaxExpansions\": 1024}");
    assertThat(fullTextMetadata("Bad[t]").getFuzzyMaxExpansions()).isEqualTo(1024);
  }

  @Test
  void cypherCreateFulltextIndexRefusesAValueAboveTheCap() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CREATE FULLTEXT INDEX cypherBad FOR (n:CypherBad) ON EACH [n.t] OPTIONS {indexConfig: {fuzzyMaxExpansions: " + ABOVE_THE_CAP
            + "}}")).hasMessageContaining("fuzzyMaxExpansions");
  }

  @Test
  void theBuilderRefusesAValueAboveTheCap() {
    createType("Built");
    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Built", new String[] { "t" })
        .withType(Schema.INDEX_TYPE.FULL_TEXT).withFullTextType().withFuzzyMaxExpansions(1025))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1024");
    assertThatThrownBy(() -> new FullTextIndexMetadata("Built", new String[] { "t" }, -1).setFuzzyMaxExpansions(Integer.MAX_VALUE))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * An index persisted with a value above the cap (written before the cap existed) still opens, and expands a fuzzy term to the
   * cap's best terms: the same matches as Lucene given that same value, which it clamps the same way. The corpus holds more terms
   * within two edits of the query than the cap, so the clamp is what decides the answer.
   */
  @Test
  void aPersistedValueAboveTheCapOpensAndBehavesAsLuceneDoes() throws Exception {
    final List<String> tokens = tokens();
    assertThat(tokens.stream().filter(t -> FullTextQueryExecutor.fuzzyDistance(t, "abcd", true, 2) <= 2).count())
        .isGreaterThan(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS);

    database.transaction(() -> {
      createType("Legacy");
      database.command("sql", "CREATE INDEX ON Legacy (t) FULL_TEXT METADATA {\"fuzzyMaxExpansions\": 5}");
      for (int k = 0; k < tokens.size(); k++)
        database.command("sql", "INSERT INTO Legacy SET k = ?, t = ?", k, tokens.get(k));
    });

    database.close();
    final File schemaFile = new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(FileUtils.readFileAsString(schemaFile));
    assertThat(replaceFuzzyMaxExpansions(schema, ABOVE_THE_CAP)).as("the schema file carries the index's fuzzyMaxExpansions")
        .isPositive();
    Files.write(schemaFile.toPath(), schema.toString().getBytes(StandardCharsets.UTF_8));
    database = factory.open();

    assertThat(fullTextMetadata("Legacy[t]").getFuzzyMaxExpansions()).isEqualTo(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS);

    try (final ByteBuffersDirectory directory = luceneIndex(tokens)) {
      for (final String query : List.of("abcd~2", "abcd~1")) {
        final Set<Integer> expected = lucene(directory, query, ABOVE_THE_CAP);
        assertThat(search("Legacy", "Legacy[t]", query)).as(query).isEqualTo(expected);
      }
      assertThat(lucene(directory, "abcd~2", ABOVE_THE_CAP)).hasSize(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS);
    }

    // every later open reads it as the cap too
    reopenDatabase();
    assertThat(fullTextMetadata("Legacy[t]").getFuzzyMaxExpansions()).isEqualTo(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS);
    assertThat(search("Legacy", "Legacy[t]", "abcd~2")).hasSize(FullTextIndexMetadata.MAX_FUZZY_MAX_EXPANSIONS);
  }

  private void createType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 2");
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
  }

  private FullTextIndexMetadata fullTextMetadata(final String indexName) {
    return ((LSMTreeFullTextIndex) ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets()[0])
        .getFullTextMetadata();
  }

  private static int replaceFuzzyMaxExpansions(final Object node, final int value) {
    int replaced = 0;
    if (node instanceof JSONObject object) {
      for (final String key : new ArrayList<>(object.keySet()))
        if (key.equals("fuzzyMaxExpansions")) {
          object.put(key, value);
          replaced++;
        } else
          replaced += replaceFuzzyMaxExpansions(object.get(key), value);
    } else if (node instanceof JSONArray array)
      for (int i = 0; i < array.length(); i++)
        replaced += replaceFuzzyMaxExpansions(array.get(i), value);
    return replaced;
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

  private static ByteBuffersDirectory luceneIndex(final List<String> tokens) throws Exception {
    final ByteBuffersDirectory directory = new ByteBuffersDirectory();
    try (final IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (int k = 0; k < tokens.size(); k++) {
        final Document document = new Document();
        document.add(new StoredField("k", k));
        document.add(new TextField("t", tokens.get(k), Field.Store.NO));
        writer.addDocument(document);
      }
    }
    return directory;
  }

  private static Set<Integer> lucene(final ByteBuffersDirectory directory, final String queryText, final int maxExpansions)
      throws Exception {
    final int tilde = queryText.indexOf('~');
    final Term term = new Term("t", queryText.substring(0, tilde));
    final int edits = Integer.parseInt(queryText.substring(tilde + 1));
    final FuzzyQuery query = new FuzzyQuery(term, edits, 0, maxExpansions, FuzzyQuery.defaultTranspositions);
    final Set<Integer> keys = new HashSet<>();
    try (final DirectoryReader reader = DirectoryReader.open(directory)) {
      final IndexSearcher searcher = new IndexSearcher(reader);
      final StoredFields stored = searcher.storedFields();
      for (final ScoreDoc hit : searcher.search(query, 100_000).scoreDocs)
        keys.add(stored.document(hit.doc).getField("k").numericValue().intValue());
    }
    return keys;
  }

  /** Every term of the forms ??cd and ab?? (lowercase letters): well over 1024 terms within two edits of "abcd". */
  private static List<String> tokens() {
    final Set<String> tokens = new LinkedHashSet<>();
    for (char a = 'a'; a <= 'z'; a++)
      for (char b = 'a'; b <= 'z'; b++) {
        tokens.add("" + a + b + "cd");
        tokens.add("ab" + a + b);
      }
    tokens.add("unrelated");
    return new ArrayList<>(tokens);
  }
}
