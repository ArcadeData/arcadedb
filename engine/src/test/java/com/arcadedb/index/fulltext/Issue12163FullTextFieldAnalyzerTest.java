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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12163: the per-field analyzer of a full-text index ({@code "<field>_analyzer"} in METADATA) was stored and never used.
 * A property with one is analyzed with it for the postings a field-qualified query reads, and the qualified query is analyzed with
 * it too. The unprefixed postings, which an unqualified query reads across every property, stay with the analyzer of the index. An
 * index whose definition names a per-field analyzer but whose postings were written without it (by a version before this fix)
 * keeps answering as it was written until it is rebuilt.
 */
class Issue12163FullTextFieldAnalyzerTest extends TestHelper {
  private static final String ENGLISH = "org.apache.lucene.analysis.en.EnglishAnalyzer";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article BUCKETS 3");
      database.command("sql", "CREATE PROPERTY Article.k INTEGER");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
    });
  }

  @Test
  void aQualifiedQueryUsesTheAnalyzerOfItsField() {
    createIndex(", \"title_analyzer\": \"" + ENGLISH + "\"");
    insertArticles();

    // title is stemmed by EnglishAnalyzer: Running is indexed as run, and the query term is stemmed the same way
    assertThat(search("title:run").keySet()).containsExactly(1);
    assertThat(search("title:running").keySet()).containsExactly(1);
    assertThat(search("title:runs").keySet()).containsExactly(1);
    assertThat(search("title:RUN*").keySet()).containsExactly(1);
    assertThat(search("title:\"run fast\"").keySet()).containsExactly(1);
    // body keeps the analyzer of the index
    assertThat(search("body:running").keySet()).containsExactly(1);
    assertThat(search("body:run").keySet()).isEmpty();
    // an unqualified query reads the unprefixed postings, analyzed by the analyzer of the index
    assertThat(search("running").keySet()).containsExactlyInAnyOrder(1);
    assertThat(search("run").keySet()).isEmpty();
    // SEARCH_FIELDS on the field is the same search as the qualified one
    assertThat(searchFields("['title']", "runs").keySet()).containsExactly(1);
  }

  @Test
  void theAnalyzerOfTheOnlyPropertyIsTheAnalyzerOfAOnePropertyIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.k INTEGER");
      database.command("sql", "CREATE PROPERTY Doc.t STRING");
      database.command("sql", "CREATE INDEX ON Doc (t) FULL_TEXT METADATA {\"t_analyzer\": \"" + ENGLISH + "\"}");
      database.command("sql", "INSERT INTO Doc SET k = 1, t = 'Running fast'");
    });
    final ResultSet rs = database.query("sql", "SELECT k FROM Doc WHERE SEARCH_INDEX('Doc[t]', 'runs') = true");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("k")).isEqualTo(1);
    assertThat(database.query("sql", "SELECT k FROM Doc WHERE t CONTAINSTEXT 'runs'").hasNext()).isTrue();
  }

  @Test
  void theDirectLookupUsesTheAnalyzerOfTheField() {
    createIndex(", \"title_analyzer\": \"" + ENGLISH + "\"");
    insertArticles();
    assertThat(database.query("sql", "SELECT k FROM Article WHERE title CONTAINSTEXT 'runs'").hasNext()).isTrue();
    assertThat(database.query("sql", "SELECT k FROM Article WHERE body CONTAINSTEXT 'runs'").hasNext()).isFalse();
  }

  @Test
  void anIndexWrittenWithoutItsFieldAnalyzerAnswersAsWrittenUntilRebuilt() throws IOException {
    // as a version before the fix wrote it: the definition names the analyzer, the postings were analyzed without it
    createIndex("");
    insertArticles();
    database.close();
    final Path schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME).toPath();
    final JSONObject schema = new JSONObject(Files.readString(schemaFile));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Article").getJSONObject("indexes");
    for (final String name : indexes.keySet())
      indexes.getJSONObject(name).put("title_analyzer", ENGLISH);
    Files.writeString(schemaFile, schema.toString());
    reopenDatabase();

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title,body]");
    final LSMTreeFullTextIndex bucketIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
    assertThat(bucketIndex.getFullTextMetadata().getAnalyzerClass("title")).isEqualTo(ENGLISH);
    assertThat(bucketIndex.getUpgradeWarning()).contains("per-field analyzers").contains("title");

    // the postings hold running, not run: the index answers as they were written, never with a stemmed query against them
    assertThat(search("title:running").keySet()).containsExactly(1);
    assertThat(search("title:run").keySet()).isEmpty();
    database.transaction(() -> database.command("sql", "INSERT INTO Article SET k = 3, title = 'Running late', body = 'x'"));
    assertThat(search("title:running").keySet()).containsExactlyInAnyOrder(1, 3);

    database.command("sql", "REBUILD INDEX `Article[title,body]`");
    assertThat(search("title:run").keySet()).containsExactlyInAnyOrder(1, 3);
    assertThat(search("title:running").keySet()).containsExactlyInAnyOrder(1, 3);
    final LSMTreeFullTextIndex rebuilt = (LSMTreeFullTextIndex) ((TypeIndex) database.getSchema()
        .getIndexByName("Article[title,body]")).getIndexesOnBuckets()[0];
    assertThat(rebuilt.getUpgradeWarning()).isNull();

    reopenDatabase();
    assertThat(search("title:run").keySet()).containsExactlyInAnyOrder(1, 3);
  }

  @Test
  void theFieldAnalyzerSurvivesAReopen() {
    createIndex(", \"title_analyzer\": \"" + ENGLISH + "\"");
    insertArticles();
    reopenDatabase();
    assertThat(search("title:run").keySet()).containsExactly(1);
    database.transaction(() -> database.command("sql", "INSERT INTO Article SET k = 3, title = 'Runs daily', body = 'x'"));
    assertThat(search("title:run").keySet()).containsExactlyInAnyOrder(1, 3);
  }

  @Test
  void aPropertyValueReachesItsAnalyzerWhole() {
    for (final String similarity : new String[] { "CLASSIC", "BM25" }) {
      final String type = "Place" + similarity;
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".title STRING");
        database.command("sql", "CREATE PROPERTY " + type + ".body STRING");
        database.command("sql", "CREATE INDEX ON " + type + " (title, body) FULL_TEXT METADATA {\"similarity\": \"" + similarity
            + "\", \"title_analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");
        database.command("sql", "INSERT INTO " + type + " SET title = 'New York', body = 'big apple'");
      });
      // KeywordAnalyzer stored the title as the one token "New York": the value is looked up whole, not word by word
      assertThat(database.query("sql", "SELECT FROM " + type + " WHERE title CONTAINSTEXT 'New York'").hasNext()).as(similarity)
          .isTrue();
      assertThat(database.query("sql", "SELECT FROM " + type + " WHERE title CONTAINSTEXT 'New'").hasNext()).as(similarity)
          .isFalse();
      // body keeps the analyzer of the index, which splits the value into words
      assertThat(database.query("sql", "SELECT FROM " + type + " WHERE body CONTAINSTEXT 'apple'").hasNext()).as(similarity)
          .isTrue();
      assertThat(database.query("sql",
          "SELECT FROM " + type + " WHERE title CONTAINSTEXT 'New York' AND body CONTAINSTEXT 'big apple'").hasNext()).as(
          similarity).isTrue();
    }
  }

  @Test
  void aWildcardOnAPropertyWithoutAnAnalyzerOfItsOwnKeepsTheQueryAnalyzer() {
    // a property named like the field the analyzers normalize under must not lend its analyzer to every other wildcard
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Note");
      database.command("sql", "CREATE PROPERTY Note.k INTEGER");
      database.command("sql", "CREATE PROPERTY Note.contents STRING");
      database.command("sql", "CREATE PROPERTY Note.body STRING");
      database.command("sql", "CREATE INDEX ON Note (contents, body) FULL_TEXT METADATA {"
          + "\"analyzer\": \"org.apache.lucene.analysis.core.WhitespaceAnalyzer\", "
          + "\"contents_analyzer\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"}");
      database.command("sql", "INSERT INTO Note SET k = 1, contents = 'x', body = 'Foo'");
    });
    for (final String query : new String[] { "body:Fo*", "Fo*" }) {
      final ResultSet rs = database.query("sql", "SELECT k FROM Note WHERE SEARCH_INDEX('Note[contents,body]', :q) = true",
          Map.of("q", query));
      assertThat(rs.hasNext()).as(query).isTrue();
      assertThat(rs.next().<Integer>getProperty("k")).isEqualTo(1);
    }
    // the property with the analyzer of its own still folds its own wildcard
    database.transaction(() -> database.command("sql", "INSERT INTO Note SET k = 2, contents = 'Bar', body = 'y'"));
    assertThat(database.query("sql", "SELECT k FROM Note WHERE SEARCH_INDEX('Note[contents,body]', 'contents:BA*') = true")
        .hasNext()).isTrue();
  }

  private void createIndex(final String extraMetadata) {
    database.transaction(() -> database.command("sql",
        "CREATE INDEX ON Article (title, body) FULL_TEXT METADATA {\"analyzer\": "
            + "\"org.apache.lucene.analysis.standard.StandardAnalyzer\"" + extraMetadata + "}"));
  }

  private void insertArticles() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Article SET k = 1, title = 'Running fast', body = 'running slow'");
      database.command("sql", "INSERT INTO Article SET k = 2, title = 'Graph', body = 'nodes'");
    });
  }

  private Map<Integer, Float> search(final String query) {
    return collect(database.query("sql",
        "SELECT k, $score AS s FROM Article WHERE SEARCH_INDEX('Article[title,body]', :q) = true", Map.of("q", query)));
  }

  private Map<Integer, Float> searchFields(final String fields, final String query) {
    return collect(database.query("sql", "SELECT k, $score AS s FROM Article WHERE SEARCH_FIELDS(" + fields + ", :q) = true",
        Map.of("q", query)));
  }

  private static Map<Integer, Float> collect(final ResultSet rs) {
    final Map<Integer, Float> scores = new HashMap<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      scores.put(row.<Integer>getProperty("k"), ((Number) row.getProperty("s")).floatValue());
    }
    return scores;
  }
}
