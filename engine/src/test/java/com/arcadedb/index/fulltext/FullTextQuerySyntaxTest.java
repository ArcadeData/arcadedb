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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Lucene query syntax support in full-text indexes.
 */
class FullTextQuerySyntaxTest extends TestHelper {

  @Test
  void booleanMust() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming tutorial'");
    });

    database.transaction(() -> {
      // +java +programming requires BOTH terms
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+java +programming') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only Doc1 has both "java" AND "programming"
      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void booleanMustNot() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java python'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });

    database.transaction(() -> {
      // java -python: must have java, must NOT have python
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'java -python') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only Doc1 has java without python
      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void phraseQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'programming java is fun'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python scripting'");
    });

    database.transaction(() -> {
      // Phrase query: "java programming" (exact sequence)
      // Note: Current implementation requires all terms but doesn't verify order
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '\"java programming\"') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Both Doc1 and Doc2 have both words (order not enforced in current impl)
      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc2");
    });
  }

  @Test
  void prefixWildcard() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'database management'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'datastore engine'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'python programming'");
    });

    database.transaction(() -> {
      // Suffix wildcard: data* should match database, datastore
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'data*') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactlyInAnyOrder("Doc1", "Doc2");
    });
  }

  @Test
  void fuzzyQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'database management'");
    });

    database.transaction(() -> {
      // Fuzzy query: databse~ should match "database" (typo tolerance)
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', 'databse~') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      assertThat(titles).containsExactly("Doc1");
    });
  }

  @Test
  void fieldSpecificQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.body STRING");
      // Multi-property index for field-specific queries
      database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Java Guide', body = 'Learn programming basics'");
      database.command("sql", "INSERT INTO Article SET title = 'Python Tutorial', body = 'Java programming guide'");
    });

    database.transaction(() -> {
      // Field-specific: title:java should only match title field
      final ResultSet result = database.query("sql",
          "SELECT title FROM Article WHERE SEARCH_INDEX('Article[title,body]', 'title:java') = true");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Only first doc has "java" in title
      assertThat(titles).containsExactly("Java Guide");
    });
  }

  @Test
  void combinedBooleanAndPhrase() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'multi model database for enterprise'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'multi model nosql database'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'relational database system'");
    });

    database.transaction(() -> {
      // Combined: must have phrase "multi model", must NOT have "nosql"
      final ResultSet result = database.query("sql",
          """
              SELECT title FROM Article WHERE SEARCH_INDEX('Article[content]', '+"multi model" -nosql') = true""");

      final Set<String> titles = new HashSet<>();
      while (result.hasNext()) {
        titles.add(result.next().getProperty("title"));
      }

      // Doc1 has "multi model" without "nosql"
      // Doc2 has "multi model" but also has "nosql" (excluded)
      assertThat(titles).containsExactly("Doc1");
    });
  }

  // ===== Issue #7000 =====
  // Regression test for issue #7000: the full-text query path and the indexing path derived the posting key and the
  // term normalization independently, and drifted apart in three places.
  // <ol>
  //   <li>a field-qualified PHRASE query looked every term up by its bare text, so {@code title:"..."} also matched a
  //   document carrying the phrase only in {@code body};</li>
  //   <li>a field qualifier on a multi-property index was used verbatim, while the postings of a {@code BY ITEM}
  //   property are prefixed with the modifier-qualified name the index declares, so {@code keywords:x} never matched;</li>
  //   <li>wildcard, prefix, fuzzy and regexp terms were unconditionally lower-cased, while the postings are produced by
  //   the configured analyzer, so on a case-preserving analyzer {@code Fo*} could never reach the stored {@code Foo}.</li>
  // </ol>

  @Test
  @DisplayName("#7000 field qualified phrase is restricted to its field")
  void fieldQualifiedPhraseIsRestrictedToItsField() {
    final DocumentType article = database.getSchema().createDocumentType("Article");
    article.createProperty("title", Type.STRING);
    article.createProperty("body", Type.STRING);
    database.command("sql", "CREATE INDEX Article_ft ON Article (title, body) FULL_TEXT");

    database.transaction(() -> {
      database.newDocument("Article").set("title", "Java Programming").set("body", "An introduction to the language").save();
      database.newDocument("Article").set("title", "Python Guide")
          .set("body", "Java programming is what the other article is about").save();
    });

    // THE SINGLE-TERM FORM IS RESTRICTED TO THE FIELD...
    assertThat(titles("SELECT title FROM Article WHERE SEARCH_INDEX('Article_ft', 'title:java') = true"))
        .containsExactly("Java Programming");

    // ...AND THE PHRASE FORM OF THE SAME QUERY MUST BE TOO
    assertThat(titles("SELECT title FROM Article WHERE SEARCH_INDEX('Article_ft', 'title:\"java programming\"') = true"))
        .as("a phrase qualified to 'title' must not match a document carrying the phrase only in 'body'")
        .containsExactly("Java Programming");

    // THE UNQUALIFIED PHRASE STILL SEES BOTH FIELDS
    assertThat(titles("SELECT title FROM Article WHERE SEARCH_INDEX('Article_ft', '\"java programming\"') = true"))
        .containsExactlyInAnyOrder("Java Programming", "Python Guide");

    // A PHRASE QUALIFIED TO THE OTHER FIELD REACHES THE OTHER DOCUMENT ONLY
    assertThat(titles("SELECT title FROM Article WHERE SEARCH_INDEX('Article_ft', 'body:\"java programming\"') = true"))
        .containsExactly("Python Guide");
  }

  @Test
  @DisplayName("#7000 field qualified clause reaches aby item property")
  void fieldQualifiedClauseReachesAByItemProperty() {
    final DocumentType metadata = database.getSchema().createDocumentType("Metadata");
    metadata.createProperty("title", Type.STRING);
    metadata.createProperty("keywords", Type.LIST);
    database.command("sql", "CREATE INDEX Metadata_ft ON Metadata (title, keywords BY ITEM) FULL_TEXT");

    database.transaction(() -> {
      database.newDocument("Metadata").set("title", "First").set("keywords", List.of("graph", "database")).save();
      database.newDocument("Metadata").set("title", "Second graph").set("keywords", List.of("vector")).save();
    });

    // THE UNQUALIFIED QUERY MATCHES BOTH (ONE BY KEYWORD, ONE BY TITLE)...
    assertThat(titles("SELECT title FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'graph') = true"))
        .containsExactlyInAnyOrder("First", "Second graph");

    // ...AND THE QUALIFIED ONE MUST REACH THE BY ITEM PROPERTY BY ITS DECLARED NAME
    assertThat(titles("SELECT title FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'keywords:graph') = true"))
        .as("a 'keywords:' qualifier must reach the postings of the 'keywords BY ITEM' property")
        .containsExactly("First");
    assertThat(titles("SELECT title FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'keywords:gra*') = true"))
        .as("the same rule applies to the prefix form")
        .containsExactly("First");
    assertThat(titles("SELECT title FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'title:graph') = true"))
        .containsExactly("Second graph");

    // THE DIRECT LOOKUP PATH (CONTAINSTEXT) ALREADY RESOLVED THE QUALIFIER: THE TWO PATHS MUST AGREE
    assertThat(titles("SELECT title FROM Metadata WHERE keywords CONTAINSTEXT 'graph'")).containsExactly("First");
  }

  /**
   * A per-field boost is declared under the property name the index carries, which for a BY ITEM property is the
   * modifier-qualified one: the executor has to look the boost up under that spelling too, or SEARCH_INDEX drops it
   * while the direct CONTAINSTEXT path applies it.
   */
  @Test
  @DisplayName("#7000 a boost on aby item property reaches the field qualified query")
  void aBoostOnAByItemPropertyReachesTheFieldQualifiedQuery() {
    final DocumentType metadata = database.getSchema().createDocumentType("Metadata");
    metadata.createProperty("name", Type.STRING);
    metadata.createProperty("title", Type.STRING);
    metadata.createProperty("keywords", Type.LIST);
    database.command("sql", "CREATE INDEX Metadata_ft ON Metadata (title, keywords BY ITEM) FULL_TEXT METADATA "
        + "{\"similarity\": \"BM25\", \"keywords by item_boost\": 5.0}");

    // THE TWO DOCUMENTS ARE SYMMETRIC (ONE TOKEN PER FIELD), SO ONLY THE BOOST CAN TELL THEIR SCORES APART
    database.transaction(() -> {
      database.newDocument("Metadata").set("name", "inTitle").set("title", "java").set("keywords", List.of("other")).save();
      database.newDocument("Metadata").set("name", "inKeywords").set("title", "other").set("keywords", List.of("java")).save();
    });

    final float keywordScore = score("SELECT name, $score FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'keywords:java') = true",
        "inKeywords");
    final float titleScore = score("SELECT name, $score FROM Metadata WHERE SEARCH_INDEX('Metadata_ft', 'title:java') = true",
        "inTitle");
    assertThat(keywordScore).as("the boosted BY ITEM field must outscore the unboosted one").isGreaterThan(titleScore);
  }

  @Test
  @DisplayName("#7000 non exact terms are normalized by the analyzer not lower cased")
  void nonExactTermsAreNormalizedByTheAnalyzerNotLowerCased() {
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("content", Type.STRING);
    database.command("sql", "CREATE INDEX Doc_ft ON Doc (content) FULL_TEXT METADATA {\"analyzer\": "
        + "\"org.apache.lucene.analysis.core.WhitespaceAnalyzer\"}");

    database.transaction(() -> {
      database.newDocument("Doc").set("content", "Foo Bar").save();
      database.newDocument("Doc").set("content", "foo baz").save();
    });

    // EXACT TERMS GO THROUGH THE ANALYZER ON BOTH SIDES, SO THEY ARE CASE-SENSITIVE HERE
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Foo') = true")).containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'foo') = true")).containsExactly("foo baz");

    // THE NON-EXACT FORMS MUST FOLD THE SAME WAY THE ANALYZER DOES, I.E. NOT AT ALL FOR A WHITESPACE ANALYZER
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Fo*') = true"))
        .as("prefix: the stored token is 'Foo', a query folded to 'fo*' can never reach it").containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'F?o') = true"))
        .as("wildcard").containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Fooo~1') = true"))
        .as("fuzzy").containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', '/Fo+/') = true"))
        .as("regexp").containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'fo*') = true"))
        .as("a lower-case prefix reaches the lower-case token only").containsExactly("foo baz");
  }

  @Test
  @DisplayName("#7000 non exact terms still fold on the default analyzer")
  void nonExactTermsStillFoldOnTheDefaultAnalyzer() {
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("content", Type.STRING);
    database.command("sql", "CREATE INDEX Doc_ft ON Doc (content) FULL_TEXT");

    database.transaction(() -> database.newDocument("Doc").set("content", "Foo Bar").save());

    // GUARD AGAINST OVER-FIXING: THE STANDARD ANALYZER LOWER-CASES, SO A MIXED-CASE PATTERN MUST STILL MATCH
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Fo*') = true")).containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'F?o') = true")).containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Fooo~1') = true")).containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', '/Fo+/') = true")).containsExactly("Foo Bar");
  }

  private float score(final String query, final String expectedName) {
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<String>getProperty("name")).isEqualTo(expectedName);
      assertThat(rs.hasNext()).isFalse();
      return ((Number) result.getProperty("$score")).floatValue();
    }
  }

  /**
   * Folds every token it stores (lower case), but only normalizes a query pattern the same way when asked under the field
   * the write path tokenizes under: under any other field name the pattern is left as it is.
   */
  public static final class FieldAwareAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
      final Tokenizer source = new WhitespaceTokenizer();
      return new TokenStreamComponents(source, new LowerCaseFilter(source));
    }

    @Override
    protected TokenStream normalize(final String fieldName, final TokenStream in) {
      return LSMTreeFullTextIndex.ANALYZED_FIELD.equals(fieldName) ? new LowerCaseFilter(in) : in;
    }
  }

  /**
   * The write path analyzes every property under one Lucene field name. A field-aware analyzer folds by that name, so
   * the query side has to normalize a non-exact term under the same one rather than under the query's own qualifier, or
   * the pattern is folded differently from the tokens it has to match.
   */
  @Test
  @DisplayName("#7000 non exact terms are normalized under the field the write path tokenizes under")
  void nonExactTermsAreNormalizedUnderTheFieldTheWritePathTokenizesUnder() {
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("content", Type.STRING);
    doc.createProperty("extra", Type.STRING);
    database.command("sql", "CREATE INDEX Doc_ft ON Doc (content, extra) FULL_TEXT METADATA {\"analyzer\": \""
        + FieldAwareAnalyzer.class.getName() + "\"}");

    database.transaction(() -> database.newDocument("Doc").set("content", "Foo Bar").set("extra", "none").save());

    // THE STORED TOKENS ARE LOWER CASE, WHATEVER THE PROPERTY
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'content:foo') = true")).containsExactly("Foo Bar");

    // A MIXED-CASE PATTERN REACHES THEM ONLY IF IT IS FOLDED UNDER THE FIELD THE WRITE PATH USED
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'content:Fo*') = true")).as("prefix")
        .containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'content:F?o') = true")).as("wildcard")
        .containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'content:Fooo~1') = true")).as("fuzzy")
        .containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'content:/Fo+/') = true")).as("regexp")
        .containsExactly("Foo Bar");
    assertThat(contents("SELECT content FROM Doc WHERE SEARCH_INDEX('Doc_ft', 'Fo*') = true")).as("unqualified prefix")
        .containsExactly("Foo Bar");
  }

  private List<String> titles(final String query) {
    return column(query, "title");
  }

  private List<String> contents(final String query) {
    return column(query, "content");
  }

  private List<String> column(final String query, final String property) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(property));
    }
    return values;
  }
}
