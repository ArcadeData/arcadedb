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
import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullTextQueryExecutorTest extends TestHelper {

  @Test
  void parseBooleanQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
      database.command("sql", "INSERT INTO Article SET content = 'python programming'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java +programming should only match first document (requires both)
      final IndexCursor cursor = executor.search("+java +programming", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void parseExclusionQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // java -programming should only match second document
      final IndexCursor cursor = executor.search("java -programming", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void parsePhraseQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming language tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'language for java programming'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // "java programming" as phrase - both docs contain both words
      // but we can only verify they both contain the terms (phrase order requires position indexing)
      final IndexCursor cursor = executor.search("\"java programming\"", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      // Both documents contain "java" and "programming"
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void parseOrQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'python scripting'");
      database.command("sql", "INSERT INTO Article SET content = 'database design'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // java OR python should match first two documents
      final IndexCursor cursor = executor.search("java OR python", -1);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void scoreSorting() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Doc1 matches 2 keywords
      database.command("sql", "INSERT INTO Article SET content = 'java programming language'");
      // Doc2 matches 1 keyword
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // Search for "java programming" - first doc should have higher score
      final IndexCursor cursor = executor.search("java programming", -1);

      assertThat(cursor.hasNext()).isTrue();
      cursor.next();
      int firstScore = cursor.getScore();

      assertThat(cursor.hasNext()).isTrue();
      cursor.next();
      int secondScore = cursor.getScore();

      // First result should have higher or equal score (results sorted by score descending)
      assertThat(firstScore).isGreaterThanOrEqualTo(secondScore);
    });
  }

  @Test
  void limitResults() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
      database.command("sql", "INSERT INTO Article SET content = 'java tutorial'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // Search for "java" with limit 2
      final IndexCursor cursor = executor.search("java", 2);

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void pureNegativeQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // -python should return all documents that do NOT contain "python"
      final IndexCursor cursor = executor.search("-python", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("database tutorial", "java programming");
    });
  }

  @Test
  void pureNegativeWildcardQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // -pyth* should return all documents that do NOT contain any term starting with "pyth"
      final IndexCursor cursor = executor.search("-pyth*", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("database tutorial", "java programming");
    });
  }

  @Test
  void pureNegativePhraseQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // -"java python" excludes docs containing both terms (phrase positions not enforced)
      final IndexCursor cursor = executor.search("-\"java python\"", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("database tutorial", "python legacy", "java programming");
    });
  }

  @Test
  void pureNegativeFuzzyQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // -pythn~ fuzzy-matches python and excludes docs containing it
      final IndexCursor cursor = executor.search("-pythn~", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("database tutorial", "java programming");
    });
  }

  @Test
  void pureNegativeRegexpQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // -/py.*/ regexp-matches python and excludes docs containing it
      final IndexCursor cursor = executor.search("-/py.*/", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("database tutorial", "java programming");
    });
  }

  @Test
  void positiveWithNegativeWildcardQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'database tutorial'");
      database.command("sql", "INSERT INTO Article SET content = 'python legacy'");
      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java -pyth* should return only "java programming", not "java python migration"
      final IndexCursor cursor = executor.search("+java -pyth*", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactly("java programming");
    });
  }

  @Test
  void positiveWithNegativePrefixQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java -python should match "java programming" and "java database" but not "java python migration"
      final IndexCursor cursor = executor.search("+java -python", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("java programming", "java database");
    });
  }

  @Test
  void positiveWithNegativeFuzzyQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java -pythn~ should exclude "java python migration" via fuzzy match against "python"
      final IndexCursor cursor = executor.search("+java -pythn~", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("java programming", "java database");
    });
  }

  @Test
  void positiveWithNegativeRegexpQuery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET content = 'java programming'");
      database.command("sql", "INSERT INTO Article SET content = 'java python migration'");
      database.command("sql", "INSERT INTO Article SET content = 'java database'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java -/py.*/ should exclude "java python migration" via regexp match
      final IndexCursor cursor = executor.search("+java -/py.*/", -1);
      final List<String> contents = new ArrayList<>();
      while (cursor.hasNext()) {
        contents.add((String) ((Document) cursor.next().getRecord()).get("content"));
      }
      assertThat(contents).containsExactlyInAnyOrder("java programming", "java database");
    });
  }

  @Test
  void mustWithShouldQuery() {
    // Tests that MUST clauses are required, while SHOULD only adds bonus score
    // This is the fix for: documents matching only SHOULD should NOT be returned
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Doc1: matches both java (MUST) and programming (SHOULD)
      database.command("sql", "INSERT INTO Article SET content = 'java programming language'");
      // Doc2: matches only java (MUST)
      database.command("sql", "INSERT INTO Article SET content = 'java database system'");
      // Doc3: matches only programming (SHOULD) - should NOT be returned
      database.command("sql", "INSERT INTO Article SET content = 'programming tutorial guide'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];

      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      // +java programming means: MUST have java, SHOULD have programming (bonus score)
      final IndexCursor cursor = executor.search("+java programming", -1);

      int count = 0;
      int firstScore = 0;
      int secondScore = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
        if (count == 1)
          firstScore = cursor.getScore();
        else if (count == 2)
          secondScore = cursor.getScore();
      }

      // Only 2 documents should match (those with java)
      // Doc3 with only "programming" should NOT be returned
      assertThat(count).isEqualTo(2);

      // Doc1 (java + programming) should have higher score than Doc2 (java only)
      assertThat(firstScore).isGreaterThan(secondScore);
    });
  }

  @Test
  void catastrophicRegexpQueryIsAbortedByRegexTimeout() {
    // Issue #5886 follow-up: a /pattern/ regexp query is matched against every token in what is, absent a
    // literal prefix, a full index scan - collectRegexpMatches() had no bound on that per-token
    // Pattern.matcher(token).matches() call, so this is arguably a worse instance of the MATCHES/=~ gap this
    // issue was originally about (one pathological pattern evaluated against every token in the index).
    // Terminator is 'b', not '!': full-text tokenization strips punctuation, and the terminator must survive
    // as part of the token for the reproducer to still be the same catastrophic pattern (a non-'a' tail is
    // what forces (.*a){20}$ to exhaustively backtrack instead of matching greedily).
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "b";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Article SET content = '" + pathological + "'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> executor.search("/(.*a){20}$/", -1)).isInstanceOf(TimeoutException.class);

      // Generous upper bound: proves the scan was aborted near the configured deadline rather than merely
      // being slow (the unbounded match takes tens of seconds).
      stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded scan");
    });
  }

  @Test
  void catastrophicWildcardQueryIsAbortedByRegexTimeout() {
    // Issue #5886 follow-up (2nd review pass): collectWildcardMatches() converts each '*' to '.*' via
    // wildcardToRegex() - no grouping/alternation/nested quantifiers, but a sequence of several '*'s reproduces
    // the exact same catastrophic-backtracking shape on its own, the same class of gap SQL LIKE/ILIKE turned
    // out to have (verified separately) despite the original "structurally safe" audit conclusion for both.
    // "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b" against a token ending in something other than 'b' forces the
    // same exhaustive backtrack-then-fail this issue's own (.*a){20}$ reproducer relies on. A literal (not '*')
    // leading character keeps this in the range-scan branch without needing allowLeadingWildcard=true - the
    // leading-wildcard, full-scan branch is the same shape collectRegexpMatches already covers a test for.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 200L);

    final String pathological = "a".repeat(40) + "c";
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Article SET content = '" + pathological + "'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);

      final String wildcardPattern = "a" + "*a".repeat(19) + "*b";

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> executor.search(wildcardPattern, -1)).isInstanceOf(TimeoutException.class);

      stopwatch.assertGaveUpWithin(5000, "the configured 200ms deadline from an unbounded scan");
    });
  }
}
