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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.FullTextQueryExecutor;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
