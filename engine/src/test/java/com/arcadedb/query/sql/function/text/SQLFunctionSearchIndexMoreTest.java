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
package com.arcadedb.query.sql.function.text;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class SQLFunctionSearchIndexMoreTest extends TestHelper {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-search-index-more").create();

    final DocumentType article = database.getSchema().createDocumentType("Article");
    article.createProperty("title", String.class);
    article.createProperty("body", String.class);

    database.command("sql", "CREATE INDEX ON Article (title, body) FULL_TEXT");

    database.transaction(() -> {
      database.newDocument("Article")
        .set("title", "Java Programming")
        .set("body", "Learn Java programming language")
        .save();
      database.newDocument("Article")
        .set("title", "Python Guide")
        .set("body", "Learn Python programming language")
        .save();
      database.newDocument("Article")
        .set("title", "Database Systems")
        .set("body", "Database management and optimization")
        .save();
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void basicMoreLikeThis() {
    // Use permissive config to ensure we get results with small dataset
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<String>getProperty("title")).isIn("Python Guide", "Database Systems");
  }

  @Test
  void withMetadata() {
    final ResultSet rs = database.query("sql",
      "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1, 'maxQueryTerms': 10}) = true");

    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void invalidIndexName() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('NonExistent', [#1:0]) = true");
    }).hasMessageContaining("Index with name 'NonExistent' was not found");
  }

  @Test
  void emptySourceRIDs() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', []) = true");
    }).hasMessageContaining("requires at least one source RID");
  }

  @Test
  void nonExistentRID() {
    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:999]) = true");
    }).hasMessageContaining("Record #1:999 not found");
  }

  @Test
  void similarityScoreExposed() {
    final ResultSet rs = database.query("sql",
      "SELECT title, $similarity FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', [#1:0], {'minTermFreq': 1, 'minDocFreq': 1}) = true");

    assertThat(rs.hasNext()).isTrue();
    final Result result = rs.next();
    assertThat(result.<Float>getProperty("$similarity")).isNotNull().isGreaterThan(0f);
  }

  @Test
  void exceedsMaxSourceDocs() {
    // Create RID list exceeding maxSourceDocs (default 25)
    final StringBuilder rids = new StringBuilder("[");
    for (int i = 0; i < 30; i++) {
      if (i > 0) rids.append(", ");
      rids.append("#1:").append(i);
    }
    rids.append("]");

    assertThatThrownBy(() -> {
      database.query("sql", "SELECT FROM Article WHERE SEARCH_INDEX_MORE('Article[title,body]', " + rids + ") = true");
    }).hasMessageContaining("exceeds maxSourceDocs limit");
  }
}
