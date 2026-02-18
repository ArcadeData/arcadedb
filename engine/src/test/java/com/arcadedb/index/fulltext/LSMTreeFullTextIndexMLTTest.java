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
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.fulltext;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for More Like This (MLT) functionality in LSMTreeFullTextIndex.
 *
 * @author Frank Reale
 */
class LSMTreeFullTextIndexMLTTest {
  private static final String   DB_PATH = "target/databases/LSMTreeFullTextIndexMLTTest";
  private              Database db;
  private              RID      javaGuideRID;
  private              RID      pythonGuideRID;
  private              RID      javaDatabaseRID;

  @BeforeEach
  void setup() {
    // Clean up any existing database
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      factory.open().drop();
    }

    // Create database
    db = factory.create();

    // Create document type with full-text index
    db.transaction(() -> {
      final DocumentType docType = db.getSchema().createDocumentType("Document");
      docType.createProperty("content", String.class);
      docType.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "content");
    });

    // Insert test documents
    db.transaction(() -> {
      // Java Guide - focuses on Java programming
      MutableDocument doc1 = db.newDocument("Document");
      doc1.set("content", """
          Java is a popular programming language. Java is used for enterprise applications. \
          Java has strong typing and object-oriented features. Java runs on the JVM.""");
      doc1.save();
      javaGuideRID = doc1.getIdentity();

      // Python Guide - focuses on Python programming
      MutableDocument doc2 = db.newDocument("Document");
      doc2.set("content", """
          Python is a popular programming language. Python is used for data science. \
          Python has dynamic typing and is easy to learn. Python is great for scripting.""");
      doc2.save();
      pythonGuideRID = doc2.getIdentity();

      // Java Database - focuses on Java and databases
      MutableDocument doc3 = db.newDocument("Document");
      doc3.set("content", """
          Java database programming uses JDBC. Java applications connect to databases. \
          Java provides excellent database support. Java is widely used in database applications.""");
      doc3.save();
      javaDatabaseRID = doc3.getIdentity();
    });
  }

  @AfterEach
  void teardown() {
    if (db != null) {
      db.drop();
      db = null;
    }
  }

  @Test
  void searchMoreLikeThis() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig()
        .setMinTermFreq(1)
        .setMinDocFreq(1)
        .setMaxQueryTerms(10);

    // Search for documents similar to Java Guide
    final Set<RID> sourceRids = Set.of(javaGuideRID);
    final IndexCursor cursor = index.searchMoreLikeThis(sourceRids, config);

    // Collect results
    final List<RID> results = new ArrayList<>();
    while (cursor.hasNext()) {
      results.add(cursor.next().getIdentity());
    }

    // Should find Java Database (similar) but not Python Guide
    // Source should be excluded by default
    assertThat(results).isNotEmpty();
    assertThat(results).contains(javaDatabaseRID);
    assertThat(results).doesNotContain(javaGuideRID);

    // Python guide might or might not be included depending on scoring,
    // but Java Database should rank higher due to more Java-related terms
  }

  @Test
  void searchMoreLikeThisExcludesSource() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig()
        .setMinTermFreq(1)
        .setMinDocFreq(1)
        .setMaxQueryTerms(10)
        .setExcludeSource(true);

    // Search for documents similar to Java Guide
    final Set<RID> sourceRids = Set.of(javaGuideRID);
    final IndexCursor cursor = index.searchMoreLikeThis(sourceRids, config);

    // Collect results
    final Set<RID> results = new HashSet<>();
    while (cursor.hasNext()) {
      results.add(cursor.next().getIdentity());
    }

    // Source document should NOT be in results
    assertThat(results).doesNotContain(javaGuideRID);
  }

  @Test
  void searchMoreLikeThisIncludesSource() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig()
        .setMinTermFreq(1)
        .setMinDocFreq(1)
        .setMaxQueryTerms(10)
        .setExcludeSource(false);

    // Search for documents similar to Java Guide
    final Set<RID> sourceRids = Set.of(javaGuideRID);
    final IndexCursor cursor = index.searchMoreLikeThis(sourceRids, config);

    // Collect results
    final Set<RID> results = new HashSet<>();
    while (cursor.hasNext()) {
      results.add(cursor.next().getIdentity());
    }

    // Source document SHOULD be in results when excludeSource is false
    assertThat(results).contains(javaGuideRID);
  }

  @Test
  void searchMoreLikeThisMultipleSources() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig()
        .setMinTermFreq(1)
        .setMinDocFreq(1)
        .setMaxQueryTerms(10);

    // Search using both Java documents as source
    final Set<RID> sourceRids = Set.of(javaGuideRID, javaDatabaseRID);
    final IndexCursor cursor = index.searchMoreLikeThis(sourceRids, config);

    // Should succeed and exclude both source documents
    final Set<RID> results = new HashSet<>();
    while (cursor.hasNext()) {
      results.add(cursor.next().getIdentity());
    }

    assertThat(results).doesNotContain(javaGuideRID, javaDatabaseRID);
  }

  @Test
  void searchMoreLikeThisNullSourceRIDs() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig();

    assertThatThrownBy(() -> index.searchMoreLikeThis(null, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void searchMoreLikeThisEmptySourceRIDs() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig();

    assertThatThrownBy(() -> index.searchMoreLikeThis(Set.of(), config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void searchMoreLikeThisExceedsMaxSourceDocs() {
    final LSMTreeFullTextIndex index = getFullTextIndex();
    final MoreLikeThisConfig config = new MoreLikeThisConfig()
        .setMaxSourceDocs(1);

    final Set<RID> sourceRids = Set.of(javaGuideRID, pythonGuideRID);

    assertThatThrownBy(() -> index.searchMoreLikeThis(sourceRids, config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Helper method to get the full-text index from the schema.
   */
  private LSMTreeFullTextIndex getFullTextIndex() {
    final DocumentType docType = db.getSchema().getType("Document");
    final TypeIndex typeIndex = docType.getAllIndexes(false).iterator().next();
    return (LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0];
  }
}
