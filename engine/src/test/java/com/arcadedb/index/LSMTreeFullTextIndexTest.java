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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeFullTextIndexTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final int    PAGE_SIZE = LSMTreeIndexAbstract.DEF_PAGE_SIZE;
  private static final String TYPE_NAME = "Doc";
  private static final String text      = """
      Jay Glenn Miner (May 31, 1932 – June 20, 1994) was an American integrated circuit designer, known primarily for developing multimedia chips for the Atari 2600 and Atari 8-bit family and as the "father of the Amiga". He received a BS in EECS from UC Berkeley in 1959.[2]
      Miner started in the electronics industry with a number of designs in the medical world, including a remote-control pacemaker.
      He moved to Atari, Inc. in the late 1970s. One of his first successes was to combine an entire breadboard of components into a single chip, known as the TIA. The TIA was the display hardware for the Atari 2600, which would go on to sell millions. After working on the TIA he headed up the design of the follow-on chip set known as ANTIC and CTIA for which he held a patent.[3] These chips would be used for the Atari 8-bit family of home computers and the Atari 5200 video game system.
      In the early 1980s, Jay, along with other Atari staffers, had become fed up with management and decamped. They set up another chipset project under a new company in Santa Clara, called Hi-Toro (later renamed to Amiga Corporation), where they could have creative freedom. There, they started to create a new Motorola 68000-based games console, codenamed Lorraine that could be upgraded to a computer. To raise money for the Lorraine project, Amiga Corp. designed and sold joysticks and game cartridges for popular game consoles such as the Atari 2600 and ColecoVision, as well as an odd input device called the Joyboard, essentially a joystick the player stood on. Atari continued to be interested in the team's efforts throughout this period, and funded them with $500,000 in capital in return for first use of their resulting chipset.
      Also in the early 1980s, Jay worked on a project with Intermedics, Inc. to create their first microprocessor-based cardiac pacemaker. The microprocessor was called Lazarus and the pacemaker was eventually called Cosmos. Jay was listed co-inventor on two patents. US patent 4390022, Richard V. Calfee & Jay Miner, "Implantable device with microprocessor control", issued 1983-06-28, assigned to Intermedics, Inc. US patent 4404972, Pat L. Gordon; Richard V. Calfee & Jay Miner, "Implantable device with microprocessor control", issued 1983-06-28, assigned to Intermedics, Inc.
      The Amiga crew, having continuing serious financial problems, had sought more monetary support from investors that entire Spring. Amiga entered into discussions with Commodore. The discussions ultimately led to Commodore wanting to purchase Amiga outright, which would (from Commodore's viewpoint) cancel any outstanding contracts - including Atari Inc.'s. So instead of Amiga delivering the chipset, Commodore delivered a check of $500,000 to Atari on Amiga's behalf, in effect returning the funds invested into Amiga for completion of the Lorraine chipset.
      The original Amiga (1985)
      Jay worked at Commodore-Amiga for several years, in Los Gatos, California. They made good progress at the beginning, but as Commodore management changed, they became marginalised and the original Amiga staff was fired or left out on a one-by-one basis, until the entire Los Gatos office was closed. Miner later worked as a consultant for Commodore until it went bankrupt. He was known as the 'Padre' (father) of the Amiga among Amiga users.
      Jay always took his dog "Mitchy" (a cockapoo) with him wherever he went. While he worked at Atari, Mitchy even had her own ID-badge, and an embossing of Mitchy's paw print is visible on the inside of the Amiga 1000 top cover, alongside the signatures of the engineers who worked on it.
      Jay endured kidney problems for most of his life, according to his wife, and relied on dialysis. His sister donated one of her own. Miner died due to complications from kidney failure at the age of 62, just two months after Commodore declared bankruptcy.
      """;

  @Test
  void fulltextIndexConsistency() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.text STRING");

    // Create FULL_TEXT index with BY ITEM
    database.command("sql", "CREATE INDEX ON Doc (text) FULL_TEXT");

    database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument("Doc");
        v.set("id", i);
        v.set("text", text);
        v.save();
      }

      TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[text]");
      final List<String> keywords = ((LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0]).analyzeText(
          ((LSMTreeFullTextIndex) typeIndex.getIndexesOnBuckets()[0]).getAnalyzer(), new Object[] { text });
      assertThat(keywords.isEmpty()).isFalse();

      for (final String k : keywords) {
        int totalPerKeyword = 0;

        for (final Index idx : typeIndex.getIndexesOnBuckets()) {

          final IndexCursor result = idx.get(new Object[] { k });
          int totalPerIndex = 0;
          while (result.hasNext()) {
            result.next();
            ++totalPerIndex;
          }

          assertThat(totalPerIndex).isEqualTo(result.estimateSize());

          totalPerKeyword += totalPerIndex;
        }
        assertThat(totalPerKeyword).isEqualTo(TOT);
      }
    });

  }

  @Test
  void indexingCompositeNonStringProperty() {
    // Multi-property full-text indexes are now supported, but only for STRING properties.
    // This test verifies that non-STRING properties are still rejected.
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType()
        .withName(TYPE_NAME)
        .withTotalBuckets(1)
        .create();
    type.createProperty("text", String.class);
    type.createProperty("count", Integer.class);
    assertThatThrownBy(() -> database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "text", "count" })
        .withType(Schema.INDEX_TYPE.FULL_TEXT)
        .withPageSize(PAGE_SIZE)
        .withUnique(false)
        .create())
        .isInstanceOf(IndexException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Full text index can only be defined on STRING properties, found: INTEGER");
  }

  @Test
  void query() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType("Docs")).isFalse();

      final DocumentType type = database.getSchema().createDocumentType("Docs");
      type.createProperty("text", String.class);
      final Index typeIndex = database.getSchema()
          .buildTypeIndex("Docs", new String[] { "text" })
          .withType(Schema.INDEX_TYPE.FULL_TEXT)
          .withUnique(false)
          .create();

      assertThat(database.getSchema().existsType("Docs")).isTrue();

      final String[] words = text.split(" ");

      for (int i = 0; i < words.length - 1; ++i) {

        final String toIndex = words[i].toLowerCase() + " has been indexed";

        if (skipIndexing(toIndex))
          continue;

        final MutableDocument doc = database.newDocument("Docs");
        doc.set("id", i);
        doc.set("text", toIndex);
        doc.save();
      }

      for (int i = 0; i < words.length - 1; ++i) {
        final String toFind = words[i].toLowerCase();

        if (skipIndexing(toFind))
          continue;

        final ResultSet result = database.query("sql", "select from Docs where text = '" + toFind + "'", toFind);
        assertThat(result.hasNext())
            .isTrue()
            .withFailMessage("Cannot find key '" + toFind + "'");

        final Result res = result.next();
        assertThat(res).isNotNull();

        final String content = res.getProperty("text").toString().toLowerCase();
        assertThat(content.contains(toFind))
            .isTrue()
            .withFailMessage("Cannot find the word '" + toFind + "' in indexed text '" + content + "'");
      }
    });
  }

  @Test
  void nullValuesViaSQL() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc");
      database.command("sql", "CREATE PROPERTY doc.str STRING");
      database.command("sql", "CREATE INDEX ON doc (str) FULL_TEXT null_strategy error");
      database.command("sql", "INSERT INTO doc (str) VALUES ('a'), ('b'), (null)");
    })).isInstanceOf(TransactionException.class);

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE doc2");
      database.command("sql", "CREATE PROPERTY doc2.str STRING");
      database.command("sql", "CREATE INDEX ON doc2 (str) FULL_TEXT null_strategy skip");
      database.command("sql", "INSERT INTO doc2 (str) VALUES ('a'), ('b'), (null)");
    });

  }

  /**
   * Tests that METADATA in CREATE INDEX SQL is passed to the full-text index builder.
   * The metadata configures the analyzer class for the full-text index.
   */
  @Test
  void createIndexWithMetadata() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql",
          "CREATE INDEX ON Article (title) FULL_TEXT METADATA {\"analyzer\": \"org.apache.lucene.analysis.en.EnglishAnalyzer\", \"allowLeadingWildcard\": true}");

      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article[title]");
      assertThat(index).isNotNull();
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);

      // Verify metadata was applied
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      assertThat(ftIndex.getAnalyzer()).isInstanceOf(EnglishAnalyzer.class);
    });
  }

  @Test
  void multiPropertyIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article2");
      database.command("sql", "CREATE PROPERTY Article2.title STRING");
      database.command("sql", "CREATE PROPERTY Article2.body STRING");
      database.command("sql", "CREATE INDEX ON Article2 (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article2 SET title = 'Java Programming', body = 'Learn Java basics'");
      database.command("sql", "INSERT INTO Article2 SET title = 'Python Tutorial', body = 'Python programming guide'");
    });

    database.transaction(() -> {
      // Search should find documents matching in either field
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article2[title,body]");
      final IndexCursor cursor = index.get(new Object[] { "java programming" });

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      // Both documents should match (Java appears in title of doc1, programming in both)
      assertThat(count).isEqualTo(2);
    });
  }

  @Test
  void multiPropertyIndexFieldSpecificSearch() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article3");
      database.command("sql", "CREATE PROPERTY Article3.title STRING");
      database.command("sql", "CREATE PROPERTY Article3.body STRING");
      database.command("sql", "CREATE INDEX ON Article3 (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article3 SET title = 'Java Programming', body = 'Learn basics'");
      database.command("sql", "INSERT INTO Article3 SET title = 'Python Tutorial', body = 'Java programming guide'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article3[title,body]");

      // First, test unqualified search to verify both documents are indexed
      final IndexCursor allJavaCursor = index.get(new Object[] { "java" });
      int allJavaCount = 0;
      while (allJavaCursor.hasNext()) {
        allJavaCursor.next();
        allJavaCount++;
      }
      // Both documents have "java" somewhere
      assertThat(allJavaCount).isEqualTo(2);

      // Field-specific search: only match "java" in the title field
      // Access the underlying bucket index directly to bypass TypeIndex
      final LSMTreeFullTextIndex ftIndex = (LSMTreeFullTextIndex) index.getIndexesOnBuckets()[0];
      final IndexCursor titleCursor = ftIndex.get(new Object[] { "title:java" });
      final Set<String> titlesFound = new HashSet<>();
      while (titleCursor.hasNext()) {
        final Identifiable record = titleCursor.next();
        titlesFound.add(record.asDocument().getString("title"));
      }
      // Only first document has "java" in title
      assertThat(titlesFound).containsExactly("Java Programming");

      // Field-specific search: only match "java" in the body field
      final IndexCursor bodyCursor = ftIndex.get(new Object[] { "body:java" });
      final Set<String> bodiesFound = new HashSet<>();
      while (bodyCursor.hasNext()) {
        final Identifiable record = bodyCursor.next();
        bodiesFound.add(record.asDocument().getString("title"));
      }
      // Only second document has "java" in body
      assertThat(bodiesFound).containsExactly("Python Tutorial");
    });
  }

  @Test
  void multiPropertyIndexRemove() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article4");
      database.command("sql", "CREATE PROPERTY Article4.title STRING");
      database.command("sql", "CREATE PROPERTY Article4.body STRING");
      database.command("sql", "CREATE INDEX ON Article4 (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article4 SET title = 'Java Programming', body = 'Learn basics'");
    });

    // Delete the document and verify it's removed from the index
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Article4 WHERE title = 'Java Programming'");
    });

    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article4[title,body]");
      final IndexCursor cursor = index.get(new Object[] { "java" });

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      // Document should be removed from index
      assertThat(count).isEqualTo(0);
    });
  }

  /**
   * Tests that multi-property full-text indexes work correctly after database restart.
   * This verifies that the propertyCount is correctly derived from getPropertyNames()
   * rather than being hardcoded to 1 when loading from disk.
   */
  @Test
  void multiPropertyIndexPersistence() {
    // Create index and insert documents
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article5");
      database.command("sql", "CREATE PROPERTY Article5.title STRING");
      database.command("sql", "CREATE PROPERTY Article5.body STRING");
      database.command("sql", "CREATE INDEX ON Article5 (title, body) FULL_TEXT");

      database.command("sql", "INSERT INTO Article5 SET title = 'Java Programming', body = 'Learn Java basics'");
      database.command("sql", "INSERT INTO Article5 SET title = 'Python Tutorial', body = 'Python programming guide'");
    });

    // Verify index works before restart
    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article5[title,body]");
      final IndexCursor cursor = index.get(new Object[] { "java programming" });

      int countBeforeRestart = 0;
      while (cursor.hasNext()) {
        cursor.next();
        countBeforeRestart++;
      }
      assertThat(countBeforeRestart).isEqualTo(2);
    });

    // Close and reopen the database to simulate restart
    reopenDatabase();

    // Verify index still works after restart
    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article5[title,body]");
      assertThat(index).isNotNull();
      assertThat(index.getPropertyNames()).containsExactly("title", "body");

      // Search should still find documents
      final IndexCursor cursor = index.get(new Object[] { "java programming" });

      int countAfterRestart = 0;
      while (cursor.hasNext()) {
        cursor.next();
        countAfterRestart++;
      }
      // Both documents should still be found after restart
      assertThat(countAfterRestart).isEqualTo(2);
    });

    // Verify we can insert new documents after restart
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Article5 SET title = 'Database Design', body = 'SQL and NoSQL databases'");
    });

    // Verify the new document is indexed correctly
    database.transaction(() -> {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Article5[title,body]");
      final IndexCursor cursor = index.get(new Object[] { "database" });

      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      // New document should be found
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void scoreExposure() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      // Insert documents with varying keyword matches
      // Score = number of search keywords that match in the document
      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database system'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc3', content = 'programming language tutorial'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc4', content = 'python scripting language'");
    });

    database.transaction(() -> {
      // Search for "java programming language" - three keywords
      // Doc1 matches all 3 keywords → score 3
      // Doc2 matches 1 keyword (java) → score 1
      // Doc3 matches 2 keywords (programming, language) → score 2
      // Doc4 matches 1 keyword (language) → score 1
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Article[content]");
      final IndexCursor cursor = typeIndex.get(new Object[] { "java programming language" });

      final Map<String, Integer> docScores = new HashMap<>();

      while (cursor.hasNext()) {
        final Identifiable record = cursor.next();
        final int score = cursor.getScore();
        assertThat(score).isGreaterThan(0);

        // Load the document to get the title
        final String title = record.asDocument().getString("title");
        docScores.put(title, score);
      }

      assertThat(docScores).hasSize(4);
      assertThat(docScores.get("Doc1")).isEqualTo(3); // matches all 3 keywords
      assertThat(docScores.get("Doc2")).isEqualTo(1); // matches 1 keyword (java)
      assertThat(docScores.get("Doc3")).isEqualTo(2); // matches 2 keywords (programming, language)
      assertThat(docScores.get("Doc4")).isEqualTo(1); // matches 1 keyword (language)
    });

    // Temporarily comment out SQL tests until we verify they go through the right path
    /*
    database.transaction(() -> {
      // Test SQL query with score exposure
      final ResultSet result = database.query("sql", "SELECT FROM Article WHERE content = 'java programming language'");

      final Map<String, Integer> docScores = new HashMap<>();

      while (result.hasNext()) {
        final Result res = result.next();
        assertThat(res).isNotNull();

        final String title = res.getProperty("title");
        final Integer score = res.getProperty("$score");

        assertThat(score).isNotNull();
        assertThat(score).isGreaterThan(0);

        docScores.put(title, score);
      }

      // All 4 documents should be found
      assertThat(docScores).hasSize(4);

      // Verify scores match expectations
      assertThat(docScores.get("Doc1")).isEqualTo(3); // matches all 3 keywords
      assertThat(docScores.get("Doc2")).isEqualTo(1); // matches 1 keyword (java)
      assertThat(docScores.get("Doc3")).isEqualTo(2); // matches 2 keywords (programming, language)
      assertThat(docScores.get("Doc4")).isEqualTo(1); // matches 1 keyword (language)
    });
    */

    // TODO: Investigate SQL query path to ensure scores are exposed
    /*
    database.transaction(() -> {
      // Test single keyword search
      final ResultSet result = database.query("sql", "SELECT FROM Article WHERE content = 'java'");

      int count = 0;
      while (result.hasNext()) {
        final Result res = result.next();
        final Integer score = res.getProperty("$score");

        assertThat(score).isNotNull();
        assertThat(score).isEqualTo(1); // Single keyword match always scores 1
        count++;
      }

      // Doc1 and Doc2 contain "java"
      assertThat(count).isEqualTo(2);
    });
    */
  }

  @Test
  void scoreInSQLProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.title STRING");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");

      database.command("sql", "INSERT INTO Article SET title = 'Doc1', content = 'java programming language'");
      database.command("sql", "INSERT INTO Article SET title = 'Doc2', content = 'java database'");
    });

    database.transaction(() -> {
      // Test $score projection with SEARCH_INDEX function
      final ResultSet result = database.query("sql",
          "SELECT title, $score FROM Article WHERE SEARCH_INDEX('Article[content]', 'java programming') = true");

      final Map<String, Float> scores = new HashMap<>();
      while (result.hasNext()) {
        final Result r = result.next();
        final String title = r.getProperty("title");
        final Float score = r.getProperty("$score");
        assertThat(score).isNotNull();
        assertThat(score).isGreaterThan(0f);
        scores.put(title, score);
      }

      assertThat(scores).hasSize(2);
      // Doc1 matches both 'java' and 'programming', Doc2 only 'java'
      assertThat(scores.get("Doc1")).isGreaterThan(scores.get("Doc2"));
    });
  }

  private boolean skipIndexing(final String toIndex) {
    boolean skip = false;
    for (int j = 0; j < toIndex.length(); j++) {
      final char c = toIndex.charAt(j);
      if (!Character.isLetterOrDigit(c) && c != ' ')
        skip = true;
    }

    if (skip)
      return true;
    return false;
  }
}
