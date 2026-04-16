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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #3714: Hash index page corruption during batch UPSERTs.
 * <p>
 * The root cause is a wrong space check in addRIDToExistingEntry: it checks
 * extraSpace (net growth) instead of newEntrySize (actual bytes appended at dataEnd).
 * This allows writing past the slot directory, corrupting page data and causing
 * "Variable length quantity is too long", "arraycopy: length negative",
 * "Cannot write outside the page space", and "Hash index bucket full" errors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3714HashIndexCorruptionTest {
  private static final String DB_PATH = "target/databases/Issue3714HashIndexCorruptionTest";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Reproduce the core bug: a non-unique hash index with small page size fills up
   * with dead space from addRIDToExistingEntry. The incorrect space check allows
   * writing past the slot directory, corrupting page data.
   * <p>
   * Before the fix, this test throws one of:
   * - "Cannot write outside the page space"
   * - "Variable length quantity is too long"
   * - "arraycopy: length is negative"
   * - "Hash index bucket full when adding RID to existing entry"
   */
  @Test
  void testNonUniqueHashIndexPageFragmentation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("FragDoc");
      type.createProperty("category", String.class);
      type.createProperty("value", Integer.class);
      // Small page size (1024) to fill pages quickly and trigger the bug
      database.getSchema().buildTypeIndex("FragDoc", new String[] { "category" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(1024).create();
    });

    // Insert many documents with same category to stress addRIDToExistingEntry.
    // With 5 categories and 200 docs each, the RID list per entry grows large.
    // With 1024-byte pages, this forces fragmentation and the space check bug.
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        final MutableDocument doc = database.newDocument("FragDoc");
        doc.set("category", "cat_" + (i % 5));
        doc.set("value", i);
        doc.save();
      }
    });

    // Verify all entries are readable
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("FragDoc[category]");
      for (int c = 0; c < 5; c++) {
        final IndexCursor cursor = index.get(new Object[] { "cat_" + c });
        int count = 0;
        while (cursor.hasNext()) {
          cursor.next();
          count++;
        }
        assertThat(count).isEqualTo(200);
      }
    });
  }

  /**
   * Test repeated UPSERTs that remove and re-add index entries, creating dead space
   * in hash index pages. This simulates the user's scenario of batch UPSERTs.
   */
  @Test
  void testRepeatedUpsertsWithNonUniqueHashIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("UpsertDoc");
      type.createProperty("recordId", String.class);
      type.createProperty("data", String.class);
      // Unique hash index on recordId for UPSERT lookup
      database.getSchema().buildTypeIndex("UpsertDoc", new String[] { "recordId" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).withPageSize(2048).create();
    });

    // Insert initial records
    database.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final MutableDocument doc = database.newDocument("UpsertDoc");
        doc.set("recordId", "key_" + i);
        doc.set("data", "initial_" + i);
        doc.save();
      }
    });

    // Repeatedly update via UPSERT - this causes index remove + re-insert for each update,
    // creating dead space in the hash index pages
    for (int iter = 0; iter < 50; iter++) {
      final int iteration = iter;
      database.transaction(() -> {
        for (int i = 0; i < 20; i++) {
          final int keyIdx = (iteration * 20 + i) % 200;
          database.command("sql",
              "UPDATE UpsertDoc CONTENT {\"recordId\":\"key_" + keyIdx + "\",\"data\":\"updated_" + iteration + "_" + i
                  + "\"} UPSERT WHERE recordId = 'key_" + keyIdx + "'");
        }
      });
    }

    // Verify integrity
    database.transaction(() -> {
      assertThat(database.countType("UpsertDoc", false)).isEqualTo(200);
      // Verify all keys are still findable via the index
      final Index index = database.getSchema().getIndexByName("UpsertDoc[recordId]");
      for (int i = 0; i < 200; i++) {
        final IndexCursor cursor = index.get(new Object[] { "key_" + i });
        assertThat(cursor.hasNext()).withFailMessage("Key key_" + i + " not found after UPSERTs").isTrue();
      }
    });
  }

  /**
   * Test that removeRIDFromEntry works correctly under fragmentation pressure.
   * Inserts many docs into a non-unique hash index, then removes some, which
   * exercises the removeRIDFromEntry code path.
   */
  @Test
  void testRemoveRIDFromEntryUnderFragmentation() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("RemoveDoc");
      type.createProperty("tag", String.class);
      type.createProperty("seq", Integer.class);
      database.getSchema().buildTypeIndex("RemoveDoc", new String[] { "tag" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(4096).create();
    });

    // Insert 500 docs with 10 tags, delete every other doc WITHIN each tag,
    // then re-insert to verify index consistency.
    // Each doc has tag="tag_X" and a within-tag sequence number.
    final int totalTags = 10;
    final int docsPerTag = 50;

    database.transaction(() -> {
      for (int t = 0; t < totalTags; t++)
        for (int s = 0; s < docsPerTag; s++) {
          final MutableDocument doc = database.newDocument("RemoveDoc");
          doc.set("tag", "tag_" + t);
          doc.set("seq", t * 1000 + s); // unique seq per doc
          doc.save();
        }
    });

    // Delete every other doc within each tag (even within-tag index)
    database.transaction(() -> {
      for (int t = 0; t < totalTags; t++)
        for (int s = 0; s < docsPerTag; s += 2) {
          final int seq = t * 1000 + s;
          try (final ResultSet rs = database.query("sql", "SELECT FROM RemoveDoc WHERE seq = " + seq)) {
            if (rs.hasNext())
              rs.next().getRecord().get().asDocument().delete();
          }
        }
    });

    // Verify: 25 per tag remain
    database.transaction(() -> {
      assertThat(database.countType("RemoveDoc", false)).isEqualTo(250);

      final Index index = database.getSchema().getIndexByName("RemoveDoc[tag]");
      for (int t = 0; t < totalTags; t++) {
        final IndexCursor cursor = index.get(new Object[] { "tag_" + t });
        int count = 0;
        while (cursor.hasNext()) {
          cursor.next();
          count++;
        }
        assertThat(count).withFailMessage("tag_%d has %d, expected 25", t, count).isEqualTo(25);
      }
    });

    // Re-insert 250 more docs
    database.transaction(() -> {
      for (int t = 0; t < totalTags; t++)
        for (int s = docsPerTag; s < docsPerTag + 25; s++) {
          final MutableDocument doc = database.newDocument("RemoveDoc");
          doc.set("tag", "tag_" + t);
          doc.set("seq", t * 1000 + s);
          doc.save();
        }
    });

    // Verify: 50 per tag
    database.transaction(() -> {
      assertThat(database.countType("RemoveDoc", false)).isEqualTo(500);

      final Index index = database.getSchema().getIndexByName("RemoveDoc[tag]");
      for (int t = 0; t < totalTags; t++) {
        final IndexCursor cursor = index.get(new Object[] { "tag_" + t });
        int count = 0;
        while (cursor.hasNext()) {
          cursor.next();
          count++;
        }
        assertThat(count).withFailMessage("tag_%d has %d, expected 50", t, count).isEqualTo(50);
      }
    });
  }

  /**
   * Regression test for the user's comment on #3714: verifies that after the addRIDToExistingEntry
   * fix, a full batch UPSERT workflow (like the Metadata ingest in the reported issue) does NOT
   * produce duplicate results in a SELECT ... ORDER BY on a non-unique hash-indexed property.
   * <p>
   * Mirrors the user's schema: unique hash index on recordId (used by UPSERT WHERE) and a
   * non-unique hash index on publicationYear. Repeatedly UPSERTs the same records and verifies
   * that the physical record count and the SELECT result count stay consistent.
   */
  @Test
  void testNoDuplicatesAfterBatchUpsertsWithHashIndexes() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("Metadata");
      type.createProperty("recordId", String.class);
      type.createProperty("publicationYear", Integer.class);
      type.createProperty("name", String.class);
      database.getSchema().buildTypeIndex("Metadata", new String[] { "recordId" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).withPageSize(2048).create();
      database.getSchema().buildTypeIndex("Metadata", new String[] { "publicationYear" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(2048).create();
    });

    final int totalRecords = 500;
    final int distinctYears = 10;

    // Initial UPSERT batch
    database.transaction(() -> {
      for (int i = 0; i < totalRecords; i++) {
        database.command("sql",
            "UPDATE Metadata CONTENT {\"recordId\":\"rec_" + i + "\",\"publicationYear\":" + (2000 + i % distinctYears)
                + ",\"name\":\"initial_" + i + "\"} UPSERT WHERE recordId = 'rec_" + i + "'");
      }
    });

    // Re-UPSERT same records multiple times with updated name (stresses remove+insert on both indexes)
    for (int iter = 0; iter < 5; iter++) {
      final int iteration = iter;
      database.transaction(() -> {
        for (int i = 0; i < totalRecords; i++) {
          database.command("sql",
              "UPDATE Metadata CONTENT {\"recordId\":\"rec_" + i + "\",\"publicationYear\":" + (2000 + i % distinctYears)
                  + ",\"name\":\"iter_" + iteration + "_" + i + "\"} UPSERT WHERE recordId = 'rec_" + i + "'");
        }
      });
    }

    // Verify physical record count
    database.transaction(() -> {
      assertThat(database.countType("Metadata", false)).isEqualTo(totalRecords);

      // Verify unique hash index lookup works for every recordId
      for (int i = 0; i < totalRecords; i++) {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Metadata WHERE recordId = 'rec_" + i + "'")) {
          int found = 0;
          while (rs.hasNext()) {
            rs.next();
            found++;
          }
          assertThat(found).withFailMessage("Expected 1 record for recordId rec_%d but got %d", i, found).isEqualTo(1);
        }
      }

      // The exact query from the user's comment - must NOT return duplicates
      try (final ResultSet rs = database.query("sql", "SELECT publicationYear FROM Metadata ORDER BY publicationYear DESC")) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).withFailMessage("Duplicate results in ORDER BY query: got %d, expected %d", count, totalRecords)
            .isEqualTo(totalRecords);
      }

      // Non-unique hash index lookups must return exactly totalRecords/distinctYears per year
      for (int y = 0; y < distinctYears; y++) {
        final int year = 2000 + y;
        try (final ResultSet rs = database.query("sql", "SELECT FROM Metadata WHERE publicationYear = " + year)) {
          int found = 0;
          while (rs.hasNext()) {
            rs.next();
            found++;
          }
          assertThat(found).withFailMessage("publicationYear %d returned %d, expected %d", year, found, totalRecords / distinctYears)
              .isEqualTo(totalRecords / distinctYears);
        }
      }
    });
  }

  /**
   * Regression test for the orphan-entries bug in removeFromBucket: when a non-unique key's
   * entries are split across multiple overflow pages (which happens after addRIDToExistingEntry
   * exhausts in-page space), a single remove(keys) call (without specific RID) must walk the
   * entire overflow chain and remove ALL matching entries, not stop at the first matching page.
   */
  @Test
  void testRemoveAllEntriesForKeyAcrossOverflowChain() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("OrphanDoc");
      type.createProperty("key", String.class);
      type.createProperty("seq", Integer.class);
      // Very small pages to force entries to split across overflow chain
      database.getSchema().buildTypeIndex("OrphanDoc", new String[] { "key" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(512).create();
    });

    // Insert many docs with the same key - forces RID list to spill into overflow pages
    final int totalDocs = 400;
    database.transaction(() -> {
      for (int i = 0; i < totalDocs; i++) {
        final MutableDocument doc = database.newDocument("OrphanDoc");
        doc.set("key", "hot_key");
        doc.set("seq", i);
        doc.save();
      }
    });

    // Drop everything for the key via the index-level remove(keys) - this is the exact path
    // that had the orphan bug: before the fix it stopped after the first matching page.
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("OrphanDoc[key]");
      index.remove(new Object[] { "hot_key" });
    });

    // The index must report zero entries for the key after the single remove call
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("OrphanDoc[key]");
      final IndexCursor cursor = index.get(new Object[] { "hot_key" });
      int remaining = 0;
      while (cursor.hasNext()) {
        cursor.next();
        remaining++;
      }
      assertThat(remaining).withFailMessage("Orphan entries left in overflow chain after remove(keys): %d", remaining).isEqualTo(0);
    });
  }

  /**
   * Test overflow chain handling with deeply chained pages.
   * Uses very small page size to force many overflow pages.
   */
  @Test
  void testDeepOverflowChainDoesNotStackOverflow() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("OverflowDoc");
      type.createProperty("key", String.class);
      type.createProperty("seq", Integer.class);
      // Very small pages to force overflow chains
      database.getSchema().buildTypeIndex("OverflowDoc", new String[] { "key" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).withPageSize(512).create();
    });

    // All docs have same key - forces single bucket with overflow chain
    database.transaction(() -> {
      for (int i = 0; i < 300; i++) {
        final MutableDocument doc = database.newDocument("OverflowDoc");
        doc.set("key", "same_key");
        doc.set("seq", i);
        doc.save();
      }
    });

    // Verify all entries are readable
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("OverflowDoc[key]");
      final IndexCursor cursor = index.get(new Object[] { "same_key" });
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(300);
    });
  }
}
