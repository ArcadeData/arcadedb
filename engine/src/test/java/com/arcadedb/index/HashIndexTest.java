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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashIndexTest extends TestHelper {
  private static final int    TOT       = 10_000;
  private static final String TYPE_NAME = "HashDoc";

  @Test
  void basicGetUniqueIndex() {
    database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found").isTrue();
        final Document doc = cursor.next().asDocument();
        assertThat((int) doc.get("id")).isEqualTo(i);
      }
    });
  }

  @Test
  void lookupMissingKey() {
    database.transaction(() -> {
      final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { -1 });
      assertThat(cursor.hasNext()).isFalse();

      final IndexCursor cursor2 = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { TOT + 1 });
      assertThat(cursor2.hasNext()).isFalse();
    });
  }

  @Test
  void duplicateKeyRejected() {
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("id", 0);
      doc.set("name", "Duplicate");
      doc.save();
    })).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void removeByKey() {
    database.transaction(() -> {
      for (int i = 0; i < 100; ++i) {
        final Object[] key = new Object[] { i };
        final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
        final IndexCursor cursor = index.get(key);
        assertThat(cursor.hasNext()).isTrue();
        final Identifiable record = cursor.next();
        index.remove(key, record);
      }

      // Verify removed
      for (int i = 0; i < 100; ++i) {
        final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " should be removed").isFalse();
      }

      // Verify remaining still present
      for (int i = 100; i < TOT; ++i) {
        final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " should still exist").isTrue();
      }
    });
  }

  @Test
  void supportsOrderedIterationsIsFalse() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
      assertThat(index.supportsOrderedIterations()).isFalse();
    });
  }

  @Test
  void indexTypeIsHash() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
    });
  }

  @Test
  void countEntries() {
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
      assertThat(index.countEntries()).isEqualTo(TOT);
    });
  }

  @Test
  void nonUniqueIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("NonUniqueDoc");
      type.createProperty("category", String.class);
      database.getSchema().buildTypeIndex("NonUniqueDoc", new String[] { "category" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();

      for (int i = 0; i < 100; ++i) {
        final MutableDocument doc = database.newDocument("NonUniqueDoc");
        doc.set("category", "cat_" + (i % 10)); // 10 categories, 10 docs each
        doc.save();
      }
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("NonUniqueDoc[category]");
      for (int c = 0; c < 10; ++c) {
        final IndexCursor cursor = index.get(new Object[] { "cat_" + c });
        int count = 0;
        while (cursor.hasNext()) {
          cursor.next();
          count++;
        }
        assertThat(count).isEqualTo(10);
      }
    });
  }

  @Test
  void stringKeys() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("StringKeyDoc");
      type.createProperty("name", String.class);
      database.getSchema().buildTypeIndex("StringKeyDoc", new String[] { "name" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      for (int i = 0; i < 1000; ++i) {
        final MutableDocument doc = database.newDocument("StringKeyDoc");
        doc.set("name", "user_" + i);
        doc.save();
      }
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("StringKeyDoc[name]");
      for (int i = 0; i < 1000; ++i) {
        final IndexCursor cursor = index.get(new Object[] { "user_" + i });
        assertThat(cursor.hasNext()).withFailMessage("String key user_" + i + " not found").isTrue();
      }
      // Missing key
      assertThat(index.get(new Object[] { "nonexistent" }).hasNext()).isFalse();
    });
  }

  @Test
  void longKeys() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("LongKeyDoc");
      type.createProperty("bigId", Long.class);
      database.getSchema().buildTypeIndex("LongKeyDoc", new String[] { "bigId" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      for (long i = 0; i < 1000; ++i) {
        final MutableDocument doc = database.newDocument("LongKeyDoc");
        doc.set("bigId", i * 1_000_000_000L);
        doc.save();
      }
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("LongKeyDoc[bigId]");
      for (long i = 0; i < 1000; ++i) {
        final IndexCursor cursor = index.get(new Object[] { i * 1_000_000_000L });
        assertThat(cursor.hasNext()).isTrue();
      }
    });
  }

  @Test
  void sqlLookupUsesHashIndex() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = 42")) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        assertThat(result.<Integer>getProperty("id")).isEqualTo(42);
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void persistenceAcrossReopen() {
    // Verify indexes are present before close
    database.transaction(() -> {
      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index idx : indexes) {
        if (idx instanceof TypeIndex)
          assertThat(idx.getName()).isEqualTo(TYPE_NAME + "[id]");
      }
    });

    database.close();
    database = factory.open();

    database.transaction(() -> {
      // First check that the TypeIndex was reconstructed
      boolean found = false;
      for (final Index idx : database.getSchema().getIndexes()) {
        if (idx.getName().equals(TYPE_NAME + "[id]")) {
          found = true;
          break;
        }
      }
      assertThat(found).withFailMessage("TypeIndex " + TYPE_NAME + "[id] not found after reopen").isTrue();

      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
      assertThat(index).isNotNull();
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.HASH);

      // Verify data is still there
      for (int i = 0; i < TOT; ++i) {
        final IndexCursor cursor = index.get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found after reopen").isTrue();
      }
    });
  }

  @Test
  void persistenceWriteAfterReopen() {
    database.close();
    database = factory.open();

    // Insert new entries after reopen
    database.transaction(() -> {
      for (int i = TOT; i < TOT + 100; ++i) {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("id", i);
        doc.set("name", "AfterReopen_" + i);
        doc.save();
      }
    });

    // Verify old and new entries coexist
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[id]");

      // Old entries still accessible
      for (int i = 0; i < 100; ++i) {
        final IndexCursor cursor = index.get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Old key " + i + " not found after reopen+write").isTrue();
      }

      // New entries accessible
      for (int i = TOT; i < TOT + 100; ++i) {
        final IndexCursor cursor = index.get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("New key " + i + " not found after reopen+write").isTrue();
        final Document doc = cursor.next().asDocument();
        assertThat(doc.getString("name")).isEqualTo("AfterReopen_" + i);
      }

      // Missing key still returns empty
      assertThat(index.get(new Object[] { -1 }).hasNext()).isFalse();

      // SQL query works after reopen
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE id = " + (TOT + 50))) {
        assertThat(rs.hasNext()).isTrue();
        final Result result = rs.next();
        assertThat(result.<Integer>getProperty("id")).isEqualTo(TOT + 50);
        assertThat(rs.hasNext()).isFalse();
      }
    });

    // Duplicate rejection still works after reopen
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = database.newDocument(TYPE_NAME);
      doc.set("id", 0);
      doc.set("name", "Duplicate");
      doc.save();
    })).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void compositeKey() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("CompositeDoc");
      type.createProperty("firstName", String.class);
      type.createProperty("lastName", String.class);
      database.getSchema().buildTypeIndex("CompositeDoc", new String[] { "firstName", "lastName" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      for (int i = 0; i < 100; ++i) {
        final MutableDocument doc = database.newDocument("CompositeDoc");
        doc.set("firstName", "first_" + i);
        doc.set("lastName", "last_" + i);
        doc.save();
      }
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("CompositeDoc[firstName,lastName]");
      for (int i = 0; i < 100; ++i) {
        final IndexCursor cursor = index.get(new Object[] { "first_" + i, "last_" + i });
        assertThat(cursor.hasNext()).isTrue();
      }
      // Wrong combination should not match
      assertThat(index.get(new Object[] { "first_0", "last_1" }).hasNext()).isFalse();
    });
  }

  @Test
  void bucketSplits() {
    // Use a very small page size to force bucket splits
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getOrCreateDocumentType("SplitDoc");
      type.createProperty("key", Integer.class);
      database.getSchema().buildTypeIndex("SplitDoc", new String[] { "key" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).withPageSize(1024).create();

      // 1024 byte pages can hold ~60-80 entries; inserting 500 forces multiple splits
      for (int i = 0; i < 500; ++i) {
        final MutableDocument doc = database.newDocument("SplitDoc");
        doc.set("key", i);
        doc.save();
      }
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("SplitDoc[key]");
      for (int i = 0; i < 500; ++i) {
        final IndexCursor cursor = index.get(new Object[] { i });
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found after splits").isTrue();
      }
    });
  }

  @Test
  void transactionRollback() {
    final int originalCount = TOT;

    try {
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("id", TOT + 100);
        doc.set("name", "Rollback");
        doc.save();

        // Should be visible within the TX
        final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { TOT + 100 });
        assertThat(cursor.hasNext()).isTrue();

        // Force rollback
        throw new RuntimeException("Intentional rollback");
      });
    } catch (final RuntimeException e) {
      // Expected
    }

    // After rollback, the key should not exist
    database.transaction(() -> {
      final IndexCursor cursor = database.getSchema().getIndexByName(TYPE_NAME + "[id]").get(new Object[] { TOT + 100 });
      assertThat(cursor.hasNext()).isFalse();
    });
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
      type.createProperty("id", Integer.class);
      type.createProperty("name", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "User_" + i);
        v.save();
      }
    });
  }
}
