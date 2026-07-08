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
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract guard for #5119: a {@code NOTUNIQUE} composite index on (word, lang) must return the same number of records
 * as a full scan for a key that has many duplicate entries, including when that single key's RID list overflows across
 * several compacted leaf pages (each indexed by its own root entry sharing the key).
 * <p>
 * The reporter observed the loss on 26.7.1, whose root cause is the compaction-atomicity bug fixed by #4946 (commit
 * b67973388, in 26.7.2): a failed compaction round left orphan leaf pages counted in the page series, and the next
 * successful round republished them, producing a malformed compacted series that the positional read logic walked
 * wrong - dropping live NOTUNIQUE duplicates. Heavy-duplicate keys make it easier to hit because they inflate the
 * compacted leaf-page count. The internal invariants of that fix are asserted by {@link LSMTreeCompactionAtomicityTest}
 * and the end-user failure sequence by {@link Issue5120CompactionDuplicatesTest}; this test locks down the specific
 * composite-key, split-across-pages read path from the issue so a future regression there is caught.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5119DuplicateCompositeKeyTest extends TestHelper {

  private static final String TYPE_NAME = "Tok";
  private static final int    DUPLICATES = 1000;

  private void createSchema() {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("word", String.class);
    type.createProperty("lang", String.class);
    type.createProperty("ref", String.class);
  }

  private void insertData() {
    database.transaction(() -> {
      // 1000 rows sharing the same (word, lang) key
      for (int i = 0; i < DUPLICATES; i++)
        database.newDocument(TYPE_NAME).set("word", "dup").set("lang", "xx").set("ref", String.format("r%04d", i)).save();

      // plus thousands of rows with distinct keys
      for (int i = 0; i < 5000; i++)
        database.newDocument(TYPE_NAME).set("word", "w" + i).set("lang", "xx").set("ref", String.format("d%04d", i)).save();
    });
  }

  private void createIndex() {
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "word", "lang" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
  }

  private long scanCount() {
    final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word.trim() = 'dup' AND lang = 'xx'");
    return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
  }

  private long indexCount() {
    final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word = 'dup' AND lang = 'xx'");
    return rs.hasNext() ? ((Number) rs.next().getProperty("c")).longValue() : 0;
  }

  @Test
  void indexAgreesWithScanRightAfterBuild() {
    createSchema();
    insertData();
    createIndex();

    assertThat(scanCount()).as("full scan sees all duplicates").isEqualTo(DUPLICATES);
    assertThat(indexCount()).as("index lookup must see all duplicates").isEqualTo(DUPLICATES);
  }

  @Test
  void indexAgreesWithScanAfterUnrelatedTransactions() {
    createSchema();
    insertData();
    createIndex();

    // "the loss grows over time as unrelated transactions run" - run many small unrelated transactions.
    for (int t = 0; t < 50; t++) {
      final int base = t;
      database.transaction(() -> {
        for (int i = 0; i < 20; i++)
          database.newDocument(TYPE_NAME).set("word", "u" + base + "_" + i).set("lang", "xx").set("ref", "x").save();
      });
    }

    assertThat(indexCount()).as("index lookup must still see all duplicates after unrelated writes").isEqualTo(DUPLICATES);
  }

  /**
   * Forces a single key's RID list to overflow across many compacted leaf pages (small page size, tens of thousands of
   * duplicates of one key) and then a successful compaction. Isolates any heavy-duplicate defect independent of the
   * #4946 compaction-atomicity fix.
   */
  @Test
  @Tag("slow")
  void heavyDuplicateKeySurvivesCompaction() {
    final int heavyDuplicates = 30_000;

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("word", String.class);
    type.createProperty("lang", String.class);

    // Small pages so one key's RID list overflows across several leaf pages during compaction.
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "word", "lang" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(4096).create();

    final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];

    // Insert in batches so the mutable index accumulates many pages before compaction.
    for (int batch = 0; batch < 30; batch++) {
      final int base = batch;
      database.transaction(() -> {
        for (int i = 0; i < heavyDuplicates / 30; i++)
          database.newDocument(TYPE_NAME).set("word", "dup").set("lang", "xx").save();
      });
    }

    // Force a (successful) compaction: many mutable pages -> single-key RID list split across leaf pages.
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final Throwable t) {
      throw new RuntimeException("compaction failed", t);
    }

    // The compaction must actually have split the single "dup" key across several leaf pages, otherwise the
    // multi-root-entry read path this test targets is not exercised.
    assertThat(index.getMutableIndex().getSubIndex()).as("compaction created the sub-index").isNotNull();
    assertThat(index.getMutableIndex().getSubIndex().getTotalPages()).as("single key overflowed across many leaf pages").isGreaterThan(2);

    final ResultSet scan = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word.trim() = 'dup'");
    final long scanCount = scan.hasNext() ? ((Number) scan.next().getProperty("c")).longValue() : 0;

    final ResultSet idx = database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE word = 'dup' AND lang = 'xx'");
    final long idxCount = idx.hasNext() ? ((Number) idx.next().getProperty("c")).longValue() : 0;

    assertThat(scanCount).as("scan sees all heavy duplicates").isEqualTo(heavyDuplicates);
    assertThat(idxCount).as("index sees all heavy duplicates after compaction").isEqualTo(heavyDuplicates);
  }
}
