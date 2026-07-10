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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #4946: a failed compaction must be atomic. Leaf pages are flushed eagerly during the merge with no WAL;
 * if compact() throws after some were written (root-page overflow, I/O error, interrupt), the orphans used
 * to stay counted in the in-RAM page count - invisible at first (page 0's series counter was never bumped)
 * but PUBLISHED by the NEXT successful round, whose setCompactedTotalPages() writes getTotalPages(): stale
 * duplicates re-exposing tombstoned entries and a malformed partial series walked by positional logic. And
 * when the FIRST compaction failed (no sub-index yet), the freshly registered compacted file was never
 * linked nor dropped, so every failed round leaked another file.
 * <p>
 * The deterministic failure used here is the real "Root index page overflow" path: with 1KB pages, a round
 * that produces more compacted leaf pages than one root page can reference throws AFTER those leaf pages
 * were flushed - exactly the mid-merge failure the issue describes.
 */
class LSMTreeCompactionAtomicityTest extends TestHelper {

  private static final String TYPE_NAME = "Doc";

  @BeforeEach
  @Override
  public void beforeTest() {
    // Disable AUTO compaction (see LSMTreeCompactionCorrectnessTest: the database snapshots this into its own
    // context configuration, so it is also set on the instance in createType).
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private LSMTreeIndex createType() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    // Small pages: a handful of entries seals immutable pages, and a round with many leaf pages overflows
    // the single root page deterministically.
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(1024).create();
    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next().getIndexesOnBuckets()[0];
  }

  /**
   * dropFile leaves a null slot in FileManager.getFiles() (ids are never reused), so size() cannot detect a
   * leak: count the REAL registered files instead, and pair it with the on-disk temp-file check below.
   */
  private long registeredFiles() {
    return ((DatabaseInternal) database).getFileManager().getFiles().stream().filter(Objects::nonNull).count();
  }

  private File[] tempFilesOnDisk() {
    return new File(((DatabaseInternal) database).getDatabasePath())
        .listFiles((d, n) -> n.contains("temp_"));
  }

  private void insert(final int from, final int to, final int keyLength) {
    database.transaction(() -> {
      for (int i = from; i < to; i++) {
        final String pad = "k".repeat(keyLength);
        database.newDocument(TYPE_NAME).set("email", pad + "-" + String.format("%08d", i)).save();
      }
    });
  }

  private void compactExpectingRootOverflow(final LSMTreeIndex index) throws Exception {
    assertThat(index.scheduleCompaction()).as("compaction scheduled").isTrue();
    assertThatThrownBy(index::compact)
        .as("the oversized round must fail with the real mid-merge root-overflow error")
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Root index page overflow");
  }

  @Test
  void failedFirstCompactionDropsTempFileAndDoesNotLeakPerRound() throws Exception {
    final LSMTreeIndex index = createType();
    // Enough long keys that the FIRST round produces more leaf pages than one root page can reference.
    insert(0, 3_000, 60);

    assertThat(index.getMutableIndex().getSubIndex()).as("no sub-index before the first compaction").isNull();
    final long filesBefore = registeredFiles();

    compactExpectingRootOverflow(index);

    assertThat(index.getMutableIndex().getSubIndex())
        .as("the failed first compaction must not link the compacted index").isNull();
    assertThat(registeredFiles())
        .as("the failed first compaction must drop the TEMP compacted file it registered (#4946)")
        .isEqualTo(filesBefore);
    assertThat(tempFilesOnDisk()).as("no temp compacted file may remain on disk (#4946)").isNullOrEmpty();

    // The issue's leak: every further failed round used to register (and orphan) ANOTHER file.
    compactExpectingRootOverflow(index);
    assertThat(registeredFiles())
        .as("repeated failed compactions must not leak one file per round (#4946)")
        .isEqualTo(filesBefore);
    assertThat(tempFilesOnDisk()).as("no temp compacted files may accumulate on disk (#4946)").isNullOrEmpty();

    // The index itself is untouched: the mutable side still answers correctly.
    assertThat(database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE email = ?",
        "k".repeat(60) + "-" + String.format("%08d", 1234)).next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void failedLaterCompactionRollsBackPageCountSoOrphansAreNeverPublished() throws Exception {
    final LSMTreeIndex index = createType();

    // Round 1: small (short keys, few leaf pages) - succeeds and links the sub-index.
    insert(0, 120, 8);
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).as("the small first round must succeed").isTrue();
    assertThat(index.getMutableIndex().getSubIndex()).isNotNull();

    final int pagesBeforeFailedRound = index.getMutableIndex().getSubIndex().getTotalPages();

    // Round 2: oversized - fails mid-merge AFTER flushing orphan leaf pages into the sub-index file.
    insert(120, 3_120, 60);
    compactExpectingRootOverflow(index);

    assertThat(index.getMutableIndex().getSubIndex().getTotalPages())
        .as("""
            the failed round must roll the in-RAM page count back, or the next successful round's \
            setCompactedTotalPages() publishes the orphan pages as live series content (#4946)""")
        .isEqualTo(pagesBeforeFailedRound);

    // The data is still fully correct after the failed round (served by round 1's series + the mutable side).
    assertThat(database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE email = ?",
        "k".repeat(8) + "-" + String.format("%08d", 42)).next().<Long>getProperty("c")).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT count(*) as c FROM " + TYPE_NAME + " WHERE email = ?",
        "k".repeat(60) + "-" + String.format("%08d", 1500)).next().<Long>getProperty("c")).isEqualTo(1L);
  }
}
