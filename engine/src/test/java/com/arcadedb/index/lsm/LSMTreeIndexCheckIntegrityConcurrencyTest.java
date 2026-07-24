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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: {@link LSMTreeIndex#checkIntegrity()} racing an index compaction must not report
 * false corruption.
 * <p>
 * Every reader of the index ({@code get()}, {@code range()}, {@code iterator()}) runs under the
 * index read lock, and {@code splitIndex()} publishes the compaction result - swapping in a new
 * mutable file and DROPPING the old one - under the write lock. {@code checkIntegrity()} was the
 * one reader that skipped the lock: it captured the mutable index and walked its pages by file id
 * with no protection, so a compaction publishing mid-walk dropped the file under its feet
 * ({@code PageManager.deleteFile} also purges the dropped file's pages from the page cache, so the
 * very next page read fails on the FileManager lookup). Every remaining page then failed with
 * "cannot be read (File with id N was not found)" (or "channel is null" while the drop was in
 * flight), the non-empty problem list got the "physical key order does not match the current
 * comparator" header prepended, and CHECK DATABASE reported a healthy index as corrupt, telling
 * the user to rebuild it.
 * <p>
 * The deterministic test drives the exact interleaving with the compactor test hook: compaction is
 * paused after its series is written (just before publication), {@code checkIntegrity()} is started
 * on a mutable file large enough that its walk takes orders of magnitude longer than the
 * publication, and the compaction is released mid-walk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexCheckIntegrityConcurrencyTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";

  @Override
  public void beforeTest() {
    // compaction is driven manually
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void checkIntegrityDuringCompactionPublicationReportsNoFalseCorruption() throws Exception {
    final LSMTreeIndex index = createTypeWithBigMutableIndex();

    final CountDownLatch compactionPaused = new CountDownLatch(1);
    final CountDownLatch releaseCompaction = new CountDownLatch(1);
    final AtomicReference<Throwable> compactorError = new AtomicReference<>();

    // compaction thread: pauses right after the compacted series is written, i.e. just before the
    // publication that swaps the mutable file and drops the old one
    final Thread compactor = new Thread(() -> {
      LSMTreeIndexCompactor.setCompactionTestHook(compactedIndex -> {
        compactionPaused.countDown();
        try {
          releaseCompaction.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      try {
        assertThat(index.scheduleCompaction()).isTrue();
        assertThat(index.compact()).isTrue();
      } catch (final Throwable e) {
        compactorError.set(e);
      } finally {
        LSMTreeIndexCompactor.setCompactionTestHook(null);
      }
    }, "index-compactor");

    final AtomicReference<List<String>> problems = new AtomicReference<>();
    final AtomicBoolean checkerStarted = new AtomicBoolean(false);

    // checker thread: starts its walk while the compaction is paused; the walk of the big mutable
    // file takes far longer than the publication released underneath it
    final Thread checker = new Thread(() -> {
      checkerStarted.set(true);
      problems.set(index.checkIntegrity());
    }, "index-checker");

    compactor.start();
    assertThat(compactionPaused.await(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

    checker.start();
    // let the checker enter its page walk, then publish the compaction underneath it
    while (!checkerStarted.get())
      Thread.onSpinWait();
    releaseCompaction.countDown();

    compactor.join(120_000);
    checker.join(120_000);

    assertThat(compactorError.get()).as("compaction must complete cleanly").isNull();
    assertThat(problems.get())
        .as("checkIntegrity() must never report corruption on a healthy index because a compaction published mid-walk")
        .isEmpty();

    // once everything is quiet the index must (still) be clean
    assertThat(index.checkIntegrity()).isEmpty();
  }

  private LSMTreeIndex createTypeWithBigMutableIndex() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    // Sizing: the checkIntegrity() walk deserializes every key of every page, so its duration grows
    // with the key count and must comfortably exceed the compaction publication (swap + schema save
    // + old-file drop, ~15-25ms) released underneath it. The compactor's merge instead scans every
    // page iterator per output key (O(keys * pages)), so the page count must stay small: mid-size
    // pages keep it in the hundreds while 800k keys stretch the walk to several times the
    // publication window.
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(32 * 1024).create();

    if (database.isTransactionActive())
      database.commit();

    for (int batch = 0; batch < 8; ++batch) {
      final int base = batch * 100_000;
      database.transaction(() -> {
        for (int i = 0; i < 100_000; ++i)
          database.newDocument(TYPE_NAME).set("id", base + i).save();
      });
    }

    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }
}
