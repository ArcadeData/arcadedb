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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A commit that loses the page version race on many pages rebases each of them onto the committed version. The rebase drops the
 * transaction's image of the page, so the copy of the committed page is written into the array of the dropped one instead of into a new
 * array: each further rebased page costs the commit a fraction of a page, not a page.
 */
class SlotRebasePageBufferTest extends TestHelper {
  private static final String BULKY                   = "x".repeat(2000);
  private static final int    FEW_PAGES               = 4;
  private static final int    MANY_PAGES              = 28;
  private static final long   HANDOFF_TIMEOUT_SECONDS = 60;

  private boolean savedSlotMerge;

  @BeforeEach
  void enableSlotMerge() {
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
  }

  @AfterEach
  void restoreSlotMerge() {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
  }

  @Test
  void eachFurtherRebasedPageCostsTheCommitLessThanAPage() throws Exception {
    database.getSchema().createDocumentType("Rebased", 1);
    final List<RID> all = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 1100; i++)
        all.add(database.newDocument("Rebased").set("tag", BULKY).set("n", i).save().getIdentity());
    });
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(all.getFirst().getBucketId());
    final int perPage = bucket.getMaxRecordsInPage();

    // the records of each page, in page order; each run takes the first two of a stretch of pages
    final TreeMap<Long, List<RID>> byPage = new TreeMap<>();
    for (final RID rid : all)
      byPage.computeIfAbsent(rid.getPosition() / perPage, k -> new ArrayList<>()).add(rid);
    final List<List<RID>> pages = new ArrayList<>(byPage.values());
    assertThat(pages.size()).as("pages the records fill").isGreaterThanOrEqualTo(FEW_PAGES + MANY_PAGES);

    final long recycledBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageBuffersRecycled;
    final long few = commitAllocation(pages.subList(0, FEW_PAGES));
    final long many = commitAllocation(pages.subList(FEW_PAGES, FEW_PAGES + MANY_PAGES));

    assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageBuffersRecycled - recycledBefore)
        .as("page copies written into a dropped page's array").isGreaterThanOrEqualTo(FEW_PAGES + MANY_PAGES);

    final long perFurtherPage = (many - few) / (MANY_PAGES - FEW_PAGES);
    assertThat(perFurtherPage).as("bytes each further rebased page cost the commit (%s pages: %s bytes, %s pages: %s bytes)", FEW_PAGES, few,
        MANY_PAGES, many).isLessThan(bucket.getPageSize() / 2L);
  }

  /**
   * Updates the first record of each page in a transaction that loses the race on every one of them to a competitor that updated the
   * second, and returns the bytes the losing commit allocated.
   */
  private long commitAllocation(final List<List<RID>> onPages) throws Exception {
    final List<RID> mine = new ArrayList<>();
    final List<RID> theirs = new ArrayList<>();
    for (final List<RID> onPage : onPages) {
      mine.add(onPage.get(0));
      theirs.add(onPage.get(1));
    }

    final CountDownLatch mainWrote = new CountDownLatch(1);
    final CountDownLatch competitorCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final Thread competitor = new Thread(() -> {
      try {
        if (!mainWrote.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
          throw new AssertionError("the main transaction never wrote");
        database.transaction(() -> {
          for (final RID rid : theirs)
            rid.asDocument(true).modify().set("tag", "b".repeat(BULKY.length())).save();
        }, true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        competitorCommitted.countDown();
      }
    }, "competitor");
    competitor.start();

    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
    final com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long thread = Thread.currentThread().threadId();
    long allocated;

    database.begin();
    try {
      try {
        for (final RID rid : mine)
          rid.asDocument(true).modify().set("tag", "a".repeat(BULKY.length())).save();
      } finally {
        mainWrote.countDown();
      }
      if (!competitorCommitted.await(HANDOFF_TIMEOUT_SECONDS, TimeUnit.SECONDS))
        throw new AssertionError("the competitor never committed");
      final long before = threads.getThreadAllocatedBytes(thread);
      database.commit();
      allocated = threads.getThreadAllocatedBytes(thread) - before;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      competitor.join(TimeUnit.SECONDS.toMillis(HANDOFF_TIMEOUT_SECONDS));
    }
    if (!errors.isEmpty())
      throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges - mergesBefore;
    assertThat(merges).as("pages rebased by the commit").isEqualTo(onPages.size());

    for (final RID rid : mine)
      assertThat(rid.asDocument(true).getString("tag")).isEqualTo("a".repeat(BULKY.length()));
    for (final RID rid : theirs)
      assertThat(rid.asDocument(true).getString("tag")).isEqualTo("b".repeat(BULKY.length()));
    for (final List<RID> onPage : onPages)
      for (final RID rid : onPage.subList(2, onPage.size()))
        assertThat(rid.asDocument(true).getString("tag")).isEqualTo(BULKY);
    return allocated;
  }
}
