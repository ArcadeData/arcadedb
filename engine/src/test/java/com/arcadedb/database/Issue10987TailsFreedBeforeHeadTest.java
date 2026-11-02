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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.RecordNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #10987: a delete publishes the pages of a chunked record one at a time, and a tail page can be visible
 * before the head page. A reader that meets the freed tails behind an intact head used to condemn the record as corrupted;
 * the head goes away a few microseconds later, so the answer is "deleted", which a scan skips.
 */
class Issue10987TailsFreedBeforeHeadTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "ChunkedRecord";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The tests free slots behind the back of the chains that reference them
    return false;
  }

  /** The tails are published, the head is not yet: the read must wait for the head and report the record deleted. */
  @Test
  void aReadThatMeetsTheFreedTailsBeforeTheHeadReportsTheRecordDeleted() throws Exception {
    final RID[] rids = createChunkedRecords(TYPE);
    final LocalBucket bucket = bucketOf(TYPE);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final RID victim = rids[0];
    tailsOf(victim).forEach(this::freeSlotBehindItsBack);

    // A budget wide enough for the head to be published while the reader waits, whatever the machine is doing
    final int previousRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 40);
    try {
      final long retriesBefore = pageManager.getStats().chunkChainReadRetries;
      final AtomicReference<Throwable> outcome = new AtomicReference<>();
      final Thread reader = new Thread(() -> {
        database.begin();
        try {
          bucket.getRecordInternal(victim, false);
        } catch (final Throwable e) {
          outcome.set(e);
        } finally {
          database.rollback();
        }
      }, "issue-10987-reader");
      reader.start();

      // The reader is now inside the wait for the head: publish it, as the delete's last page
      while (pageManager.getStats().chunkChainReadRetries == retriesBefore && reader.isAlive())
        Thread.onSpinWait();
      freeSlotBehindItsBack(victim);
      reader.join(60_000);

      assertThat(reader.isAlive()).isFalse();
      assertThat(outcome.get()).isInstanceOf(RecordNotFoundException.class).isNotInstanceOf(BrokenChunkChainException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, previousRetries);
    }
  }

  /** The other side: a head that never goes away is corruption, and is still reported as such. */
  @Test
  void aHeadThatStaysIntactBehindFreedTailsIsStillCorruption() {
    final RID[] rids = createChunkedRecords(TYPE);
    final LocalBucket bucket = bucketOf(TYPE);
    tailsOf(rids[0]).forEach(this::freeSlotBehindItsBack);

    database.begin();
    try {
      assertThatThrownBy(() -> bucket.getRecordInternal(rids[0], false)).isInstanceOf(BrokenChunkChainException.class)
          .hasMessageContaining("chunk slot cleaned at chunk 0");
    } finally {
      database.rollback();
    }
  }

  /** The continuation chunks of {@code head}, in chain order. */
  private List<RID> tailsOf(final RID head) {
    final List<RID> tails = new ArrayList<>();
    long pointer = head.getPosition();
    for (;;) {
      final long current = pointer;
      final long[] next = new long[1];
      final RID chunk = new RID(head.getBucketId(), current);
      database.transaction(() -> onSlot(chunk, page -> {
        final int offset = recordOffsetOf(page, chunk);
        final long[] size = page.readNumberAndSize(offset);
        next[0] = page.readLong((int) (offset + size[1] + Binary.INT_SERIALIZED_SIZE));
        return 0L;
      }));
      if (next[0] == 0L)
        return tails;
      // The link is stored as the position, or as -(position + 1) in the newer encoding
      pointer = next[0] > 0L ? next[0] : -(next[0] + 1L);
      tails.add(new RID(head.getBucketId(), pointer));
    }
  }
}
