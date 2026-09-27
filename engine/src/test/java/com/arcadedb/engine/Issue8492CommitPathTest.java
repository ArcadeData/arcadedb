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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8492: what a small transaction's commit costs per page it touched. Three things used to cost in proportion to
 * the page or to history rather than to the change: packing every modified bucket page (a list and a sort of every
 * record, even on a page with no hole to close), copying every published page whole into the read cache, and
 * clearing transaction maps a past large transaction had grown. Each is asserted here on its own terms: the fast path
 * of the compression must still send every page with a hole through the full one, a published page is shared and
 * can no longer be written, and the maps are replaced once outgrown.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8492CommitPathTest extends BucketPageLayoutTestSupport {
  private static final String TYPE    = "Commit8492";
  private static final int    RECORDS = 20;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1);
  }

  /** A page filled by appends only has no hole: the fast walk proves it packed and says where its free tail begins. */
  @Test
  void aPageOfAppendsIsProvedPackedByTheFastWalk() {
    final RID[] rids = insertRecords();
    final LocalBucket bucket = bucketOf(TYPE);

    database.transaction(() -> onSlot(rids[0], page -> {
      final int contentEnd = bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET));
      final RID last = rids[RECORDS - 1];
      assertThat(contentEnd).as("the free tail starts right after the last record appended")
          .isGreaterThan(recordOffsetOf(page, last));
      return 0L;
    }));
    checkDatabase();
  }

  /**
   * A shrink and a delete leave holes: the fast walk must refuse both pages, so the commit packs them through the full
   * compression - after which the next transaction finds the page packed again and every record intact.
   */
  @Test
  void aHoleSendsThePageThroughTheFullCompression() {
    final RID[] rids = insertRecords();
    final LocalBucket bucket = bucketOf(TYPE);

    database.transaction(() -> {
      final MutableDocument shrunk = rids[5].asDocument(true).modify();
      shrunk.set("v", "short").save();
      rids[9].asDocument(true).delete();

      onSlot(rids[0], page -> {
        assertThat(bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET)))
            .as("a page with holes is never vouched for by the fast walk").isEqualTo(-1);
        return 0L;
      });
    });

    database.transaction(() -> onSlot(rids[0], page -> {
      assertThat(bucket.packedContentEnd(page, page.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET)))
          .as("the commit packed the page").isPositive();
      return 0L;
    }));

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        if (i == 9)
          continue;
        assertThat(rids[i].asDocument(true).getString("v")).isEqualTo(i == 5 ? "short" : value(i));
      }
    });
    checkDatabase();
  }

  /**
   * A published page is final: the read cache shares its array instead of copying the whole page, and a write to it
   * afterwards - which would change a page other transactions already read - is refused. A page that was not
   * published (the index compaction keeps filling the page it hands over) is still copied.
   */
  @Test
  void aPublishedPageIsSharedByTheReadCacheAndRefusesWrites() {
    final MutablePage page = new MutablePage(new PageId(database, bucketOf(TYPE).getFileId(), 0), 4096);
    page.writeInt(0, 42);

    final CachedPage copied = new CachedPage(page, true);
    assertThat(copied.useAsImmutable().getContent().array()).isNotSameAs(page.getContent().array());

    page.markPublished();
    final CachedPage shared = new CachedPage(page, true);
    assertThat(shared.useAsImmutable().getContent().array()).isSameAs(page.getContent().array());
    assertThat(shared.useAsImmutable().readInt(0)).isEqualTo(42);

    // REFUSED IN EVERY JVM, NOT ONLY UNDER ASSERTIONS: THE ALTERNATIVE IS SILENTLY CHANGING A PAGE OTHERS ARE READING
    assertThatThrownBy(() -> page.writeInt(0, 7)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(page::updateMetadata).isInstanceOf(IllegalStateException.class);
  }

  /**
   * A large transaction grows the maps its context reuses; the next reset must replace them rather than keep a table
   * every later clear and iteration would walk whole. A small transaction keeps them.
   */
  @Test
  void aLargeTransactionDoesNotLeaveItsCapacityToTheNextOnes() throws Exception {
    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    final Field field = TransactionContext.class.getDeclaredField("modifiedRecordsCache");
    field.setAccessible(true);

    database.transaction(() -> database.newDocument(TYPE).set("v", "small").save());
    final Object afterSmall = field.get(tx);
    database.transaction(() -> database.newDocument(TYPE).set("v", "small").save());
    assertThat(field.get(tx)).as("a small transaction keeps the map").isSameAs(afterSmall);

    database.transaction(() -> {
      for (int i = 0; i < 5_000; i++)
        database.newDocument(TYPE).set("v", "bulk" + i).save();
    });
    final Object afterLarge = field.get(tx);
    assertThat(afterLarge).as("the grown map is replaced").isNotSameAs(afterSmall);
    assertThat((Map<?, ?>) afterLarge).isEmpty();
    checkDatabase();
  }

  private RID[] insertRecords() {
    final RID[] rids = new RID[RECORDS];
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument(TYPE).set("v", value(i)).save().getIdentity();
    });
    return rids;
  }

  private static String value(final int i) {
    return "record-" + i + "-" + "x".repeat(200);
  }
}
