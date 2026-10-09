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

import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9070: under {@code REPEATABLE_READ} a read of a small record pins page 0, which also holds the head chunk of a
 * multi-page record but none of its continuation pages. Another transaction rewrites the multi-page record and commits.
 * Reading the multi-page record must then return the old record, the complete new one, or refuse with a
 * {@link ConcurrentModificationException}, and every later read in the same transaction must answer the same way. It used
 * to return the new record once (assembled from a head the transaction did not hold) and then a mix of the pinned old
 * head with the new tails that first read had pinned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9070RepeatableReadNeighbourPinnedHeadTest extends BucketPageLayoutTestSupport {
  private static final int OLD_SIZE = 70_000;
  private static final int NEW_SIZE = 70_001;

  /** The reported case: the other transaction rewrites both the head chunk and the continuation chunks. */
  @Test
  void headPinnedByNeighbourReadIsNeverTorn() {
    assertNeighbourPinnedHeadReadIsRefused(OLD_SIZE, NEW_SIZE, 'y');
  }

  /** Only the head chunk changes (same length, same filler): the continuation chunks are byte-identical. */
  @Test
  void headOnlyChangePinnedByNeighbourReadIsNeverTorn() {
    assertNeighbourPinnedHeadReadIsRefused(OLD_SIZE, OLD_SIZE, 'x');
  }

  /** The same with a chain of several continuation chunks, on several pages. */
  @Test
  void longChainPinnedByNeighbourReadIsNeverTorn() {
    assertNeighbourPinnedHeadReadIsRefused(250_000, 250_001, 'y');
  }

  /**
   * Every page of the record is pinned before its first read, but by reads of other records made on both sides of the
   * commit that rewrote it: the head page by the read of a neighbour before the commit, the continuation page by the
   * read of a record that lives on it after the commit. A chain whose pages merely happen to all be pinned is not a
   * snapshot, and must not be returned as one.
   */
  @Test
  void chainPinnedByNeighboursOnBothSidesOfACommitIsNeverTorn() {
    final RID[] rids = createSmallAndLarge();
    final LocalBucket bucket = bucketOf("Doc");
    assertThat(bucket.getTotalPages()).as("the continuation chunks of the large record are all on page 1").isEqualTo(2);

    final RID[] onTailPage = new RID[1];
    database.transaction(() -> onTailPage[0] = database.newDocument("Doc").set("v", 0).set("s", "tail page").save().getIdentity());
    assertThat(onTailPage[0].getPosition() / bucket.getMaxRecordsInPage()).as("a record on page 1").isEqualTo(1L);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // pins page 0, with the head chunk, before the commit
      assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);

      // same length: the chain keeps its shape, so the old head points at the new tail chunk
      rewriteLarge(rids[1], OLD_SIZE, 'y');

      // pins page 1, with the new continuation chunk, after the commit
      assertThat(database.lookupByRID(onTailPage[0], true).asDocument().getString("s")).isEqualTo("tail page");

      // the old head cannot be completed (its tail chunk is gone) and the transaction holds no newer head: refused
      assertThat(readConsistentOrRefusal(rids[1])).isEqualTo("CME");
      assertThat(isPinned(bucket, 1)).as("a pin taken by an earlier read is not released by the failed one").isTrue();
      assertThat(readConsistentOrRefusal(rids[1])).as("a second read must answer as the first").isEqualTo("CME");
    } finally {
      database.rollback();
    }
  }

  /**
   * A record read whole and validated by the transaction stays its snapshot after a concurrent rewrite, even when the
   * read of a neighbour pinned its head page first (#8987 must keep holding).
   */
  @Test
  void recordReadBeforeTheRewriteStaysTheSnapshot() {
    final RID[] rids = createSmallAndLarge();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);
      assertThat(readLarge(rids[1])).isEqualTo("v=0 s=" + OLD_SIZE + "x");

      rewriteLarge(rids[1], NEW_SIZE, 'y');

      assertThat(readLarge(rids[1])).isEqualTo("v=0 s=" + OLD_SIZE + "x");
      assertThat(readLarge(rids[1])).isEqualTo("v=0 s=" + OLD_SIZE + "x");
    } finally {
      database.rollback();
    }
  }

  /**
   * The transaction's own update of a record it read before is what it reads afterwards, not the snapshot of the chain.
   */
  @Test
  void ownUpdateAfterASnapshotReadIsReadBack() {
    final RID[] rids = createSmallAndLarge();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(readLarge(rids[1])).isEqualTo("v=0 s=" + OLD_SIZE + "x");
      database.lookupByRID(rids[1], true).asDocument().modify().set("v", 7).set("s", "z".repeat(NEW_SIZE)).save();
      assertThat(readLarge(rids[1])).isEqualTo("v=7 s=" + NEW_SIZE + "z");
      database.commit();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(readLarge(rids[1])).isEqualTo("v=7 s=" + NEW_SIZE + "z");
    } finally {
      database.rollback();
    }
  }

  /**
   * Reads under REPEATABLE_READ while another thread keeps rewriting the record: whatever the first read of a transaction
   * returns, a second read in the same transaction returns it again, never a mix and never a different version.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES) // hang detector for the writer join, not a latency bound
  void everyReadOfATransactionAgreesUnderConcurrentRewrites() throws Exception {
    final RID[] rids = createSmallAndLarge();

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    final Thread writer = new Thread(() -> {
      try {
        for (int v = 1; !stop.get(); v++) {
          final int version = v;
          database.transaction(() -> database.lookupByRID(rids[1], true).asDocument().modify().set("v", version)
              .set("s", String.valueOf((char) ('a' + version % 26)).repeat(OLD_SIZE + version % 3 * 20_000)).save(), false, 100);
        }
      } catch (final Throwable e) {
        writerFailure.set(e);
      }
    });
    writer.start();
    try {
      for (int i = 0; i < 300; i++) {
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        try {
          if (i % 2 == 0)
            // half of the transactions pin the head page through the neighbour first
            database.lookupByRID(rids[0], true).asDocument().getInteger("v");

          final String first = readConsistentOrRefusal(rids[1]);
          final String second = readConsistentOrRefusal(rids[1]);
          assertThat(second).as("second read of transaction " + i).isEqualTo(first);
        } finally {
          database.rollback();
        }
      }
    } finally {
      stop.set(true);
      writer.join();
    }
    assertThat(writerFailure.get()).as("the writer thread must not have failed").isNull();
  }

  /**
   * The head page is pinned by the read of the neighbour, the continuation pages are not pinned at all. Once the large
   * record is rewritten its old version cannot be completed (the old tail chunks are gone) and the transaction holds no
   * newer head, so every read in the transaction is refused, and the failed reads leave no pin behind.
   */
  private void assertNeighbourPinnedHeadReadIsRefused(final int oldSize, final int newSize, final char newFiller) {
    final RID[] rids = createSmallAndLarge(oldSize);
    final LocalBucket bucket = bucketOf("Doc");
    final int pages = bucket.getTotalPages();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);

      rewriteLarge(rids[1], newSize, newFiller);

      assertThat(readConsistentOrRefusal(rids[1])).isEqualTo("CME");
      for (int page = 1; page < pages; page++)
        assertThat(isPinned(bucket, page)).as("page " + page + " pinned by the failed read only").isFalse();
      assertThat(readConsistentOrRefusal(rids[1])).as("a second read must answer as the first").isEqualTo("CME");
      assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);
    } finally {
      database.rollback();
    }

    // a new transaction reads the new version whole
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(readLarge(rids[1])).isEqualTo("v=1 s=" + newSize + newFiller);
    } finally {
      database.rollback();
    }
  }

  private boolean isPinned(final LocalBucket bucket, final int pageNumber) {
    return ((DatabaseInternal) database).getTransaction().getPinnedPage(new PageId(database, bucket.getFileId(), pageNumber)) != null;
  }

  private RID[] createSmallAndLarge() {
    return createSmallAndLarge(OLD_SIZE);
  }

  /** A small record and a large one whose head chunk shares page 0 with it, its continuation chunks elsewhere. */
  private RID[] createSmallAndLarge(final int size) {
    database.transaction(() -> database.getSchema().createDocumentType("Doc", 1));
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("Doc").set("v", 0).set("s", "small").save().getIdentity();
      rids[1] = database.newDocument("Doc").set("v", 0).set("s", "x").save().getIdentity();
    });
    // grown in place, so it keeps its slot on page 0 and spills into a chunk chain on the pages that follow
    database.transaction(() -> rids[1].asDocument(true).modify().set("s", "x".repeat(size)).save());
    assertThat(rids[1].getPosition()).as("both records on page 0").isEqualTo(rids[0].getPosition() + 1);
    assertThat((Long) bucketStats("Doc").get("totalMultiPageRecords")).isEqualTo(1L);
    return rids;
  }

  private void rewriteLarge(final RID rid, final int size, final char filler) {
    inAnotherThread(() -> database.transaction(() -> database.lookupByRID(rid, true).asDocument().modify().set("v", 1)
        .set("s", String.valueOf(filler).repeat(size)).save()));
  }

  /** {@code v=<v> s=<length><filler>}, failing on a value of {@code s} that is not one filler repeated. */
  private String readLarge(final RID rid) {
    final Document read = database.lookupByRID(rid, true).asDocument();
    final int v = read.getInteger("v");
    final String s = read.getString("s");
    assertThat(s).as("v=" + v + " must come with its s").isNotNull();
    final char filler = s.charAt(0);
    assertThat(s.chars().allMatch(c -> c == filler)).as("s of v=" + v + " is a mix").isTrue();
    return "v=" + v + " s=" + s.length() + filler;
  }

  private String readConsistentOrRefusal(final RID rid) {
    try {
      return readLarge(rid);
    } catch (final ConcurrentModificationException e) {
      return "CME";
    }
  }

}
