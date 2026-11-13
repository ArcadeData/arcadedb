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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests of the REPEATABLE_READ snapshot of multi-page records. Each nested class keeps the regression of one reported issue (see the Javadoc of each).
 */
public class RepeatableReadMultiPageRecordTest {
  /**
   * Issue #8987: under {@code REPEATABLE_READ} a second read of a multi-page record must return the snapshot the
   * transaction already pinned, exactly as it does for a single-page record. It used to return the other transaction's
   * newer head chunk (when only the head moved) or fail with a {@code ConcurrentModificationException} (when a
   * continuation chunk moved too), because the chain validation compared the pinned pages with the newest committed ones.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatableReadLargeRecord extends BucketPageLayoutTestSupport {
    /**
     * Two records whose chunk chains share continuation pages: the read of the neighbour pins those pages, then the other
     * record is rewritten and read for the first time, so its head is pinned fresh on top of older tails. The read must
     * never answer with a mix of the two versions.
     */
    @Test
    void headPinnedFreshOnOlderTailsIsNeverTorn() {
      final RID[] rids = createChunkedRecords("SharedTails");
      final String before = payload(0, 'x');
      final String after = payload(0, 'y');

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        database.lookupByRID(rids[1], true).asDocument().getString("payload");

        inAnotherThread(() -> database.transaction(
            () -> rids[0].asDocument(true).modify().set("payload", after).save()));

        try {
          final String read = database.lookupByRID(rids[0], true).asDocument().getString("payload");
          assertThat(read).as("neither the old nor the new version: a mix").isIn(before, after);
        } catch (final ConcurrentModificationException e) {
          // a refused read is consistent too
        }
      } finally {
        database.rollback();
      }
    }

    @Test
    void largeRecordHeadChangedByAnotherTransaction() {
      assertRepeatable(70_000, false);
    }

    @Test
    void largeRecordHeadAndTailChangedByAnotherTransaction() {
      assertRepeatable(70_000, true);
    }

    @Test
    void smallRecordIsRepeatable() {
      assertRepeatable(1_000, true);
    }

    private void assertRepeatable(final int size, final boolean changeTail) {
      database.transaction(() -> database.getSchema().createDocumentType("Doc"));
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument("Doc").set("v", 0).set("s", "x".repeat(size)).save().getIdentity());

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final Document first = database.lookupByRID(rid[0], true).asDocument();
        assertThat(first.getInteger("v")).isEqualTo(0);

        inAnotherThread(() -> database.transaction(() -> {
          final MutableDocument d = database.lookupByRID(rid[0], true).asDocument().modify();
          d.set("v", 1);
          if (changeTail)
            d.set("s", d.getString("s") + "y");
          d.save();
        }));

        final Document again = database.lookupByRID(rid[0], true).asDocument();
        assertThat(again.getInteger("v")).isEqualTo(0);
        assertThat(again.getString("s")).hasSize(size);
      } finally {
        database.rollback();
      }
    }

    /**
     * First reads of large records under REPEATABLE_READ while another thread rewrites them: the pages the walk pins itself
     * can come from different commits, so a read must never return a head of one version with the chunks of another.
     */
    @Test
    @Tag("slow")
    void firstReadsNeverReturnATornRecord() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Doc"));
      final RID[] rid = new RID[1];
      database.transaction(
          () -> rid[0] = database.newDocument("Doc").set("v", 0).set("s", "0".repeat(150_000)).save().getIdentity());

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          for (int v = 1; !stop.get(); v++) {
            final int version = v;
            database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("v", version)
                .set("s", String.valueOf(version % 10).repeat(150_000 + version % 3 * 20_000)).save(), false, 100);
          }
        } catch (final Throwable e) {
          writerFailure.set(e);
        }
      });
      writer.start();
      try {
        for (int i = 0; i < 400; i++) {
          database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
          try {
            final Document read = database.lookupByRID(rid[0], true).asDocument();
            final int v = read.getInteger("v");
            final String s = read.getString("s");
            assertThat(s).as("v=" + v).isNotNull();
            assertThat(s.chars().allMatch(c -> c == (char) ('0' + v % 10))).as("s of version " + v + " is a mix").isTrue();
          } catch (final ConcurrentModificationException e) {
            // a refused read is consistent too
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
  }

  /**
   * Issue #9070: under {@code REPEATABLE_READ} a read of a small record pins page 0, which also holds the head chunk of a
   * multi-page record but none of its continuation pages. Another transaction rewrites the multi-page record and commits.
   * Reading the multi-page record must then return the old record, the complete new one, or refuse with a
   * {@link ConcurrentModificationException}, and every later read in the same transaction must answer the same way. It used
   * to return the new record once (assembled from a head the transaction did not hold) and then a mix of the pinned old
   * head with the new tails that first read had pinned.
   * <p>
   * Since #9620 a transaction that never read the record is served its complete new version, which every later read of the
   * transaction answers again: the refusal is kept for a record the transaction did read.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatableReadNeighbourPinnedHead extends BucketPageLayoutTestSupport {
    private static final int OLD_SIZE = 70_000;
    private static final int NEW_SIZE = 70_001;

    /** The reported case: the other transaction rewrites both the head chunk and the continuation chunks. */
    @Test
    void headPinnedByNeighbourReadIsNeverTorn() {
      assertNeighbourPinnedHeadReadServesTheNewVersion(OLD_SIZE, NEW_SIZE, 'y');
    }

    /** Only the head chunk changes (same length, same filler): the continuation chunks are byte-identical. */
    @Test
    void headOnlyChangePinnedByNeighbourReadIsNeverTorn() {
      assertNeighbourPinnedHeadReadServesTheNewVersion(OLD_SIZE, OLD_SIZE, 'x');
    }

    /** The same with a chain of several continuation chunks, on several pages. */
    @Test
    void longChainPinnedByNeighbourReadIsNeverTorn() {
      assertNeighbourPinnedHeadReadServesTheNewVersion(250_000, 250_001, 'y');
    }

    /**
     * Every page of the record is pinned before its first read, but by reads of other records made on both sides of the
     * commit that rewrote it: the head page by the read of a neighbour before the commit, the continuation page by the
     * read of a record that lives on it after the commit. A chain whose pages merely happen to all be pinned is not a
     * snapshot, and must not be returned as one: the transaction never read the record, so it is served the new version
     * whole (#9620), at every read, and the pins taken by the earlier reads stay.
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

        // the old head cannot be completed (its tail chunk is gone): the newest committed version, whole
        assertThat(readConsistentOrRefusal(rids[1])).isEqualTo("v=1 s=" + OLD_SIZE + "y");
        assertThat(isPinned(bucket, 0)).as("the pin taken by the read of the neighbour stays").isTrue();
        assertThat(isPinned(bucket, 1)).as("a pin taken by an earlier read is not released by the read").isTrue();
        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(rids[1]);
        assertThat(readConsistentOrRefusal(rids[1])).as("a second read must answer as the first").isEqualTo("v=1 s=" + OLD_SIZE + "y");
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

    /** READ_COMMITTED pins nothing: the same sequence reads the complete new version, at every read. */
    @Test
    void readCommittedReadsTheNewVersionWhole() {
      final RID[] rids = createSmallAndLarge();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);
        rewriteLarge(rids[1], NEW_SIZE, 'y');
        assertThat(readLarge(rids[1])).isEqualTo("v=1 s=" + NEW_SIZE + "y");
        assertThat(readLarge(rids[1])).isEqualTo("v=1 s=" + NEW_SIZE + "y");
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
     * newer head. The transaction never read the record, so it is served the new version whole (#9620), every later read
     * answers the same even once the record cache is emptied, and no page is pinned or released for it.
     */
    private void assertNeighbourPinnedHeadReadServesTheNewVersion(final int oldSize, final int newSize, final char newFiller) {
      final RID[] rids = createSmallAndLarge(oldSize);
      final LocalBucket bucket = bucketOf("Doc");
      final int pages = bucket.getTotalPages();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByRID(rids[0], true).asDocument().getInteger("v")).isEqualTo(0);

        rewriteLarge(rids[1], newSize, newFiller);

        final String served = "v=1 s=" + newSize + newFiller;
        assertThat(readConsistentOrRefusal(rids[1])).isEqualTo(served);
        for (int page = 1; page < pages; page++)
          assertThat(isPinned(bucket, page)).as("page " + page + " pinned by the failed walk only").isFalse();
        assertThat(isPinned(bucket, 0)).as("the pin taken by the read of the neighbour stays").isTrue();
        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(rids[1]);
        assertThat(readConsistentOrRefusal(rids[1])).as("a second read must answer as the first").isEqualTo(served);
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

  /**
   * Issue #9620: under {@code REPEATABLE_READ} the read of a small record pins its page, and a continuation chunk of a
   * multi-page record lives on the same page. After a concurrent rewrite of the multi-page record its FIRST read in the
   * transaction was refused with a {@code ConcurrentModificationException}, and so was every query that scanned the type,
   * although the transaction had never read that record and owed no image of it. It is served the newest committed version
   * whole, and every later read of the transaction answers the same.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RepeatableReadFirstReadOfMultiPageRecord extends BucketPageLayoutTestSupport {
    private static final int SIZE = 200_000;

    private RID a;
    private RID b;
    private RID c;

    @Override
    protected void beginTest() {
      database.transaction(() -> database.getSchema().createDocumentType("T", 1));
      database.transaction(() -> a = database.newDocument("T").set("v", 0).set("s", "a").save().getIdentity());
      database.transaction(() -> b = database.newDocument("T").set("v", 0).set("s", "Q".repeat(SIZE)).save().getIdentity());
      database.transaction(() -> c = database.newDocument("T").set("v", 0).set("s", "c").save().getIdentity());
      assertThat((Long) bucketStats("T").get("totalMultiPageRecords")).isEqualTo(1L);
    }

    @Test
    void theChainOfTheLargeRecordSharesThePageOfTheSmallOne() {
      // the premise of the issue: a chunk of the large record lives on the page of the small one
      final LocalBucket bucket = bucketOf("T");
      final long pageOfA = a.getPosition() / bucket.getMaxRecordsInPage();
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        readLarge(b);
        assertThat(isPinned(bucket, (int) pageOfA)).as("reading the large record pins the page of the small one").isTrue();
        assertThat(pageOfA).isNotEqualTo(b.getPosition() / bucket.getMaxRecordsInPage());
      } finally {
        database.rollback();
      }
    }

    @Test
    void firstReadAfterANeighbourPinnedAPageOfTheChainIsServed() {
      final LocalBucket bucket = bucketOf("T");
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");

        rewriteLarge(1, 'X');

        assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

        // later reads answer the same, from the transaction's own copy, whatever commits next
        rewriteLarge(2, 'Z');
        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
        assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");
        assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

        // the read of the neighbour stays repeatable: its page was neither released nor re-pinned
        assertThat(isPinned(bucket, (int) (a.getPosition() / bucket.getMaxRecordsInPage()))).isTrue();
        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(a);
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
      } finally {
        database.rollback();
      }

      // a new transaction reads the newest version
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(readLarge(b)).isEqualTo("v=2 s=" + SIZE + "Z");
      } finally {
        database.rollback();
      }
    }

    @Test
    void scanAfterANeighbourPinnedAPageOfTheChainSucceeds() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
        rewriteLarge(1, 'X');

        assertThat(scan()).containsExactly("a", SIZE + "X", "c");
        // a second scan in the same transaction answers the same, after one more concurrent rewrite
        rewriteLarge(2, 'Z');
        assertThat(scan()).containsExactly("a", SIZE + "X", "c");
      } finally {
        database.rollback();
      }
    }

    @Test
    void recordReadBeforeTheRewriteIsRepeated() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
        assertThat(readLarge(b)).isEqualTo("v=0 s=" + SIZE + "Q");

        rewriteLarge(1, 'X');

        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
        assertThat(readLarge(b)).isEqualTo("v=0 s=" + SIZE + "Q");
      } finally {
        database.rollback();
      }
    }

    /**
     * The served version is for reading: the page of the chain the transaction holds at its older image cannot take the
     * rewritten record, so an update is refused with the retryable conflict as soon as the record is taken for it, never
     * laid over a state the commit would have to merge blindly. A new transaction updates it.
     */
    @Test
    void updateAfterAServedFirstReadIsARetryableConflict() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
        rewriteLarge(1, 'X');
        assertThat(readLarge(b)).isEqualTo("v=1 s=" + SIZE + "X");

        final MutableDocument toUpdate = database.lookupByRID(b, true).asDocument().modify();
        assertThatThrownBy(() -> toUpdate.set("v", 7).set("s", "W".repeat(SIZE + 1)).save())
            .isInstanceOf(ConcurrentModificationException.class);
      } finally {
        database.rollback();
      }

      database.transaction(() -> database.lookupByRID(b, true).asDocument().modify().set("v", 7).set("s", "W".repeat(SIZE + 1)).save(),
          false, 3);
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(readLarge(b)).isEqualTo("v=7 s=" + (SIZE + 1) + "W");
      } finally {
        database.rollback();
      }
    }

    @Test
    void readCommittedReadsTheNewVersion() {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        assertThat(database.lookupByRID(a, true).asDocument().getString("s")).isEqualTo("a");
        rewriteLarge(1, 'X');
        assertThat(scan()).containsExactly("a", SIZE + "X", "c");
      } finally {
        database.rollback();
      }
    }

    /**
     * A commit puts its pages in the read cache one at a time. A read whose validation ran while one was being published could
     * find the new head chunk and the old continuation chunk both current, and accepted a record no commit ever wrote (under
     * REPEATABLE_READ it then kept it as its snapshot). Every read must be a record some commit wrote: its filler and its length
     * follow from its {@code v}.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES) // hang detector for the writer join, not a latency bound
    void noReadAssemblesAHalfPublishedCommit() throws Exception {
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final Thread writer = new Thread(() -> {
        try {
          for (int v = 1; !stop.get(); v++) {
            final int version = v;
            database.transaction(() -> database.lookupByRID(b, true).asDocument().modify().set("v", version)
                .set("s", String.valueOf(fillerOf(version)).repeat(sizeOf(version))).save(), false, 100);
          }
        } catch (final Throwable e) {
          writerFailure.set(e);
        }
      });
      writer.start();
      try {
        for (int i = 0; i < 600; i++) {
          final boolean repeatableRead = i % 2 == 0;
          database.begin(repeatableRead ?
              Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ :
              Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
          try {
            if (i % 4 == 0)
              // pins the page of the neighbour, which a chunk of the large record shares
              database.lookupByRID(a, true).asDocument().getString("s");
            final String first = readWrittenByACommit(i);
            final String second = readWrittenByACommit(i);
            if (repeatableRead && !first.equals("CME"))
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

    private static char fillerOf(final int v) {
      return (char) ('a' + v % 26);
    }

    private static int sizeOf(final int v) {
      return SIZE - 120_000 + v % 3 * 20_000;
    }

    private String readWrittenByACommit(final int transaction) {
      final Document read;
      try {
        ((DatabaseInternal) database).getTransaction().removeRecordFromCache(b);
        read = database.lookupByRID(b, true).asDocument();
      } catch (final ConcurrentModificationException e) {
        return "CME";
      }
      final int v = read.getInteger("v");
      final String s = read.getString("s");
      final String expected = v == 0 ? "Q".repeat(SIZE) : String.valueOf(fillerOf(v)).repeat(sizeOf(v));
      assertThat(expected.equals(s)).as("transaction %d read v=%d with an s no commit wrote: %s", transaction, v,
          s == null ? "null" : s.length() + " chars from '" + s.charAt(0) + "' to '" + s.charAt(s.length() - 1) + "'").isTrue();
      return "v=" + v;
    }

    private List<String> scan() {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM T ORDER BY @rid")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final String s = row.getProperty("s");
          rows.add(s.length() == 1 ? s : s.length() + s.substring(0, 1));
        }
      }
      return rows;
    }

    private void rewriteLarge(final int v, final char filler) {
      inAnotherThread(() -> database.transaction(
          () -> database.lookupByRID(b, true).asDocument().modify().set("v", v).set("s", String.valueOf(filler).repeat(SIZE)).save()));
    }

    /** {@code v=<v> s=<length><filler>}, failing on a value of {@code s} that is not one filler repeated. */
    private String readLarge(final RID rid) {
      final Document read = database.lookupByRID(rid, true).asDocument();
      final int v = read.getInteger("v");
      final String s = read.getString("s");
      final char filler = s.charAt(0);
      assertThat(s.chars().allMatch(ch -> ch == filler)).as("s of v=" + v + " is a mix").isTrue();
      return "v=" + v + " s=" + s.length() + filler;
    }

    private boolean isPinned(final LocalBucket bucket, final int pageNumber) {
      return ((DatabaseInternal) database).getTransaction().getPinnedPage(new PageId(database, bucket.getFileId(), pageNumber)) != null;
    }
  }
}
