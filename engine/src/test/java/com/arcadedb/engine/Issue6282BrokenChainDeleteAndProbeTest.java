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
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6282: the three places the corruption-is-not-contention distinction #6258 drew on the read path was missing
 * from, plus the hook that makes the highest-consequence of them testable at all.
 * <ol>
 * <li><b>The delete path</b> positively identified a structurally broken chunk chain and then reported it as a
 * {@link ConcurrentModificationException} - the #4932 retry signal - costing the caller a full round of transactions
 * before it gave up, and forcing five call sites to walk the chain a SECOND time just to find out which of the two
 * they were holding.</li>
 * <li><b>{@code isChunkChainBroken}</b>, the probe those call sites used, walked the chain through the CALLER's
 * transaction. Under {@code REPEATABLE_READ} that is a pinned - possibly stale - snapshot, and the answer gates
 * FORCE-DELETE, so a false positive removes a healthy record.</li>
 * <li><b>{@code IOException} meant two opposite things</b> within a few lines of the same walk: "the disk failed,
 * prove nothing" at the page load, and "this record is corrupt, report a break" from the slot-offset check. The only
 * thing keeping them apart was where the {@code try} sat.</li>
 * <li><b>No fault-injection hook</b> for page reads, so the path whose wrong answer is a permanent corruption verdict
 * on a healthy record had no coverage.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6282BrokenChainDeleteAndProbeTest extends TestHelper {

  private static final String TYPE = "Chunked";

  /** The healthy continuation pointer {@link #createBrokenMultiPageVertex()} overwrote. */
  private long originalChunkPointer;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Every fixture here injects on-disk corruption on purpose, which the post-test integrity check would flag.
    return false;
  }

  @AfterEach
  void clearFaultInjection() {
    PageManager.setPageReadFaultInjector(null);
  }

  /**
   * Item 1. The delete path has already established, from the bytes, that the continuation pointer leads nowhere.
   * Saying "modified concurrently. Please retry" about it is wrong twice over: the retry cannot succeed, and the
   * exception is a {@link NeedRetryException}, so the machinery above re-runs the whole transaction for it.
   */
  @Test
  void aBrokenChunkChainIsNamedByTheDeletePathInsteadOfBeingCalledContention() {
    final RID broken = createBrokenMultiPageVertex();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());

    database.begin();
    try {
      assertThatThrownBy(() -> bucket.deleteRecord(broken))
          .as("a chain the delete path itself proved broken is corruption, not contention")
          .isInstanceOf(BrokenChunkChainException.class)
          .hasMessageContaining("broken chunk chain")
          .hasMessageContaining("CHECK DATABASE FIX")
          // THE POINT OF THE TYPE, NOT MERELY OF THE MESSAGE: NOTHING ABOVE MAY SPEND A RETRY BUDGET ON IT
          .isNotInstanceOf(NeedRetryException.class);
    } finally {
      database.rollback();
    }

    assertThat(bucket.existsRecord(broken)).as("a refused delete must leave the record where it was").isTrue();

    // THE PAIRED HALF: THE RECORD IS STILL REMOVABLE, SO THE NEW VERDICT DID NOT MAKE IT UNDELETABLE
    database.transaction(() -> bucket.deleteRecord(broken, true));
    assertThat(bucket.existsRecord(broken)).isFalse();
  }

  /**
   * Item 1, through the public API: the arm #6258 deliberately left out of {@code LocalDatabase}'s plain-record
   * branch, with a comment saying it would be dead code because {@code deleteRecordInternal} never named a broken
   * chain as one, is live now. The record is deletable through it WITHOUT the second structural walk that branch
   * used to need.
   */
  @Test
  void theTolerantDeletePathTakesTheLoaderVerdictOnTheDeleteSideToo() {
    final RID broken = createBrokenMultiPageVertex();
    database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, true);

    database.transaction(() -> database.command("sql", "DELETE FROM " + broken));

    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isFalse();
  }

  /**
   * Item 1, the other direction and the one that keeps the change honest: a chain the committed image does NOT agree
   * is broken must keep the retryable exception. Here the record is healthy and the delete is made to fail on a page
   * it cannot resolve - it must not be promoted to a permanent corruption verdict.
   */
  @Test
  void aBreakTheCommittedImageDoesNotConfirmKeepsTheRetrySignal() {
    final RID healthy = createMultiPageVertex();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(healthy.getBucketId());

    // The delete's own walk reads its chunk pages with getPageToModify; the CONFIRMATION reads the head page from
    // the page manager. Failing every read after the delete's own ones leaves the confirmation unable to prove
    // anything, which must fall back to the retry rather than condemn a healthy record.
    database.begin();
    try {
      final AtomicInteger reads = new AtomicInteger();
      PageManager.setPageReadFaultInjector(pageId -> {
        if (pageId.getFileId() == healthy.getBucketId() && reads.incrementAndGet() > 1)
          throw new IOException("injected fault on " + pageId);
      });

      assertThatThrownBy(() -> bucket.deleteRecord(healthy))
          .as("a delete that could not read its chain must never end in a permanent corruption verdict")
          .isNotInstanceOf(BrokenChunkChainException.class);
    } finally {
      PageManager.setPageReadFaultInjector(null);
      database.rollback();
    }

    assertThat(bucket.existsRecord(healthy)).as("and the healthy record is still there").isTrue();
  }

  /**
   * Item 2. The probe used to walk the chain through the caller's transaction, so under {@code REPEATABLE_READ} it
   * answered from whatever that transaction had PINNED. Here the transaction pins a page whose chain is broken, a
   * concurrent commit repairs it, and the probe has to report the committed truth - because every caller escalates a
   * {@code true} to a force-delete, and {@code check(fix)} deletes the record it flags.
   */
  @Test
  void theStructuralProbeJudgesFromTheCommittedImageAndNotFromThePinnedOne() throws Exception {
    final RID broken = createBrokenMultiPageVertex();
    final long originalPointer = originalChunkPointer;
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(broken.getBucketId());
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(broken.getBucketId())).getPageSize();

    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // PIN THE BROKEN IMAGE OF THE HEAD PAGE IN THIS TRANSACTION'S SNAPSHOT
      db.getTransaction().getPage(new PageId(db, broken.getBucketId(), 0), pageSize);

      assertThat(bucket.isChunkChainBroken(broken))
          .as("precondition: the chain really is broken in the committed image right now").isTrue();

      // ANOTHER THREAD REPAIRS THE POINTER AND COMMITS. THIS TRANSACTION KEEPS ITS PINNED, STILL-BROKEN PAGE
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread repairer = new Thread(() -> {
        try {
          db.transaction(() -> writeFirstChunkPointer(broken.getBucketId(), originalPointer));
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6282-repairer");
      repairer.start();
      repairer.join(30_000);
      assertThat(failure.get()).isNull();

      assertThat(bucket.isChunkChainBroken(broken))
          .as("the probe must answer from the newest committed image: this transaction's pinned page still says "
              + "broken, and a true here would force-delete a record that is healthy on disk")
          .isFalse();
    } finally {
      db.rollback();
    }
  }

  /**
   * Item 3. A slot-table entry pointing inside the page header is CORRUPTION - the page read fine, its bytes are
   * nonsense - and it used to leave the delete path as a bare {@link IOException}, indistinguishable to the caller
   * from a disk that had just failed. It is now the same structural verdict as a pointer that leads off the end of
   * the file, which is what it has always been.
   */
  @Test
  void aCorruptChunkSlotIsCorruptionAndNotAnIoError() {
    final RID rid = createMultiPageVertex();
    corruptFirstContinuationSlot(rid.getBucketId());
    reopenDatabase();

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());

    assertThat(bucket.isChunkChainBroken(rid))
        .as("the version-blind walk has to read a corrupt slot offset as a break").isTrue();

    database.begin();
    try {
      assertThatThrownBy(() -> bucket.deleteRecord(rid))
          .as("and the delete path must name it rather than let a raw IOException out")
          .isInstanceOf(BrokenChunkChainException.class)
          .isNotInstanceOf(NeedRetryException.class);
    } finally {
      database.rollback();
    }

    // THE PAIRED HALF: THE RECORD IS STILL REMOVABLE WITH FORCE
    database.transaction(() -> bucket.deleteRecord(rid, true));
    assertThat(bucket.existsRecord(rid)).isFalse();
  }

  /**
   * Item 4, and the reason it was filed first: the confirmation walk answering "the chain is broken" for a page it
   * merely could not READ condemns a healthy record permanently and points the operator at
   * {@code CHECK DATABASE FIX}, which deletes it. There was no way to make a page read fail on demand, so this path
   * had no coverage at all.
   */
  @Test
  void aPageReadFaultIsNeverALicenceToForceDeleteARecord() {
    final RID broken = createBrokenMultiPageVertex();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());

    database.transaction(() -> {
      assertThat(bucket.isChunkChainBroken(broken))
          .as("precondition: with a healthy disk the probe does prove the break, so the assertion below is not "
              + "passing for the wrong reason").isTrue();

      PageManager.setPageReadFaultInjector(pageId -> {
        if (pageId.getFileId() == broken.getBucketId())
          throw new IOException("injected fault on " + pageId);
      });

      assertThat(bucket.isChunkChainBroken(broken))
          .as("a probe that could not read the page has proved nothing, and every caller reads a true here as "
              + "permission to force-delete").isFalse();
    });
  }

  /**
   * A chunk that does not describe itself legally is corruption too, and the walk used to follow its continuation
   * pointer straight past it: a size running off the end of the page reached the end of a chain of nonsense and was
   * reported CLEAN. {@code loadMultiPageRecord} could then never confirm the break the read had just hit, so the
   * record spent its whole retry budget and came back as a concurrency problem that did not exist (PR review).
   * <p>
   * Three sizes, because the bound has two ways to be wrong: a large one that stays in range of an int, the largest
   * representable one - which overflows {@code offset + header + size} to a NEGATIVE number and so passes any
   * {@code >} test written in int arithmetic - and a negative one.
   */
  @ParameterizedTest(name = "declared chunk size {0}")
  @ValueSource(ints = { 1_073_741_823, Integer.MAX_VALUE, -1 })
  void aChunkSizeThatRunsOffThePageIsABreakRatherThanACleanWalk(final int corruptSize) {
    final RID rid = createMultiPageVertex();
    corruptFirstChunkSize(rid.getBucketId(), corruptSize);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());

    assertThat(bucket.isChunkChainBroken(rid))
        .as("a chunk whose declared size leaves the page cannot be walked past as if the chain were sound").isTrue();

    database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(rid, true).asVertex().toJSON())
        .as("and the read must confirm it rather than spend TX_RETRIES calling it contention")
        .isInstanceOf(BrokenChunkChainException.class)
        .isNotInstanceOf(NeedRetryException.class));
  }

  /**
   * The delete-side counterpart of the test above, and the reason the size check lives only on the walk that needs
   * it (PR review round 4 asked for the scope to be explicit rather than implicit).
   * <p>
   * {@code deleteRecordInternal} walks the chain by POINTERS and MARKERS and never reads a chunk's declared size -
   * and it does not need to, because the size field sits beside the continuation pointer rather than in front of it.
   * A chain whose only corruption is a bad size therefore still walks perfectly: every chunk is found, every slot is
   * freed, and the delete simply succeeds. There is nothing here to name and nothing to retry.
   * <p>
   * The combined shape - a bad size AND a bad pointer - is the one where the two walks disagree about which hop
   * failed, and there the confirmation deliberately declines to match and the delete keeps its retryable exception.
   * That fails SAFE: {@code CHECK DATABASE FIX} force-deletes on its own detection (which does read sizes), so no
   * record is ever left permanently stuck.
   */
  @Test
  void aBadChunkSizeAloneStillDeletesBecauseTheChainItselfIsIntact() {
    final RID rid = createMultiPageVertex();
    corruptFirstChunkSize(rid.getBucketId(), Integer.MAX_VALUE);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    assertThat(bucket.isChunkChainBroken(rid))
        .as("precondition: the READ path does see this as corruption").isTrue();

    // NO force, NO opt-in: the pointers are all sound, so the physical free walks the whole chain and completes
    database.transaction(() -> bucket.deleteRecord(rid));

    assertThat(bucket.existsRecord(rid))
        .as("a chain whose only fault is a declared size is still fully walkable, so it deletes like any other")
        .isFalse();
  }

  /** The hook is absent unless a test installs it, which is what makes it free in production. */
  @Test
  void theFaultInjectorIsAbsentByDefault() {
    assertThat(PageManager.getPageReadFaultInjector()).isNull();
  }

  // ------------------------------------------------------------------------------------------------------ HELPERS

  /** A vertex whose payload spans several pages, so it is stored as a FIRST_CHUNK with a real continuation chain. */
  private RID createMultiPageVertex() {
    final DatabaseInternal db = (DatabaseInternal) database;

    final int[] bucketIdHolder = new int[1];
    db.transaction(() -> {
      final VertexType type = db.getSchema().createVertexType(TYPE, 1);
      type.createProperty("id", Type.INTEGER);
      type.createProperty("data", Type.STRING);
      bucketIdHolder[0] = type.getBuckets(false).get(0).getFileId();
    });

    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketIdHolder[0])).getPageSize();
    final String bigData = "x".repeat(pageSize * 4);

    final RID[] rids = new RID[1];
    db.transaction(() -> {
      // FIRST, SO IT LANDS AT PAGE 0 / SLOT 0
      rids[0] = db.newVertex(TYPE).set("id", 0).set("data", bigData).save().getIdentity();
      db.newVertex(TYPE).set("id", 1).set("data", "small").save();
    });
    assertThat(rids[0].getPosition()).isZero();

    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
    return rids[0];
  }

  /**
   * The same record, with the FIRST_CHUNK's continuation pointer aimed at a page well beyond the file, and the
   * database reopened so the chain is walked from the corrupted page rather than from a cached one.
   */
  private RID createBrokenMultiPageVertex() {
    final RID rid = createMultiPageVertex();
    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getMaxRecordsInPage();
    // KEPT SO A TEST CAN PUT THE CHAIN BACK TOGETHER AGAIN, WHICH IS HOW THE PINNED-VIEW GAP IS PROVOKED
    originalChunkPointer = readFirstChunkPointer(rid.getBucketId());
    assertThat(originalChunkPointer).as("the fixture record must really have a continuation chunk").isPositive();
    writeFirstChunkPointer(rid.getBucketId(), 1_000_000L * maxRecordsInPage);
    reopenDatabase();
    return rid;
  }

  private long readFirstChunkPointer(final int bucketId) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final long[] holder = new long[1];
    db.transaction(() -> {
      try {
        final BasePage page = db.getTransaction().getPage(new PageId(db, bucketId, 0), pageSize);
        final int recordOffset = firstChunkOffset(page);
        holder[0] = page.readLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
    return holder[0];
  }

  /** Overwrites the continuation pointer of the FIRST_CHUNK at page 0 / slot 0. */
  private void writeFirstChunkPointer(final int bucketId, final long pointer) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucketId, 0), pageSize, false);
        page.writeLong(firstChunkOffset(page) + 1 + Binary.INT_SERIALIZED_SIZE, pointer);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Points the slot-table entry of the record's FIRST continuation chunk inside the page header, which no writer can
   * produce: the page reads fine and its bytes are nonsense. That is what {@code PageCorruptionException} names.
   */
  private void corruptFirstContinuationSlot(final int bucketId) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
    final long pointer = readFirstChunkPointer(bucketId);
    assertThat(pointer).as("the fixture record must really have a continuation chunk").isPositive();

    final int chunkPageNumber = (int) (pointer / maxRecordsInPage);
    final int chunkSlot = (int) (pointer % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, bucketId, chunkPageNumber), pageSize, false);
        // ANY NON-ZERO VALUE BELOW contentHeaderSize IS ILLEGAL; 1 IS BELOW EVERY POSSIBLE ONE
        page.writeInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + chunkSlot * Binary.INT_SERIALIZED_SIZE, 1);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);
  }

  /**
   * Declares a head-chunk size far larger than the page can hold - the shape a torn or mis-written header leaves.
   * <p>
   * Written straight into the CLOSED file rather than through a transaction: a commit compresses the pages it
   * touched, and that pass has a size guard of its own which deletes the record before the walk under test ever
   * sees it. Corrupting the bytes on disk is also the more honest fixture for the fault this is about. The database
   * is left closed; the caller reopens it.
   *
   * @param corruptSize the size to declare. {@code Integer.MAX_VALUE} is the case that matters most: it makes
   *                    {@code offset + header + size} overflow an int to a NEGATIVE number, so a bounds check
   *                    written in int arithmetic lets the largest corrupt size representable straight through the
   *                    guard meant to stop it (PR review).
   */
  private void corruptFirstChunkSize(final int bucketId, final int corruptSize) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(bucketId);
    final int pageSize = file.getPageSize();
    final String path = file.getFilePath();

    final int[] chunkOffset = new int[1];
    db.transaction(() -> {
      try {
        chunkOffset[0] = firstChunkOffset(db.getTransaction().getPage(new PageId(db, bucketId, 0), pageSize));
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });

    database.close();
    try (final RandomAccessFile raw = new RandomAccessFile(path, "rw")) {
      // Page 0, content-relative offset: the marker is one byte, then the declared chunk size.
      raw.seek(BasePage.PAGE_HEADER_SIZE + chunkOffset[0] + 1L);
      raw.writeInt(corruptSize);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    database = factory.open();
  }

  private int firstChunkOffset(final BasePage page) throws IOException {
    final int recordOffset = (int) page.readUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET);
    // FIRST_CHUNK (-2) IS ZIGZAG-ENCODED AS THE SINGLE BYTE 0x03
    assertThat(page.readByte(recordOffset)).as("record at page 0 / slot 0 must be a multi-page head")
        .isEqualTo((byte) 3);
    return recordOffset;
  }
}
