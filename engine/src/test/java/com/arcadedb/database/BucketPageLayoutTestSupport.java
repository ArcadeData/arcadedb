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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared fixtures for the tests that drive a bucket page into a specific PHYSICAL layout - a full page, a page with
 * no free tail at all - and then assert what the engine made of it. Extracted once the third such test class
 * ({@code Issue6149PlaceholderPrefersChunksTest}, next to {@code Issue5279ConcurrentUpdateTest} and
 * {@code Issue6129ChunkedSlotMergeTest}) would have copied these verbatim for a fourth time.
 * <p>
 * The layout these build is a contract of its own: change how a page fills up or how a spill claims the free tail,
 * and every test below has to be re-read, not just re-run. Keeping the fixtures in one place is what makes that
 * possible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class BucketPageLayoutTestSupport extends TestHelper {
  /** How many records {@link #createChunkedRecords} puts on page 0 before growing them all into chunk chains. */
  protected static final int CHUNKED_RECORDS = 12;

  /** Payload length every record built by {@link #createChunkedRecords} ends up with once they have all spilled. */
  private int spilledPayloadSize;

  /**
   * Fills page 0 of a single-bucket type until a record no longer fits it: the next insert lands on another page,
   * which shows up as a RID position that is not the previous one plus one (a new page restarts at a multiple of the
   * page's slot count).
   * <p>
   * Joins the caller's transaction when there is one, so it can be used either inside a fixture transaction or on
   * its own.
   *
   * @return the RID of the last record that landed on page 0.
   */
  protected RID fillFirstPage(final String typeName) {
    final String filler = "f".repeat(8 * 1024);
    final RID[] result = new RID[1];
    database.transaction(() -> {
      RID previous = null;
      for (int i = 0; i < 64; i++) {
        final RID rid = database.newDocument(typeName).set("v", filler).save().getIdentity();
        if (previous != null && rid.getPosition() != previous.getPosition() + 1) {
          result[0] = previous;
          return;
        }
        previous = rid;
      }
      throw new AssertionError("Page 0 of " + typeName + " did not fill up");
    });
    return result[0];
  }

  /**
   * Leaves page 0 with a free tail of exactly ZERO bytes, the only page shape that still forces a record too small to
   * host a chunk header into a placeholder (#6149). Inserts cannot produce it - the allocator always keeps
   * {@code SPARE_SPACE_FOR_GROWTH} in hand - but a spill can: the head chunk of a record that outgrows its page while
   * being the LAST record of that page takes the record's own footprint plus the whole free tail, so the page ends
   * exactly at its maximum content size.
   * <p>
   * Must be called OUTSIDE a transaction: the fill has to be committed before the record that seals the page is
   * grown, so the spill sees the page the fill produced.
   * <p>
   * The postcondition is checked here rather than left to the callers: the sealing record must really have SPILLED,
   * because the spill is the only thing that eats the free tail. Should a page-geometry change ever let 70 KB stay
   * in the page, every test built on this fixture would otherwise go on passing while quietly testing a page that
   * still has room - the failure would surface as six confusing assertions elsewhere instead of one here.
   *
   * @return the RID of the record that seals the page, so a caller can free the tail again by deleting it.
   */
  protected RID sealFirstPage(final String typeName) {
    final RID last = fillFirstPage(typeName);
    database.transaction(() -> last.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());

    final Map<String, Object> layout = bucketStats(typeName);
    assertThat((Long) layout.get("totalMultiPageRecords"))
        .as("sealing page 0 of " + typeName + " requires the last record to spill into chunks, taking the free tail "
            + "with it - it did not: " + layout)
        .isPositive();
    return last;
  }

  /** Physical layout of a single-bucket type: how many records are placeholders, chunked, and so on. */
  protected Map<String, Object> bucketStats(final String typeName) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).get(0);
    final Map<String, Object>[] stats = new Map[1];
    database.transaction(() -> stats[0] = bucket.check(0, false));
    return stats[0];
  }

  /** The sanity net every layout test ends with: the whole database must check out with nothing to fix. */
  protected void checkDatabase() {
    try (final ResultSet rs = database.command("SQL", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(numberProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
        assertThat(numberProperty(row, "autoFix")).as("check database: " + row.toJSON()).isZero();
      }
    }
  }

  /**
   * Creates {@link #CHUNKED_RECORDS} records whose slots all live on page 0 of a single-bucket type and grows every
   * one of them until it has spilled into a chunk chain, so their continuation chunks share the pages that follow -
   * the layout that produced #6217, and the one every chunked-read test needs. The RID at index {@code i} sits at
   * slot {@code i} of page 0.
   * <p>
   * Lifted here from {@code Issue6217ChunkedReadFalseConflictTest} once
   * {@code Issue6258ChunkedReadRetryAndCorruptionTest} needed the same layout.
   */
  protected RID[] createChunkedRecords(final String typeName) {
    final RID[] rids = new RID[CHUNKED_RECORDS];
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("payload", Type.STRING);
      for (int i = 0; i < CHUNKED_RECORDS; i++)
        rids[i] = database.newDocument(typeName).set("payload", "r" + i).save().getIdentity();
    });

    // A record only spills once its page cannot host its growth, and a record that spilled leaves its head chunk
    // behind, so the shared page fills up in steps: keep growing everybody until the last one has left.
    for (int size = 1_000; size <= 24_000; size += 1_000) {
      spilledPayloadSize = size;
      database.transaction(() -> {
        for (int i = 0; i < CHUNKED_RECORDS; i++)
          rids[i].asDocument(true).modify().set("payload", payload(i, 'x')).save();
      });

      if (CHUNKED_RECORDS == (Long) bucketStats(typeName).get("totalMultiPageRecords"))
        return rids;
    }
    throw new AssertionError(
        "Not every record of " + typeName + " spilled into a chunk chain: " + bucketStats(typeName));
  }

  /**
   * Payload for {@link #createChunkedRecords}: the same length for every record and every round, so a rewrite reuses
   * the chunk chain the record already has, and a per-round filler so a rewrite differs in EVERY chunk.
   */
  protected String payload(final int record, final char filler) {
    final String marker = "r" + record + "-";
    return marker + String.valueOf(filler).repeat(spilledPayloadSize - marker.length());
  }

  /** The single bucket of a type built by {@link #createChunkedRecords}. */
  protected LocalBucket bucketOf(final String typeName) {
    return (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).get(0);
  }

  /** Runs {@code body} on a thread of its own, so it commits in a transaction other than the caller's. */
  protected void inAnotherThread(final Runnable body) {
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "bucket-page-layout-writer");
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
    if (!errors.isEmpty())
      throw new AssertionError("the concurrent write failed: " + errors.get(0), errors.get(0));
  }

  /** Null-tolerant read of a numeric check-database property, so a missing field fails clearly instead of NPE. */
  protected static long numberProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }

  /** The single row {@code CHECK DATABASE} answers with, run with or without {@code FIX}. */
  protected Result checkDatabaseRow(final boolean fix) {
    try (final ResultSet rs = database.command("sql", fix ? "check database fix" : "check database")) {
      return rs.next();
    }
  }

  /** {@code CHECK DATABASE} folds the per-bucket warning lists into a set, so this reads it as the collection it is. */
  protected static Collection<?> warningsOf(final Result row) {
    final Object warnings = row.getProperty("warnings");
    return warnings == null ? List.of() : (Collection<?>) warnings;
  }

  /**
   * Records a full SCAN returns. {@code count(@rid)} and not {@code count(*)}: the latter answers from the bucket's
   * cached counter without reading a page, so the two disagree exactly when a slot the counter counts is one a scan
   * skips - which is a difference several of these tests are about rather than something they may paper over.
   */
  protected long countRecords(final String typeName) {
    final long[] total = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL", "select count(@rid) as c from " + typeName)) {
        total[0] = ((Number) rs.next().getProperty("c")).longValue();
      }
    });
    return total[0];
  }

  /** The counter {@code count(*)} answers from: O(1), never a scan, and the other half of the comparison above. */
  protected long countRecordsFromCounter(final String typeName) {
    final long[] total = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL", "select count(*) as c from " + typeName)) {
        total[0] = ((Number) rs.next().getProperty("c")).longValue();
      }
    });
    return total[0];
  }

  /** Records a scan returns holding exactly {@code value} - the count that notices a record handed out twice. */
  protected long countRecordsHolding(final String typeName, final String value) {
    final long[] total = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("SQL", "select count(@rid) as c from " + typeName + " where v = :v",
          Map.of("v", value))) {
        total[0] = ((Number) rs.next().getProperty("c")).longValue();
      }
    });
    return total[0];
  }

  /**
   * Runs {@code body} on the page holding {@code rid}, taken for modification so a write lands in the caller's
   * transaction. The way every test here fabricates a physical shape the engine no longer produces - a legacy marker,
   * a chain broken mid-flight, a slot freed behind the record that references it.
   */
  protected long onSlot(final RID rid, final PageAccess body) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
    final int pageNumber = (int) (rid.getPosition() / maxRecordsInPageOf(rid));
    try {
      return body.apply(db.getTransaction().getPageToModify(new PageId(db, rid.getBucketId(), pageNumber), pageSize, false));
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Content offset of {@code rid}'s slot on {@code page}, marker included: what the record table entry holds. */
  protected int recordOffsetOf(final MutablePage page, final RID rid) {
    final int slot = (int) (rid.getPosition() % maxRecordsInPageOf(rid));
    // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; one uint per slot.
    return (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
  }

  /** Frees a slot without touching anything that references it: an interrupted repair, or physical corruption. */
  protected void freeSlotBehindItsBack(final RID rid) {
    database.transaction(() -> onSlot(rid, page -> {
      final int slot = (int) (rid.getPosition() % maxRecordsInPageOf(rid));
      page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE, 0L);
      return 0L;
    }));
  }

  private int maxRecordsInPageOf(final RID rid) {
    return ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).getMaxRecordsInPage();
  }

  @FunctionalInterface
  protected interface PageAccess {
    long apply(MutablePage page) throws Exception;
  }
}
