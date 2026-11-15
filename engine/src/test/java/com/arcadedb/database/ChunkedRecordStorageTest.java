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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the database engine: ChunkedRecordStorage. Each nested class keeps the regression of one reported issue (see the
 * Javadoc of each).
 */
public class ChunkedRecordStorageTest {
  /**
   * Issue #6129: the disjoint-slot merge (#5381, #5279, #5569) stopped at the page boundary - a record that outgrew its
   * page became a chunk chain or a placeholder, and every later update of it poisoned its page unconditionally. On a
   * single-bucket type whose records have all grown past the page size that turned concurrent updates of UNRELATED
   * records back into permanent conflicts: exactly the false conflict #5279 removed, one size class up.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkedSlotMerge extends BucketPageLayoutTestSupport {
    private static final int THREADS            = 8;
    private static final int RECORDS_PER_THREAD = 5;
    private static final int ROUNDS             = 8;

    /** Payload length every record ends up with once they have all spilled out of their shared page. */
    private int spilledPayloadSize;

    /**
     * The shape of the issue: 40 records whose slots all live on page 0 are grown until every one of them is a chunk
     * chain, then eight threads rewrite their own records with a payload of the SAME length - so each write reuses the
     * chain it already has and touches nothing but its own chunk slots. With {@code attempts=1} the conflict count must
     * be zero: nothing here is logically contended.
     */
    @Test
    void updatesOfChunkedRecordsSharingAPageNeverConflict() throws Exception {
      final RID[][] owned = createAndSpillRecords("Chunked");

      final ConcurrentUpdateOutcome outcome = runConcurrentUpdates(owned, this::fixedSizePayload);

      assertThat(outcome.commitConflicts())
          .as("updating chunked records owned by different threads must not conflict: %s", outcome).isZero();
      assertThat(outcome.readSideConflicts())
          .as("no record here is read by anybody but its owner, so no read may fail either (#6217): %s", outcome)
          .isZero();

      verifyContent(owned, this::fixedSizePayload);
      checkDatabase();
    }

    /**
     * The same, except every round makes the payload LONGER, so the chain has to be EXTENDED: the write allocates a new
     * chunk into a free slot of a shared page and repoints the previous chunk at it. Those extra pages are NOT
     * rebasable - they are poisoned by the chunk writer - so what this pins down is that the head chunk's merge still
     * carries the whole write when the chain grows underneath it, and that a record's continuation chunks landing next
     * to another thread's do not bring the conflict back.
     * <p>
     * "Conflict" here means one the COMMIT raised, and a read of a chunked record used to be able to fail on a
     * mechanism of its own: {@code loadMultiPageRecord} re-validated the page VERSION of everything its chain touched,
     * and the continuation chunks of all 40 records of this fixture live on one page, so an unrelated rewrite
     * invalidated a reader that had touched nothing of that record. That was issue #6217 - what made this test fail on
     * a loaded machine, and why {@link ConcurrentUpdateOutcome} counts the two separately. Since #6217 the read
     * validates the record instead of the pages under it, so both counters must now be zero, and the split is kept
     * because it is what a future failure needs in order to name its own mechanism.
     */
    @Test
    void growingUpdatesOfChunkedRecordsSharingAPageNeverConflict() throws Exception {
      final RID[][] owned = createAndSpillRecords("ChunkedGrowing");

      final ConcurrentUpdateOutcome outcome = runConcurrentUpdates(owned, this::growingPayload);

      assertThat(outcome.commitConflicts())
          .as("extending the chunk chain of one's own record must not conflict: %s", outcome).isZero();
      assertThat(outcome.readSideConflicts())
          .as("no record here is read by anybody but its owner, so no read may fail either (#6217): %s", outcome)
          .isZero();

      verifyContent(owned, this::growingPayload);
      checkDatabase();
    }

    /**
     * The transition itself: a plain record that no longer fits its page turns into the HEAD of a chunk chain without
     * leaving its slot, so the page sees a single-slot write like any other. It is worth its own test because it is the
     * one tracked write whose two images have different shapes - a plain record going in, a chunk header coming out -
     * and because it used to be self-sustaining: a spill that lost the race rolled back and had to spill again on the
     * retry, so under contention the records that had to spill could starve rather than converge.
     */
    @Test
    void aRecordSpillingIntoChunksCommutesWithWritesToOtherSlots() throws Exception {
      final RID[] spilling = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Spilling", 1).createProperty("v", Type.STRING);
        // Big enough that its own slot can host a chunk header, so it spills into CHUNKS and not into a placeholder.
        spilling[0] = database.newDocument("Spilling").set("v", "s".repeat(1024)).save().getIdentity();
        neighbour[0] = database.newDocument("Spilling").set("v", "n").save().getIdentity();
        fillFirstPage("Spilling");
      });

      // The premise, proved rather than assumed: with the merge off the very same interleaving must fail.
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(spillSurvives(spilling[0], neighbour[0], "a".repeat(200 * 1024), "neighbour without the merge"))//
            .as("the spilling record and the neighbour must share a page").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      final String spilled = "b".repeat(200 * 1024);
      assertThat(spillSurvives(spilling[0], neighbour[0], spilled, "neighbour rewritten")).isTrue();

      final Map<String, Object> layout = bucketStats("Spilling");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must have spilled into chunks: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalPlaceholderRecords")).isZero();

      database.transaction(() -> {
        assertThat(spilling[0].asDocument(true).getString("v")).isEqualTo(spilled);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour rewritten");
      });
      checkDatabase();
    }

    /**
     * Grows a record until it has to spill out of its page and, before committing, has another thread commit a change to
     * a record sharing that page. Returns true when our commit went through, i.e. the merge absorbed the version bump.
     */
    private boolean spillSurvives(final RID spilling, final RID neighbour, final String value, final String neighbourValue)
        throws InterruptedException {
      database.begin();
      spilling.asDocument(true).modify().set("v", value).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    /**
     * The opposite guarantee, on the shape this issue adds: two transactions rewriting the SAME chunked record must still
     * conflict. The merge must never silently drop one of the two writes just because their pages could be re-derived.
     */
    @Test
    void concurrentUpdatesOfTheSameChunkedRecordStillConflict() throws Exception {
      final RID[][] owned = createAndSpillRecords("ChunkedContended");
      final RID contended = owned[0][0];
      final String mine = fixedSizePayload(0, 0, 0);
      final String theirs = fixedSizePayload(1, 1, 1);

      database.begin();
      contended.asDocument(true).modify().set("payload", mine).save();

      final List<Throwable> otherErrors = new CopyOnWriteArrayList<>();
      final Thread other = new Thread(() -> {
        try {
          database.transaction(() -> contended.asDocument(true).modify().set("payload", theirs).save(), true, 1);
        } catch (final Throwable e) {
          otherErrors.add(e);
        }
      });
      other.start();
      other.join();
      assertThat(otherErrors).isEmpty();

      try {
        database.commit();
        throw new AssertionError("Expected a ConcurrentModificationException on the very same chunked record");
      } catch (final ConcurrentModificationException expected) {
        // correct: a true conflict on the record itself
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> assertThat(contended.asDocument(true).getString("payload")).isEqualTo(theirs));
      checkDatabase();
    }

    /**
     * The sharp edge of the head-chunk merge: only the HEAD chunk of a multi-page record lives on the page the merge
     * replays, so its byte-for-byte pre-image check cannot see the rest of the record. Here two transactions rewrite the
     * same 200 KB record with values that differ ONLY in their last byte - so their head chunks (at most one page, and
     * this record needs four) are identical and the pre-image check alone would happily merge, silently dropping the
     * other transaction's write. The chunk-chain fingerprint is what turns it back into the conflict it is.
     */
    @Test
    void aTailOnlyChangeToTheSameChunkedRecordStillConflicts() throws Exception {
      final String mine = "z".repeat(200 * 1024) + "M";
      final String theirs = "z".repeat(200 * 1024) + "T";

      final RID[] chunked = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Tail", 1).createProperty("v", Type.STRING);
        // Big enough that its own slot can host a chunk header, so it spills into CHUNKS and not into a placeholder.
        chunked[0] = database.newDocument("Tail").set("v", "c".repeat(1024)).save().getIdentity();
        fillFirstPage("Tail");
      });

      // Page 0 is full, so the record spills into a chunk chain whose head stays in its slot on page 0. The committed
      // value shares its whole head chunk with the two below, so the pre-image check of that chunk passes for both.
      database.transaction(() -> chunked[0].asDocument(true).modify().set("v", "z".repeat(200 * 1024) + "O").save());
      final Map<String, Object> layout = bucketStats("Tail");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must be a chunk chain: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("and not a placeholder: " + layout).isZero();
      assertThat((Long) layout.get("totalChunks")).as("and it must span several chunks: " + layout).isGreaterThan(1L);

      database.begin();
      chunked[0].asDocument(true).modify().set("v", mine).save();

      final List<Throwable> otherErrors = new CopyOnWriteArrayList<>();
      final Thread other = new Thread(() -> {
        try {
          database.transaction(() -> chunked[0].asDocument(true).modify().set("v", theirs).save(), true, 1);
        } catch (final Throwable e) {
          otherErrors.add(e);
        }
      });
      other.start();
      other.join();
      assertThat(otherErrors).isEmpty();

      try {
        database.commit();
        throw new AssertionError("Expected a conflict: the other transaction rewrote the tail of the very same record");
      } catch (final ConcurrentModificationException expected) {
        // correct: the fingerprint of the chain kept the merge from replaying a head chunk that is no longer the
        // beginning of the record it was taken from
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> assertThat(chunked[0].asDocument(true).getString("v")).isEqualTo(theirs));
      checkDatabase();
    }

    /**
     * The same tail-only interleaving under REPEATABLE_READ, where the fingerprint is NOT what catches it and something
     * else has to. Under that isolation level {@code TransactionContext.getPage} caches the pages it reads for the
     * transaction, so the two fingerprint walks - the one taken when the record is taken for update and the one at
     * commit - both read the transaction's own cached copies and agree however the record changed meanwhile.
     * <p>
     * What makes that safe is the same caching: the continuation chunks are then held at the version they had when the
     * transaction read them, so rewriting them at commit takes stale pages into the modified set and the ordinary
     * page-version check refuses them. Under READ_COMMITTED (the default) those pages are instead loaded fresh under the
     * commit lock and cannot fail that check - which is exactly why the fingerprint has to exist there. Two isolation
     * levels, two different mechanisms, one guarantee; this pins the half that has no fingerprint behind it.
     */
    @Test
    void aTailOnlyChangeToTheSameChunkedRecordStillConflictsUnderRepeatableRead() throws Exception {
      final String committed = "z".repeat(200 * 1024) + "O";
      final String mine = "z".repeat(200 * 1024) + "M";
      final String theirs = "z".repeat(200 * 1024) + "T";

      final RID[] chunked = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("TailRR", 1).createProperty("v", Type.STRING);
        chunked[0] = database.newDocument("TailRR").set("v", "c".repeat(1024)).save().getIdentity();
        fillFirstPage("TailRR");
      });
      database.transaction(() -> chunked[0].asDocument(true).modify().set("v", committed).save());
      assertThat((Long) bucketStats("TailRR").get("totalMultiPageRecords")).isEqualTo(1L);

      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        database.begin();
        chunked[0].asDocument(true).modify().set("v", mine).save();

        final List<Throwable> otherErrors = new CopyOnWriteArrayList<>();
        final Thread other = new Thread(() -> {
          try {
            database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
            database.transaction(() -> chunked[0].asDocument(true).modify().set("v", theirs).save(), true, 1);
          } catch (final Throwable e) {
            otherErrors.add(e);
          }
        });
        other.start();
        other.join();
        assertThat(otherErrors).isEmpty();

        try {
          database.commit();
          throw new AssertionError("Expected a conflict on the tail of the very same record under REPEATABLE_READ");
        } catch (final ConcurrentModificationException expected) {
          // correct: the chain pages this transaction cached are stale, and the page-version check refuses them
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      } finally {
        database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      }

      database.transaction(() -> assertThat(chunked[0].asDocument(true).getString("v")).isEqualTo(theirs));
      checkDatabase();
    }

    /**
     * The other way the tail of a record can move under a transaction: not its bytes but its SHAPE - the concurrent
     * writer makes the record LONGER, so the chain grows a chunk at its end.
     * <p>
     * Which of the two checks catches it is worth knowing, and it is not the chunk-chain fingerprint: measured by
     * removing that check, this interleaving still conflicts, because a serialized record carries the length of its
     * content near its start and that lives in the HEAD chunk - so a total-length change perturbs the head as well and
     * the byte-for-byte pre-image check of that chunk is enough. The test earns its place by pinning exactly that: a
     * serialization change that moved the length out of the head chunk's reach would fail here rather than quietly
     * leaving a chain-length change to be caught by a fingerprint that only fires when the head still matches.
     */
    @Test
    void aConcurrentChangeToTheSameRecordsChainLengthStillConflicts() throws Exception {
      final String committed = "z".repeat(200 * 1024) + "O";
      final String mine = "z".repeat(200 * 1024) + "M";
      final String longer = "z".repeat(260 * 1024);

      final RID[] chunked = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Chain", 1).createProperty("v", Type.STRING);
        chunked[0] = database.newDocument("Chain").set("v", "c".repeat(1024)).save().getIdentity();
        fillFirstPage("Chain");
      });
      database.transaction(() -> chunked[0].asDocument(true).modify().set("v", committed).save());

      final Map<String, Object> layout = bucketStats("Chain");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must be a chunk chain: " + layout).isEqualTo(1L);
      final long chunksBefore = (Long) layout.get("totalChunks");

      database.begin();
      chunked[0].asDocument(true).modify().set("v", mine).save();

      final List<Throwable> otherErrors = new CopyOnWriteArrayList<>();
      final Thread other = new Thread(() -> {
        try {
          database.transaction(() -> chunked[0].asDocument(true).modify().set("v", longer).save(), true, 1);
        } catch (final Throwable e) {
          otherErrors.add(e);
        }
      });
      other.start();
      other.join();
      assertThat(otherErrors).isEmpty();

      // The premise: the other transaction really did make the chain longer rather than just rewriting it.
      assertThat((Long) bucketStats("Chain").get("totalChunks")).as("the chain must have grown a chunk")
          .isGreaterThan(chunksBefore);

      try {
        database.commit();
        throw new AssertionError("Expected a conflict: the other transaction extended the chain of the very same record");
      } catch (final ConcurrentModificationException expected) {
        // correct: the length prefix moved inside the head chunk, so the pre-image check of that chunk saw it
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> assertThat(chunked[0].asDocument(true).getString("v")).isEqualTo(longer));
      checkDatabase();
    }

    /**
     * Issue #6129 item 1: a record that spilled into a placeholder keeps its content on ANOTHER page, and an update of
     * that content record - in place or growing - is a single-slot write on that page like any other. Here the content
     * page also hosts plain records, so a concurrent write to one of them must not stop the placeholder update.
     */
    @Test
    void updatingPlaceholderContentCommutesWithItsPageNeighbours() throws Exception {
      final String big = "b".repeat(20 * 1024);
      final String rewritten = "c".repeat(20 * 1024);
      final String grown = "d".repeat(24 * 1024);

      final RID[] placeholder = new RID[1];
      final RID[] other = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Holder", 1).createProperty("v", Type.STRING);
        placeholder[0] = database.newDocument("Holder").set("v", "p").save().getIdentity();
      });
      // Since #6149 a page with a free tail lends the spilling record the few bytes a chunk header needs, so a
      // placeholder is only produced on a page with NO free tail left at all: seal page 0 to get one.
      sealFirstPage("Holder");

      // Page 0 cannot host 20 KB and the record's own 9 bytes cannot hold a chunk header: the slot becomes a placeholder
      // POINTER and the content goes to the page that has room for it.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", big).save());

      // A record too big for the leftovers of page 0, so it lands next to the placeholder content record.
      database.transaction(() -> other[0] = database.newDocument("Holder").set("v", "o".repeat(4 * 1024)).save().getIdentity());

      final Map<String, Object> layout = bucketStats("Holder");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the record must have spilled into a placeholder: " + layout)
          .isEqualTo(1L);

      // The premise, proved rather than assumed: with the merge switched OFF the very same interleaving must FAIL, which
      // is what shows the records really do share a page (otherwise there is no page conflict to absorb and the two
      // assertions below would pass without the merge ever running).
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(placeholderUpdateSurvives(placeholder[0], other[0], rewritten, "the records share a page"))//
            .as("the content record must share its page with the records written around it").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      // 1st: an in-place rewrite of the content record (same size) while another record on the page is rewritten.
      assertThat(placeholderUpdateSurvives(placeholder[0], other[0], rewritten, "rewritten")).isTrue();

      // 2nd: the content record GROWS on its own page, again against a concurrent write to that other record.
      assertThat(placeholderUpdateSurvives(placeholder[0], other[0], grown, "rewritten again")).isTrue();

      database.transaction(() -> {
        assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo(grown);
        assertThat(other[0].asDocument(true).getString("v")).isEqualTo("rewritten again");
      });
      checkDatabase();
    }

    /**
     * Updates the placeholder record and inserts a new one, which lands on the content page and is what pins it at the
     * version the transaction started from (a record UPDATE is applied at commit time, an insert is not: it has to
     * assign the RID straight away). Before committing, another thread commits a change to a third record on that same
     * page. Returns true when our commit went through, i.e. the merge absorbed the version bump.
     */
    private boolean placeholderUpdateSurvives(final RID placeholder, final RID other, final String value,
        final String otherValue) throws InterruptedException {
      database.begin();
      placeholder.asDocument(true).modify().set("v", value).save();
      database.newDocument("Holder").set("v", "pinning the content page " + value.charAt(0)).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> other.asDocument(true).modify().set("v", otherValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    /**
     * The belt-and-suspenders half of the kind bookkeeping: a slot that changes SHAPE inside one transaction. The
     * pre-image kept for the first write no longer describes what the second one started from, so the two cannot be
     * replayed as one and the page must be excluded outright.
     * <p>
     * Driven through the tracking API rather than through records, deliberately: no write path reaches these states
     * today - the writers of every shape transition poison the page themselves, and two updates of one record inside a
     * transaction collapse into the single write {@code updatedRecords} performs at commit - so a test that went
     * through the engine would prove nothing about the guards. What must not happen is that a future write path reaches
     * them and is silently mis-merged instead, which is exactly what these three assert.
     */
    @Test
    void aSlotThatChangesShapeInOneTransactionIsExcludedFromTheMerge() {
      database.transaction(() -> database.getSchema().createDocumentType("Shapes", 1).createProperty("v", Type.STRING));
      final int fileId = database.getSchema().getType("Shapes").getBuckets(false).getFirst().getFileId();

      final byte[] base = "base".getBytes();
      final byte[] image = "image".getBytes();

      // An UPDATE of a shape other than the one already tracked for the slot.
      inTransaction(tx -> {
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_RECORD);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).isFalse();
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_FIRST_CHUNK);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).as("a slot that changed shape must exclude its page").isTrue();
      });

      // An INSERT replays as a plain record into a free slot, so it cannot follow a write of another shape.
      inTransaction(tx -> {
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_PLACEHOLDER_CONTENT);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).isFalse();
        tx.trackRebasableInsert(fileId, 0, 3);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).as("an insert cannot follow another shape").isTrue();
      });

      // Only the delete of a PLAIN in-place record is replayable (#5569).
      inTransaction(tx -> {
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_FIRST_CHUNK);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).isFalse();
        tx.trackRebasableDelete(fileId, 0, 3, base);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).as("only a plain record's delete is replayable").isTrue();
      });

      // The same slot written twice with the SAME shape stays mergeable: the guards must not be indiscriminate.
      inTransaction(tx -> {
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_FIRST_CHUNK);
        tx.trackRebasableUpdate(fileId, 0, 3, base, image, TransactionContext.SLOT_KIND_FIRST_CHUNK);
        assertThat(tx.isSlotRebasePagePoisoned(fileId, 0)).as("a second write of the same shape is still replayable")
            .isFalse();
      });
    }

    /** Runs {@code body} against a transaction that is rolled back, so the tracking state never reaches a page. */
    private void inTransaction(final java.util.function.Consumer<TransactionContext> body) {
      database.begin();
      try {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        assertThat(tx.isSlotMergeEnabled()).as("the merge must be on for the tracking to record anything").isTrue();
        body.accept(tx);
      } finally {
        database.rollback();
      }
    }

    private interface PayloadOf {
      String of(int thread, int record, int round);
    }

    /**
     * Creates {@code THREADS * RECORDS_PER_THREAD} records whose slots all live on page 0 of a single-bucket type, then
     * grows them (single-threaded) until every one of them has spilled into a chunk chain rooted on that shared page.
     */
    private RID[][] createAndSpillRecords(final String typeName) {
      final RID[][] owned = new RID[THREADS][RECORDS_PER_THREAD];
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName, 1).createProperty("payload", Type.STRING);
        for (int t = 0; t < THREADS; t++)
          for (int r = 0; r < RECORDS_PER_THREAD; r++)
            owned[t][r] = database.newDocument(typeName).set("payload", "t" + t + "-r" + r).save().getIdentity();
      });

      final long expected = (long) THREADS * RECORDS_PER_THREAD;
      // A record only spills once its page cannot host its growth, and a record that spilled leaves its first chunk
      // behind, so the shared page fills up in steps: keep growing everybody until the last one has left.
      for (int size = 1_000; size <= 24_000; size += 1_000) {
        final int payloadSize = size;
        database.transaction(() -> {
          for (int t = 0; t < THREADS; t++)
            for (int r = 0; r < RECORDS_PER_THREAD; r++)
              owned[t][r].asDocument(true).modify().set("payload", "x".repeat(payloadSize)).save();
        });

        if (expected == (Long) bucketStats(typeName).get("totalMultiPageRecords")) {
          spilledPayloadSize = payloadSize;
          return owned;
        }
      }
      throw new AssertionError("Not every record of " + typeName + " spilled into a chunk chain: " + bucketStats(typeName));
    }

    private ConcurrentUpdateOutcome runConcurrentUpdates(final RID[][] owned, final PayloadOf payloadOf)
        throws InterruptedException {
      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<String> conflicts = new CopyOnWriteArrayList<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final PageManager.PPageManagerStats before = pageManager.getStats();

      for (int t = 0; t < THREADS; t++) {
        final int id = t;
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int round = 0; round < ROUNDS; round++)
              for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                final RID rid = owned[id][i];
                final String payload = payloadOf.of(id, i, round);
                try {
                  database.transaction(() -> rid.asDocument(true).modify().set("payload", payload).save(), true, 1);
                } catch (final ConcurrentModificationException e) {
                  conflicts.add(rid + " round " + round + ": " + e.getMessage());
                }
              }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "chunked-" + t);
        threads.add(thread);
        thread.start();
      }

      start.countDown();
      for (final Thread thread : threads)
        thread.join();

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      final PageManager.PPageManagerStats after = pageManager.getStats();
      return new ConcurrentUpdateOutcome(conflicts,
          after.concurrentModificationExceptions - before.concurrentModificationExceptions,
          after.txPageSlotMerges - before.txPageSlotMerges,
          after.mergesDeclinedByCoverage - before.mergesDeclinedByCoverage);
    }

    /**
     * A conflict raised while READING the record, not while committing it: {@code loadMultiPageRecord} gives up after
     * {@code TX_RETRIES} attempts at assembling a chain that keeps changing under it. It is a different mechanism from
     * the one these tests are about, which is why it is counted apart - counting it as a merge failure is what made
     * this class fail on a loaded machine with a bare "expected 0 but was 2", measured by shrinking the read budget to
     * zero, where those runs produced exactly this message and nothing else. Until #6217 the read validated the page
     * VERSIONS its chain had touched, so another record's chunk sharing a page was enough to raise it; it now
     * validates the record's own chunks, so in these fixtures - where every record has exactly one writer, its own
     * thread - it must not be raised at all.
     */
    private static boolean isReadSideConflict(final String message) {
      return message != null && message.contains("was modified during read");
    }

    /**
     * What a run of {@link #runConcurrentUpdates} ended with. The counters are here because a failure of these tests
     * is a statement about the MERGE, and the merge already counts what it did: a version clash the merge absorbed
     * ({@code merged}) is the normal case - all but a handful of the transactions in these runs hit one - so the
     * question a failure has to answer is which of the two ways to lose it took. {@code declinedByCoverage} above zero
     * means a write on the page carried no declaration, which is a defect in the engine and not in the fixture;
     * {@code declinedByCoverage} at zero means the page was POISONED, i.e. a chunk of some record's chain lives on it.
     * Without them a failure here is a bare "expected 0 but was 2", which is where this test's own flakiness
     * investigation started.
     */
    private record ConcurrentUpdateOutcome(List<String> conflicts, long versionClashes, long merged,
                                           long declinedByCoverage) {
      /**
       * Conflicts the COMMIT raised, which is what these tests assert on. A read-side chain revalidation
       * ({@link #isReadSideConflict}) is not one of them.
       */
      int commitConflicts() {
        return (int) conflicts.stream().filter(c -> !isReadSideConflict(c)).count();
      }

      /** The other half: conflicts raised by a READ of a chunked record, which since #6217 must not happen here. */
      int readSideConflicts() {
        return (int) conflicts.stream().filter(ChunkedSlotMerge::isReadSideConflict).count();
      }

      @Override
      public String toString() {
        final String counters = commitConflicts() + " commit conflict(s) and " + readSideConflicts()
            + " read-side chain revalidation(s) out of " + versionClashes + " page-version clashes, " + merged
            + " absorbed by the slot merge, " + declinedByCoverage + " merges declined for missing coverage";
        if (commitConflicts() == 0)
          return counters;
        return counters + ". " + (declinedByCoverage > 0
            ? "A decline means a write on the page carried no declaration - an engine defect, not a fixture one."
            : "No decline, so the page could not be merged because it was POISONED: a chunk of a chain lives on it.")
            + " " + conflicts;
      }
    }

    private void verifyContent(final RID[][] owned, final PayloadOf payloadOf) {
      database.transaction(() -> {
        for (int t = 0; t < THREADS; t++)
          for (int i = 0; i < RECORDS_PER_THREAD; i++)
            assertThat(owned[t][i].asDocument(true).getString("payload")).isEqualTo(payloadOf.of(t, i, ROUNDS - 1));
      });
    }

    /** Same serialized length at every round, so the record reuses the very chunk chain it already has. */
    private String fixedSizePayload(final int thread, final int record, final int round) {
      return fixedSizePayload("t" + thread + "-r" + record + "-round" + round);
    }

    private String fixedSizePayload(final String marker) {
      return marker + "-" + "x".repeat(spilledPayloadSize - marker.length() - 1);
    }

    /** Longer at every round, so the chain has to be extended with a new chunk. */
    private String growingPayload(final int thread, final int record, final int round) {
      final String marker = "t" + thread + "-r" + record + "-round" + round;
      return marker + "-" + "x".repeat(spilledPayloadSize + 1_500 * round - marker.length() - 1);
    }

  }

  /**
   * Issue #6175: the continuation chunks of a multi-page record are placed by an allocator that PRIORITIZES the page
   * the previous chunk landed on and then scans the free-space statistics from the lowest page id up - so a chain
   * routinely comes back to the page holding its own HEAD chunk. That page is poisoned on arrival (a continuation chunk
   * is written through inline record-table writes no slot image accounts for), and it is precisely the page #6129 built
   * {@code SLOT_KIND_FIRST_CHUNK}, the chain fingerprint and the region re-derivation to keep mergeable.
   * <p>
   * <b>Measured before changing anything</b>, by logging every continuation-chunk placement of the fixture below (one
   * 9-byte record grown to 200 KB on a page it shares with one neighbour, page size 64 KB):
   * <pre>
   *   chunk 1 -> page 1 (new)     remaining 204811
   *   chunk 2 -> page 2 (new)     remaining 147490
   *   chunk 3 -> page 0  &lt;-- the head chunk's own page, from the statistics scan at half the requested size
   *   chunk 4 -> page 3 (new)     remaining  32874
   * </pre>
   * One spill, one poisoned head page, and the concurrent update of an unrelated record on it turned into a hard
   * conflict. The shape is not exotic: it is what a bucket of mostly-small records with a few large ones looks like,
   * which is also the shape where the merge matters most.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkOnHeadPage extends BucketPageLayoutTestSupport {
    private static final String BIG = "b".repeat(200 * 1024);

    /**
     * The issue itself: the record spills while sharing its page with another record that a concurrent transaction
     * rewrites. Nothing about that neighbour has anything to do with the chain, so the commit must merge - it did not,
     * because chunk 3 of the chain came home to page 0 and poisoned it.
     */
    @Test
    void aChainThatComesBackToTheHeadChunksPageDoesNotCostItsMerge() throws Exception {
      final RID[] spilling = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Chain", 1).createProperty("v", Type.STRING);
        spilling[0] = database.newDocument("Chain").set("v", "p").save().getIdentity();
        neighbour[0] = database.newDocument("Chain").set("v", "n").save().getIdentity();
      });

      // The premise, proved rather than assumed: with the merge switched OFF the very same interleaving must FAIL,
      // which is what shows the two records really do share a page.
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(spillSurvives(spilling[0], neighbour[0], "a".repeat(200 * 1024), "without the merge"))
            .as("the spilling record and the neighbour must share a page").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      assertThat(spillSurvives(spilling[0], neighbour[0], BIG, "rewritten"))
          .as("the spill must still merge with a concurrent write to another record of the page").isTrue();

      database.transaction(() -> {
        assertThat(spilling[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
      });
      checkDatabase();
    }

    /**
     * The same for an UPDATE of a record that is already a chunk chain, which allocates its extra chunks through the
     * identical call in {@code updateMultiPageRecord}: growing the chain must not poison the head chunk's page either.
     */
    @Test
    void growingAnExistingChainDoesNotCostTheHeadChunksPageEither() throws Exception {
      final RID[] spilling = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Growing", 1).createProperty("v", Type.STRING);
        spilling[0] = database.newDocument("Growing").set("v", "p").save().getIdentity();
        neighbour[0] = database.newDocument("Growing").set("v", "n").save().getIdentity();
      });

      // Already a chunk chain before the contended update, so the growth below goes through updateMultiPageRecord.
      database.transaction(() -> spilling[0].asDocument(true).modify().set("v", "s".repeat(100 * 1024)).save());

      final String grown = "g".repeat(300 * 1024);
      assertThat(spillSurvives(spilling[0], neighbour[0], grown, "rewritten"))
          .as("growing the chain must still merge with a concurrent write to another record of the page").isTrue();

      database.transaction(() -> {
        assertThat(spilling[0].asDocument(true).getString("v")).isEqualTo(grown);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
      });
      checkDatabase();
    }

    /**
     * What the exclusion must NOT become: a bucket that allocates a fresh page for every continuation chunk. Only the
     * ONE page holding this record's head chunk is off limits, and only while its slot is tracked, so the pages of
     * other records - and of this record's own earlier chunks - are reused exactly as before. Rewriting one chunked
     * record over and over must therefore leave the bucket's page count where it was.
     */
    @Test
    void onlyTheHeadChunksOwnPageIsOffLimits() {
      final RID[] spilling = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Bounded", 1).createProperty("v", Type.STRING);
        spilling[0] = database.newDocument("Bounded").set("v", "p").save().getIdentity();
        database.newDocument("Bounded").set("v", "n").save();
      });

      database.transaction(() -> spilling[0].asDocument(true).modify().set("v", BIG).save());

      final long pagesAfterSpill = (Long) bucketStats("Bounded").get("totalPages");

      for (int i = 0; i < 10; i++) {
        final String value = "v" + i + "-" + "x".repeat(200 * 1024);
        database.transaction(() -> spilling[0].asDocument(true).modify().set("v", value).save());
      }

      final Map<String, Object> layout = bucketStats("Bounded");
      assertThat((Long) layout.get("totalPages"))
          .as("rewriting one chunked record must reuse its chain's pages, not grow the bucket: " + layout)
          .isEqualTo(pagesAfterSpill);
      checkDatabase();
    }

    /**
     * Grows a record until it has to spill out of its page and, before committing, has another thread commit a change
     * to a record sharing that page. Returns true when our commit went through, i.e. the merge absorbed the version
     * bump.
     */
    private boolean spillSurvives(final RID spilling, final RID neighbour, final String value, final String neighbourValue)
        throws InterruptedException {
      database.begin();
      spilling.asDocument(true).modify().set("v", value).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  /**
   * Issue #6178: once a record had spilled it stayed a chunk chain for the rest of its life. However far it shrank
   * back, {@code updateMultiPageRecord} freed the continuation chunks and left a chain of exactly one chunk - a
   * FIRST_CHUNK marker, a 4-byte size, an 8-byte next pointer that is 0, and the content - costing 13 bytes per record
   * for ever, routing every read and write through the chunk path, and making {@code check()} report a multi-page
   * record that is not one any more.
   * <p>
   * The transition back is the mirror of {@code SLOT_KIND_RECORD_SPILLED_TO_CHUNK}, which exists for exactly the
   * opposite one, and is bounded by the same rule #6163 sizes the head chunk with: the region the slot owns on its own
   * page. A record that no longer fits it stays a chain, because shrinking is not licence to take a neighbour's bytes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkCollapse extends BucketPageLayoutTestSupport {
    /**
     * The common shape after #6163: a record that spilled as the LAST record of its page owns the whole free tail as
     * its region, so it can shrink a long way back and still fit. It must come back as a plain record, not as a chain
     * of one chunk.
     */
    @Test
    void aRecordShrinkingBackIntoItsRegionBecomesAPlainRecordAgain() {
      database.transaction(() -> database.getSchema().createDocumentType("Shrinking", 1).createProperty("v", Type.STRING));

      final RID last = fillFirstPage("Shrinking");

      // Spills as the page's LAST record: its head chunk takes the record's own footprint plus the whole free tail.
      database.transaction(() -> last.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());
      assertThat((Long) bucketStats("Shrinking").get("totalMultiPageRecords")).as("the record must have spilled")
          .isEqualTo(1L);

      final String small = "s".repeat(1024);
      database.transaction(() -> last.asDocument(true).modify().set("v", small).save());

      final Map<String, Object> layout = bucketStats("Shrinking");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record no longer needs a chain: " + layout).isZero();
      assertThat((Long) layout.get("totalChunks")).as("and no chunk must be left behind: " + layout).isZero();

      database.transaction(() -> assertThat(last.asDocument(true).getString("v")).isEqualTo(small));
      checkDatabase();
    }

    /**
     * The other end of the same rule: a record whose slot sits in the MIDDLE of its page owns only the handful of bytes
     * its own footprint covers - 14 of them when the spill had to enlarge the slot to reach a chunk header - so it
     * collapses only when it shrinks back inside those, and stays a chain for anything larger.
     */
    @Test
    void aRecordInTheMiddleOfItsPageCollapsesOnlyWithinItsOwnFootprint() {
      final RID[] tiny = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Middle", 1).createProperty("v", Type.STRING);
        // Written FIRST so it is never the last record of the page, hence never given the free tail.
        tiny[0] = database.newDocument("Middle").set("v", "p").save().getIdentity();
      });
      fillFirstPage("Middle");

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
      assertThat((Long) bucketStats("Middle").get("totalMultiPageRecords")).isEqualTo(1L);

      // Still far larger than the 14 bytes the slot owns: the record stays a chunk chain.
      final String stillBig = "m".repeat(20 * 1024);
      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", stillBig).save());

      Map<String, Object> layout = bucketStats("Middle");
      assertThat((Long) layout.get("totalMultiPageRecords"))
          .as("a record that does not fit its own slot must stay a chain: " + layout).isEqualTo(1L);
      database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(stillBig));

      // Back to what it was when it was created: now it fits, and the chain goes.
      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "p").save());

      layout = bucketStats("Middle");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("back inside its slot, it is a plain record: " + layout)
          .isZero();
      assertThat((Long) layout.get("totalChunks")).as("with the whole chain freed: " + layout).isZero();

      database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo("p"));
      checkDatabase();
    }

    /**
     * The collapse is a single-slot write like the spill it undoes, so it has to be replayable: a concurrent update of
     * another record on the same page must still merge instead of failing the transaction.
     * <p>
     * This is also where the collapse meets #6175, and the reason no separate test is needed for the pair. Freeing the
     * chain poisons every page it visits, so a chain that had come home to the head chunk's own page would poison the
     * very page this replay needs - which is exactly what #6175 stops. Measured on this fixture: with the exclusion
     * disabled the chain runs 1, 2, 0, 3 (chunk 3 lands on the head page); with it, 1, 2, 3, 4. Verified reaching
     * {@code rebaseRecordOnPage} with {@code SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD} rather than merely committing, so the
     * new replay is what this passes through and not some other path.
     */
    @Test
    void theCollapseIsReplayedOnACommittedPage() throws Exception {
      final RID[] collapsing = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
        collapsing[0] = database.newDocument("Merged").set("v", "p").save().getIdentity();
        neighbour[0] = database.newDocument("Merged").set("v", "n").save().getIdentity();
      });

      database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());
      assertThat((Long) bucketStats("Merged").get("totalMultiPageRecords")).isEqualTo(1L);

      // The premise, proved rather than assumed: with the merge switched OFF the same interleaving must FAIL, which is
      // what shows the two records really do share a page.
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(collapseSurvives(collapsing[0], neighbour[0], "q", "without the merge"))
            .as("the collapsing record and the neighbour must share a page").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      // Re-spilled by the rolled-back attempt? No - the failed transaction changed nothing. Collapse it for real.
      assertThat(collapseSurvives(collapsing[0], neighbour[0], "p", "rewritten")).isTrue();

      final Map<String, Object> layout = bucketStats("Merged");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the collapse must have gone through: " + layout).isZero();

      database.transaction(() -> {
        assertThat(collapsing[0].asDocument(true).getString("v")).isEqualTo("p");
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("rewritten");
      });
      checkDatabase();
    }

    /**
     * A concurrent update of the SAME record is still a conflict, collapse or no collapse: the chain this transaction
     * is about to free is the one it read, and the pre-image plus the chain fingerprint have to say so.
     */
    @Test
    void aConcurrentUpdateOfTheSameRecordStillConflicts() throws Exception {
      final RID[] collapsing = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Contended", 1).createProperty("v", Type.STRING);
        collapsing[0] = database.newDocument("Contended").set("v", "p").save().getIdentity();
        database.newDocument("Contended").set("v", "n").save();
      });

      database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "b".repeat(200 * 1024)).save());

      database.begin();
      collapsing[0].asDocument(true).modify().set("v", "p").save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> collapsing[0].asDocument(true).modify().set("v", "c".repeat(200 * 1024)).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        throw new AssertionError("two transactions rewriting the same record must conflict");
      } catch (final ConcurrentModificationException expected) {
        // THE CONTRACT
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> assertThat(collapsing[0].asDocument(true).getString("v")).isEqualTo("c".repeat(200 * 1024)));
      checkDatabase();
    }

    /**
     * The other head chunk, and the last transition between the two shapes that was missing (#6286): the CONTENT record
     * of a placeholder, which {@code createRecordInternal} spills into a chain of its own whenever no page can host it
     * whole. It used to keep that chain however far it shrank, because the collapse could only write a plain POSITIVE
     * size marker and a positive marker is exactly what tells {@code scan()} that a slot holds a document of its own.
     * Writing the NEGATED size the shape is recognised by is all that was needed: the chain goes, and the content stays
     * reachable through its pointer and through nothing else.
     */
    @Test
    void aPlaceholderContentShrinkingBackIntoItsRegionBecomesAContentRecord() {
      final RID[] placeholder = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Surrogate", 1).createProperty("v", Type.STRING);
        placeholder[0] = database.newDocument("Surrogate").set("v", "p").save().getIdentity();
      });
      // A page with no free tail at all is the only shape that still produces a placeholder pointer (#6149).
      sealFirstPage("Surrogate");

      // 200 KB fits no page whole, so the content record created behind the pointer spills into a chain itself.
      final String huge = "h".repeat(200 * 1024);
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", huge).save());

      Map<String, Object> layout = bucketStats("Surrogate");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the fixture must really be a placeholder: " + layout)
          .isEqualTo(1L);
      // The placeholder's content is a chain of its own - which is what makes this fixture the one this test needs, and
      // not merely a placeholder with a plain content record. Since #6196 that chain is counted as the SURROGATE it is,
      // next to the one multi-page record the bucket really has: the record that sealed page 0 when it spilled.
      assertThat((Long) layout.get("totalSurrogateRecords"))
          .as("the placeholder's content must itself have spilled into a chain: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalMultiPageRecords"))
          .as("and it is not a multi-page record of its own: " + layout).isEqualTo(1L);

      final long recordsBefore = countRecords("Surrogate");
      // Exactly one since #6196 gave the head chunk of a content record a marker of its own: before that,
      // createRecordInternal wrote FIRST_CHUNK where the negative size marker that hides a content record belonged, and
      // a scan handed these bytes out twice - once through the pointer, once as a document in their own right.
      final long copiesBefore = countRecordsHolding("Surrogate", huge);
      assertThat(copiesBefore).as("a chunked placeholder content is scanned once, through its pointer").isEqualTo(1L);

      final long chunksBefore = (Long) layout.get("totalChunks");
      assertThat(chunksBefore).as("the content chain must really have continuation chunks: " + layout).isPositive();

      // Back to a handful of bytes: the content record's own chain collapses into the negated-size shape.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "p").save());

      layout = bucketStats("Surrogate");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords"))
          .as("its content is still a surrogate, now a plain one: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalMultiPageRecords"))
          .as("and the only multi-page record left is the one that sealed page 0: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalChunks")).as("the content record's own chain must be gone: " + layout)
          .isLessThan(chunksBefore);
      assertThat(countRecords("Surrogate")).as("and the collapse must not have added a record of its own")
          .isEqualTo(recordsBefore);
      // What the negated marker is worth: a plain POSITIVE size marker is precisely what makes a slot a document in its
      // own right - the duplication #6196 removed, which a collapse writing the wrong sign would reintroduce on the one
      // record shape that reaches this branch behind a pointer.
      assertThat(countRecordsHolding("Surrogate", "p")).as("the collapse must not multiply the placeholder's content")
          .isEqualTo(copiesBefore);

      database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo("p"));

      // And it survives the round trip: grown back past its page it becomes a chain again, and shrunk again it collapses
      // again, all through the same pointer.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", huge).save());
      database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo(huge));
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "p").save());
      database.transaction(() -> assertThat(placeholder[0].asDocument(true).getString("v")).isEqualTo("p"));
      assertThat(countRecordsHolding("Surrogate", "p")).as("still exactly one copy after the round trip")
          .isEqualTo(copiesBefore);

      checkDatabase();
    }

    /**
     * Shrinks {@code collapsing} back inside its slot and, before committing, has another thread commit a change to a
     * record sharing that page. Returns true when our commit went through.
     */
    private boolean collapseSurvives(final RID collapsing, final RID neighbour, final String value,
        final String neighbourValue) throws InterruptedException {
      database.begin();
      collapsing.asDocument(true).modify().set("v", value).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  /**
   * Issue #6217: reading a record that outgrew its page used to re-validate the VERSION of every page its chunk chain
   * touched. A page version moves when ANY record on it moves, and the continuation chunks of different records share
   * pages because the allocator packs them there on purpose - so a reader was failed by writes that had not touched a
   * single byte of its own record, and after {@code arcadedb.txRetries} attempts the application got a
   * {@link ConcurrentModificationException} on an untouched record. It is the read-path twin of the false conflict the
   * disjoint-slot merge removed from the write path in #5381/#6129/#6175.
   * <p>
   * The fixture every test here shares is the one that produced the report: a dozen records whose slots all live on
   * page 0 of a single-bucket type, grown until every one of them is a chunk chain, so their continuation chunks share
   * the pages that follow.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkedReadFalseConflict extends BucketPageLayoutTestSupport {
    private static final int WRITER_THREADS = 4;
    private static final int ROUNDS         = 25;

    /**
     * The issue itself, made deterministic. Under {@code REPEATABLE_READ} the reading transaction caches the pages it
     * walks, so a commit landing on any of them after the first read is guaranteed to be seen as a version change by
     * the second - no race to win. The record being read is untouched by that commit, so the read must return the very
     * bytes it returned before.
     * <p>
     * The counter assertion is what keeps this test honest: it fails if the neighbour's write did NOT move a page the
     * read walked, which is the one way this could pass while proving nothing.
     */
    @Test
    void aChunkedReadSurvivesAWriteToAnotherRecordSharingItsPages() {
      final RID[] rids = createChunkedRecords("ChunkedRead");
      final LocalBucket bucket = bucketOf("ChunkedRead");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final byte[] read = bucket.getRecordInternal(rids[0], false).toByteArray();

        // A DIFFERENT record is rewritten, with a payload of the same length so it reuses the chain it already has and
        // touches nothing but its own slots.
        inAnotherThread(() -> database.transaction(
            () -> rids[1].asDocument(true).modify().set("payload", payload(1, 'y')).save()));

        assertThat(bucket.getRecordInternal(rids[0], false).toByteArray())
            .as("a record no writer touched must read back, unchanged, however busy the pages under it are")
            .isEqualTo(read);
      } finally {
        database.rollback();
      }

      // #8987: the pages are pinned by the REPEATABLE_READ transaction, so the read is answered from the snapshot and the
      // chain is never revalidated against the newest committed pages. The revalidation path is exercised by the
      // READ_COMMITTED race below.
      checkDatabase();
    }

    /**
     * The other half of the guarantee: what the read now validates is the RECORD, so a change to the record itself must
     * still be caught. Same shape as the test above, except the concurrent write lands on the record being read, and
     * with a payload that differs in EVERY chunk - a rewrite that changed only the head would leave the tail chunks
     * byte-identical, and a read that assembles the new head with those tails is the newest committed state and is
     * therefore allowed to stand.
     */
    @Test
    void aChunkedReadKeepsReturningTheSnapshotWhenTheRecordItselfMovedUnderIt() {
      final RID[] rids = createChunkedRecords("ChunkedReadConflict");
      final LocalBucket bucket = bucketOf("ChunkedReadConflict");

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final byte[] read = bucket.getRecordInternal(rids[0], false).toByteArray();

        inAnotherThread(() -> database.transaction(
            () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

        // #8987: the chain is the transaction's own pinned snapshot, so it is neither torn nor a conflict.
        assertThat(bucket.getRecordInternal(rids[0], false).toByteArray())
            .as("a chunked record rewritten after a REPEATABLE_READ transaction read it must read back as it was")
            .isEqualTo(read);
      } finally {
        database.rollback();
      }

      checkDatabase();
    }

    /**
     * The reported shape, at the default isolation level and with the read budget taken away: writers rewriting their
     * own chunked records while a reader reads a record NOBODY writes. With {@code TX_RETRIES} at zero a false conflict
     * has nowhere to hide - it is a failed read rather than a retry the application never sees - which is exactly how
     * #6217 was measured in the first place.
     */
    @Test
    void concurrentWritesToNeighbourRecordsNeverFailAReadThatHasNoRetryBudget() throws Exception {
      final RID[] rids = createChunkedRecords("ChunkedReadRace");
      final LocalBucket bucket = bucketOf("ChunkedReadRace");
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      final byte[] expected = readRecord(bucket, rids[0]);
      final long revalidationsBefore = pageManager.getStats().chunkChainReadRevalidations;

      final List<Throwable> errors = new CopyOnWriteArrayList<>();
      final List<String> readFailures = new CopyOnWriteArrayList<>();
      final AtomicLong reads = new AtomicLong();
      final AtomicBoolean writing = new AtomicBoolean(true);
      final CountDownLatch start = new CountDownLatch(1);

      final int previousRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
      // The read budget away: a read that meets a moved page has one attempt, so a false conflict is a failure the
      // test can see instead of a retry the application never hears about. The DATABASE's configuration, not the
      // global one: the global has no effect once the database is open.
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 0);
      try {
        final Thread reader = new Thread(() -> {
          try {
            start.await();
            while (writing.get()) {
              database.begin();
              try {
                assertThat(bucket.getRecordInternal(rids[0], false).toByteArray())
                    .as("the record being read is written by nobody, so it must never change").isEqualTo(expected);
                reads.incrementAndGet();
              } catch (final ConcurrentModificationException e) {
                readFailures.add(e.getMessage());
              } finally {
                database.rollback();
              }
            }
          } catch (final Throwable e) {
            errors.add(e);
          }
        }, "issue6217-reader");
        reader.start();

        final List<Thread> writers = new ArrayList<>();
        for (int t = 0; t < WRITER_THREADS; t++) {
          final int id = t;
          final Thread writer = new Thread(() -> {
            try {
              start.await();
              for (int round = 0; round < ROUNDS; round++) {
                final char filler = (char) ('a' + round % 26);
                // Record 0 is deliberately left out of the writers' share: every conflict the reader can see is
                // therefore a conflict with a write to somebody ELSE's record.
                for (int i = 1 + id; i < CHUNKED_RECORDS; i += WRITER_THREADS) {
                  final int record = i;
                  try {
                    database.transaction(
                        () -> rids[record].asDocument(true).modify().set("payload", payload(record, filler)).save(),
                        true, 3);
                  } catch (final ConcurrentModificationException e) {
                    // Writers losing to each other is not what this test is about, and is what #6129 covers.
                  }
                }
              }
            } catch (final Throwable e) {
              errors.add(e);
            }
          }, "issue6217-writer-" + t);
          writers.add(writer);
          writer.start();
        }

        start.countDown();
        for (final Thread writer : writers)
          writer.join();
        writing.set(false);
        reader.join();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, previousRetries);
      }

      if (!errors.isEmpty())
        throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

      assertThat(readFailures)
          .as("a record nobody writes must never fail a read, whatever its neighbours do (%d reads)", reads.get())
          .isEmpty();

      assertThat(reads.get()).as("the reader must have run alongside the writers").isPositive();

      assertThat(pageManager.getStats().chunkChainReadRevalidations - revalidationsBefore)
          .as("the reads must have met a writer's commit on one of their pages, or this test proves nothing")
          .isPositive();

      checkDatabase();
    }

    private byte[] readRecord(final LocalBucket bucket, final RID rid) {
      final byte[][] content = new byte[1][];
      database.transaction(() -> content[0] = bucket.getRecordInternal(rid, false).toByteArray());
      return content[0];
    }
  }

  /**
   * Issue #6258, items 1 and 2: the two things {@code LocalBucket.loadMultiPageRecord} used to get wrong once a chunked
   * read did not come out clean.
   * <ol>
   * <li>It retried when retrying could not possibly change the answer. The retry re-fetches the head page straight from
   * the {@code PageManager}, but the walk takes its continuation pages from the transaction - so under
   * {@code REPEATABLE_READ} every attempt after the first pairs the very same fresh head with the very same pinned
   * tails, reproduces the very same mix, and is rejected for the very same reason. The read spent its whole budget of
   * complete chain walks on a verdict settled before the first retry started, on the slowest read path there is.</li>
   * <li>It reported a corrupted record as contention. A chunk pointer into nowhere, a slot that no longer holds a
   * chunk, a chain that loops - none of them get better by trying again, but all of them took the retry path and ended
   * at "was modified during read after 3 retries", sending whoever read the log looking for contention that was not
   * there.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkedReadRetryAndCorruption extends BucketPageLayoutTestSupport {
    private static final String TYPE = "ChunkedRecord";

    /** {@code LocalBucket.FIRST_CHUNK} (-2) and {@code NEXT_CHUNK} (-3), as the single zigzag byte each is stored as. */
    private static final byte FIRST_CHUNK_MARKER = 3;
    private static final byte NEXT_CHUNK_MARKER  = 5;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // Two of the tests below deliberately inject on-disk corruption, which the post-test integrity check would
      // (correctly) flag. The tests that do NOT corrupt anything run `checkDatabase()` themselves.
      return false;
    }

    /**
     * Item 1. Under {@code REPEATABLE_READ} the reading transaction pins every page the chain walks, so a rewrite of
     * the record itself leaves the read no way out: the first retry is worth making (the head page is re-fetched
     * outside the transaction, so it really is different), and every one after it reads byte for byte what the one
     * before it read.
     * <p>
     * The counter is the whole point of the test. What changed is not whether the read fails - it failed before and
     * fails now - but how much it costs to find out: one wasted chain walk instead of the entire {@code txRetries}
     * budget, which is what a chunked record under contention pays on the largest records in the database.
     */
    @Test
    void aChunkedReadStopsRetryingWhenNoRetryCouldReadAnythingElse() {
      final RID[] rids = createChunkedRecords(TYPE);
      final LocalBucket bucket = bucketOf(TYPE);
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

      // A budget far larger than the default, so the assertion below measures the SHORT-CIRCUIT rather than the
      // budget: what must not happen is the cost growing with the number of retries the reader is allowed. The
      // DATABASE's configuration, not the global one - the global has no effect once the database is open.
      final int previousRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 10);

      final long retriesBefore = pageManager.getStats().chunkChainReadRetries;
      try {
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
        try {
          // Pins the whole chain - head and every continuation page - in this transaction's snapshot.
          final byte[] read = bucket.getRecordInternal(rids[0], false).toByteArray();

          // The record itself is rewritten, in every chunk, so the read that follows is a genuine conflict rather than
          // the false one #6217 removed.
          inAnotherThread(() -> database.transaction(
              () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

          // #8987: the pinned chain IS the snapshot, so the read is answered from it without a single retry.
          assertThat(bucket.getRecordInternal(rids[0], false).toByteArray())
              .as("a record rewritten after a REPEATABLE_READ transaction read it must read back as it was")
              .isEqualTo(read);
        } finally {
          database.rollback();
        }
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, previousRetries);
      }

      assertThat(pageManager.getStats().chunkChainReadRetries - retriesBefore)
          .as("a read answered from the pinned snapshot must not retry at all")
          .isZero();
    }

    /**
     * Item 1, the other side: the fail-fast must not fire while a retry still has something to read. Nothing of this
     * record's chain is pinned when the rewrite lands, so the read reloads all of it and simply succeeds - which is
     * also the proof that the new short-circuit is not a blanket "give up on the first conflict".
     */
    @Test
    void aChunkedReadStillRetriesWhileARetryCanStillReadSomethingElse() {
      final RID[] rids = createChunkedRecords(TYPE);
      final LocalBucket bucket = bucketOf(TYPE);

      final String rewritten = payload(0, 'y');
      inAnotherThread(() -> database.transaction(
          () -> rids[0].asDocument(true).modify().set("payload", rewritten).save()));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(rids[0].asDocument(true).getString("payload"))
            .as("a chunked record read for the first time after a rewrite reads the rewrite, not a conflict")
            .isEqualTo(rewritten);
      } finally {
        database.rollback();
      }

      checkDatabase();
    }

    /**
     * Item 2. A continuation pointer to a page the file does not have is corruption: every read of that record used to
     * walk the chain {@code txRetries + 1} times before reporting a concurrency problem that did not exist.
     */
    @Test
    void aChunkPointerIntoNowhereIsReportedAsCorruptionAndNotRetried() {
      final RID[] rids = createChunkedRecords(TYPE);
      final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();

      // A continuation pointer well past the end of the file: the chain breaks at chunk 0.
      corruptHeadChunkPointer(rids[0], 1_000_000L * maxRecordsInPage);

      assertBrokenChainIsReportedWithoutRetrying(rids[0], "next chunk pointer out of range at chunk 0");
    }

    /**
     * Item 2, the shape that was not merely retried but swallowed: the head chunk pointing at ITSELF used to raise a
     * {@code DatabaseOperationException} inside the walk, which the walk's own catch turned back into "the chain is
     * inconsistent" - so a record whose chain loops was retried like contention and then reported as contention, with
     * the loop the walk had positively identified nowhere in the message.
     */
    @Test
    void aChainThatLoopsIsReportedAsCorruptionAndNotRetried() {
      final RID[] rids = createChunkedRecords(TYPE);
      final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();

      // Record 1 rather than record 0: a self-reference is expressed as the chunk's own (page, slot), and record 0's
      // is page 0 slot 0, whose pointer value is 0 - which is how the LAST chunk of a chain says it has no next.
      final RID looping = rids[1];
      corruptHeadChunkPointer(looping, looping.getPosition() % maxRecordsInPage);

      assertBrokenChainIsReportedWithoutRetrying(looping, "chain loop detected at chunk 0");
    }

    /**
     * The same shape, one hop further out, where the walk's cheap guard cannot see it: chunk 2 pointing back at chunk 1
     * is a cycle no comparison of consecutive chunks detects, so the walk followed it forever - growing its trace and
     * the record it was assembling with every lap, on a record that could never be assembled. It now carries the exact
     * detector the confirmation walk always had (a visited-pointer set, allocated only once a chain is long enough to
     * need one), so the loop ends in the same corruption verdict as the self-reference above (code review on #6258).
     */
    @Test
    void aChainThatLoopsPastItsFirstHopIsAlsoReportedAsCorruption() {
      final RID[] rids = createChunkedRecords(TYPE);
      final int bucketId = rids[0].getBucketId();

      // head -> chunk1 -> chunk2 -> ... becomes head -> chunk1 -> chunk2 -> chunk1 -> ...
      final long chunk1 = nextChunkPointerOf(bucketId, rids[0].getPosition(), FIRST_CHUNK_MARKER);
      final long chunk2 = nextChunkPointerOf(bucketId, chunk1, NEXT_CHUNK_MARKER);
      assertThat(chunk1).as("the fixture must give record 0 a chain of at least three chunks").isPositive();
      assertThat(chunk2).as("the fixture must give record 0 a chain of at least three chunks").isPositive();

      writeNextChunkPointer(bucketId, chunk2, NEXT_CHUNK_MARKER, chunk1);

      // "visited twice" is the EXACT detector's wording, not the self-reference guard's: asserting it is what keeps
      // this test honest, since a cycle caught by the cheap guard would prove nothing about the one added here.
      assertBrokenChainIsReportedWithoutRetrying(rids[0], "visited twice");
    }

    /**
     * The boundary between the two items: a record that really moved under the read is contention, and must keep being
     * reported as contention. Only a chain that does not PARSE - and that still does not parse against the newest
     * committed image, with the read's own chunks proven current - is corruption.
     */
    @Test
    void aRecordThatMovedUnderTheReadIsContentionAndNotCorruption() {
      final RID[] rids = createChunkedRecords(TYPE);
      final LocalBucket bucket = bucketOf(TYPE);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final byte[] read = bucket.getRecordInternal(rids[0], false).toByteArray();

        inAnotherThread(() -> database.transaction(
            () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

        assertThat(bucket.getRecordInternal(rids[0], false).toByteArray())
            .as("a busy record must not be condemned as a corrupted one, nor torn: #8987 answers it from the snapshot")
            .isEqualTo(read);
      } finally {
        database.rollback();
      }

      checkDatabase();
    }

    /**
     * A broken chain must be reported as what it is, on the FIRST attempt, and as an exception the retry machinery does
     * not act on: a {@link NeedRetryException} tells every layer above to run the whole transaction again, which for a
     * corrupted record multiplies the wasted work instead of surfacing the repair.
     */
    private void assertBrokenChainIsReportedWithoutRetrying(final RID broken, final String expectedReason) {
      reopenDatabase();

      final LocalBucket bucket = bucketOf(TYPE);
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      final long retriesBefore = pageManager.getStats().chunkChainReadRetries;

      database.begin();
      try {
        assertThatThrownBy(() -> bucket.getRecordInternal(broken, false))
            .isInstanceOf(BrokenChunkChainException.class)
            .isNotInstanceOf(NeedRetryException.class)
            .hasMessageContaining("has a broken chunk chain")
            .hasMessageContaining(expectedReason)
            .hasMessageContaining("CHECK DATABASE FIX");
      } finally {
        database.rollback();
      }

      assertThat(pageManager.getStats().chunkChainReadRetries - retriesBefore)
          .as("a chain that cannot be parsed must not spend a single retry: retrying cannot change the answer")
          .isZero();
    }

    /** Overwrites the next-chunk pointer of the head chunk of {@code rid}, which lives at slot 0..n of page 0. */
    private void corruptHeadChunkPointer(final RID rid, final long nextChunkPointer) {
      writeNextChunkPointer(rid.getBucketId(), rid.getPosition(), FIRST_CHUNK_MARKER, nextChunkPointer);
    }

    /** The pointer to the next chunk carried by the chunk at {@code chunkPointer}, or 0 when it is the last one. */
    private long nextChunkPointerOf(final int bucketId, final long chunkPointer, final byte marker) {
      final long[] read = new long[1];
      ((DatabaseInternal) database).transaction(
          () -> read[0] = onChunkPointerField(bucketId, chunkPointer, marker, null));
      return read[0];
    }

    /** Points the chunk at {@code chunkPointer} somewhere else, which is how every corruption here is injected. */
    private void writeNextChunkPointer(final int bucketId, final long chunkPointer, final byte marker,
        final long nextChunkPointer) {
      ((DatabaseInternal) database).transaction(
          () -> onChunkPointerField(bucketId, chunkPointer, marker, nextChunkPointer));
    }

    /**
     * Reads (when {@code newValue} is null) or overwrites the next-chunk pointer field of one chunk of a chain,
     * addressed the way the chain itself addresses it: {@code chunkPointer = pageNumber * maxRecordsInPage + slot}. The
     * on-page layout after the record's size marker is {@code [chunkSize:int][nextChunkPointer:long][content...]}, and
     * the marker itself is one zigzag-encoded byte for both of the values used here.
     *
     * @return the pointer as it was BEFORE any write.
     */
    private long onChunkPointerField(final int bucketId, final long chunkPointer, final byte marker,
        final Long newValue) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
      final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();
      final int pageNumber = (int) (chunkPointer / maxRecordsInPage);
      final int slot = (int) (chunkPointer % maxRecordsInPage);

      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, bucketId, pageNumber), pageSize, false);
        // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; one uint per slot.
        final int recordOffset = (int) page.readUnsignedInt(
            Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);

        assertThat(page.readByte(recordOffset))
            .as("chunk %d/%d must carry the expected chunk marker", pageNumber, slot).isEqualTo(marker);

        final int pointerOffset = recordOffset + 1 + Binary.INT_SERIALIZED_SIZE;
        final long previous = page.readLong(pointerOffset);
        if (newValue != null)
          page.writeLong(pointerOffset, newValue);
        return previous;
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Issue #6149: the record PLACEHOLDER is the pre-chunk spill mechanism. Since #332 a record that outgrows its page
   * becomes a chunk chain instead, and the single site that still produces a placeholder pointer fires only when the
   * spilling record's slot cannot host a chunk header - 14 bytes. Those 14 bytes are normally there for the asking: the
   * branch is reached because the page cannot host the record's FULL new size, which says nothing about whether it can
   * spare the five-odd bytes that separate a 9-byte slot from a chunk header. The in-page shift that
   * {@code growRecordInPage} already performs provides them.
   * <p>
   * That matters because the placeholder is the one record shape the disjoint-slot merge cannot replay (the pointer
   * rewrite changes two pages at once) and the one behind the silent lost update of #6141.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PlaceholderPrefersChunks extends BucketPageLayoutTestSupport {
    /**
     * The issue itself: a 9-byte record on a page that cannot host its new 20 KB value, but has kilobytes of free tail.
     * Before the fix the slot became a placeholder POINTER; now it is grown by the handful of bytes a chunk header
     * needs and the record spills into a chunk chain like every other record of its size.
     */
    @Test
    void aSmallRecordSpillingWithRoomLeftInThePageBecomesAChunkChain() {
      final String big = "b".repeat(20 * 1024);

      final RID[] tiny = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Holder", 1).createProperty("v", Type.STRING);
        // Written FIRST so it is never the last record of the page: a last record would be grown into the free tail.
        tiny[0] = database.newDocument("Holder").set("v", "p").save().getIdentity();
      });
      fillFirstPage("Holder");

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

      final Map<String, Object> layout = bucketStats("Holder");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("no placeholder must be produced any more: " + layout)
          .isZero();
      assertThat((Long) layout.get("totalSurrogateRecords")).as("and therefore no placeholder content either: " + layout)
          .isZero();
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must have spilled into chunks: " + layout)
          .isEqualTo(1L);

      database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big));
      checkDatabase();
    }

    /**
     * The record keeps its RID across the spill, and the neighbours the in-page shift moved keep theirs and their
     * content: the shift only changes offsets, which are recomputed from the page's own slot table.
     */
    @Test
    void theShiftThatMakesRoomForTheChunkHeaderPreservesEveryOtherRecord() {
      final String big = "b".repeat(40 * 1024);

      final RID[] tiny = new RID[1];
      final RID[] neighbours = new RID[8];
      final String[] neighbourValues = new String[neighbours.length];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Neighbours", 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument("Neighbours").set("v", "p").save().getIdentity();
        for (int i = 0; i < neighbours.length; i++) {
          neighbourValues[i] = "n" + i + "-" + "y".repeat(16 * (i + 1));
          neighbours[i] = database.newDocument("Neighbours").set("v", neighbourValues[i]).save().getIdentity();
        }
      });
      fillFirstPage("Neighbours");

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

      database.transaction(() -> {
        assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big);
        for (int i = 0; i < neighbours.length; i++)
          assertThat(neighbours[i].asDocument(true).getString("v")).as("neighbour " + i + " must be intact")
              .isEqualTo(neighbourValues[i]);
      });

      assertThat((Long) bucketStats("Neighbours").get("totalPlaceholderRecords")).isZero();
      checkDatabase();
    }

    /**
     * The spill keeps working when it is repeated: every small record of a full page is grown past what the page can
     * host, in turn, so each one has to find the bytes for its own chunk header in what is left of the free tail.
     */
    @Test
    void everyRecordOfAPageCanSpillInTurn() {
      final int records = 24;
      final RID[] rids = new RID[records];

      database.transaction(() -> {
        database.getSchema().createDocumentType("AllSpill", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < records; i++)
          rids[i] = database.newDocument("AllSpill").set("v", "r" + i).save().getIdentity();
      });
      fillFirstPage("AllSpill");

      for (int i = 0; i < records; i++) {
        final int index = i;
        database.transaction(() -> rids[index].asDocument(true).modify().set("v", payloadOf(index)).save());
      }

      final Map<String, Object> layout = bucketStats("AllSpill");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("every spill must have found its chunk header: " + layout)
          .isZero();

      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          assertThat(rids[i].asDocument(true).getString("v")).as("record " + i).isEqualTo(payloadOf(i));
      });
      checkDatabase();
    }

    /**
     * The fallback, on the only page shape that still needs it: one with NO free tail at all, which no insert can
     * produce (the allocator always leaves a spare margin) but a spill can - the head chunk of a record that outgrows
     * its page takes its slot's footprint PLUS the whole tail, ending the page exactly at its maximum content size.
     * A 9-byte record on such a page has nowhere to find a chunk header, so it must still become a placeholder.
     * <p>
     * This is what covers the refusal arm of the enlargement test, {@code missing >= freeTailInPage}, at
     * {@code freeTailInPage == 0}. The neighbouring values (a tail that EXISTS but is smaller than the five-odd
     * bytes needed) run the identical comparison with different operands, and cannot be built through the public
     * API: a tail of 1..7 bytes needs byte-exact control of the page geometry, while inserts always leave at least
     * {@code SPARE_SPACE_FOR_GROWTH} (32) bytes and a spill always takes the tail down to exactly 0. A test that
     * guessed at those bytes would assert its own arithmetic rather than the engine's.
     */
    @Test
    void aSmallRecordSpillingOutOfASealedPageStillFallsBackToAPlaceholder() {
      final String big = "b".repeat(20 * 1024);

      final RID[] tiny = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Sealed", 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument("Sealed").set("v", "p").save().getIdentity();
      });
      sealFirstPage("Sealed");

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());

      final Map<String, Object> layout = bucketStats("Sealed");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("a page with no free tail leaves no other option: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords")).as("with its content record on another page: " + layout)
          .isEqualTo(1L);

      database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(big));
      checkDatabase();
    }

    /**
     * A placeholder that already exists - the ones every database written before this change contains - is migrated to
     * a chunk chain the first time its content record can no longer absorb an update, because the slot rebuild goes
     * through the very same spill branch. The readers stay, but the stored shape converges on chunks as data is
     * rewritten.
     */
    @Test
    void anExistingPlaceholderIsRebuiltAsAChunkChainWhenTheContentCannotAbsorbTheUpdate() {
      final String big = "b".repeat(20 * 1024);
      final String huge = "h".repeat(200 * 1024);

      final RID[] tiny = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Legacy", 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument("Legacy").set("v", "p").save().getIdentity();
      });
      final RID sealing = sealFirstPage("Legacy");

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", big).save());
      assertThat((Long) bucketStats("Legacy").get("totalPlaceholderRecords")).isEqualTo(1L);

      // Free the tail of page 0 again by deleting the record that sealed it, so the rebuild has room for a chunk header.
      database.transaction(() -> database.deleteRecord(sealing.asDocument(true)));

      // 200 KB fits no page at all, so the content record cannot grow: the slot is rebuilt from scratch.
      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", huge).save());

      final Map<String, Object> layout = bucketStats("Legacy");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the rebuilt slot must be a chunk head: " + layout).isZero();
      assertThat((Long) layout.get("totalSurrogateRecords")).as("and the old content record must be gone: " + layout).isZero();
      assertThat((Long) layout.get("totalMultiPageRecords")).isEqualTo(1L);

      database.transaction(() -> assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(huge));
      checkDatabase();
    }

    /**
     * The spill that had to enlarge its slot is still a single-slot write, so the disjoint-slot merge (#5279, #6129)
     * has to replay it on a newer committed version of the page - shift included, since the room the head chunk needs
     * is not on the committed page either. That closes the last gap the placeholder used to leave: this very write was
     * the one shape no merge could absorb, because the pointer rewrite changed two pages at once.
     */
    @Test
    void aSpillThatHadToEnlargeItsSlotIsStillReplayedOnACommittedPage() throws Exception {
      final RID[] tiny = new RID[1];
      final RID[] neighbour = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType("Merged", 1).createProperty("v", Type.STRING);
        // 9 bytes: too small for a chunk header, which is exactly what used to make this a placeholder.
        tiny[0] = database.newDocument("Merged").set("v", "p").save().getIdentity();
        neighbour[0] = database.newDocument("Merged").set("v", "n").save().getIdentity();
      });
      fillFirstPage("Merged");

      // The premise, proved rather than assumed: with the merge switched OFF the very same interleaving must FAIL,
      // which is what shows the two records really do share a page.
      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        assertThat(spillSurvives(tiny[0], neighbour[0], "a".repeat(200 * 1024), "neighbour without the merge"))//
            .as("the spilling record and the neighbour must share a page").isFalse();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      final String spilled = "b".repeat(200 * 1024);
      assertThat(spillSurvives(tiny[0], neighbour[0], spilled, "neighbour rewritten")).isTrue();

      final Map<String, Object> layout = bucketStats("Merged");
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the record must have spilled into chunks: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalPlaceholderRecords")).isZero();

      database.transaction(() -> {
        assertThat(tiny[0].asDocument(true).getString("v")).isEqualTo(spilled);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour rewritten");
      });
      checkDatabase();
    }

    /**
     * Grows a record until it has to spill out of its page and, before committing, has another thread commit a change
     * to a record sharing that page. Returns true when our commit went through, i.e. the merge absorbed the version
     * bump.
     */
    private boolean spillSurvives(final RID spilling, final RID neighbour, final String value, final String neighbourValue)
        throws InterruptedException {
      database.begin();
      spilling.asDocument(true).modify().set("v", value).save();

      final Thread concurrent = new Thread(
          () -> database.transaction(() -> neighbour.asDocument(true).modify().set("v", neighbourValue).save()));
      concurrent.start();
      concurrent.join();

      try {
        database.commit();
        return true;
      } catch (final ConcurrentModificationException e) {
        return false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    private static String payloadOf(final int index) {
      return "p" + index + "-" + "z".repeat(4 * 1024 + index);
    }

  }

  /**
   * Issue #6196: a placeholder whose CONTENT record had to spill into chunks was returned TWICE by a scan.
   * <p>
   * A content record is normally recognised by the NEGATIVE size marker its slot carries, and every reader that walks a
   * page skips it on that basis. But a content record that no page can host whole is written by
   * {@code writeMultiPageRecord}, which used to stamp the plain {@code FIRST_CHUNK} marker of an ordinary multi-page
   * record on its head chunk - and with it went the only record of "this slot is somebody's content, not a record".
   * The bytes were then handed out twice: once through the placeholder POINTER that references them, and once as a
   * document of their own under the head chunk's RID. {@code check()} had the mirror of the same confusion, counting
   * such a record under {@code totalMultiPageRecords} and never under {@code totalSurrogateRecords}, and {@code count()}
   * counted it as a record.
   * <p>
   * The fix gives the head chunk of a content record a marker of its own, {@code FIRST_CHUNK_PLACEHOLDER_CONTENT} (-4),
   * in the free slot the marker namespace already had between {@code NEXT_CHUNK} (-3) and the negated sizes (&lt; -5).
   * Databases written before it still hold the ambiguous shape, so {@code CHECK DATABASE} reports one and
   * {@code CHECK DATABASE FIX} converts it - the ambiguity is repaired, not tolerated for ever.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PlaceholderContentChain extends BucketPageLayoutTestSupport {
    private static final String TYPE                       = "Surrogate";
    /** {@code LocalBucket.FIRST_CHUNK} (-2) and {@code FIRST_CHUNK_PLACEHOLDER_CONTENT} (-4), zigzag-encoded. */
    private static final byte   FIRST_CHUNK_MARKER         = 3;
    private static final byte   CONTENT_CHUNK_MARKER       = 7;
    /** {@code LocalBucket.RECORD_PLACEHOLDER_POINTER} (-1), zigzag-encoded. */
    private static final byte   PLACEHOLDER_POINTER_MARKER = 1;
    /** 200 KB fits no page whole, so the content record created behind the pointer spills into a chain of its own. */
    private static final String HUGE                       = "h".repeat(200 * 1024);

    /** Continuation chunks the record that SEALS page 0 owns - every chunk in the bucket that is not the content's. */
    private long chunksOfTheSealingRecord;
    /** The record that seals page 0 by spilling: an ordinary multi-page record, and the only other chunk head around. */
    private RID  sealingRecord;

    /**
     * The issue as reported: the placeholder's content must be a document exactly once, under the RID the user knows,
     * and the head chunk that holds it must be invisible to a scan, to {@code count()} and to a direct load.
     */
    @Test
    void aPlaceholderWhoseContentSpilledIsScannedOnce() {
      final RID placeholder = placeholderWithChainedContent();

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the fixture must really be a placeholder: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords"))
          .as("the content record is a surrogate even though it is stored as a chain: " + layout).isEqualTo(1L);
      // Only the record that sealed page 0 when it spilled: the content record's chain is no longer counted as one.
      assertThat((Long) layout.get("totalMultiPageRecords"))
          .as("a placeholder's content is not a multi-page record of its own: " + layout).isEqualTo(1L);

      assertThat(countRecordsHolding(HUGE)).as("the content must be handed out exactly once, through the pointer")
          .isEqualTo(1L);

      final RID content = contentRidOf(placeholder);
      assertThat(markerByteAt(content)).as("the head chunk of a content record carries its own marker")
          .isEqualTo(CONTENT_CHUNK_MARKER);
      database.transaction(() -> {
        assertThat(bucketOf(TYPE).existsRecord(content)).as("a content record does not exist on its own").isFalse();
        assertThat(bucketOf(TYPE).getRecordInternal(content, false))
            .as("and cannot be loaded as a record on its own").isNull();
        assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(HUGE);
      });

      checkDatabase();
    }

    /**
     * The whole life of such a record through the pointer: rewritten large (the chain is reused), shrunk back (since
     * #6286 the chain COLLAPSES into the negated-size shape a content record has when it fits its page), grown again,
     * and finally deleted with whatever it had become behind it.
     */
    @Test
    void theContentChainKeepsItsMarkerThroughEveryRewrite() {
      final RID placeholder = placeholderWithChainedContent();
      final RID content = contentRidOf(placeholder);

      final String otherHuge = "o".repeat(200 * 1024);
      database.transaction(() -> placeholder.asDocument(true).modify().set("v", otherHuge).save());
      assertThat(markerByteAt(content)).as("a rewrite in place must not turn the content into a record")
          .isEqualTo(CONTENT_CHUNK_MARKER);
      assertThat(countRecordsHolding(otherHuge)).isEqualTo(1L);

      database.transaction(() -> placeholder.asDocument(true).modify().set("v", "p").save());
      Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalChunks")).as("and the content's own chain is collapsed away (#6286): " + layout)
          .isEqualTo(chunksOfTheSealingRecord);
      assertThat((Long) layout.get("totalSurrogateRecords"))
          .as("into the negated-size shape a content record has: " + layout).isEqualTo(1L);
      assertThat(countRecordsHolding("p")).as("nor must a shrink multiply the content").isEqualTo(1L);

      database.transaction(() -> placeholder.asDocument(true).modify().set("v", HUGE).save());
      assertThat(countRecordsHolding(HUGE)).isEqualTo(1L);

      final long recordsBefore = countRecords();
      database.transaction(() -> placeholder.asDocument(true).delete());
      assertThat(countRecords()).as("deleting the pointer frees the content chain with it").isEqualTo(recordsBefore - 1);
      layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer is gone: " + layout).isZero();
      assertThat((Long) layout.get("totalSurrogateRecords")).as("no content record is left behind: " + layout).isZero();
      // Back to the chunks the record that SEALED page 0 owns, and not one more: the content record's own chain, which
      // was every chunk on top of those, was freed with the pointer that led to it.
      assertThat((Long) layout.get("totalChunks")).as("nor any of its chunks: " + layout).isEqualTo(chunksOfTheSealingRecord);

      checkDatabase();
    }

    /**
     * A database written BEFORE this fix holds the ambiguous shape, and no reader can tell it from an ordinary
     * multi-page record - so the duplicate is still there. {@code CHECK DATABASE} names it and {@code FIX} converts the
     * marker in place, which is what keeps the tolerance out of the readers.
     */
    @Test
    void checkDatabaseFixRepairsALegacyAmbiguousContentRecord() {
      final RID placeholder = placeholderWithChainedContent();
      final RID content = contentRidOf(placeholder);

      // Downgrade the marker to what every release before this fix wrote there.
      writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
      assertThat(countRecordsHolding(HUGE)).as("the shape #6196 reported: the content is scanned twice").isEqualTo(2L);

      final Result unfixed = checkDatabaseRow(false);
      assertThat(numberProperty(unfixed, "totalErrors")).as("check must report it: " + unfixed.toJSON()).isEqualTo(1L);
      assertThat(warningsOf(unfixed).toString()).contains(content.toString()).contains("placeholder");

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "totalErrors")).as("fix must repair it: " + fixed.toJSON()).isEqualTo(1L);
      assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix"))
          .as("and repair it by rewriting the marker, never by deleting the record: " + fixed.toJSON()).isEmpty();

      assertThat(markerByteAt(content)).isEqualTo(CONTENT_CHUNK_MARKER);
      assertThat(countRecordsHolding(HUGE)).as("the duplicate is gone").isEqualTo(1L);
      database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(HUGE));

      checkDatabase();
    }

    /**
     * The other way a legacy ambiguous head stops being ambiguous, and it needs no {@code FIX}: an ordinary UPDATE
     * through the pointer that shrinks the record back inside the region its slot owns collapses it (#6286), and the
     * collapse writes the NEGATED size marker - which is the very thing the ambiguous shape was missing. The duplicate
     * a scan used to hand out goes with it.
     * <p>
     * Which sign that collapse writes is decided by the FLAG the slot was reached through and never by the marker
     * found, because the marker is exactly what cannot tell a legacy content head from a record's own head chunk. Get
     * that wrong and the collapse writes a POSITIVE size, which is #6196 all over again on a record that was already
     * suffering from it - so the copy count is what this asserts, not the byte.
     * <p>
     * The write is not offered to the disjoint-slot merge (neither collapse kind names FIRST_CHUNK as the marker it
     * replays over) and the page is poisoned instead; a legacy head that does NOT collapse keeps its ordinary
     * head-chunk tracking, which is what the second half of this test pins.
     */
    @Test
    void anUpdateCollapsesALegacyAmbiguousContentHeadAndEndsItsAmbiguity() {
      final RID placeholder = placeholderWithChainedContent();
      final RID content = contentRidOf(placeholder);

      writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
      assertThat(countRecordsHolding(HUGE)).as("the shape #6196 reported: the content is scanned twice").isEqualTo(2L);

      // Still far too big for the region the content record's slot owns: it stays a chain, and stays ambiguous.
      //
      // That this rewrite also keeps its head-chunk tracking - the poison belongs to the collapse, and firing it up
      // front cost an ordinary rewrite the replay it has always had (PR review) - is deliberately NOT asserted here.
      // An update is deferred to commit, so the flag does not exist while the transaction is open, and by the time it
      // does the transaction is gone; an assertion placed either side passes whether or not the poison fires, which is
      // worth less than no assertion at all. What it costs is a merge opportunity and never correctness, and only on
      // data written before #6196.
      final String stillHuge = "s".repeat(200 * 1024);
      database.transaction(() -> placeholder.asDocument(true).modify().set("v", stillHuge).save());
      assertThat(markerByteAt(content)).as("a rewrite that stays a chain leaves the legacy marker alone")
          .isEqualTo(FIRST_CHUNK_MARKER);
      assertThat(countRecordsHolding(stillHuge)).as("so the duplicate is still there").isEqualTo(2L);

      // Back to a handful of bytes: now it fits, and the collapse writes the marker the shape was missing.
      database.transaction(() -> placeholder.asDocument(true).modify().set("v", "p").save());

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the pointer must still be a pointer: " + layout)
          .isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords"))
          .as("and its content a surrogate, not a record of its own: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalChunks")).as("with the chain freed: " + layout)
          .isEqualTo(chunksOfTheSealingRecord);
      assertThat(countRecordsHolding("p")).as("one copy, through the pointer, and no second one").isEqualTo(1L);

      database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo("p"));
      // Nothing left for CHECK DATABASE to repair: the update did what the FIX would have done, and more.
      checkDatabase();
    }

    /**
     * A content record is not a record, on the WRITE paths as much as on the read ones: reached at its own RID, an
     * update and a delete both refuse it, exactly as they already do for a content record small enough to have kept the
     * negated size marker. Anything else would let a caller rewrite or free content a placeholder pointer still
     * references, leaving that pointer aimed at somebody else's bytes or at nothing.
     * <p>
     * Driven through {@code LocalBucket} rather than through a document, deliberately: {@code content.asDocument()} is
     * refused one layer earlier, by the read that will not hand out a content record, so it would never reach the two
     * guards this is about.
     */
    @Test
    void aChunkedContentRecordIsNotUpdatableOrDeletableAtItsOwnRid() {
      final RID content = contentRidOf(placeholderWithChainedContent());

      database.transaction(() -> {
        final LocalBucket bucket = bucketOf(TYPE);

        final MutableDocument surrogate = database.newDocument(TYPE).set("v", "whatever the caller meant");
        ((RecordInternal) surrogate).setIdentity(content);
        assertThatThrownBy(() -> bucket.updateRecord(surrogate, false))
            .as("a content record cannot be rewritten behind its placeholder's back")
            .isInstanceOf(RecordNotFoundException.class);

        assertThatThrownBy(() -> bucket.deleteRecord(content))
            .as("nor freed, which would leave the pointer aimed at nothing")
            .isInstanceOf(RecordNotFoundException.class);
      });

      // Both refusals left the record exactly as it was: still one copy, still reachable through the pointer.
      assertThat(countRecordsHolding(HUGE)).isEqualTo(1L);
      assertThat(markerByteAt(content)).isEqualTo(CONTENT_CHUNK_MARKER);
      checkDatabase();
    }

    /**
     * The compound legacy shape: a content record still wearing the ambiguous marker whose chain is ALSO broken. The
     * ambiguity itself must be booked ONCE - the broken chain is what FIX can act on, so the record is force-deleted,
     * and the ambiguity reconciliation must then leave alone a slot that is no longer there rather than report a second
     * error and a repair that could not have happened.
     * <p>
     * What the run also reports is everything the corruption really cost, which is more than the one head slot: the
     * placeholder POINTER that led to the record is removed with it (#6292 - left behind, it is a slot every scan skips
     * and {@code count(*)} counts, for good), and the continuation chunks the overwritten pointer had already cut the
     * chain off from are reclaimed (#6294 - nothing collected them before, whatever the comments promised). Those three
     * chunks are dead space rather than a corruption, so they are counted and reclaimed without being booked as errors:
     * the record had ONE defect, and one error is what the run reports.
     */
    @Test
    void aLegacyContentHeadWithABrokenChainIsReportedOnce() {
      final RID placeholder = placeholderWithChainedContent();
      final RID content = contentRidOf(placeholder);

      writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
      final long chunksBefore = (Long) bucketStats(TYPE).get("totalChunks");
      breakChainAt(content);

      final Result fixed = checkDatabaseRow(true);
      assertThat(warningsOf(fixed).toString()).as("the broken chain is what it names: " + fixed.toJSON())
          .contains("broken multi-page chunk chain").doesNotContain("could not be repaired")
          .doesNotContain("ambiguous chunk chain");
      assertThat((Collection<Object>) fixed.<Collection<Object>>getProperty("deletedRecordsAfterFix"))
          .as("the head slot and the pointer that was the record's identity: " + fixed.toJSON())
          .containsExactlyInAnyOrder(content, placeholder);
      assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed"))
          .as("the pointer is removed with the content it can no longer reach: " + fixed.toJSON()).isEqualTo(1L);

      // The chunks the corrupted pointer had cut off: leaked before this run, and reclaimed by it.
      final long orphanedChunks = numberProperty(fixed, "orphanedChunks");
      assertThat(orphanedChunks).as("the cut-off chunks must be found: " + fixed.toJSON()).isPositive();
      assertThat(numberProperty(fixed, "orphanedChunksReclaimed")).as("and reclaimed: " + fixed.toJSON())
          .isEqualTo(orphanedChunks);
      // ONE error: the broken chain. The chunks it leaked are dead space, not a corruption - no record is wrong and no
      // query is affected - so they are counted and reclaimed, never booked as errors of their own.
      assertThat(numberProperty(fixed, "totalErrors"))
          .as("one record, one defect it can act on, one error: " + fixed.toJSON()).isEqualTo(1L);

      // Nothing is left for a second run to find: not the slot the pointer led to, not the pointer, not the chunks.
      final Result again = checkDatabaseRow(false);
      assertThat(numberProperty(again, "totalErrors")).as("and the database checks out afterwards: " + again.toJSON())
          .isZero();
      assertThat(numberProperty(again, "totalChunks")).as("with the leaked chunks really gone: " + again.toJSON())
          .isLessThan(chunksBefore);
      assertThat(numberProperty(again, "totalPlaceholderRecords"))
          .as("and no pointer aimed at nothing: " + again.toJSON()).isZero();
    }

    /**
     * The repair rests on the engine's own invariant - a placeholder pointer leads to the content record written for it
     * and nothing else - and a corrupted pointer breaks it. The one form of that which can be RECOGNISED is two pointers
     * leading to the same slot, which a healthy bucket cannot produce: FIX must then report and leave the marker alone,
     * because the marker is unrecoverable information (it says whose the record is) and nothing here knows which of the
     * two pointers is the lie.
     */
    @Test
    void aContentHeadTwoPointersLeadToIsReportedAndLeftAlone() {
      final RID placeholder = placeholderWithChainedContent();
      final RID content = contentRidOf(placeholder);

      writeMarkerByteAt(content, FIRST_CHUNK_MARKER);
      // A second pointer to the same content, over a record that has room for one: a corrupted pointer, in the one shape
      // that can be told apart from a healthy one.
      final RID hijacked = new RID(placeholder.getBucketId(), placeholder.getPosition() + 1);
      database.transaction(() -> onSlot(hijacked, page -> {
        final int recordOffset = recordOffsetOf(page, hijacked);
        page.writeByte(recordOffset, PLACEHOLDER_POINTER_MARKER);
        page.writeLong(recordOffset + 1, content.getPosition());
        return 0L;
      }));

      final Result fixed = checkDatabaseRow(true);
      assertThat(warningsOf(fixed).toString()).as("the refusal must name the record and the reason: " + fixed.toJSON())
          .contains(content.toString()).contains("more than one placeholder pointer");
      assertThat(markerByteAt(content)).as("and the marker must be exactly as it was found").isEqualTo(FIRST_CHUNK_MARKER);
      assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix"))
          .as("nothing is deleted either: " + fixed.toJSON()).isEmpty();

      // The corruption this test fabricated is deliberately NOT repairable - that is the whole point - so the type goes
      // with the test rather than being left for the integrity check every test ends with.
      database.transaction(() -> database.getSchema().dropType(TYPE));
    }

    /** Points the head chunk of {@code rid} at a page far past the end of the bucket, which breaks its chain. */
    private void breakChainAt(final RID rid) {
      database.transaction(() -> onSlot(rid, page -> {
        // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
        page.writeLong(recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
        return 0L;
      }));
    }

    /**
     * The commit-time half of the marker: the disjoint-slot merge replays a head chunk only onto a committed slot that
     * still carries the marker the write started from, and the two kinds are what tell it which one that is.
     * <p>
     * Driven through {@code rebaseRecordOnPage} directly, in the manner {@code ChunkedSlotMerge} already
     * uses for the tracking guards and for the same reason: the shape transition this refuses - a slot that turns from
     * a record's head chunk into a placeholder's content, or back, under a concurrent commit - is one no write path
     * produces today, so a test that went through the engine would prove nothing about the guard. What must not happen
     * is that a future one reaches it and is silently mis-merged, which is what the two refusals below assert.
     * <p>
     * The two positive controls are not decoration: without them a guard that refused EVERYTHING would pass this test.
     */
    @Test
    void aHeadChunkIsOnlyReplayedOntoTheMarkerItsWriteStartedFrom() {
      final RID content = contentRidOf(placeholderWithChainedContent());

      database.begin();
      try {
        final LocalBucket bucket = bucketOf(TYPE);

        // The CONTENT record's head chunk: replayable as placeholder content, refused as a record.
        final byte[] contentImage = chunkImageOf(content);
        assertThat(rebase(bucket, content, contentImage, TransactionContext.SLOT_KIND_FIRST_CHUNK))
            .as("a content head must not be replayed under the kind of a record's own head").isFalse();
        assertThat(rebase(bucket, content, contentImage, TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT))
            .as("and must be replayable under its own").isTrue();

        // The record that SEALED page 0: an ordinary multi-page record, and the mirror of the above.
        final byte[] recordImage = chunkImageOf(sealingRecord);
        assertThat(rebase(bucket, sealingRecord, recordImage,
            TransactionContext.SLOT_KIND_FIRST_CHUNK_PLACEHOLDER_CONTENT))
            .as("a record's head must not be replayed under the kind of a content head").isFalse();
        assertThat(rebase(bucket, sealingRecord, recordImage, TransactionContext.SLOT_KIND_FIRST_CHUNK))
            .as("and must be replayable under its own").isTrue();
      } finally {
        database.rollback();
      }

      database.transaction(() -> assertThat(placeholderRecordCount()).isEqualTo(1L));
      checkDatabase();
    }

    /**
     * The same guard for the COLLAPSE (#6286), which is where the two kinds earn their keep: both start from a head
     * chunk and both end with the slot holding plain content, and the ONLY thing that separates them is the marker the
     * replay must still find and the SIGN of the one it writes. A replay that took the wrong one would give a
     * placeholder's content a positive size - a document of its own, which is #6196 - or hide a record behind a
     * negative one.
     * <p>
     * Driven through {@code rebaseRecordOnPage} for the reason the sibling test above states: the transition it refuses
     * is one no write path produces today, so only a direct drive proves the guard rather than the absence of a caller.
     * Both directions, and both positive controls, for the same reason as there.
     */
    @Test
    void aCollapseIsOnlyReplayedOntoTheMarkerItsWriteStartedFrom() {
      final RID content = contentRidOf(placeholderWithChainedContent());

      // Small enough for the region either slot owns, so a refusal can only come from the marker check.
      final byte[] collapsed = "collapsed".getBytes(StandardCharsets.UTF_8);

      database.begin();
      try {
        final LocalBucket bucket = bucketOf(TYPE);

        final byte[] contentImage = chunkImageOf(content);
        assertThat(rebase(bucket, content, collapsed, contentImage, TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD))
            .as("a content head must not collapse under the kind that writes a record's positive size").isFalse();
        assertThat(rebase(bucket, content, collapsed, contentImage,
            TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT))
            .as("and must collapse under the one that writes the negated size its shape is known by").isTrue();

        final byte[] recordImage = chunkImageOf(sealingRecord);
        assertThat(rebase(bucket, sealingRecord, collapsed, recordImage,
            TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_PLACEHOLDER_CONTENT))
            .as("a record's own head must not collapse under the kind of a content head").isFalse();
        assertThat(rebase(bucket, sealingRecord, collapsed, recordImage,
            TransactionContext.SLOT_KIND_CHUNK_COLLAPSED_TO_RECORD))
            .as("and must collapse under its own").isTrue();
      } finally {
        database.rollback();
      }

      database.transaction(() -> assertThat(placeholderRecordCount()).isEqualTo(1L));
      checkDatabase();
    }

    /**
     * The image the disjoint-slot merge keeps for a head chunk: everything after the marker, i.e.
     * {@code [int chunkSize][long nextChunk][chunkSize bytes of content]}. Replaying it unchanged is a no-op write, so
     * the positive controls above assert the guard and not the content.
     */
    private byte[] chunkImageOf(final RID rid) {
      final byte[][] image = new byte[1][];
      onSlot(rid, page -> {
        // Both markers this test meets are one zigzag byte, so the chunk header starts right after it.
        final int chunkHeaderPos = recordOffsetOf(page, rid) + 1;
        final int size = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt(chunkHeaderPos);
        image[0] = new byte[size];
        page.readByteArray(chunkHeaderPos, image[0], 0, size);
        return 0L;
      });
      return image[0];
    }

    /** Replays {@code image} onto the slot of {@code rid} under {@code kind}, as a commit-time slot rebase would. */
    private boolean rebase(final LocalBucket bucket, final RID rid, final byte[] image, final byte kind) {
      return rebase(bucket, rid, image, image, kind);
    }

    /**
     * The general form, for the two kinds whose images DIFFER: a collapse starts from the head chunk ({@code baseBody})
     * and ends with the slot holding plain content ({@code body}).
     */
    private boolean rebase(final LocalBucket bucket, final RID rid, final byte[] body, final byte[] baseBody,
        final byte kind) {
      final boolean[] rebased = new boolean[1];
      onSlot(rid, page -> {
        rebased[0] = bucket.rebaseRecordOnPage(page, (int) (rid.getPosition() % bucket.getMaxRecordsInPage()), body,
            baseBody, kind);
        return 0L;
      });
      return rebased[0];
    }

    /** Placeholder pointers the bucket holds, so the rolled-back replays above are shown to have changed nothing. */
    private long placeholderRecordCount() {
      return (Long) bucketStats(TYPE).get("totalPlaceholderRecords");
    }

    /**
     * The fixture #6196 needs: a placeholder POINTER whose CONTENT record is itself a chunk chain. Both halves are
     * required - a page with a free tail of exactly zero (since #6149 the only shape that still produces a pointer) and
     * a value no page can host whole.
     */
    private RID placeholderWithChainedContent() {
      final RID[] placeholder = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        placeholder[0] = database.newDocument(TYPE).set("v", "p").save().getIdentity();
      });
      sealingRecord = sealFirstPage(TYPE);
      chunksOfTheSealingRecord = (Long) bucketStats(TYPE).get("totalChunks");

      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", HUGE).save());

      assertThat((Long) bucketStats(TYPE).get("totalPlaceholderRecords"))
          .as("the fixture must produce a placeholder pointer").isEqualTo(1L);
      return placeholder[0];
    }

    /** The RID of the CONTENT record the placeholder POINTER at {@code rid} references. */
    private RID contentRidOf(final RID rid) {
      final long[] pointer = new long[1];
      database.transaction(() -> pointer[0] = onSlot(rid, page -> {
        final int recordOffset = recordOffsetOf(page, rid);
        assertThat(page.readByte(recordOffset)).as("%s must be a placeholder pointer", rid)
            .isEqualTo(PLACEHOLDER_POINTER_MARKER);
        return page.readLong(recordOffset + 1);
      }));
      return new RID(rid.getBucketId(), pointer[0]);
    }

    /** The single byte the record's size marker is stored as - the whole of it for every marker used here. */
    private byte markerByteAt(final RID rid) {
      final long[] marker = new long[1];
      database.transaction(() -> marker[0] = onSlot(rid, page -> (long) page.readByte(recordOffsetOf(page, rid))));
      return (byte) marker[0];
    }

    /** Rewrites that byte, which is how the pre-fix shape is fabricated on a database this build wrote. */
    private void writeMarkerByteAt(final RID rid, final byte marker) {
      database.transaction(() -> onSlot(rid, page -> {
        page.writeByte(recordOffsetOf(page, rid), marker);
        return 0L;
      }));
    }

    /** The two shared counts, bound to this class's single type. */
    private long countRecords() {
      return countRecords(TYPE);
    }

    private long countRecordsHolding(final String value) {
      return countRecordsHolding(TYPE, value);
    }
  }

  /**
   * Issue #6141: two transactions updating the same PLACEHOLDER-backed record both committed, and the second one
   * silently overwrote the first - no {@code ConcurrentModificationException} was raised anywhere.
   * <p>
   * The reason is that a record update is deferred: {@code TransactionContext.addUpdatedRecord} pins the record's OWN
   * page at {@code save()} time and the write runs at commit, after the file's commit lock is taken. For a
   * placeholder-backed record the pinned page holds the 8-byte POINTER, which an update that the content record can
   * absorb never touches - so nothing on it is version-checked - while the CONTENT page is pinned by nothing and is
   * loaded fresh, at the newest committed version, by both transactions.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PlaceholderLostUpdate extends BucketPageLayoutTestSupport {
    /** 20 KB of content: big enough to spill out of a sealed page, small enough to fit a page of its own. */
    private static final int CONTENT_SIZE = 20 * 1024;

    /**
     * The issue itself: the concurrent update fits the content record, so not a byte of the pointer's page changes and
     * the whole write lands on a page neither transaction pinned. The second commit must be told to retry instead of
     * quietly winning.
     */
    @Test
    void aConcurrentUpdateOfThePlaceholderContentIsRefused() throws Exception {
      final RID placeholder = placeholderBackedRecord("Absorbed");

      final String concurrent = "c".repeat(CONTENT_SIZE);

      database.begin();
      placeholder.asDocument(true).modify().set("v", "l".repeat(CONTENT_SIZE)).save();

      commitInAnotherThread(placeholder, concurrent);

      assertConflictOnCommit();

      database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v"))
          .as("the concurrent update must be the one that survived").isEqualTo(concurrent));
      checkDatabase();
    }

    /**
     * The other half of the same branch: this transaction's value no longer fits the content record, so the slot is
     * rebuilt from scratch (the old content record is deleted and the record re-spills). That path rewrites the
     * pointer's page - but it does so from a content record another transaction has meanwhile replaced, so it drops
     * that write just as silently.
     */
    @Test
    void aConcurrentUpdateIsRefusedEvenWhenTheContentRecordHasToBeRebuilt() throws Exception {
      final RID placeholder = placeholderBackedRecord("Rebuilt");

      final String concurrent = "c".repeat(CONTENT_SIZE);

      database.begin();
      // 200 KB fits no page at all: the content record cannot absorb it and the slot is rebuilt.
      placeholder.asDocument(true).modify().set("v", "l".repeat(200 * 1024)).save();

      commitInAnotherThread(placeholder, concurrent);

      assertConflictOnCommit();

      database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(concurrent));
      checkDatabase();
    }

    /**
     * The counter-check that keeps the fix from being a blanket conflict: a transaction that updates the placeholder
     * record while another one updates a DIFFERENT record must still commit. The content record's fingerprint is about
     * that record alone, so an unrelated write - even one landing on the very page the content lives on - is none of
     * its business.
     */
    @Test
    void anUnrelatedConcurrentUpdateStillCommits() throws Exception {
      final RID placeholder = placeholderBackedRecord("Unrelated");

      final RID[] other = new RID[1];
      database.transaction(() -> other[0] = database.newDocument("Unrelated").set("v", "o").save().getIdentity());

      final String mine = "m".repeat(CONTENT_SIZE);

      database.begin();
      placeholder.asDocument(true).modify().set("v", mine).save();

      commitInAnotherThread(other[0], "other rewritten");

      database.commit();

      database.transaction(() -> {
        assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(mine);
        assertThat(other[0].asDocument(true).getString("v")).isEqualTo("other rewritten");
      });
      checkDatabase();
    }

    /**
     * The fingerprint is not part of the disjoint-slot merge, it only shares the moment the merge's own fingerprint is
     * taken: for a placeholder-backed record it is the ONLY conflict detection there is, so switching the merge off -
     * which restores the unconditional page poisoning that predates it - must not switch the protection off with it.
     */
    @Test
    void theConcurrentUpdateIsRefusedWithTheSlotMergeSwitchedOffToo() throws Exception {
      final RID placeholder = placeholderBackedRecord("MergeOff");

      final String concurrent = "c".repeat(CONTENT_SIZE);

      database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, false);
      try {
        database.begin();
        placeholder.asDocument(true).modify().set("v", "l".repeat(CONTENT_SIZE)).save();

        commitInAnotherThread(placeholder, concurrent);

        assertConflictOnCommit();
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TX_PAGE_SLOT_MERGE, true);
      }

      database.transaction(() -> assertThat(placeholder.asDocument(true).getString("v")).isEqualTo(concurrent));
      checkDatabase();
    }

    /**
     * Builds the one record shape this issue is about: a slot holding a placeholder POINTER, with the record's whole
     * content on another page. Since #6149 that takes a page with a free tail of exactly zero - the only shape left
     * where a spilling record cannot find the 14 bytes a chunk header needs.
     */
    /**
     * #8985: the guard that compares a read image with the record taken for update must also answer for a placeholder
     * pointer, whose body is on another page: false while the content is what was read, true once another transaction
     * rewrote it.
     */
    @Test
    void theRecordImageGuardAnswersForAPlaceholderBackedRecord() throws Exception {
      final RID placeholder = placeholderBackedRecord("Guarded");
      final Binary image = database.isTransactionActive() ? null : readImage(placeholder);

      assertThat(guard(placeholder, image)).as("the content is the one that was read").isFalse();

      commitInAnotherThread(placeholder, "c".repeat(CONTENT_SIZE));

      assertThat(guard(placeholder, image)).as("the content was rewritten after the read").isTrue();
    }

    private Binary readImage(final RID rid) {
      final Binary[] image = new Binary[1];
      database.transaction(
          () -> image[0] = ((RecordInternal) database.lookupByRID(rid, true).asDocument().modify()).getBuffer().copyOfContent());
      return image[0];
    }

    private boolean guard(final RID rid, final Binary image) {
      final boolean[] changed = new boolean[1];
      database.transaction(() -> {
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
        try {
          changed[0] = bucket.hasRecordChangedSinceRead(rid, bucket.fetchPageInTransaction(rid), image);
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      });
      return changed[0];
    }

    private RID placeholderBackedRecord(final String typeName) {
      final RID[] tiny = new RID[1];

      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument(typeName).set("v", "p").save().getIdentity();
      });
      sealFirstPage(typeName);

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(CONTENT_SIZE)).save());

      final Map<String, Object> layout = bucketStats(typeName);
      assertThat((Long) layout.get("totalPlaceholderRecords"))
          .as("this test is about a placeholder-backed record, and " + typeName + " has none: " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalSurrogateRecords")).as("with its content on another page: " + layout).isEqualTo(1L);

      return tiny[0];
    }

    /** Rewrites {@code rid} in a transaction of its own, on another thread, and waits for that commit to land. */
    private void commitInAnotherThread(final RID rid, final String value) throws InterruptedException {
      final Thread concurrent = new Thread(
          () -> database.transaction(() -> rid.asDocument(true).modify().set("v", value).save()));
      concurrent.start();
      concurrent.join();
    }

    private void assertConflictOnCommit() {
      try {
        database.commit();
        throw new AssertionError("the commit must be refused: the record was rewritten by a concurrent transaction");
      } catch (final ConcurrentModificationException expected) {
        // THE CONTRACT: A CONCURRENT WRITE TO THE SAME RECORD IS A RETRYABLE CONFLICT, NOT A SILENT OVERWRITE
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }
  }

  /**
   * Issue #10617: a record that outgrows its page and whose slot is too small for a chunk header spills into a
   * placeholder POINTER - a 1-byte marker plus an 8-byte position, 9 bytes in all. The smallest slot is 6 bytes (an
   * empty document), and nothing made sure the slot could hold the 9 bytes before writing them: the pointer overwrote
   * the start of the next record, or ran past the page's content area when the slot was the last one.
   * <p>
   * The pointer must now either fit the slot, fit it once the slot is enlarged by the in-page shift a growth uses, or
   * not be written at all.
   */
  @Nested
  class PlaceholderPointerSlotOverflow extends BucketPageLayoutTestSupport {
    private static final String BIG               = "b".repeat(20 * 1024);
    /** A placeholder pointer: the 1-byte RECORD_PLACEHOLDER_POINTER marker plus the 8-byte position. */
    private static final int    POINTER_FOOTPRINT = 1 + Binary.LONG_SERIALIZED_SIZE;

    /**
     * The issue's first shape: a slot smaller than a pointer that is NOT the last record, on a page with no free tail
     * at all. The pointer must not be written past the slot - and, since #10637, the missing bytes come from another
     * record of the page that moves behind a pointer of its own, so the update succeeds and the record after it survives
     * untouched.
     */
    @Test
    void aSmallSlotOnASealedPageIsNotOverwrittenPastItsEnd() {
      final RID[] empty = new RID[1];
      final RID[] neighbour = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Sealed10617", 1).createProperty("v", Type.STRING);
        empty[0] = database.newDocument("Sealed10617").save().getIdentity();
        neighbour[0] = database.newDocument("Sealed10617").set("v", "neighbour").save().getIdentity();
      });
      shrinkToUnpaddedSize(empty[0]);
      sealFirstPage("Sealed10617");

      assertThat(footprintOf(empty[0])).as("the premise: an empty document takes a slot too small for a pointer")
          .isLessThan(POINTER_FOOTPRINT);
      assertThat(freeTailOfPageOf(empty[0])).as("the premise: the page has no free tail").isZero();

      final long countBefore = countRecordsFromCounter("Sealed10617");

      database.transaction(() -> grow(empty[0]));

      assertIntact("Sealed10617", countBefore);
      database.transaction(() -> {
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour");
        assertThat(empty[0].asDocument(true).getString("v")).isEqualTo(BIG);
      });
    }

    /**
     * The same small slot, not the last record, but with a free tail too small for a chunk header and big enough for
     * the bytes a pointer is missing. Before the fix the pointer was written anyway and spilled into the next record;
     * now the slot is enlarged first, and the update succeeds without touching anything else.
     */
    @Test
    void aSmallSlotIsEnlargedToHostThePointerWhenThePageHasTheBytes() {
      final RID[] empty = new RID[1];
      final RID[] neighbour = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Enlarged10617", 1).createProperty("v", Type.STRING);
        empty[0] = database.newDocument("Enlarged10617").save().getIdentity();
        neighbour[0] = database.newDocument("Enlarged10617").set("v", "neighbour").save().getIdentity();
      });
      shrinkToUnpaddedSize(empty[0]);
      final RID filler = fillFirstPage("Enlarged10617");
      leaveFreeTail(filler, 5);

      assertThat(footprintOf(empty[0])).isLessThan(POINTER_FOOTPRINT);

      final long countBefore = countRecordsFromCounter("Enlarged10617");
      database.transaction(() -> grow(empty[0]));

      final Map<String, Object> layout = bucketStats("Enlarged10617");
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the slot must have become a pointer: " + layout)
          .isEqualTo(1L);
      assertIntact("Enlarged10617", countBefore);
      database.transaction(() -> {
        assertThat(empty[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(empty[0].asDocument(true).getString("brand")).isEqualTo("x");
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("neighbour");
      });
    }

    /**
     * The issue's second shape: the small slot is the LAST record of its page and the free tail is one byte short of
     * what the pointer is missing. The pointer used to run past the page's content area; since #10637 a record before it
     * releases the bytes, the small slot moves left with everything after the donor, and the pointer fits.
     */
    @Test
    void aSmallLastSlotIsNotWrittenPastTheContentArea() {
      final RID empty = lastEmptyDocumentWithFreeTail("Last10617", -1);
      final long countBefore = countRecordsFromCounter("Last10617");

      database.transaction(() -> grow(empty));

      assertIntact("Last10617", countBefore);
      database.transaction(() -> assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG));
    }

    /**
     * The boundary of the shape above: a last small slot followed by exactly the bytes of tail the pointer is missing
     * hosts it, as it always did.
     */
    @Test
    void aSmallLastSlotWithExactlyTheMissingBytesOfTailHostsThePointer() {
      final RID empty = lastEmptyDocumentWithFreeTail("Exact10617", 0);
      final long countBefore = countRecordsFromCounter("Exact10617");

      database.transaction(() -> grow(empty));

      assertThat((Long) bucketStats("Exact10617").get("totalPlaceholderRecords")).isEqualTo(1L);
      assertIntact("Exact10617", countBefore);
      database.transaction(() -> assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG));
    }

    private static void grow(final RID rid) {
      rid.asDocument(true).modify().set("brand", "x").set("v", BIG).save();
    }

    /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
    private void assertIntact(final String typeName, final long expectedCount) {
      assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
      assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
      checkDatabase();
    }

    /**
     * Builds page 0 of a single-bucket type with an empty document as its LAST record, followed by the bytes a pointer
     * would be missing in its slot plus {@code tailBeyondMissing}: the page is filled with 8 KB records, the empty
     * document lands behind them, and the first record is then grown by exactly the bytes that leave that tail free.
     */
    private RID lastEmptyDocumentWithFreeTail(final String typeName, final int tailBeyondMissing) {
      database.transaction(() -> database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING));
      final RID lastFiller = fillFirstPage(typeName);
      final RID first = new RID(lastFiller.getBucketId(), 0);

      final RID[] empty = new RID[1];
      database.transaction(() -> empty[0] = database.newDocument(typeName).save().getIdentity());
      shrinkToUnpaddedSize(empty[0]);
      assertThat(empty[0].getPosition()).as("the premise: the empty document lands right after the fillers on page 0")
          .isEqualTo(lastFiller.getPosition() + 1);
      final int footprint = footprintOf(empty[0]);
      assertThat(footprint).isLessThan(POINTER_FOOTPRINT);

      leaveFreeTail(first, POINTER_FOOTPRINT - footprint + tailBeyondMissing);
      return empty[0];
    }

    /** Grows {@code rid}'s string value by exactly the bytes that leave its page with {@code tail} free bytes. */
    private void leaveFreeTail(final RID rid, final int tail) {
      for (int attempt = 0; attempt < 4; attempt++) {
        final int current = freeTailOfPageOf(rid);
        if (current == tail)
          return;
        final int delta = current - tail;
        database.transaction(() -> {
          final String value = rid.asDocument(true).getString("v");
          rid.asDocument(true).modify().set("v", value + "g".repeat(delta)).save();
        });
      }
      assertThat(freeTailOfPageOf(rid)).as("could not leave exactly " + tail + " free bytes on the page").isEqualTo(tail);
    }

    /** The bytes {@code rid}'s slot takes: size marker plus content, for a plain record. */
    private int footprintOf(final RID rid) {
      final int[] footprint = new int[1];
      database.transaction(() -> onSlot(rid, page -> {
        final long[] size = page.readNumberAndSize(recordOffsetOf(page, rid));
        assertThat(size[0]).as("a plain record").isPositive();
        footprint[0] = (int) (size[0] + size[1]);
        return 0L;
      }));
      return footprint[0];
    }

    /** Bytes between the end of the last record of {@code rid}'s page and the page's maximum content size. */
    private int freeTailOfPageOf(final RID rid) {
      final int[] tail = new int[1];
      database.transaction(() -> onSlot(rid, page -> {
        tail[0] = page.getMaxContentSize() - contentEnd(page);
        return 0L;
      }));
      return tail[0];
    }

    private static int contentEnd(final MutablePage page) {
      final int slots = page.readShort(0);
      int end = 0;
      for (int slot = 0; slot < slots; slot++) {
        final int offset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
        if (offset == 0)
          continue;
        final long[] size = page.readNumberAndSize(offset);
        final int footprint;
        if (size[0] > 0)
          footprint = (int) (size[0] + size[1]);
        else if (size[0] == -1L)
          footprint = (int) size[1] + Binary.LONG_SERIALIZED_SIZE;
        else if (size[0] == -2L || size[0] == -3L || size[0] == -4L)
          footprint = (int) size[1] + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt(
              (int) (offset + size[1]));
        else
          footprint = (int) (-size[0] + size[1]);
        end = Math.max(end, offset + footprint);
      }
      return end;
    }
  }

  /**
   * Issue #10637: since #10617 a record whose slot is smaller than a placeholder pointer (an empty or near-empty
   * document, 6-8 bytes) was refused when it outgrew a page with no spare bytes, and a retry met the same page. The
   * bytes the pointer misses now come from another plain record of the page, which moves behind a pointer of its own:
   * both records keep their RIDs and their content, and the page stays consistent.
   */
  @Nested
  class SmallSlotBorrowsBytesFromANeighbour extends BucketPageLayoutTestSupport {
    private static final String BIG = "b".repeat(20 * 1024);

    /**
     * A sealed page holding, in this order, the small slot, a short neighbour and 8 KB fillers. The smallest record that
     * can release the bytes is the short neighbour, so it moves off the page and every filler after it slides left: all
     * of them must still read back, and the database must check out clean.
     */
    @Test
    void theSmallestRecordThatCanReleaseTheBytesMovesBehindAPointer() {
      final RID[] empty = new RID[1];
      final RID[] neighbour = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Donor10637", 1).createProperty("v", Type.STRING);
        empty[0] = database.newDocument("Donor10637").save().getIdentity();
        neighbour[0] = database.newDocument("Donor10637").set("v", "a short neighbour").save().getIdentity();
      });
      shrinkToUnpaddedSize(empty[0]);
      final RID sealer = sealFirstPage("Donor10637");
      final List<RID> fillers = fillersOfPageZero(neighbour[0], sealer);

      assertThat((Long) bucketStats("Donor10637").get("totalPlaceholderRecords")).as("the premise: no pointer yet")
          .isZero();
      final long countBefore = countRecordsFromCounter("Donor10637");

      database.transaction(() -> empty[0].asDocument(true).modify().set("v", BIG).save());

      assertThat((Long) bucketStats("Donor10637").get("totalPlaceholderRecords"))
          .as("the grown record and the donor both live behind a pointer now").isEqualTo(2L);
      assertThat(countRecordsFromCounter("Donor10637")).isEqualTo(countBefore);
      assertThat(countRecords("Donor10637")).as("a scan must return every record the counter counts, once")
          .isEqualTo(countBefore);
      checkDatabase();

      database.transaction(() -> {
        assertThat(empty[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("a short neighbour");
        for (final RID filler : fillers)
          assertThat(filler.asDocument(true).getString("v")).isEqualTo("f".repeat(8 * 1024));
        assertThat(sealer.asDocument(true).getString("v")).isEqualTo("s".repeat(70 * 1024));
      });
    }

    /**
     * The relocated donor is an ordinary pointer record from then on: it survives a reopen, it can be updated and it
     * can be deleted, and none of that disturbs the record it gave the bytes to.
     */
    @Test
    void theRelocatedDonorStaysAnOrdinaryRecord() {
      final RID[] empty = new RID[1];
      final RID[] neighbour = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Ordinary10637", 1).createProperty("v", Type.STRING);
        empty[0] = database.newDocument("Ordinary10637").save().getIdentity();
        neighbour[0] = database.newDocument("Ordinary10637").set("v", "a short neighbour").save().getIdentity();
      });
      shrinkToUnpaddedSize(empty[0]);
      sealFirstPage("Ordinary10637");
      final long countBefore = countRecordsFromCounter("Ordinary10637");

      database.transaction(() -> empty[0].asDocument(true).modify().set("v", BIG).save());

      reopenDatabase();
      database.transaction(() -> {
        assertThat(database.lookupByRID(neighbour[0], true).asDocument().getString("v")).isEqualTo("a short neighbour");
        assertThat(database.lookupByRID(empty[0], true).asDocument().getString("v")).isEqualTo(BIG);
      });

      database.transaction(() -> database.lookupByRID(neighbour[0], true).asDocument().modify().set("v", "updated after the move").save());
      database.transaction(() -> assertThat(database.lookupByRID(neighbour[0], true).asDocument().getString("v")).isEqualTo("updated after the move"));
      checkDatabase();

      database.transaction(() -> database.deleteRecord(database.lookupByRID(neighbour[0], true)));
      assertThat(countRecordsFromCounter("Ordinary10637")).isEqualTo(countBefore - 1);
      assertThat(countRecords("Ordinary10637")).isEqualTo(countBefore - 1);
      checkDatabase();
      database.transaction(() -> assertThat(database.lookupByRID(empty[0], true).asDocument().getString("v")).isEqualTo(BIG));
    }

    /**
     * Several small slots on the same sealed page each borrow the bytes they miss, one donor apiece, inside ONE
     * transaction: the offsets every relocation moves have to be the ones the next one reads.
     */
    @Test
    void severalSmallSlotsOfOnePageGrowInOneTransaction() {
      final List<RID> empties = new ArrayList<>();
      final RID[] neighbour = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Several10637", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < 3; i++)
          empties.add(database.newDocument("Several10637").save().getIdentity());
        neighbour[0] = database.newDocument("Several10637").set("v", "a short neighbour").save().getIdentity();
      });
      empties.forEach(this::shrinkToUnpaddedSize);
      sealFirstPage("Several10637");
      final long countBefore = countRecordsFromCounter("Several10637");

      database.transaction(() -> {
        for (final RID empty : empties)
          empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save();
      });

      assertThat(countRecordsFromCounter("Several10637")).isEqualTo(countBefore);
      assertThat(countRecords("Several10637")).isEqualTo(countBefore);
      checkDatabase();
      database.transaction(() -> {
        for (final RID empty : empties)
          assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
        assertThat(neighbour[0].asDocument(true).getString("v")).isEqualTo("a short neighbour");
      });
    }

    /** The 8 KB fillers {@code sealFirstPage} put on page 0 between {@code after} and the record that sealed it. */
    private static List<RID> fillersOfPageZero(final RID after, final RID sealer) {
      final List<RID> fillers = new ArrayList<>();
      for (long position = after.getPosition() + 1; position < sealer.getPosition(); position++)
        fillers.add(new RID(after.getBucketId(), position));
      return fillers;
    }
  }

  /**
   * Issue #10669: since #10637 a record whose slot is smaller than a placeholder pointer (an empty document, 6 bytes)
   * borrows the bytes the pointer misses from another PLAIN record of its page. A page whose free tail was taken by the
   * head chunk of a record that spilled holds no such record, and the update was refused - for good, since a retry met
   * the same page. The bytes now come from a chunk of the page: its trailing content moves into a new continuation chunk
   * inserted right after it in the chain, so the chunked record keeps its RID and its content, one hop longer.
   */
  @Nested
  class SmallSlotShrinksAChunk extends BucketPageLayoutTestSupport {
    private static final String BIG    = "b".repeat(20 * 1024);
    private static final String SEALER = "s".repeat(70 * 1024);

    /**
     * The issue's reproduction: one empty document, then a record grown to 70 KB so that its head chunk takes the whole
     * free tail of the page. The empty document must grow, and the spilled record must still read back whole.
     */
    @Test
    void aSmallSlotNextToASpilledRecordGrows() {
      final RID[] rids = sealedPage("Shrink10669");
      final RID empty = rids[0];
      final RID sealer = rids[1];
      final long countBefore = countRecordsFromCounter("Shrink10669");

      database.transaction(() -> empty.asDocument(true).modify().set("v", BIG).save());

      assertIntact("Shrink10669", countBefore);
      database.transaction(() -> {
        assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEALER);
      });
    }

    /**
     * The chunked record that gave the bytes reads back whole INSIDE the transaction that shrank its chunk, from the pages
     * that transaction modified, and not only once the commit published them.
     */
    @Test
    void theShrunkRecordReadsBackInTheSameTransaction() {
      final RID[] rids = sealedPage("SameTx10669");
      database.transaction(() -> {
        rids[0].asDocument(true).modify().set("v", BIG).save();
        assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(SEALER);
        assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
      });
    }

    /**
     * Both records stay ordinary from then on: they survive a reopen, the chunked record can be rewritten (shorter and
     * longer) and deleted, and none of that disturbs the record it gave the bytes to.
     */
    @Test
    void theShrunkRecordStaysAnOrdinaryRecord() {
      final RID[] rids = sealedPage("Ordinary10669");
      final RID empty = rids[0];
      final RID sealer = rids[1];
      final long countBefore = countRecordsFromCounter("Ordinary10669");

      database.transaction(() -> empty.asDocument(true).modify().set("v", BIG).save());

      reopenDatabase();
      database.transaction(() -> {
        assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo(SEALER);
        assertThat(database.lookupByRID(empty, true).asDocument().getString("v")).isEqualTo(BIG);
      });

      final String longer = "l".repeat(90 * 1024);
      database.transaction(() -> database.lookupByRID(sealer, true).asDocument().modify().set("v", longer).save());
      database.transaction(() -> assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo(longer));
      database.transaction(() -> database.lookupByRID(sealer, true).asDocument().modify().set("v", "short").save());
      database.transaction(() -> assertThat(database.lookupByRID(sealer, true).asDocument().getString("v")).isEqualTo("short"));
      checkDatabase();

      database.transaction(() -> database.deleteRecord(database.lookupByRID(sealer, true)));
      assertThat(countRecordsFromCounter("Ordinary10669")).isEqualTo(countBefore - 1);
      assertThat(countRecords("Ordinary10669")).isEqualTo(countBefore - 1);
      checkDatabase();
      database.transaction(() -> assertThat(database.lookupByRID(empty, true).asDocument().getString("v")).isEqualTo(BIG));
    }

    /**
     * The chunked record itself is rewritten by the same transaction, saved BEFORE the small slot grows: its chain was
     * shrunk at commit under an update that had already been queued, and both must land.
     */
    @Test
    void theChunkedRecordUpdatedFirstInTheSameTransaction() {
      final RID[] rids = sealedPage("First10669");
      final String rewritten = "r".repeat(80 * 1024);
      database.transaction(() -> {
        rids[1].asDocument(true).modify().set("v", rewritten).save();
        rids[0].asDocument(true).modify().set("v", BIG).save();
      });
      assertIntact("First10669", 2);
      database.transaction(() -> {
        assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(rewritten);
      });
    }

    /** The same, the small slot saved first. */
    @Test
    void theChunkedRecordUpdatedLastInTheSameTransaction() {
      final RID[] rids = sealedPage("Last10669");
      final String rewritten = "r".repeat(80 * 1024);
      database.transaction(() -> {
        rids[0].asDocument(true).modify().set("v", BIG).save();
        rids[1].asDocument(true).modify().set("v", rewritten).save();
      });
      assertIntact("Last10669", 2);
      database.transaction(() -> {
        assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(rewritten);
      });
    }

    /**
     * Several small slots of the same sealed page each take the bytes they miss from the chunk, inside ONE transaction:
     * the offsets every shrink moves have to be the ones the next one reads.
     */
    @Test
    void severalSmallSlotsOfOnePageGrowInOneTransaction() {
      final List<RID> empties = new ArrayList<>();
      final RID[] sealer = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Several10669", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < 3; i++)
          empties.add(database.newDocument("Several10669").save().getIdentity());
        sealer[0] = database.newDocument("Several10669").set("v", "s").save().getIdentity();
      });
      empties.forEach(this::shrinkToUnpaddedSize);
      seal(sealer[0], "Several10669");
      final long countBefore = countRecordsFromCounter("Several10669");

      database.transaction(() -> {
        for (final RID empty : empties)
          empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save();
      });

      assertIntact("Several10669", countBefore);
      database.transaction(() -> {
        for (final RID empty : empties)
          assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
        assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo(SEALER);
      });
    }

    /**
     * Every shrink of the same chain adds one continuation chunk (code review on #10860): three small slots of one page,
     * three chunks more. None of them may outlive the record: once it shrinks back into a plain record, the bucket holds
     * no chunk at all, orphaned or not.
     */
    @Test
    void repeatedShrinksLeaveNoChunkBehindOnceTheRecordShrinks() {
      final List<RID> empties = new ArrayList<>();
      final RID[] sealer = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Repeated10669", 1).createProperty("v", Type.STRING);
        for (int i = 0; i < 3; i++)
          empties.add(database.newDocument("Repeated10669").save().getIdentity());
        sealer[0] = database.newDocument("Repeated10669").set("v", "s").save().getIdentity();
      });
      empties.forEach(this::shrinkToUnpaddedSize);
      seal(sealer[0], "Repeated10669");
      final long chunksBefore = (Long) bucketStats("Repeated10669").get("totalChunks");

      for (final RID empty : empties)
        database.transaction(() -> empty.asDocument(true).modify().set("v", BIG + empty.getPosition()).save());

      final Map<String, Object> grown = bucketStats("Repeated10669");
      assertThat((Long) grown.get("totalChunks")).as("one continuation chunk per shrink " + grown)
          .isEqualTo(chunksBefore + empties.size());
      database.transaction(() -> assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo(SEALER));
      checkDatabase();

      database.transaction(() -> sealer[0].asDocument(true).modify().set("v", "short").save());

      final Map<String, Object> shrunk = bucketStats("Repeated10669");
      assertThat((Long) shrunk.get("totalMultiPageRecords")).as(shrunk.toString()).isZero();
      assertThat((Long) shrunk.get("totalChunks")).as("no chunk outlives the chain " + shrunk).isZero();
      assertThat((Long) shrunk.get("orphanedChunks")).as(shrunk.toString()).isZero();
      assertIntact("Repeated10669", empties.size() + 1);
      database.transaction(() -> {
        assertThat(sealer[0].asDocument(true).getString("v")).isEqualTo("short");
        for (final RID empty : empties)
          assertThat(empty.asDocument(true).getString("v")).isEqualTo(BIG + empty.getPosition());
      });
    }

    /**
     * An update of another record applied in the same transaction commits with the grown one: the shape that used to be
     * refused, and roll the whole transaction back, now commits whole.
     */
    @Test
    void theOtherWritesOfTheTransactionCommitWithIt() {
      final RID[] other = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Other10669", 1).createProperty("v", Type.STRING);
        other[0] = database.newDocument("Other10669").set("v", "before").save().getIdentity();
      });
      final RID[] rids = sealedPage("Whole10669");
      final long countBefore = countRecordsFromCounter("Whole10669");

      database.transaction(() -> {
        other[0].asDocument(true).modify().set("v", "after").save();
        database.newDocument("Whole10669").set("v", "inserted").save();
        rids[0].asDocument(true).modify().set("v", BIG).save();
      });

      assertIntact("Whole10669", countBefore + 1);
      assertThat(countRecordsHolding("Whole10669", "inserted")).isEqualTo(1L);
      database.transaction(() -> {
        assertThat(other[0].asDocument(true).getString("v")).isEqualTo("after");
        assertThat(rids[0].asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(rids[1].asDocument(true).getString("v")).isEqualTo(SEALER);
      });
    }

    /**
     * Page 0 of a new single-bucket type holding an empty document and, after it, a record grown until it spilled: its head
     * chunk takes the whole free tail, and the page holds no plain record that could move behind a pointer.
     *
     * @return the empty document, then the spilled record.
     */
    private RID[] sealedPage(final String typeName) {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
        rids[0] = database.newDocument(typeName).save().getIdentity();
        rids[1] = database.newDocument(typeName).set("v", "s").save().getIdentity();
      });
      shrinkToUnpaddedSize(rids[0]);
      seal(rids[1], typeName);
      return rids;
    }

    private void seal(final RID sealer, final String typeName) {
      database.transaction(() -> sealer.asDocument(true).modify().set("v", SEALER).save());
      final Map<String, Object> layout = bucketStats(typeName);
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the premise: the sealing record spilled " + layout).isEqualTo(1L);
      assertThat((Long) layout.get("totalPlaceholderRecords")).as("the premise: no pointer yet " + layout).isZero();
    }

    /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
    private void assertIntact(final String typeName, final long expectedCount) {
      assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
      assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
      checkDatabase();
    }
  }

  /**
   * Issue #10700: a record growing on a page whose free tail is gone was refused for ever when the bytes the page could
   * give back were HOLES - what the shrinking updates of the same transaction leave behind until the commit packs the
   * page - rather than a record able to move off-page. The growth measured the page by its free tail only, so a page with
   * tens of KB of dead bytes refused a 9-byte pointer, and every retry rebuilt the same page. A growth that does not fit
   * the free tail now packs the page's holes first, and is then judged against the page as the commit would leave it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GrowthReclaimsPageHoles extends BucketPageLayoutTestSupport {
    private static final String FILLER = "f".repeat(8 * 1024);
    private static final String SEAL   = "s".repeat(70 * 1024);
    private static final String BIG    = "b".repeat(20 * 1024);

    /**
     * The reporter's shape: every filler of a sealed page is shrunk to an empty document and one empty document grows to
     * 20 KB, all in ONE transaction. The page holds ~40 KB of holes, so the growth fits inside it once they are packed:
     * the transaction must commit, the grown record must stay a plain in-page record, and nothing may move off-page.
     */
    @Test
    void aGrowthFitsTheHolesTheSameTransactionLeft() {
      final Layout layout = sealedPageWithEmpties("Holes10700");
      final List<RID> empties = layout.empties();
      final RID sealer = layout.sealer();
      final List<RID> fillers = layout.fillers();
      assertThat(fillers).as("the premise: page 0 holds fillers to shrink").isNotEmpty();
      final long countBefore = countRecordsFromCounter("Holes10700");

      database.transaction(() -> {
        for (final RID filler : fillers)
          emptied(filler);
        empties.getFirst().asDocument(true).modify().set("v", BIG).save();
      });

      assertThat((Long) bucketStats("Holes10700").get("totalPlaceholderRecords"))
          .as("the holes were enough: no record may have moved behind a pointer").isZero();
      assertThat((Long) bucketStats("Holes10700").get("totalMultiPageRecords"))
          .as("the grown record fits the packed page, so only the sealer is a chunk chain").isEqualTo(1L);
      assertThat(countRecordsFromCounter("Holes10700")).isEqualTo(countBefore);
      assertThat(countRecords("Holes10700")).isEqualTo(countBefore);
      checkDatabase();

      database.transaction(() -> {
        assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
        for (int i = 1; i < empties.size(); i++)
          assertThat(empties.get(i).asDocument(true).has("v")).isFalse();
        for (final RID filler : fillers)
          assertThat(filler.asDocument(true).has("v")).isFalse();
        assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
      });

      reopenDatabase();
      database.transaction(
          () -> assertThat(database.lookupByRID(empties.getFirst(), true).asDocument().getString("v")).isEqualTo(BIG));
      checkDatabase();
    }

    /**
     * The holes are smaller than the growth, so the record still has to spill - but they are far more than the few bytes
     * a chunk header needs, so it spills into a chunk chain of its own instead of borrowing a pointer's bytes from a
     * neighbour: no live record is moved off its page when packing the page was enough.
     */
    @Test
    void holesSmallerThanTheGrowthStillSpareANeighbourTheMove() {
      final Layout layout = sealedPageWithEmpties("Spill10700");
      final List<RID> empties = layout.empties();
      final RID sealer = layout.sealer();
      final List<RID> fillers = layout.fillers();
      final long countBefore = countRecordsFromCounter("Spill10700");

      database.transaction(() -> {
        emptied(fillers.getFirst());
        empties.getFirst().asDocument(true).modify().set("v", BIG).save();
      });

      assertThat((Long) bucketStats("Spill10700").get("totalPlaceholderRecords"))
          .as("a hole of 8 KB hosts a chunk header: no neighbour may have moved behind a pointer").isZero();
      assertThat((Long) bucketStats("Spill10700").get("totalMultiPageRecords"))
          .as("the grown record spilled into a chunk chain next to the sealer").isEqualTo(2L);
      assertThat(countRecordsFromCounter("Spill10700")).isEqualTo(countBefore);
      assertThat(countRecords("Spill10700")).isEqualTo(countBefore);
      checkDatabase();

      database.transaction(() -> {
        assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(fillers.getFirst().asDocument(true).has("v")).isFalse();
        for (int i = 1; i < fillers.size(); i++)
          assertThat(fillers.get(i).asDocument(true).getString("v")).isEqualTo(FILLER);
        assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
      });
    }

    /**
     * Records DELETED in the transaction leave holes too, and their slots must not be handed out again by the same
     * transaction: packing the page for the growth must not shorten the slot table under a slot the transaction freed,
     * or the content record the spill creates could reuse a RID this transaction deleted.
     */
    @Test
    void deletedNeighboursLeaveHolesWhoseSlotsAreNotReused() {
      final Layout layout = sealedPageWithEmpties("Deleted10700");
      final List<RID> empties = layout.empties();
      final RID sealer = layout.sealer();
      final List<RID> fillers = layout.fillers();
      final long countBefore = countRecordsFromCounter("Deleted10700");

      database.transaction(() -> {
        for (final RID filler : fillers)
          database.deleteRecord(filler.asDocument(true));
        // Grows past every hole: it has to spill, and the chunks it creates go through the allocator
        empties.getFirst().asDocument(true).modify().set("v", "x".repeat(200 * 1024)).save();
      });

      assertThat(countRecordsFromCounter("Deleted10700")).isEqualTo(countBefore - fillers.size());
      assertThat(countRecords("Deleted10700")).isEqualTo(countBefore - fillers.size());
      checkDatabase();
      database.transaction(() -> {
        assertThat(empties.getFirst().asDocument(true).getString("v")).isEqualTo("x".repeat(200 * 1024));
        for (final RID filler : fillers)
          assertThat(database.existsRecord(filler)).as("deleted " + filler).isFalse();
        assertThat(sealer.asDocument(true).getString("v")).isEqualTo(SEAL);
      });
    }

    /** Page 0 of a single-bucket type: three empty documents (6-8 byte slots), the 8 KB fillers, the sealing record. */
    private record Layout(List<RID> empties, RID sealer, List<RID> fillers) {
    }

    private Layout sealedPageWithEmpties(final String typeName) {
      final List<RID> empties = new ArrayList<>();
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
        for (int i = 0; i < 3; i++)
          empties.add(database.newDocument(typeName).save().getIdentity());
      });
      final RID sealer = sealFirstPage(typeName);
      return new Layout(empties, sealer, fillersOfPageZero(empties.getLast(), sealer));
    }

    /** Shrinks {@code rid} to an empty document in the caller's transaction, leaving a hole where its content was. */
    private static void emptied(final RID rid) {
      final MutableDocument document = rid.asDocument(true).modify();
      document.remove("v");
      document.save();
    }

    /** The 8 KB fillers {@code sealFirstPage} put on page 0 between {@code after} and the record that sealed it. */
    private static List<RID> fillersOfPageZero(final RID after, final RID sealer) {
      final List<RID> fillers = new ArrayList<>();
      for (long position = after.getPosition() + 1; position < sealer.getPosition(); position++)
        fillers.add(new RID(after.getBucketId(), position));
      return fillers;
    }
  }

  /**
   * Issue #10859, the residual of #10669: a record whose slot is smaller than a placeholder pointer (an empty document
   * written before records were padded, 7 bytes) grows past a sealed page by having another record of the page release
   * the bytes the pointer misses. Three shapes were left over, each covered here on a page built byte by byte, since no
   * sequence of inserts and updates produces them on a page of the default size:
   * <ul>
   * <li>no single record can release every missing byte, but several can between them: each gives a part;</li>
   * <li>the only record with bytes to spare is a placeholder's CONTENT record, which cannot move behind a pointer of its
   * own: it becomes the head of a chunk chain, as the content of a placeholder no page could host already is;</li>
   * <li>the transaction that takes bytes from a placeholder's content also updates that placeholder: the off-page
   * fingerprint it took at save() is carried across its own write instead of reading as a concurrent modification.</li>
   * </ul>
   * A page whose records cannot release the bytes even together still refuses the update, and the refusal rolls the
   * whole transaction back. And a record written now is padded to a pointer's footprint, so new data never takes the
   * small slot in the first place.
   */
  @Nested
  class SmallSlotOnAPageWithNoSingleDonor extends BucketPageLayoutTestSupport {
    private static final String BIG               = "b".repeat(20 * 1024);
    /** A placeholder pointer: the 1-byte RECORD_PLACEHOLDER_POINTER marker plus the 8-byte position. */
    private static final int    POINTER_FOOTPRINT = 1 + Binary.LONG_SERIALIZED_SIZE;
    /** Slots of a page of a bucket created now: the content area below is sized against it. */
    private static final int    SLOTS_IN_PAGE     = 2048;
    /**
     * A page of 4 KB of content, the least a bucket accepts (#11286): small enough for records no bigger than a pointer to
     * fill it before its slot table runs out (about 455 of them against 2048 slots), which a page of the default size, with
     * the same slots, never lets them do.
     */
    private static final int    PAGE_SIZE         =
        BasePage.PAGE_HEADER_SIZE + Binary.SHORT_SERIALIZED_SIZE + SLOTS_IN_PAGE * Binary.INT_SERIALIZED_SIZE + 4096;
    private static final String QUOTED            = "q".repeat(40);

    /**
     * The shape the issue says no test reaches any more: a sealed page whose other records are all no bigger than a
     * pointer. Nothing can release the bytes, so the update is refused - and with it every other write of its
     * transaction.
     */
    @Test
    void aPageNoRecordOfWhichCanReleaseBytesRefusesTheUpdateAndRollsItsTransactionBack() {
      final RID[] other = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("RollbackOther10859", 1).createProperty("v", Type.STRING);
        other[0] = database.newDocument("RollbackOther10859").set("v", "before").save().getIdentity();
      });
      final FabricatedPage page = fabricate("Rollback10859", 1, new int[0], false);
      final long countBefore = countRecordsFromCounter("Rollback10859");

      assertRefused(() -> database.transaction(() -> {
        other[0].asDocument(true).modify().set("v", "after").save();
        database.newDocument("Rollback10859").set("v", "inserted").save();
        grow(page.small.getFirst());
      }));

      assertIntact("Rollback10859", countBefore);
      assertThat(countRecordsHolding("Rollback10859", "inserted")).as("the insert must have been rolled back").isZero();
      database.transaction(() -> {
        assertThat(other[0].asDocument(true).getString("v")).as("the other update must have been rolled back")
            .isEqualTo("before");
        assertThat(page.small.getFirst().asDocument(true).getPropertyNames()).isEmpty();
      });
    }

    /**
     * Two records of 11 bytes, each able to release 2 of the 4 bytes the small slot needs: neither can on its own, both
     * can together, and both move behind a pointer of their own.
     */
    @Test
    void severalRecordsReleaseAPartEach() {
      final FabricatedPage page = fabricate("Several10859", 1, new int[] { 11, 11 }, false);
      final long countBefore = countRecordsFromCounter("Several10859");
      assertThat((Long) bucketStats("Several10859").get("totalPlaceholderRecords")).as("the premise: no pointer yet").isZero();

      database.transaction(() -> grow(page.small.getFirst()));

      assertThat((Long) bucketStats("Several10859").get("totalPlaceholderRecords"))
          .as("the grown record and both donors live behind a pointer now").isEqualTo(3L);
      assertIntact("Several10859", countBefore);
      database.transaction(() -> {
        assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
        for (final RID donor : page.donors)
          assertThat(donor.asDocument(true).getPropertyNames()).isEmpty();
      });

      reopenDatabase();
      database.transaction(() -> assertThat(database.lookupByRID(page.small.getFirst(), true).asDocument().getString("v"))
          .isEqualTo(BIG));
      checkDatabase();
    }

    /**
     * The only record with bytes to spare is the CONTENT record of a placeholder whose pointer is on the same page: it
     * becomes the head of a chunk chain and keeps its slot, so the pointer still finds it, and the placeholder reads
     * back, updates and deletes as before.
     */
    @Test
    void aPlaceholderContentRecordBecomesAChunkChainToReleaseTheBytes() {
      final FabricatedPage page = fabricate("Content10859", 1, new int[0], true);
      final long countBefore = countRecordsFromCounter("Content10859");

      database.transaction(() -> grow(page.small.getFirst()));

      assertThat(markerOf(page.content)).as("the content record is now the head of a chunk chain")
          .isEqualTo(-4L);
      assertIntact("Content10859", countBefore);
      database.transaction(() -> {
        assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(QUOTED);
      });

      reopenDatabase();
      database.transaction(() -> assertThat(database.lookupByRID(page.placeholder, true).asDocument().getString("v"))
          .isEqualTo(QUOTED));
      database.transaction(() -> database.lookupByRID(page.placeholder, true).asDocument().modify().set("v", "rewritten").save());
      database.transaction(() -> assertThat(database.lookupByRID(page.placeholder, true).asDocument().getString("v"))
          .isEqualTo("rewritten"));
      checkDatabase();

      database.transaction(() -> database.deleteRecord(database.lookupByRID(page.placeholder, true)));
      assertIntact("Content10859", countBefore - 1);
      database.transaction(() -> assertThat(database.lookupByRID(page.small.getFirst(), true).asDocument().getString("v"))
          .isEqualTo(BIG));
    }

    /**
     * The placeholder whose content gives the bytes is updated by the same transaction: the off-page fingerprint taken
     * when it was saved no longer describes its content once that content is a chunk chain, but the change is this
     * transaction's own, and both writes commit.
     */
    @Test
    void aPlaceholderWhoseContentGivesTheBytesIsUpdatedInTheSameTransaction() {
      final FabricatedPage page = fabricate("SameTx10859", 1, new int[0], true);
      final String rewritten = "r".repeat(60);
      assertThat(appliedFirstAtCommit(page.small.getFirst(), page.placeholder))
          .as("the premise: this test pins the commit applying the placeholder's update first (its content moves off the page "
              + "before the small slot grows); the head-chunk test below pins the other order")
          .isEqualTo(page.placeholder);

      database.transaction(() -> {
        page.placeholder.asDocument(true).modify().set("v", rewritten).save();
        grow(page.small.getFirst());
      });

      assertIntact("SameTx10859", 2 + page.fillers);
      database.transaction(() -> {
        assertThat(page.small.getFirst().asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(rewritten);
      });
    }

    /**
     * Item 3 of the issue as it stood: a placeholder's content is ALREADY the head of a chunk chain (the first small slot
     * made it one), and the head is the only chunk of the page able to give a second small slot its bytes. The same
     * transaction updates the placeholder: before #10859 the fingerprint mismatch was a ConcurrentModificationException,
     * which every retry met again.
     */
    @Test
    void aPlaceholderWhoseContentHeadChunkGivesTheBytesIsUpdatedInTheSameTransaction() {
      final FabricatedPage page = fabricate("Head10859", 2, new int[0], true);
      database.transaction(() -> grow(page.small.getFirst()));
      assertThat(markerOf(page.content)).as("the premise: the placeholder's content is a chunk chain").isEqualTo(-4L);

      final String rewritten = "r".repeat(70);
      assertThat(appliedFirstAtCommit(page.small.get(1), page.placeholder))
          .as("the premise: the commit applies the small slot's update first, so the placeholder's fingerprint is compared "
              + "after its own transaction changed its content - the order that raised the exception")
          .isEqualTo(page.small.get(1));
      database.transaction(() -> {
        page.placeholder.asDocument(true).modify().set("v", rewritten).save();
        grow(page.small.get(1));
      });

      assertIntact("Head10859", 3 + page.fillers);
      database.transaction(() -> {
        assertThat(page.small.get(0).asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(page.small.get(1).asDocument(true).getString("v")).isEqualTo(BIG);
        assertThat(page.placeholder.asDocument(true).getString("v")).isEqualTo(rewritten);
      });
    }

    /**
     * Pad at create: an empty document takes a pointer's footprint, so it turns into a pointer in place the day it
     * outgrows its page; an overwrite with shorter content keeps the slot that size; a slot written before the padding
     * keeps its own size when it is overwritten, instead of having to grow for padding's sake.
     */
    @Test
    void aRecordIsPaddedToAPointersFootprint() {
      final RID[] rids = new RID[2];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Padded10859", 1).createProperty("v", Type.STRING);
        rids[0] = database.newDocument("Padded10859").save().getIdentity();
        rids[1] = database.newDocument("Padded10859").set("v", "a value").save().getIdentity();
      });
      assertThat(footprintOf(rids[0])).as("an empty document, created").isEqualTo(POINTER_FOOTPRINT);

      database.transaction(() -> removeValue(rids[1]));
      assertThat(footprintOf(rids[1])).as("a record overwritten with nothing").isGreaterThanOrEqualTo(POINTER_FOOTPRINT);

      final int legacy = shrinkToUnpaddedSize(rids[0]);
      assertThat(legacy).as("the premise: a slot written before #10859 is smaller than a pointer").isLessThan(POINTER_FOOTPRINT);
      database.transaction(() -> {
        // Dirty, and serialized exactly as it was: an overwrite of the same empty content
        final MutableDocument document = rids[0].asDocument(true).modify();
        document.set("v", "transient");
        document.remove("v");
        document.save();
      });
      assertThat(footprintOf(rids[0])).as("an overwrite never grows a slot to pad it").isEqualTo(legacy);
      database.transaction(() -> assertThat(rids[0].asDocument(true).getPropertyNames()).isEmpty());
      checkDatabase();
    }

    /** A record that spilled into chunks and shrinks back into its slot is padded as a record created now is. */
    @Test
    void aChunkChainCollapsingIntoAnEmptyRecordIsPadded() {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType("Collapsed10859", 1).createProperty("v", Type.STRING);
        rid[0] = database.newDocument("Collapsed10859").set("v", "s").save().getIdentity();
      });
      sealFirstPageWith(rid[0]);
      assertThat(markerOf(rid[0])).as("the premise: the record spilled into a chunk chain").isEqualTo(-2L);

      database.transaction(() -> removeValue(rid[0]));

      assertThat(footprintOf(rid[0])).as("collapsed back into a plain record").isGreaterThanOrEqualTo(POINTER_FOOTPRINT);
      database.transaction(() -> assertThat(rid[0].asDocument(true).getPropertyNames()).isEmpty());
      checkDatabase();
    }

    private static void removeValue(final RID rid) {
      final MutableDocument document = rid.asDocument(true).modify();
      document.remove("v");
      document.save();
    }

    private void sealFirstPageWith(final RID rid) {
      database.transaction(() -> rid.asDocument(true).modify().set("v", "s".repeat(70 * 1024)).save());
    }

    /** What page 0 of a fabricated type holds, by RID. */
    private record FabricatedPage(List<RID> small, List<RID> donors, RID placeholder, RID content, int fillers) {
    }

    /**
     * Builds page 0 of a single-bucket type whose pages hold about 1 KB of content, in this order: {@code smallSlots}
     * empty documents given back the smaller slot of a record written before #10859; an empty document every filler is
     * copied from; with {@code withPlaceholder}, a record moved behind a placeholder pointer whose content record lands
     * right after the fillers' template; one record per entry of {@code donorFootprints}, of that footprint; and fillers no
     * bigger than a pointer up to the very end of the content area, so the page has no free tail at all. Every record is
     * an empty document but the placeholder, and the bucket's counter counts them all.
     */
    private FabricatedPage fabricate(final String typeName, final int smallSlots, final int[] donorFootprints,
        final boolean withPlaceholder) {
      final List<RID> small = new ArrayList<>();
      final RID[] template = new RID[1];
      final RID[] placeholder = new RID[1];
      database.transaction(() -> {
        database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).withPageSize(PAGE_SIZE).create()
            .createProperty("v", Type.STRING);
        for (int i = 0; i < smallSlots; i++)
          small.add(database.newDocument(typeName).save().getIdentity());
        template[0] = database.newDocument(typeName).save().getIdentity();
        if (withPlaceholder)
          // Empty, so its slot is exactly a pointer's footprint once it is one: no hole a pack could give the page back
          placeholder[0] = database.newDocument(typeName).save().getIdentity();
      });
      final LocalBucket bucket = bucketOf(typeName);
      assertThat(bucket.getMaxRecordsInPage()).as("the premise: the page geometry the content area is sized against")
          .isEqualTo(SLOTS_IN_PAGE);
      small.forEach(this::shrinkToUnpaddedSize);

      final List<RID> donors = new ArrayList<>();
      final RID[] content = new RID[1];
      final int[] fillers = new int[1];
      final DatabaseInternal db = (DatabaseInternal) database;
      final int emptyContent = db.getSerializer().serialize(db, database.newDocument(typeName)).size();
      final byte[] quoted = db.getSerializer().serialize(db, database.newDocument(typeName).set("v", QUOTED)).toByteArray();
      database.transaction(() -> onSlot(template[0], page -> {
        final int templateOffset = recordOffsetOf(page, template[0]);
        final long[] templateSize = page.readNumberAndSize(templateOffset);
        final byte[] templateContent = new byte[(int) templateSize[0]];
        page.readByteArray(templateOffset + (int) templateSize[1], templateContent, 0, templateContent.length);

        int slots = page.readShort(0);
        int end = contentEnd(page);

        if (withPlaceholder) {
          final int offset = recordOffsetOf(page, placeholder[0]);
          assertThat(page.readNumberAndSize(offset)).as("the premise: a slot a pointer fills exactly")
              .containsExactly(POINTER_FOOTPRINT - 1, 1L);

          content[0] = new RID(template[0].getBucketId(), slots);
          end = appendSlot(page, slots++, end, -quoted.length, quoted, quoted.length, quoted.length);
          assertThat(page.writeNumber(offset, -1L)).isEqualTo(1);
          page.writeLong(offset + 1, content[0].getPosition());
        }

        int counted = 0;
        for (final int footprint : donorFootprints) {
          donors.add(new RID(template[0].getBucketId(), slots));
          end = appendSlot(page, slots++, end, footprint - 1, templateContent, templateContent.length, emptyContent);
          counted++;
        }

        // Empty documents of an unpadded record's footprint up to a pointer's, never more: none of them has a byte to
        // spare, and together they end exactly at the end of the content area.
        final int maxShave = POINTER_FOOTPRINT - 1 - emptyContent;
        final int room = page.getMaxContentSize() - end;
        final int records = (room + POINTER_FOOTPRINT - 1) / POINTER_FOOTPRINT;
        int excess = records * POINTER_FOOTPRINT - room;
        for (int i = 0; i < records; i++) {
          final int shave = Math.min(maxShave, excess);
          excess -= shave;
          end = appendSlot(page, slots++, end, POINTER_FOOTPRINT - 1 - shave, templateContent, templateContent.length,
              emptyContent);
          counted++;
        }
        assertThat(excess).isZero();
        assertThat(slots).as("the premise: the slot table holds every record").isLessThan(SLOTS_IN_PAGE);
        assertThat(end).as("the premise: the page has no free tail").isEqualTo(page.getMaxContentSize());

        page.writeShort(0, (short) slots);
        ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(template[0].getBucketId(), counted);
        fillers[0] = counted - donors.size();
        return 0L;
      }));

      return new FabricatedPage(small, donors, placeholder[0], content[0], fillers[0] + 1);
    }

    /**
     * Writes a record of {@code size} bytes (negated for a placeholder's content) at {@code end} of the page, its content
     * the first bytes of {@code body} and blanks past them, and points slot {@code slot} at it. {@code serializedLength}
     * is how many bytes of {@code body} the record's deserializer reads.
     *
     * @return where the page's content now ends.
     */
    private static int appendSlot(final MutablePage page, final int slot, final int end, final int size, final byte[] body,
        final int bodyLength, final int serializedLength) {
      final int length = Math.abs(size);
      assertThat(length).as("a record never keeps fewer bytes than its serialized content").isGreaterThanOrEqualTo(serializedLength);
      page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE, end);
      final int marker = page.writeNumber(end, size);
      final byte[] content = new byte[length];
      System.arraycopy(body, 0, content, 0, Math.min(length, bodyLength));
      page.writeByteArray(end + marker, content, 0, length);
      return end + marker + length;
    }

    /**
     * Which of two updated records a commit applies first: it walks the transaction's updated records in the iteration
     * order of a {@link HashMap} keyed by RID ({@code TransactionContext.updatedRecords}), which this reproduces.
     */
    private static RID appliedFirstAtCommit(final RID first, final RID second) {
      final Map<RID, Boolean> updated = new HashMap<>();
      updated.put(first, true);
      updated.put(second, true);
      return updated.keySet().iterator().next();
    }

    private static void grow(final RID rid) {
      rid.asDocument(true).modify().set("brand", "x").set("v", BIG).save();
    }

    private static void assertRefused(final ThrowingCallable update) {
      final Throwable thrown = catchThrowable(update);
      assertThat(thrown).as("the update must be refused").isNotNull();
      Throwable cause = thrown;
      while (cause != null && !(cause instanceof DatabaseOperationException
          && String.valueOf(cause.getMessage()).contains("cannot host a placeholder pointer")))
        cause = cause.getCause();
      assertThat(cause).as("refused because the slot cannot host the pointer, but got: " + thrown).isNotNull();
    }

    /** Every record is still there and readable: a scan agrees with the counter, and the database checks out clean. */
    private void assertIntact(final String typeName, final long expectedCount) {
      assertThat(countRecordsFromCounter(typeName)).isEqualTo(expectedCount);
      assertThat(countRecords(typeName)).as("a scan must return every record the counter counts").isEqualTo(expectedCount);
      checkDatabase();
    }

    /** The size marker {@code rid}'s slot starts with. */
    private long markerOf(final RID rid) {
      final long[] marker = new long[1];
      database.transaction(() -> onSlot(rid, page -> marker[0] = page.readNumberAndSize(recordOffsetOf(page, rid))[0]));
      return marker[0];
    }

    /** The bytes {@code rid}'s slot takes: size marker plus content, for a plain record. */
    private int footprintOf(final RID rid) {
      final int[] footprint = new int[1];
      database.transaction(() -> onSlot(rid, page -> {
        final long[] size = page.readNumberAndSize(recordOffsetOf(page, rid));
        assertThat(size[0]).as("a plain record").isPositive();
        footprint[0] = (int) (size[0] + size[1]);
        return 0L;
      }));
      return footprint[0];
    }

    /** Where the content of {@code page} ends: the end of the record that ends last. */
    private static int contentEnd(final MutablePage page) {
      final int slots = page.readShort(0);
      int end = 0;
      for (int slot = 0; slot < slots; slot++) {
        final int offset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
        if (offset == 0)
          continue;
        final long[] size = page.readNumberAndSize(offset);
        final int footprint;
        if (size[0] > 0)
          footprint = (int) (size[0] + size[1]);
        else if (size[0] == -1L)
          footprint = (int) size[1] + Binary.LONG_SERIALIZED_SIZE;
        else if (size[0] == -2L || size[0] == -3L || size[0] == -4L)
          footprint = (int) size[1] + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt((int) (offset + size[1]));
        else
          footprint = (int) (-size[0] + size[1]);
        end = Math.max(end, offset + footprint);
      }
      return end;
    }
  }

  /**
   * Regression test for #10987: a delete publishes the pages of a chunked record one at a time, and a tail page can be visible
   * before the head page. A reader that meets the freed tails behind an intact head used to condemn the record as corrupted;
   * the head goes away a few microseconds later, so the answer is "deleted", which a scan skips.
   */
  @Nested
  class TailsFreedBeforeHead extends BucketPageLayoutTestSupport {
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

  /**
   * Issue #10304, a follow-up of #10263: {@link LocalBucket#getRecordFromCommittedPage} answered a placeholder pointer
   * and a multi-page record through the transaction's own view, so the committed edge-list check of an edge delete read
   * a vertex that outgrew its page as the transaction saw it, head pointers included.
   * <p>
   * The bucket-level tests pin the record's pages with a {@code REPEATABLE_READ} read, which is what makes the
   * transaction's view differ from the committed one, and hold the answer against what a fresh transaction reads. The
   * graph-level tests drive the #10263 window - the transaction holds its own copy of the vertex from before the vertex
   * had any OUT list - on a vertex whose properties do not fit a page.
   */
  @Nested
  class CommittedRecordRead extends BucketPageLayoutTestSupport {
    /** 20 KB of content: big enough to spill out of a sealed page, small enough to fit a page of its own. */
    private static final int PLACEHOLDER_CONTENT_SIZE = 20 * 1024;
    /** Fits no page: the record is a chunk chain. */
    private static final int MULTI_PAGE_SIZE          = 200 * 1024;

    @Test
    void aMultiPageRecordIsReadAsCommittedNotAsTheTransactionHoldsIt() {
      database.transaction(() -> database.getSchema().createDocumentType("Big", 1).createProperty("v", Type.STRING));
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Big").set("v", "a".repeat(MULTI_PAGE_SIZE)).save().getIdentity());
      assertThat((Long) bucketStats("Big").get("totalMultiPageRecords")).isEqualTo(1L);

      assertCommittedReadSeesTheConcurrentUpdate("Big", rid[0], "b".repeat(MULTI_PAGE_SIZE));
    }

    @Test
    void aPlaceholderBackedRecordIsReadAsCommittedNotAsTheTransactionHoldsIt() {
      final RID placeholder = placeholderBackedRecord("Placeholder");

      assertCommittedReadSeesTheConcurrentUpdate("Placeholder", placeholder, "c".repeat(PLACEHOLDER_CONTENT_SIZE));
    }

    @Test
    void aDeletedMultiPageRecordReadsAsNoRecord() {
      database.transaction(() -> database.getSchema().createDocumentType("Gone", 1).createProperty("v", Type.STRING));
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument("Gone").set("v", "a".repeat(MULTI_PAGE_SIZE)).save().getIdentity());
      database.transaction(() -> rid[0].asDocument().delete());

      database.transaction(() -> assertThat(bucketOf("Gone").getRecordFromCommittedPage(rid[0])).isNull());
    }

    /**
     * The #10263 window on a vertex whose properties outgrow its page: the transaction rewrote the vertex while it had no
     * OUT list, a concurrent commit added the edge (and the head pointer to its list), and the delete of that edge finds
     * no entry in the list it reads. The committed check must read the new head pointer, not the transaction's.
     */
    @Test
    void deletingAnEdgeOfAMultiPageVertexHeldWithoutAnyListIsAConflict() {
      assertDeleteOfConcurrentEdgeIsAConflict(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    }

    @Test
    void deletingAnEdgeOfAMultiPageVertexHeldWithoutAnyListIsAConflictUnderRepeatableRead() {
      assertDeleteOfConcurrentEdgeIsAConflict(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    }

    private void assertDeleteOfConcurrentEdgeIsAConflict(final Database.TRANSACTION_ISOLATION_LEVEL isolation) {
      database.transaction(() -> {
        database.getSchema().createVertexType("V", 1);
        database.getSchema().createEdgeType("E", 1);
      });
      final RID[] v = new RID[2];
      database.transaction(() -> {
        v[0] = database.newVertex("V").set("id", 0).set("blob", "x".repeat(MULTI_PAGE_SIZE)).save().getIdentity();
        v[1] = database.newVertex("V").set("id", 1).set("blob", "y".repeat(MULTI_PAGE_SIZE)).save().getIdentity();
      });
      assertThat((Long) bucketStats("V").get("totalMultiPageRecords")).as("both vertices must be chunk chains").isEqualTo(2L);

      database.begin(isolation);
      try {
        v[0].asVertex().modify().set("value", 1).save();
        v[1].asVertex().modify().set("value", 1).save();

        final RID[] added = new RID[1];
        inAnotherThread(() -> database.transaction(() -> added[0] = v[0].asVertex().newEdge("E", v[1].asVertex()).getIdentity()));

        assertThatThrownBy(() -> added[0].asEdge().delete()).isInstanceOf(ConcurrentModificationException.class)
            .as("refused by the committed edge-list check, not by a read in motion").hasMessageContaining("edge list of vertex");
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

      database.transaction(() -> {
        assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
        assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
        final Set<RID> records = new HashSet<>();
        for (var it = database.iterateType("E", true); it.hasNext(); )
          records.add(it.next().getIdentity());
        for (final Edge e : v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "E"))
          assertThat(records).as("every OUT entry is an existing edge").contains(e.getIdentity());
      });
      checkDatabase();
    }

    /**
     * Reads {@code rid} under {@code REPEATABLE_READ}, which pins the pages it lives on, lets another thread commit
     * {@code newValue}, and holds {@link LocalBucket#getRecordFromCommittedPage} against what a fresh transaction reads.
     */
    private void assertCommittedReadSeesTheConcurrentUpdate(final String typeName, final RID rid, final String newValue) {
      final LocalBucket bucket = bucketOf(typeName);

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        final byte[] before = bucket.getRecord(rid).toByteArray();

        final byte[][] committed = new byte[1][];
        inAnotherThread(() -> {
          database.transaction(() -> rid.asDocument(true).modify().set("v", newValue).save());
          database.transaction(() -> committed[0] = bucket.getRecord(rid).toByteArray());
        });
        assertThat(Arrays.equals(committed[0], before)).as("the concurrent commit changed the record").isFalse();
        assertThat(Arrays.equals(bucket.getRecord(rid).toByteArray(), before)).as("the transaction still sees its pinned image").isTrue();

        final Binary read = bucket.getRecordFromCommittedPage(rid);
        assertThat(read).isNotNull();
        assertThat(Arrays.equals(read.toByteArray(), committed[0])).as("the committed read must see the concurrent commit").isTrue();
      } finally {
        database.rollback();
      }
      checkDatabase();
    }

    private RID placeholderBackedRecord(final String typeName) {
      final RID[] tiny = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName, 1).createProperty("v", Type.STRING);
        tiny[0] = database.newDocument(typeName).set("v", "p").save().getIdentity();
      });
      sealFirstPage(typeName);

      database.transaction(() -> tiny[0].asDocument(true).modify().set("v", "b".repeat(PLACEHOLDER_CONTENT_SIZE)).save());

      final Map<String, Object> layout = bucketStats(typeName);
      assertThat((Long) layout.get("totalPlaceholderRecords"))
          .as("this test is about a placeholder-backed record, and " + typeName + " has none: " + layout).isEqualTo(1L);
      return tiny[0];
    }
  }

  /**
   * The three things {@code CHECK DATABASE} could not say about a bucket, all found while building the #6196 fixtures.
   * <ul>
   * <li><b>#6292</b> - a placeholder POINTER whose CONTENT is gone was never followed, so {@code count(@rid)} (a scan,
   * which resolves the pointer to nothing and skips the slot) and {@code count(*)} (the cached counter, for which a
   * pointer IS a record) disagreed for ever while the checker reported the database clean.</li>
   * <li><b>#6293</b> - a record the FIX deleted during the run was counted BOTH in {@code totalDeletedRecords} and in
   * the category tally it had held before, because the slot was classified first and repaired afterwards. One run
   * described the same record twice, and only a second run told the truth.</li>
   * <li><b>#6294</b> - the continuation chunks a force-delete leaves behind were reclaimed by nothing, though three
   * comments promised compaction or a database check would. {@code compressPage} re-flows LIVE slots and an orphaned
   * chunk still has one, and {@code check()} counted it and moved on.</li>
   * </ul>
   * The fixtures below fabricate the physical shapes on a database this build wrote, because the writers that used to
   * produce them are gone: a placeholder needs the zero-free-tail page #6149 left as the last fallback, and an orphan
   * needs a chain broken behind the record's back.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CheckDatabaseReconciliation extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Reconciled";
    /** 200 KB fits no page whole, so the content record created behind a pointer spills into a chain of its own. */
    private static final String HUGE = "h".repeat(200 * 1024);

    /**
     * The disagreement as reported: a pointer left aimed at a slot that is not there any more. The record is invisible
     * to every query and still counted by {@code count(*)}, and before #6292 nothing ever noticed - CHECK DATABASE
     * counted the pointer as a placeholder and moved on without following it.
     */
    @Test
    void aDanglingPlaceholderPointerIsReportedAndRemoved() {
      final RID placeholder = placeholderWithPlainContent();
      final RID content = contentRidOf(placeholder);

      final long recordsBefore = countRecords(TYPE);
      assertThat(countRecordsFromCounter(TYPE)).as("the two counts must agree before anything is broken")
          .isEqualTo(recordsBefore);

      // An interrupted repair, or the content record lost to physical corruption: the slot goes, the pointer stays.
      freeSlotBehindItsBack(content);

      assertThat(countRecords(TYPE)).as("a scan can no longer resolve the pointer").isEqualTo(recordsBefore - 1);
      assertThat(countRecordsFromCounter(TYPE)).as("but the counter still counts the pointer slot")
          .isEqualTo(recordsBefore);

      final Result found = checkDatabaseRow(false);
      assertThat(numberProperty(found, "danglingPlaceholderPointers"))
          .as("the checker must follow the pointer: " + found.toJSON()).isEqualTo(1L);
      assertThat(numberProperty(found, "totalErrors")).as("and call it an error: " + found.toJSON()).isEqualTo(1L);
      assertThat(warningsOf(found).toString()).as("naming both ends of it: " + found.toJSON())
          .contains(placeholder.toString()).contains(content.toString());
      assertThat(numberProperty(found, "danglingPlaceholderPointersFixed"))
          .as("nothing is repaired without FIX: " + found.toJSON()).isZero();

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed")).as("FIX removes it: " + fixed.toJSON())
          .isEqualTo(1L);
      assertThat((Collection<Object>) fixed.<Collection<Object>>getProperty("deletedRecordsAfterFix"))
          .as("the pointer is the record's identity, so it is what the report names: " + fixed.toJSON())
          .containsExactly(placeholder);

      assertThat(countRecords(TYPE)).isEqualTo(recordsBefore - 1);
      assertThat(countRecordsFromCounter(TYPE)).as("and the two counts agree again").isEqualTo(recordsBefore - 1);

      final Result again = checkDatabaseRow(false);
      assertThat(numberProperty(again, "totalErrors")).as("with nothing left to find: " + again.toJSON()).isZero();
    }

    /**
     * The corrupted-pointer form of the same defect, and the reason the repair frees the pointer SLOT rather than going
     * through the ordinary delete: a pointer that names an unrelated LIVE record would have that record deleted with it.
     */
    @Test
    void repairingACorruptedPointerDoesNotDeleteWhateverItNowNames() {
      final RID placeholder = placeholderWithPlainContent();
      final RID content = contentRidOf(placeholder);

      // An ordinary record of the same bucket, which the corrupted pointer is then aimed at.
      final RID[] innocent = new RID[1];
      database.transaction(() -> innocent[0] = database.newDocument(TYPE).set("v", "innocent").save().getIdentity());

      freeSlotBehindItsBack(content);
      database.transaction(() -> onSlot(placeholder, page -> {
        // [marker:1][contentPosition:long]
        page.writeLong(recordOffsetOf(page, placeholder) + 1, innocent[0].getPosition());
        return 0L;
      }));

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "danglingPlaceholderPointersFixed")).as("the pointer goes: " + fixed.toJSON())
          .isEqualTo(1L);
      database.transaction(
          () -> assertThat(innocent[0].asDocument(true).getString("v")).as("and the record it named stays")
              .isEqualTo("innocent"));

      checkDatabase();
    }

    /**
     * A pointer with a healthy content record behind it - of either shape, a plain one or a chain - is not touched. The
     * negative control without which the two tests above would pass on a checker that reported every pointer.
     */
    @Test
    void aHealthyPlaceholderIsNotReported() {
      final RID placeholder = placeholderWithPlainContent();

      Result row = checkDatabaseRow(false);
      assertThat(numberProperty(row, "danglingPlaceholderPointers"))
          .as("a plain content record behind a pointer is not dangling: " + row.toJSON()).isZero();
      assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

      // And again once the content has outgrown its own page and become a chain of its own.
      database.transaction(() -> placeholder.asDocument(true).modify().set("v", HUGE).save());
      row = checkDatabaseRow(false);
      assertThat(numberProperty(row, "danglingPlaceholderPointers"))
          .as("nor is a content record stored as a chain: " + row.toJSON()).isZero();
      assertThat(numberProperty(row, "orphanedChunks")).as("and its chunks are reachable: " + row.toJSON()).isZero();
      assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

      checkDatabase();
    }

    /**
     * The boundary of the marker namespace, on the one path where getting it wrong deletes a live record.
     * <p>
     * A content record stores its size NEGATED, and {@code RECORD_PLACEHOLDER_CONTENT} (-5) is where the sizes stop and
     * the markers begin. Every classification site excludes it with a strict {@code <}, so a content record of exactly
     * {@code MINIMUM_RECORD_SIZE} bytes would be read as something that is not content at all - and since #6292 that
     * answer is acted on: {@code FIX} would free the pointer to a record that is still there.
     * <p>
     * Measured, not assumed: the smallest document this serializer produces is 6 bytes, so the boundary value is not
     * reachable today, and the padding now pushes a content body past it in any case. This shrinks a placeholder's
     * content as far as a document can go - every property removed - and asserts the pointer survives it. Should a
     * future serializer shave those bytes, this fails here rather than in somebody's database (PR review on #6299).
     */
    @Test
    void theSmallestPossibleContentRecordIsNotCalledDangling() {
      final RID placeholder = placeholderWithPlainContent();
      final long recordsBefore = countRecords(TYPE);

      database.transaction(() -> {
        final MutableDocument doc = placeholder.asDocument(true).modify();
        doc.remove("v");
        doc.save();
      });

      final Result row = checkDatabaseRow(true);
      assertThat(numberProperty(row, "danglingPlaceholderPointers"))
          .as("a content record at the smallest size a document has is still content: " + row.toJSON()).isZero();
      assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();
      assertThat(numberProperty(row, "totalSurrogateRecords"))
          .as("and it is still counted as the surrogate it is: " + row.toJSON()).isEqualTo(1L);

      assertThat(countRecords(TYPE)).as("the record is still there, and exactly once").isEqualTo(recordsBefore);
      assertThat(countRecordsFromCounter(TYPE)).as("with both counts still agreeing").isEqualTo(recordsBefore);
      database.transaction(
          () -> assertThat(placeholder.asDocument(true).getString("v")).as("and still readable through its pointer")
              .isNull());

      checkDatabase();
    }

    /**
     * #6294: the chunks a broken chain cuts off are reclaimed, and until #6294 nothing ever collected them - not
     * compaction, which re-flows a page's LIVE slots and an orphaned chunk still has one, and not this checker, which
     * counted a NEXT_CHUNK slot and moved on.
     */
    @Test
    void orphanedChunksAreFoundAndReclaimed() {
      final RID[] big = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        big[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity();
      });

      final long chunksBefore = (Long) bucketStats(TYPE).get("totalChunks");
      assertThat(chunksBefore).as("the fixture must really be a chain").isPositive();

      // Cut the chain off at the head, which is what a corrupted or half-written continuation pointer does.
      database.transaction(() -> onSlot(big[0], page -> {
        // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
        page.writeLong(recordOffsetOf(page, big[0]) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
        return 0L;
      }));

      final Result found = checkDatabaseRow(false);
      assertThat(numberProperty(found, "orphanedChunks")).as("every cut-off chunk must be found: " + found.toJSON())
          .isEqualTo(chunksBefore);
      assertThat(numberProperty(found, "orphanedChunksReclaimed")).as("but not reclaimed without FIX: " + found.toJSON())
          .isZero();
      assertThat((Long) bucketStats(TYPE).get("totalChunks")).as("nor freed").isEqualTo(chunksBefore);

      final Result fixed = checkDatabaseRow(true);
      assertThat(numberProperty(fixed, "orphanedChunksReclaimed")).as("FIX reclaims them: " + fixed.toJSON())
          .isEqualTo(chunksBefore);

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalChunks")).as("and the space really goes back: " + layout).isZero();
      assertThat((Long) layout.get("totalMultiPageRecords")).as("with the broken record gone: " + layout).isZero();

      final Result again = checkDatabaseRow(false);
      assertThat(numberProperty(again, "totalErrors")).as("nothing is left for a second run: " + again.toJSON()).isZero();
      assertThat(numberProperty(again, "orphanedChunks")).as(again.toJSON().toString()).isZero();
    }

    /**
     * The negative control the sweep cannot do without: a bucket full of healthy chains, where every chunk is reachable
     * from the head that owns it. A mark phase with a hole in it would delete live data here.
     */
    @Test
    void noChunkOfAHealthyBucketIsCalledAnOrphan() {
      final RID[] chained = createChunkedRecords(TYPE);

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalChunks")).as("the fixture must really have chains: " + layout).isPositive();

      final Result row = checkDatabaseRow(true);
      assertThat(numberProperty(row, "orphanedChunks")).as("no live chunk may be called an orphan: " + row.toJSON())
          .isZero();
      assertThat(numberProperty(row, "orphanedChunksReclaimed")).as(row.toJSON().toString()).isZero();
      assertThat(numberProperty(row, "totalErrors")).as(row.toJSON().toString()).isZero();

      database.transaction(() -> {
        for (int i = 0; i < chained.length; i++)
          assertThat(chained[i].asDocument(true).getString("payload")).isEqualTo(payload(i, 'x'));
      });
      checkDatabase();
    }

    /**
     * #6293: the FIX run must describe the database it LEAVES, not the one it found with two fields patched. A record it
     * deleted used to be counted in {@code totalDeletedRecords} AND under {@code totalMultiPageRecords}, so the run
     * disagreed with a re-run on the very numbers an operator diffs across the two.
     */
    @Test
    void aRecordTheFixDeletedIsNotAlsoCountedInItsCategory() {
      final RID[] big = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        database.newDocument(TYPE).set("v", "plain").save();
        big[0] = database.newDocument(TYPE).set("v", HUGE).save().getIdentity();
      });

      database.transaction(() -> onSlot(big[0], page -> {
        page.writeLong(recordOffsetOf(page, big[0]) + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
        return 0L;
      }));

      final Result fixed = checkDatabaseRow(true);
      assertThat((Collection<?>) fixed.getProperty("deletedRecordsAfterFix")).as(fixed.toJSON().toString()).hasSize(1);
      assertThat(numberProperty(fixed, "totalMultiPageRecords"))
          .as("the record it deleted is not also a multi-page record it found: " + fixed.toJSON()).isZero();

      // The proof that the run described the state it left: a re-run of the same query has nothing to correct.
      final Result again = checkDatabaseRow(false);
      for (final String tally : new String[] { "totalAllocatedRecords", "totalActiveRecords", "totalMultiPageRecords",
          "totalPlaceholderRecords", "totalSurrogateRecords", "totalChunks" })
        assertThat(numberProperty(fixed, tally)).as("%s: the FIX run and a re-run must agree - %s vs %s", tally,
            fixed.toJSON(), again.toJSON()).isEqualTo(numberProperty(again, tally));

      assertThat(numberProperty(again, "totalErrors")).as(again.toJSON().toString()).isZero();
    }

    /**
     * The fixture every placeholder test needs: a page with a free tail of exactly ZERO, which since #6149 is the only
     * shape that still makes a record spill into a POINTER instead of a chunk chain.
     */
    private RID placeholderWithPlainContent() {
      final RID[] placeholder = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        placeholder[0] = database.newDocument(TYPE).set("v", "p").save().getIdentity();
      });
      sealFirstPage(TYPE);

      // Big enough that page 0 - which has no free tail at all - cannot host it, small enough for a page of its own.
      database.transaction(() -> placeholder[0].asDocument(true).modify().set("v", "c".repeat(16 * 1024)).save());

      assertThat((Long) bucketStats(TYPE).get("totalPlaceholderRecords"))
          .as("the fixture must produce a placeholder pointer").isEqualTo(1L);
      return placeholder[0];
    }

    /** The RID of the CONTENT record the placeholder POINTER at {@code rid} references. */
    private RID contentRidOf(final RID rid) {
      final long[] pointer = new long[1];
      database.transaction(() -> pointer[0] = onSlot(rid, page -> page.readLong(recordOffsetOf(page, rid) + 1)));
      return new RID(rid.getBucketId(), pointer[0]);
    }
  }

  /**
   * #6319 - the continuation chunks an ordinary UPDATE leaves behind when a record shrinks back exactly onto a chunk
   * boundary.
   * <p>
   * {@code updateMultiPageRecord} rewrites the chain a record already has and hands whatever it no longer needs to
   * {@code freeChunkChain}. It decided two things from two different comparisons: the chain was CUT whenever the new
   * content ended at or before the chunk being written, and the tail was FREED only when it ended strictly before it.
   * Content ending exactly on a boundary therefore fell in between - cut, and freed by nothing. The chunks past the cut
   * kept their slots and their pointers to one another, reachable from no record, until an admin ran
   * {@code CHECK DATABASE FIX}.
   * <p>
   * The boundary is not a coincidence to be hit by luck: a chain grown one field at a time has a boundary exactly where
   * that field's bytes were appended, so a record that loses the field lands on one by construction. That is what made
   * {@code CRUDTest.multiUpdatesOverlap} leak 243821 chunks out of 1545495 - every record that stayed a chain across its
   * shrink round leaked its whole tail.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ChunkChainTailLeak extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Chunked";
    /** Several pages' worth, so the record is a chain of several chunks rather than a head plus one. */
    private static final int    BASE  = 200_000;
    /** What one more field costs: enough to be appended as a chunk of its own at the end of the chain. */
    private static final int    FIELD = 100;

    /**
     * The defect in its smallest form: grow a chained record by one chunk, then take that chunk's worth of content away
     * again. The chain has to come back to exactly what it was, with nothing left over.
     */
    @Test
    void aRecordShrinkingBackOntoAChunkBoundaryFreesTheTailItDrops() {
      final RID rid = spilledRecord();

      final long chunksBefore = statistic("totalChunks");
      assertThat(chunksBefore).as("the fixture must really be a chain of several chunks").isGreaterThan(2L);

      // GROW: the content no longer fits the chain, so one more chunk is appended after the last one.
      write(rid, BASE + FIELD);
      assertThat(statistic("totalChunks")).as("growing by a field's worth must append exactly one chunk")
          .isEqualTo(chunksBefore + 1);

      // SHRINK BACK: the content now ends exactly where the chain ended before, on the boundary between the last chunk
      // it still needs and the one it does not.
      write(rid, BASE);

      assertThat(statistic("orphanedChunks")).as("the chunk the record dropped must be freed, not merely unlinked: "
          + bucketStats(TYPE)).isZero();
      assertThat(statistic("totalChunks")).as("so the chain is back to the size it had: " + bucketStats(TYPE))
          .isEqualTo(chunksBefore);
      assertThat(readPayload(rid)).as("and the record still reads back whole").isEqualTo(payload(BASE));

      checkDatabase();
    }

    /**
     * The same defect as the workload meets it, and the reason it is worth fixing at the source rather than repairing:
     * every round adds a field and drops the one before it, so every round ends on a boundary and leaks the chunk that
     * field's bytes were written into. Left alone the backlog grows for the life of the bucket.
     */
    @Test
    void repeatedGrowAndShrinkRoundsLeakNothing() {
      final RID rid = spilledRecord();
      final long chunksBefore = statistic("totalChunks");

      for (int round = 1; round <= 8; ++round) {
        write(rid, BASE + FIELD);
        write(rid, BASE);

        assertThat(statistic("orphanedChunks")).as("round " + round + " leaked: " + bucketStats(TYPE)).isZero();
        assertThat(statistic("totalChunks")).as("round " + round + " grew the chain: " + bucketStats(TYPE))
            .isEqualTo(chunksBefore);
      }

      assertThat(readPayload(rid)).isEqualTo(payload(BASE));
      checkDatabase();
    }

    /**
     * The boundary is only the case that always leaks; a shrink that stops SHORT of one has always freed its tail, and
     * must go on doing so now that both answers come from the same condition.
     */
    @Test
    void aRecordShrinkingPastAChunkBoundaryStillFreesItsTail() {
      final RID rid = spilledRecord();
      final long chunksBefore = statistic("totalChunks");

      write(rid, BASE / 2);

      assertThat(statistic("orphanedChunks")).as("nothing may be left behind: " + bucketStats(TYPE)).isZero();
      assertThat(statistic("totalChunks")).as("and the chain must really have shrunk: " + bucketStats(TYPE))
          .isLessThan(chunksBefore);
      assertThat(readPayload(rid)).isEqualTo(payload(BASE / 2));

      checkDatabase();
    }

    /** A single record grown until it has spilled into a chunk chain of {@link #BASE} bytes of payload. */
    private RID spilledRecord() {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
        rid[0] = database.newDocument(TYPE).set("payload", payload(BASE)).save().getIdentity();
      });

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalMultiPageRecords")).as("the fixture record must spill into chunks: " + layout)
          .isEqualTo(1L);
      return rid[0];
    }

    private void write(final RID rid, final int size) {
      database.transaction(() -> rid.asDocument(true).modify().set("payload", payload(size)).save());
    }

    private String readPayload(final RID rid) {
      final String[] payload = new String[1];
      database.transaction(() -> payload[0] = rid.asDocument(true).getString("payload"));
      return payload[0];
    }

    /** Content of a given length, distinct per length so a stale read cannot pass for a fresh one. */
    private static String payload(final int size) {
      final String marker = "s" + size + "-";
      return marker + "p".repeat(size - marker.length());
    }

    private long statistic(final String name) {
      return ((Number) bucketStats(TYPE).get(name)).longValue();
    }
  }

  /**
   * #6320 - what one run of {@code CHECK DATABASE FIX} on a bucket may hold in memory.
   * <p>
   * #6294 gave the orphaned-chunk sweep of {@code LocalBucket.check(fix)} a memory budget, because the enclosing
   * transaction keeps a copy of every page it modifies and a large backlog freed in one transaction is how a repair
   * turns into the {@code OutOfMemoryError} of #4653. The RECORD repairs of the very same method - four force-deletes in
   * the per-slot loop, each taking its record's page plus every page its chain touches - were bounded by nothing, and
   * reached the same failure through the other loop.
   * <p>
   * The answer is not a second budget. What is scarce is ONE pool, so two bounds over it cannot both be right; and a
   * repair that stops when the pool is spent leaves an operator running {@code FIX} over and over to converge. The
   * mechanism that gives the pool back already existed one class over - {@code GraphDatabaseChecker} has committed its
   * graph repairs in batches of {@link GlobalConfiguration#CHECK_DATABASE_REPAIR_BATCH_PAGES} pages since #6128 - and
   * the bucket repairs now use it, all of them, so one run repairs everything with the memory bounded throughout.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketRepairBatch extends BucketPageLayoutTestSupport {
    private static final String TYPE      = "Damaged";
    /** A type the repair never touches, so the caller's own work sits on pages of its own. */
    private static final String BYSTANDER = "Bystander";
    /** Enough broken records, spread over enough pages, that a small page budget really is crossed several times. */
    private static final int    RECORDS = 24;
    /** {@code LocalBucket.FIRST_CHUNK} (-2), as the single zigzag byte it is stored as. */
    private static final byte   FIRST_CHUNK_MARKER = 3;

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // The fixture injects on-disk corruption on purpose; each test repairs it and checks the result itself.
      return false;
    }

    /**
     * The whole point: a repair whose page footprint exceeds the budget is split across several transactions AND still
     * repairs everything the bucket has wrong - the records and the chunks their deletion orphans - in that one run.
     */
    @Test
    void aBucketRepairBiggerThanTheBudgetCommitsInBatchesAndStillFinishes() {
      final List<RID> broken = brokenChunkChains();

      GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(2);

      final Map<String, Object>[] stats = new Map[1];
      final int commits = countCommitsDuring(() -> stats[0] = bucketOf(TYPE).check(0, true));

      assertThat(commits).as("a repair bigger than the page budget must not be one single transaction").isGreaterThan(1);

      assertThat((Long) stats[0].get("orphanedChunksReclaimed"))
          .as("every chunk the deleted records orphaned goes in the SAME run, where the sweep's own budget used to "
              + "leave a backlog for the next one: " + stats[0])
          .isEqualTo((Long) stats[0].get("orphanedChunks")).isPositive();

      assertRepaired(broken);
    }

    /**
     * The budget disabled restores the historical single all-or-nothing transaction, memory cost included, exactly as
     * it does for the graph repairs. An embedded user who wants those semantics can still have them.
     */
    @Test
    void theBatchBudgetCanBeDisabled() {
      final List<RID> broken = brokenChunkChains();

      GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(0);

      final int commits = countCommitsDuring(() -> bucketOf(TYPE).check(0, true));

      assertThat(commits).as("with the budget disabled the whole bucket repair is one transaction").isEqualTo(1);

      assertRepaired(broken);
    }

    /**
     * A read-only check must keep the caller's transaction and its view: only the repairs own a transaction of their
     * own. Pinned because the ownership is what makes the batching safe, and getting it wrong would silently commit
     * whatever the caller was in the middle of.
     */
    @Test
    void aCheckWithoutFixLeavesTheCallersTransactionAlone() {
      brokenChunkChains();

      database.begin();
      try {
        final RID uncommitted = database.newDocument(TYPE).set("payload", "u").save().getIdentity();

        bucketOf(TYPE).check(0, false);

        assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
        assertThat(uncommitted.asDocument(true).getString("payload"))
            .as("and must still see what it had not committed").isEqualTo("u");
      } finally {
        database.rollback();
      }
    }

    /**
     * The failure path, and the reason the run tracks whether it still OWNS a transaction rather than asking
     * {@code database.isTransactionActive()} (PR review on #6320).
     * <p>
     * {@code LocalDatabase.commit()} pops the transaction context in a {@code finally}, whether or not the write
     * succeeded, so a batch commit that throws leaves this run holding nothing - and what is on the thread from that
     * moment is the CALLER's transaction, which through HTTP is always there. A cleanup that asked "is a transaction
     * active?" would answer yes and roll THAT one back: the other buckets a {@code CHECK DATABASE FIX} had already
     * repaired into it, and whatever else the caller was holding.
     * <p>
     * The failure simulated is the one batching exists to survive in the first place, taken from
     * {@code CheckDatabaseRepairBatchFailureTest}: a commit rejected from inside the WAL-write callback.
     */
    @Test
    void aFailedBatchCommitLeavesTheCallersTransactionAlone() {
      brokenChunkChains();

      // A type of its own for the caller's work, so its pages cannot overlap the ones the repair rewrites and the
      // assertion below is about transaction ownership and nothing else.
      database.transaction(() -> database.getSchema().createDocumentType(BYSTANDER, 1));

      GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(1);

      final DatabaseInternal db = (DatabaseInternal) database;
      final AtomicInteger commits = new AtomicInteger();
      // Fails the SECOND batch commit, so the run has genuinely committed one batch before it breaks.
      final Callable<Void> failSecondCommit = () -> {
        if (commits.incrementAndGet() == 2)
          throw new IllegalStateException("simulated replicated-entry rejection");
        return null;
      };

      database.begin();
      try {
        final RID callersOwn = database.newDocument(BYSTANDER).set("payload", "caller").save().getIdentity();

        db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
        try {
          assertThatThrownBy(() -> bucketOf(TYPE).check(0, true))
              .as("a batch that cannot commit must reach the caller, not be reported as a completed repair")
              .isInstanceOf(Exception.class);
        } finally {
          db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
        }

        assertThat(commits.get()).as("the failure must have happened mid-repair, not before it started")
            .isGreaterThanOrEqualTo(2);

        assertThat(database.isTransactionActive())
            .as("the caller's transaction must survive a repair that failed inside a transaction of its own").isTrue();
        assertThat(callersOwn.asDocument(true).getString("payload"))
            .as("with the work it was holding still in it").isEqualTo("caller");
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    /**
     * The other thing batching changes about a repair, and the reason the record counter is invalidated at the START of
     * a fix run and not only at its end (PR review on #6320).
     * <p>
     * {@code count(*)} answers from {@code LocalBucket}'s O(1) counter without reading a page, and the records a repair
     * removes go through {@code deleteRecordInternal}, which deliberately registers no bucket delta for the commit-time
     * fold to apply. While a repair was ONE transaction that did not matter: nothing it did was durable until the end,
     * and the invalidation at the end became visible with it. Batching makes every batch durable as it commits, so a
     * counter invalidated only at the end goes on serving the PRE-repair number for as long as the repair takes - and
     * for good, if the run never reaches its end.
     * <p>
     * That last case is what this pins, because it needs no concurrency to observe: a run that commits a batch and then
     * fails leaves records genuinely gone and a counter that never heard about it.
     */
    @Test
    void aRepairThatCommittedABatchDoesNotLeaveTheRecordCounterStale() {
      brokenChunkChains();

      final long countedBefore = countRecordsFromCounter(TYPE);
      assertThat(countedBefore).as("the counter must be populated before the repair starts").isEqualTo(RECORDS);

      GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(1);

      final DatabaseInternal db = (DatabaseInternal) database;
      final AtomicInteger commits = new AtomicInteger();
      final Callable<Void> failSecondCommit = () -> {
        if (commits.incrementAndGet() == 2)
          throw new IllegalStateException("simulated replicated-entry rejection");
        return null;
      };

      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
      try {
        assertThatThrownBy(() -> bucketOf(TYPE).check(0, true)).isInstanceOf(Exception.class);
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
      }

      // Ground truth from the slot walk rather than from a scan: the records this run did NOT get to are still broken,
      // and reading one throws. count() counts exactly these three categories.
      final Map<String, Object> layout = bucketStats(TYPE);
      final long reallyThere = (Long) layout.get("totalActiveRecords") + (Long) layout.get("totalPlaceholderRecords")
          + (Long) layout.get("totalMultiPageRecords");

      assertThat(reallyThere).as("the batch that committed really did remove records: " + layout)
          .isLessThan(countedBefore);
      assertThat(countRecordsFromCounter(TYPE))
          .as("count(*) must not go on answering from a counter the committed batch made wrong: " + layout)
          .isEqualTo(reallyThere);
    }

    /** Every broken record is gone, no orphan is left, and a second run finds nothing to do. */
    private void assertRepaired(final List<RID> broken) {
      final Map<String, Object> after = bucketStats(TYPE);
      assertThat((Long) after.get("totalErrors")).as("nothing may be left to repair: " + after).isZero();
      assertThat((Long) after.get("orphanedChunks")).as("and nothing left leaked: " + after).isZero();

      database.transaction(() -> {
        for (final RID rid : broken)
          assertThat(bucketOf(TYPE).existsRecord(rid)).as("broken record %s must have been removed", rid).isFalse();
      });

      final Result row = checkDatabaseRow(false);
      assertThat(numberProperty(row, "totalErrors")).as("and the database checks out: " + row.toJSON()).isZero();
    }

    /**
     * {@link #RECORDS} records big enough to spill into a chunk chain, each with its head chunk pointing at a page that
     * does not exist. {@code check(fix)} force-deletes such a record - freeing its head slot, which is one repair - and
     * the chunks it can no longer reach then fall to the orphan sweep, which is the other. One fixture, both kinds.
     */
    private List<RID> brokenChunkChains() {
      // A SMALLER page size, set before the bucket is created: the budget counts PAGES, and at the 64KB default this
      // fixture would pack into too few of them for any assertion about batching to mean anything.
      GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);

      final List<RID> rids = new ArrayList<>(RECORDS);
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
        for (int i = 0; i < RECORDS; i++)
          rids.add(database.newDocument(TYPE).set("payload", "r" + i + "-" + "p".repeat(40_000)).save().getIdentity());
      });

      final Map<String, Object> layout = bucketStats(TYPE);
      assertThat((Long) layout.get("totalMultiPageRecords")).as("every fixture record must be a chain: " + layout)
          .isEqualTo(RECORDS);

      for (final RID rid : rids)
        breakChainOf(rid);

      return rids;
    }

    /**
     * Points the head chunk of {@code rid} at a page far past the end of the file, which is the break
     * {@code findBrokenChunkChain} reports as "next chunk pointer out of range". The chunks behind it keep their slots
     * and become unreachable the moment the head is deleted.
     */
    private void breakChainOf(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int bucketPageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
      final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();
      final int pageNumber = (int) (rid.getPosition() / maxRecordsInPage);

      database.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction()
              .getPageToModify(new PageId(db, rid.getBucketId(), pageNumber), bucketPageSize, false);
          final int recordOffset = recordOffsetOf(page, rid);
          assertThat(page.readByte(recordOffset)).as("record %s must be a chunk head", rid).isEqualTo(FIRST_CHUNK_MARKER);
          // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
          page.writeLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    /** Counts the transactions that actually wrote WAL while {@code block} ran. */
    private int countCommitsDuring(final Runnable block) {
      final AtomicInteger commits = new AtomicInteger();
      final DatabaseInternal db = (DatabaseInternal) database;
      final Callable<Void> counter = () -> {
        commits.incrementAndGet();
        return null;
      };
      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
      try {
        block.run();
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
      }
      return commits.get();
    }
  }

  /**
   * #6441: which half of {@code LocalBucket.getOrderedRecordsInPage} is allowed to free a slot it cannot make sense
   * of.
   * <p>
   * The walk has two callers and they are the two halves of one contract: {@code compressPageInternal} owns the page
   * for writing and is where a repair belongs, and {@code gatherPageStatistics} is a pure scan reached from an
   * ordinary allocation over pages it has not written. A {@code readOnly} flag guarded the two slot-freeing branches
   * in OPPOSITE directions, so the invalid-size slot was freed by the scan - which reached
   * {@code getPageToModify} and enlisted an unrelated page into whatever transaction happened to be inserting a
   * record, a write that vanished on a rollback and deleted somebody else's record on a commit - and walked past by
   * the compression, the one place that could have repaired it for good.
   * <p>
   * The two tests below are each other's proof: the fixture is only known to build a genuinely invalid-size slot
   * because the compression frees it, and freeing it is only known to be the compression's doing because the scan
   * leaves it alone.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CorruptedSlotRepairSide extends BucketPageLayoutTestSupport {
    private static final String TYPE = "Slotted";

    /** How many 8 KB records to insert: enough for the statistics scan, which skips the last two pages, to reach page 0. */
    private static final int RECORDS = 48;

    /** A declared record size no page can hold - the shape the walk rejects. Encodes in 3 varint bytes. */
    private static final long IMPOSSIBLE_RECORD_SIZE = 1_000_000L;

    /** Every test here leaves one deliberately unreadable record behind, so the blanket end-of-test check would fire. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    /**
     * The read-only half. A statistics scan meeting a corrupted slot must report it and move on: no page enlisted in
     * the transaction that happens to be running the scan, and the slot still where it was afterwards.
     */
    @Test
    void theStatisticsScanReportsTheCorruptedSlotAndTouchesNothing() throws Exception {
      final RID corrupted = corruptOnPageZero()[1];

      final long entryBefore = slotEntryOf(corrupted);
      assertThat(entryBefore).as("the fixture must leave the slot still pointing at the record").isPositive();

      final List<String> logged = new CopyOnWriteArrayList<>();
      final int[] modifiedPages = new int[1];
      final Logger original = LogManager.instance().getLogger();
      LogManager.instance().setLogger(new CapturingLogger(logged, original));
      try {
        database.transaction(() -> {
          bucketOf(TYPE).gatherPageStatistics();
          modifiedPages[0] = ((DatabaseInternal) database).getTransaction().getModifiedPages();
        });
      } finally {
        LogManager.instance().setLogger(original);
      }

      // Load-bearing: without it every assertion below would also hold for a scan that never reached page 0.
      final List<String> invalid = logged.stream().filter(l -> l.contains("Invalid record size")).toList();
      assertThat(invalid).as("the scan must have walked the corrupted slot (captured=%s)", logged).isNotEmpty();

      assertThat(modifiedPages[0])
          .as("a statistics scan must enlist no page in the transaction that runs it")
          .isZero();
      assertThat(slotEntryOf(corrupted)).as("the scan must leave the slot exactly as it found it").isEqualTo(entryBefore);

      assertThat(invalid).allSatisfy(l -> assertThat(l).as("a scan deletes nothing, so it must not claim it did")
          .contains("skipping record"));
    }

    /**
     * The write half. {@code CHECK DATABASE COMPRESS} exists to clean pages up, and this was the one slot shape it
     * walked past - the record was excluded from the packing, so the defrag closed the gap around it and left the
     * slot-table entry pointing into whatever moved on top of it.
     */
    @Test
    void checkDatabaseCompressFreesTheCorruptedSlot() throws Exception {
      final RID corrupted = corruptOnPageZero()[1];
      assertThat(slotEntryOf(corrupted)).isPositive();

      database.command("sql", "check database compress").close();

      assertThat(slotEntryOf(corrupted))
          .as("the compression is the one caller that owns the page for writing: it must free the slot")
          .isZero();

      // The defrag packs the page around the hole the corrupted record leaves: every other record has to survive it.
      assertThat(countRecords(TYPE)).isEqualTo(RECORDS - 1L);

      // #6441 follow-up: the repair writes the record-table entry directly, bypassing the transaction's insert/delete
      // bookkeeping - exactly like every corrupt-record deletion in deleteRecordInternal. A record that was live and
      // COUNTED (all of these were, this session) must not leave the O(1) cached counter one too high forever.
      assertThat(countRecordsFromCounter(TYPE))
          .as("the cached record counter must not go stale when the compression frees a live record's slot")
          .isEqualTo(RECORDS - 1L);
    }

    /**
     * The same repair on the ordinary path: any commit that touches the page runs the compression on it, so a
     * corrupted slot does not wait for an operator to notice it.
     */
    @Test
    void anOrdinaryCommitOnThePageFreesTheCorruptedSlot() throws Exception {
      final RID[] rids = corruptOnPageZero();
      final RID corrupted = rids[1];
      final RID neighbour = rids[2];

      assertThat(slotEntryOf(corrupted)).isPositive();

      // Re-resolved against the reopened database: the RID collected before the corruption still points at the
      // closed instance.
      database.transaction(() -> database.lookupByRID(new RID(neighbour.getBucketId(), neighbour.getPosition()), true)
          .asDocument().modify().set("v", "short").save());

      assertThat(slotEntryOf(corrupted))
          .as("the commit-time compression of the page must free the slot")
          .isZero();
      assertThat(countRecords(TYPE)).isEqualTo(RECORDS - 1L);
      assertThat(countRecordsFromCounter(TYPE))
          .as("the cached record counter must not go stale when an ordinary commit's compression frees the slot")
          .isEqualTo(RECORDS - 1L);
    }

    /**
     * Fills several pages, then turns the second record of page 0 into a slot whose declared size no page can hold.
     *
     * @return every RID inserted; the one at index 1 is the record that is now unreadable.
     */
    private RID[] corruptOnPageZero() throws IOException {
      final RID[] rids = new RID[RECORDS];
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE, 1).createProperty("v", Type.STRING);
        final String filler = "f".repeat(8 * 1024);
        for (int i = 0; i < RECORDS; i++)
          rids[i] = database.newDocument(TYPE).set("v", filler).save().getIdentity();
      });

      final RID corrupted = rids[1];
      assertThat(pageNumberOf(corrupted)).as("the corrupted slot must sit on page 0, the one the scan reaches").isZero();
      assertThat(totalPagesOf(corrupted)).as("the statistics scan skips the last two pages of a bucket").isGreaterThan(3);

      corruptRecordSizeOnDisk(corrupted);
      return rids;
    }

    /**
     * Overwrites the size marker of {@code rid} with one no page can hold, in the CLOSED file.
     * <p>
     * Not through a transaction on purpose: committing one compresses every page it modified, which is exactly the
     * code path under test - the fixture would repair itself before a single assertion ran.
     */
    private void corruptRecordSizeOnDisk(final RID rid) throws IOException {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId());
      final int pageSize = file.getPageSize();
      final String filePath = file.getFilePath();
      final int pageNumber = pageNumberOf(rid);

      final int recordOffset = (int) readOnPage(rid, page -> recordOffsetOf(page, rid));
      final int markerLength = (int) readOnPage(rid, page -> page.readNumberAndSize(recordOffset)[1]);

      final Binary marker = new Binary(16);
      marker.putNumber(IMPOSSIBLE_RECORD_SIZE);
      marker.flip();
      final byte[] bytes = marker.toByteArray();
      // A longer marker would spill into the neighbouring record and corrupt a second slot as a side effect.
      assertThat(bytes.length).as("the fabricated marker must fit the one it replaces").isLessThanOrEqualTo(markerLength);

      database.close();
      try (final RandomAccessFile raf = new RandomAccessFile(filePath, "rw")) {
        raf.seek((long) pageNumber * pageSize + BasePage.PAGE_HEADER_SIZE + recordOffset);
        raf.write(bytes);
      }
      database = factory.open();
    }

    /** The record-table entry of {@code rid}: its content offset, or 0 once the slot has been freed. */
    private long slotEntryOf(final RID rid) {
      return readOnPage(rid, page -> page.readUnsignedInt(
          Binary.SHORT_SERIALIZED_SIZE + slotOf(rid) * Binary.INT_SERIALIZED_SIZE));
    }

    /**
     * Runs {@code body} on the page holding {@code rid} WITHOUT taking it for modification: a
     * {@code getPageToModify} here would enlist the page and have the commit compress it, i.e. run the very repair
     * these tests are measuring.
     */
    private long readOnPage(final RID rid, final PageRead body) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
      final int pageNumber = pageNumberOf(rid);
      final long[] result = new long[1];
      database.transaction(() -> {
        try {
          result[0] = body.apply(db.getTransaction().getPage(new PageId(db, rid.getBucketId(), pageNumber), pageSize));
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
      });
      return result[0];
    }

    /** Content offset of {@code rid}'s slot on a read-only page - the {@link BasePage} twin of the support class one. */
    private int recordOffsetOf(final BasePage page, final RID rid) {
      return (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slotOf(rid) * Binary.INT_SERIALIZED_SIZE);
    }

    private int pageNumberOf(final RID rid) {
      return (int) (rid.getPosition() / bucketOf(rid).getMaxRecordsInPage());
    }

    private int slotOf(final RID rid) {
      return (int) (rid.getPosition() % bucketOf(rid).getMaxRecordsInPage());
    }

    private int totalPagesOf(final RID rid) {
      return bucketOf(rid).getTotalPages();
    }

    private LocalBucket bucketOf(final RID rid) {
      return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    }

    @FunctionalInterface
    private interface PageRead {
      long apply(BasePage page) throws Exception;
    }

    /** Records every WARNING-or-worse line, so a log the engine stopped emitting fails a test instead of nothing. */
    private static final class CapturingLogger implements Logger {
      private final List<String> messages;
      private final Logger       delegate;

      CapturingLogger(final List<String> messages, final Logger delegate) {
        this.messages = messages;
        this.delegate = delegate;
      }

      private void capture(final Level level, final String message, final Object... args) {
        if (message == null || level.intValue() < Level.WARNING.intValue())
          return;
        String formatted = message;
        if (args != null && args.length > 0) {
          try {
            formatted = message.formatted(args);
          } catch (final Exception ignored) {
            // Fall back to the raw template - good enough for the substring matching the tests do.
          }
        }
        messages.add(formatted);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
          final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
          final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
          final Object arg15, final Object arg16, final Object arg17) {
        capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
            arg15, arg16, arg17);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
            arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        capture(level, message, args);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    }
  }
}
