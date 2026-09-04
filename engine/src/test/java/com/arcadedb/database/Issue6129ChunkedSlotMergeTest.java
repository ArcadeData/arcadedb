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
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6129: the disjoint-slot merge (#5381, #5279, #5569) stopped at the page boundary - a record that outgrew its
 * page became a chunk chain or a placeholder, and every later update of it poisoned its page unconditionally. On a
 * single-bucket type whose records have all grown past the page size that turned concurrent updates of UNRELATED
 * records back into permanent conflicts: exactly the false conflict #5279 removed, one size class up.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6129ChunkedSlotMergeTest extends BucketPageLayoutTestSupport {
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
    final int fileId = database.getSchema().getType("Shapes").getBuckets(false).get(0).getFileId();

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
      tx.trackRebasableInsert(fileId, 0, 3, image);
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
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.get(0), errors.get(0));

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
      return (int) conflicts.stream().filter(Issue6129ChunkedSlotMergeTest::isReadSideConflict).count();
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
