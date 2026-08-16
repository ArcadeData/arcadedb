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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue6217ChunkedReadFalseConflictTest extends BucketPageLayoutTestSupport {
  private static final int RECORDS        = 12;
  private static final int WRITER_THREADS = 4;
  private static final int ROUNDS         = 25;

  /** Payload length every record ends up with once they have all spilled out of their shared page. */
  private int spilledPayloadSize;

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

    final long revalidationsBefore = pageManager.getStats().chunkChainReadRevalidations;

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

    assertThat(pageManager.getStats().chunkChainReadRevalidations - revalidationsBefore)
        .as("the neighbour's write must have moved a page the read walked, or this test proves nothing")
        .isPositive();

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
  void aChunkedReadStillFailsWhenTheRecordItselfMovedUnderIt() {
    final RID[] rids = createChunkedRecords("ChunkedReadConflict");
    final LocalBucket bucket = bucketOf("ChunkedReadConflict");

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      bucket.getRecordInternal(rids[0], false);

      inAnotherThread(() -> database.transaction(
          () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

      assertThatThrownBy(() -> bucket.getRecordInternal(rids[0], false))
          .as("a chunked record rewritten under a read must not be assembled out of two commits")
          .isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining("was modified during read");
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
              for (int i = 1 + id; i < RECORDS; i += WRITER_THREADS) {
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

  /** Creates {@code RECORDS} records whose slots all live on page 0 and grows them until every one has spilled. */
  private RID[] createChunkedRecords(final String typeName) {
    final RID[] rids = new RID[RECORDS];
    database.transaction(() -> {
      database.getSchema().createDocumentType(typeName, 1).createProperty("payload", Type.STRING);
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument(typeName).set("payload", "r" + i).save().getIdentity();
    });

    // A record only spills once its page cannot host its growth, and a record that spilled leaves its head chunk
    // behind, so the shared page fills up in steps: keep growing everybody until the last one has left.
    for (int size = 1_000; size <= 24_000; size += 1_000) {
      spilledPayloadSize = size;
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          rids[i].asDocument(true).modify().set("payload", payload(i, 'x')).save();
      });

      if (RECORDS == (Long) bucketStats(typeName).get("totalMultiPageRecords"))
        return rids;
    }
    throw new AssertionError(
        "Not every record of " + typeName + " spilled into a chunk chain: " + bucketStats(typeName));
  }

  /** Same length for every record and every round, so a rewrite reuses the chunk chain the record already has. */
  private String payload(final int record, final char filler) {
    final String marker = "r" + record + "-";
    return marker + String.valueOf(filler).repeat(spilledPayloadSize - marker.length());
  }

  private LocalBucket bucketOf(final String typeName) {
    return (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).getFirst();
  }

  /** Runs {@code body} on a thread of its own, so it commits in a transaction other than the caller's. */
  private void inAnotherThread(final Runnable body) {
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final Thread thread = new Thread(() -> {
      try {
        body.run();
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "issue6217-writer");
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
    if (!errors.isEmpty())
      throw new AssertionError("the concurrent write failed: " + errors.getFirst(), errors.getFirst());
  }
}
