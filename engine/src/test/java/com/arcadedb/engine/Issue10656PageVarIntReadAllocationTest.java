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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Issue #10656: reading a variable-length number off a page used to allocate a {@code long[2]} (value and bytes read)
 * every time, once per record header of every scan and lookup. {@link BasePage#readNumber(int)} returns the value and
 * {@link BasePage#readNumberSize(int)} the byte count, so the hot read paths allocate nothing.
 */
class Issue10656PageVarIntReadAllocationTest extends TestHelper {

  // EVERY ENCODED LENGTH, BOTH SIGNS, AND THE RECORD MARKERS THE BUCKET STORES IN A RECORD HEADER
  private static final long[] VALUES = { 0L, 1L, -1L, 63L, -64L, 64L, -65L, 127L, 128L, 8191L, -8192L, 8192L, 1L << 20, -(1L << 20),
      1L << 27, 1L << 34, 1L << 41, 1L << 48, 1L << 55, -(1L << 55), 1L << 62, Long.MAX_VALUE, Long.MIN_VALUE,
      LocalBucket.FIRST_CHUNK, LocalBucket.NEXT_CHUNK, LocalBucket.RECORD_PLACEHOLDER_POINTER, -5L };

  @Test
  void readNumberAgreesWithReadNumberAndSizeOnEveryLength() {
    final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
    final int[] positions = new int[VALUES.length];
    final int[] written = new int[VALUES.length];
    int position = 0;
    for (int i = 0; i < VALUES.length; i++) {
      positions[i] = position;
      written[i] = page.writeNumber(position, VALUES[i]);
      position += written[i];
    }

    for (int i = 0; i < VALUES.length; i++) {
      final long value = page.readNumber(positions[i]);
      final int size = page.readNumberSize(positions[i]);
      assertThat(value).as("value at #%d", i).isEqualTo(VALUES[i]);
      assertThat(size).as("bytes read at #%d", i).isEqualTo(written[i]).isEqualTo(Binary.getNumberSpace(VALUES[i]));

      // THE ALLOCATING FORM IS NOW A WRAPPER OF THE SAME READ: IT MUST ANSWER THE SAME PAIR
      final long[] pair = page.readNumberAndSize(positions[i]);
      assertThat(pair[0]).isEqualTo(value);
      assertThat(pair[1]).isEqualTo(size);
    }

    // OUT OF ORDER: THE SIZE IS THE ONE OF THE NUMBER ASKED FOR, NOT OF THE LAST READ NOR OF A RUNNING POSITION (#10930)
    final int last = VALUES.length - 1;
    page.readNumber(positions[last]);
    assertThat(page.readNumberSize(positions[0])).isEqualTo(written[0]);
  }

  @Test
  void readNumberOnAnImmutablePageAgrees() {
    final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
    final int size = page.writeNumber(10, 300_000L);
    final ImmutablePage immutable = new ImmutablePage(page.getPageId(), 1024, page.getContent().array(), 0, 1024);

    assertThat(immutable.readNumber(10)).isEqualTo(300_000L);
    assertThat(immutable.readNumberSize(10)).isEqualTo(size);
  }

  @Test
  void readNumberPastThePageFailsLikeReadNumberAndSize() {
    final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
    assertThatThrownBy(() -> page.readNumberAndSize(1_000_000)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> page.readNumber(1_000_000)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> page.readNumberSize(1_000_000)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readNumberAllocatesNothing() {
    assumeThat(ManagementFactory.getThreadMXBean()).isInstanceOf(ThreadMXBean.class);
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeThat(threads.isThreadAllocatedMemorySupported()).isTrue();
    threads.setThreadAllocatedMemoryEnabled(true);

    final MutablePage page = new MutablePage(new PageId(database, 1, 0), 1024);
    int position = 0;
    for (final long v : VALUES)
      position += page.writeNumber(position, v);
    final int end = position;

    final int rounds = 100_000;
    long checksum = readAll(page, end, 20_000); // WARM UP: LET THE JIT COMPILE THE LOOP BEFORE MEASURING
    final long threadId = Thread.currentThread().threadId();
    final long before = threads.getThreadAllocatedBytes(threadId);
    checksum += readAll(page, end, rounds);
    final long allocated = threads.getThreadAllocatedBytes(threadId) - before;

    assertThat(checksum).isNotZero();
    // A long[2] IS 32 BYTES: ONE PER READ WOULD BE rounds * VALUES.length * 32, ABOUT 86 MB HERE. THE BOUND LEAVES ROOM
    // FOR THE JVM'S OWN INCIDENTAL ALLOCATIONS ON THIS THREAD, NOT FOR ONE ARRAY PER READ
    assertThat(allocated).as("bytes allocated by %d varint reads", rounds * VALUES.length).isLessThan(256 * 1024L);
  }

  /**
   * The read paths that moved to the allocation-free form, end to end: a scan, an iterator, a lookup and a count over
   * in-page records, records that grew after they were written, and records split in chunks.
   */
  @Test
  void bucketReadPathsStillAnswerEveryRecordShape() {
    final DocumentType type = database.getSchema().createDocumentType("VarIntRead", 1);
    final List<RID> rids = new ArrayList<>();
    final int pageSize = ((LocalBucket) type.getBuckets(false).getFirst()).getPageSize();

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        final MutableDocument doc = database.newDocument("VarIntRead");
        doc.set("i", i);
        doc.set("text", "x".repeat(i % 10 == 0 ? pageSize * 2 : 10)); // EVERY 10TH SPANS PAGES
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    // GROW SOME SMALL RECORDS PAST THEIR SLOT, SO THEY ARE SHIFTED IN THEIR PAGE OR SPILLED OFF IT
    database.transaction(() -> {
      for (int i = 1; i < 50; i += 7) {
        final MutableDocument doc = rids.get(i).asDocument(true).modify();
        doc.set("text", "y".repeat(2_000));
        doc.save();
      }
    });

    database.transaction(() -> {
      assertThat(database.countType("VarIntRead", false)).isEqualTo(50);

      final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
      assertThat(bucket.count()).isEqualTo(50);

      final boolean[] seenByIterator = new boolean[50];
      bucket.iterator().forEachRemaining(r -> seenByIterator[r.asDocument().getInteger("i")] = true);
      for (final boolean seen : seenByIterator)
        assertThat(seen).isTrue();

      final int[] scanned = { 0 };
      bucket.scan((rid, view) -> {
        scanned[0]++;
        return true;
      }, null);
      assertThat(scanned[0]).isEqualTo(50);

      for (int i = 0; i < 50; i++) {
        final String text = rids.get(i).asDocument(false).getString("text");
        if (i % 7 == 1)
          assertThat(text).isEqualTo("y".repeat(2_000));
        else
          assertThat(text).hasSize(i % 10 == 0 ? pageSize * 2 : 10);
      }
    });
  }

  /**
   * The write-side readers that moved to the allocation-free form: the slot marker read of the existence and layout
   * checks, the content end an insert appends at, the per-slot walk of the commit-time page compression, and the
   * re-read of a record taken for update.
   */
  @Test
  void writePathsStillMeasureEveryRecordShape() {
    final DocumentType type = database.getSchema().createDocumentType("VarIntWrite", 1);
    final List<RID> rids = new ArrayList<>();
    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).getFirst();
    final int pageSize = bucket.getPageSize();

    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        final MutableDocument doc = database.newDocument("VarIntWrite");
        doc.set("i", i);
        doc.set("text", "x".repeat(i % 10 == 0 ? pageSize * 2 : 10)); // EVERY 10TH SPANS PAGES
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        assertThat(bucket.isRecordPresent(rids.get(i))).as("record %d present", i).isTrue();
        // AN IN-PAGE RECORD HAS A POSITIVE MARKER, A MULTI-PAGE ONE FIRST_CHUNK
        assertThat(bucket.isRecordStoredInSinglePage(rids.get(i))).as("record %d in one page", i).isEqualTo(i % 10 != 0);
      }
      // A SLOT NEVER WRITTEN, ON A PAGE THAT DOES NOT EXIST
      final RID missing = new RID(rids.getFirst().getBucketId(), 1_000_000_000L);
      assertThat(bucket.isRecordPresent(missing)).isFalse();
      assertThat(bucket.isRecordStoredInSinglePage(missing)).isFalse();
    });

    // DELETE SOME, GROW OTHERS (A RECORD TAKEN FOR UPDATE IS RE-READ AND COMPARED), THEN APPEND MORE: EVERY COMMIT
    // COMPRESSES THE PAGES IT TOUCHED AND EVERY INSERT MEASURES WHERE THE PAGE CONTENT ENDS
    database.transaction(() -> {
      for (int i = 3; i < 40; i += 9)
        rids.get(i).asDocument(true).delete();
      for (int i = 1; i < 40; i += 6) {
        if (i % 9 == 3)
          continue;
        final MutableDocument doc = rids.get(i).asDocument(true).modify();
        doc.set("text", "y".repeat(3_000));
        doc.save();
      }
    });
    database.transaction(() -> {
      for (int i = 40; i < 60; i++) {
        final MutableDocument doc = database.newDocument("VarIntWrite");
        doc.set("i", i);
        doc.set("text", "z".repeat(i % 5 == 0 ? 1_500 : 20));
        doc.save();
        rids.add(doc.getIdentity());
      }
    });

    database.transaction(() -> {
      int live = 0;
      for (int i = 0; i < 60; i++) {
        if (i < 40 && i % 9 == 3 && rids.subList(40, 60).contains(rids.get(i)))
          // A DELETED SLOT HANDED TO A LATER INSERT: THE RID IS LIVE AGAIN, CHECKED AND COUNTED UNDER THAT INSERT
          continue;
        final boolean deleted = i < 40 && i % 9 == 3;
        assertThat(bucket.isRecordPresent(rids.get(i))).as("record %d present", i).isEqualTo(!deleted);
        if (deleted)
          continue;
        live++;
        final String text = rids.get(i).asDocument(false).getString("text");
        if (i >= 40)
          assertThat(text).isEqualTo("z".repeat(i % 5 == 0 ? 1_500 : 20));
        else if (i % 6 == 1)
          assertThat(text).isEqualTo("y".repeat(3_000));
        else
          assertThat(text).hasSize(i % 10 == 0 ? pageSize * 2 : 10);
      }
      assertThat(bucket.count()).isEqualTo(live);
      assertThat(database.countType("VarIntWrite", false)).isEqualTo(live);
    });

    // THE PAGES THE WRITES LEFT BEHIND ARE CONSISTENT ON DISK
    assertThat(database.command("sql", "check database").next().<Number>getProperty("totalErrors").longValue()).isZero();
  }

  private static long readAll(final BasePage page, final int end, final int rounds) {
    long checksum = 0;
    for (int r = 0; r < rounds; r++) {
      int position = 0;
      while (position < end) {
        checksum += page.readNumber(position);
        position += page.readNumberSize(position);
      }
    }
    return checksum;
  }
}
