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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13050: inside a {@code REPEATABLE_READ} transaction an index lookup answered NO entry for a key committed before the
 * transaction began and never touched since, once an INCREMENTAL compaction of the index overlapped the transaction.
 * <p>
 * The compacted file keeps its series and appends a new one per incremental round, publishing it by rewriting its header page
 * (page 0), while the swap puts in a NEW, empty mutable file. The transaction had pinned the header it first read, which names
 * the series of the earlier round only, and read the new mutable file afterwards: the two never coexisted, and the entries moved
 * by the round (living in the new series alone) were visible in neither. The same stale header fed the uniqueness check, so a
 * duplicate of a key the transaction could not see was accepted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue13050RepeatableReadCompactionTest extends TestHelper {
  private static final String TYPE_NAME    = "T";
  private static final int    FIRST_ROUND  = 500;
  private static final int    SECOND_ROUND = 300;
  private static final int[]  PINNING_KEYS = { 3, 77, 150, 260, 410 };

  @BeforeEach
  void noBackgroundCompaction() {
    // the compactions of the test are the explicit ones: a mutable file schedules its own once it holds this many pages
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
  }

  @Test
  void aPointLookupFindsEveryCommittedKeyAfterAnIncrementalCompaction() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);
      compactFromAnotherThread(index);

      assertThat(missingKeys(index, 0, FIRST_ROUND + SECOND_ROUND)).as("keys the transaction cannot find").isEmpty();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aSqlEqualityFindsTheRecordAfterAnIncrementalCompaction() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);
      compactFromAnotherThread(index);

      // the keys of the last page of the first round are the ones the stale header hid
      for (int k = FIRST_ROUND - 34; k < FIRST_ROUND; ++k) {
        try (final ResultSet result = database.query("sql", "select from " + TYPE_NAME + " where k = ?", k)) {
          assertThat(result.stream().count()).as("rows with k = " + k).isEqualTo(1);
        }
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void anOrderedScanAndARangeSeeEveryCommittedKeyAfterAnIncrementalCompaction() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);
      compactFromAnotherThread(index);

      assertThat(count(index.iterator(true))).as("entries of the ascending scan").isEqualTo(FIRST_ROUND + SECOND_ROUND);
      assertThat(count(index.iterator(false))).as("entries of the descending scan").isEqualTo(FIRST_ROUND + SECOND_ROUND);
      assertThat(count(index.range(true, new Object[] { 0 }, true, new Object[] { FIRST_ROUND - 1 }, true))).as(
          "entries of the range over the keys the first round moved").isEqualTo(FIRST_ROUND);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aScanThatPinnedTheHeaderDoesNotHideTheSeriesOfALaterCompaction() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      // the first read of the index is a scan, which reads the header of the compacted file as well
      assertThat(count(index.iterator(true))).isEqualTo(FIRST_ROUND + SECOND_ROUND);
      compactFromAnotherThread(index);

      assertThat(missingKeys(index, 0, FIRST_ROUND + SECOND_ROUND)).as("keys the transaction cannot find").isEmpty();
      assertThat(count(index.iterator(true))).as("entries of the scan after the compaction").isEqualTo(FIRST_ROUND + SECOND_ROUND);
    } finally {
      database.rollback();
    }
  }

  @Test
  void everyCompactionRoundOfALongTransactionIsFollowed() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);

      int total = FIRST_ROUND + SECOND_ROUND;
      for (int round = 0; round < 3; ++round) {
        compactFromAnotherThread(index);
        assertThat(missingKeys(index, 0, total)).as("keys the transaction cannot find after round " + round).isEmpty();

        // a commit of another thread between two rounds puts entries in the mutable file, which the next round moves
        final int from = total;
        final Thread writer = new Thread(() -> insert(from, 120));
        run(writer);
        total += 120;
      }
    } finally {
      database.rollback();
    }
  }

  @Test
  void aDuplicateOfAKeyTheCompactionMovedIsRefusedUnderRepeatableRead() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);
      compactFromAnotherThread(index);

      assertThatThrownBy(() -> {
        database.newDocument(TYPE_NAME).set("k", FIRST_ROUND - 20).save();
        database.commit();
      }).isInstanceOf(DuplicatedKeyException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(database.countType(TYPE_NAME, false)).as("records after the refused duplicate")
        .isEqualTo(FIRST_ROUND + SECOND_ROUND);
  }

  @Test
  void aReadCommittedTransactionFindsEveryCommittedKeyAfterAnIncrementalCompaction() {
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    try {
      pinThePagesOfTheIndex(index);
      compactFromAnotherThread(index);

      assertThat(missingKeys(index, 0, FIRST_ROUND + SECOND_ROUND)).as("keys the transaction cannot find").isEmpty();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aFullCompactionDoesNotHideTheKeysFromARepeatableReadTransaction() {
    GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES.setValue(1);
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      pinThePagesOfTheIndex(index);
      // any compaction that finds a compacted sub-index goes full here: it writes a NEW compacted file
      compactFromAnotherThread(index);

      assertThat(missingKeys(index, 0, FIRST_ROUND + SECOND_ROUND)).as("keys the transaction cannot find").isEmpty();
    } finally {
      database.rollback();
    }
  }

  @Test
  void aKeyReadBeforeTheCompactionAnswersTheSameAfterIt() {
    // only the header is read as committed: the pages of a published series never change and stay pinned, so what the
    // transaction reads again is what it read
    final TypeIndex index = createTypeAndFirstCompaction();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final List<Object> before = new ArrayList<>();
      for (final int key : PINNING_KEYS)
        before.add(lookup(index, key));

      compactFromAnotherThread(index);

      final List<Object> after = new ArrayList<>();
      for (final int key : PINNING_KEYS)
        after.add(lookup(index, key));

      assertThat(after).isEqualTo(before);
    } finally {
      database.rollback();
    }
  }

  @Test
  void theHeaderPageIsReadAsCommittedAndNeverPinnedWhilePinnedPagesAreKept() throws Exception {
    final TypeIndex index = createTypeAndFirstCompaction();
    final LSMTreeIndexCompacted compacted = bucketIndex(index).getMutableIndex().getSubIndex();
    final PageId header = new PageId(database, compacted.getFileId(), 0);
    final int pageSize = compacted.getPageSize();

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      // getPage keeps the page it reads first for the rest of the transaction
      final long pinnedVersion = tx.getPage(header, pageSize).getVersion();
      assertThat(tx.getPageForAbsoluteReadsNeverPinned(header, pageSize).getVersion()).isEqualTo(pinnedVersion);

      // an incremental round rewrites the header to name its series
      compactFromAnotherThread(index);

      assertThat(tx.getPage(header, pageSize).getVersion()).as("version of the pinned header").isEqualTo(pinnedVersion);
      assertThat(tx.getPageForAbsoluteReadsNeverPinned(header, pageSize).getVersion()).as("version of the header read as committed")
          .isGreaterThan(pinnedVersion);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aHeaderPageTheTransactionWroteIsReadAsItsOwn() throws Exception {
    final TypeIndex index = createTypeAndFirstCompaction();
    final LSMTreeIndexCompacted compacted = bucketIndex(index).getMutableIndex().getSubIndex();
    final int pageSize = compacted.getPageSize();
    final PageId header = new PageId(database, compacted.getFileId(), 0);
    final PageId appended = new PageId(database, compacted.getFileId(), compacted.getTotalPages());

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      final MutablePage modified = tx.getPageToModify(header, pageSize, false);
      assertThat(tx.getPageForAbsoluteReadsNeverPinned(header, pageSize)).as("a page the transaction modified").isSameAs(modified);

      final MutablePage created = tx.addPage(appended, pageSize);
      assertThat(tx.getPageForAbsoluteReadsNeverPinned(appended, pageSize)).as("a page the transaction created").isSameAs(created);
    } finally {
      database.rollback();
    }
  }

  private TypeIndex createTypeAndFirstCompaction() {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("k", Type.INTEGER);
    // a small page makes the entries of a round span many pages, so the first round leaves an open page behind for the mutable file
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(1_024).create();

    insert(0, FIRST_ROUND);

    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(TYPE_NAME + "[k]");
    compact(index);
    assertThat(bucketIndex(index).getMutableIndex().getSubIndex()).as("the compacted file exists").isNotNull();

    insert(FIRST_ROUND, SECOND_ROUND);
    return index;
  }

  private void insert(final int from, final int count) {
    database.transaction(() -> {
      for (int k = from; k < from + count; ++k)
        database.newDocument(TYPE_NAME).set("k", k).save();
    });
  }

  private static void compact(final TypeIndex index) {
    try {
      assertThat(index.scheduleCompaction()).as("the compaction is scheduled").isTrue();
      assertThat(index.compact()).as("the compaction ran").isTrue();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** The compaction runs on a thread of its own: it refuses to swap the files under a transaction of the calling thread. */
  private void compactFromAnotherThread(final TypeIndex index) {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    run(new Thread(() -> {
      try {
        compact(index);
      } catch (final Throwable t) {
        failure.set(t);
      }
    }));
    assertThat(failure.get()).as("failure of the compaction").isNull();
  }

  private static void run(final Thread thread) {
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /** Reads keys of the first series, which pins the pages of the index the transaction goes on to read. */
  private static void pinThePagesOfTheIndex(final TypeIndex index) {
    for (final int key : PINNING_KEYS)
      assertThat(index.get(new Object[] { key }).hasNext()).as("key " + key).isTrue();
  }

  private static List<Integer> missingKeys(final TypeIndex index, final int from, final int to) {
    final List<Integer> missing = new ArrayList<>();
    for (int k = from; k < to; ++k)
      if (!index.get(new Object[] { k }).hasNext())
        missing.add(k);
    return missing;
  }

  private static Object lookup(final TypeIndex index, final int key) {
    final IndexCursor cursor = index.get(new Object[] { key });
    return cursor.hasNext() ? cursor.next().getIdentity() : null;
  }

  private static long count(final IndexCursor cursor) {
    long total = 0;
    // hasNext() of a scan is optimistic: what next() answers is the entry
    while (cursor.hasNext())
      if (cursor.next() != null)
        ++total;
    return total;
  }

  private static LSMTreeIndex bucketIndex(final TypeIndex index) {
    return (LSMTreeIndex) index.getIndexesOnBuckets()[0];
  }
}
