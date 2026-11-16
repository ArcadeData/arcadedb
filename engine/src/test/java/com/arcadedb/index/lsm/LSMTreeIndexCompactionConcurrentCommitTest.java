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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12206: an LSM-tree compaction must not lose what a commit writes to the index while the compaction runs.
 * <p>
 * The compactor merges the pages of the mutable file up to the last SEALED one without any lock, and the swap that ends
 * it copies the pages after them into the new mutable file under the commit lock. A sealed page never changes again, so
 * nothing a commit writes can fall between the two. A mutable file holding no sealed page is the exception: the empty
 * first page a swap leaves behind plus the page it carried over, still open for writes. The compactor merged that page
 * too, without the lock, and the swap then copied nothing: whatever a commit wrote to it after the merge had read it
 * reached neither the compacted series nor the new mutable file, and the old file was dropped. Records inserted in that
 * window had no index entry (a UNIQUE index then accepted their key a second time) and deleted records kept theirs.
 * <p>
 * Each test holds the compactor right after it has written the merged series and before the swap, through
 * {@link LSMTreeIndexCompactor#setCompactionTestHook}, and commits an insert and a delete from another thread there.
 *
 * @author Shaun Patterson (shaunpatterson@gmail.com)
 */
class LSMTreeIndexCompactionConcurrentCommitTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";
  private static final int    PAGE_SIZE = 2_048;
  private static final int    RECORDS   = 300;

  @BeforeEach
  void noBackgroundCompaction() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
  }

  @AfterEach
  void removeHook() {
    LSMTreeIndexCompactor.setCompactionTestHook(null);
  }

  @Test
  void commitOnTheOpenPageDuringAnIncrementalCompactionIsKept() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex(Schema.INDEX_TYPE.LSM_TREE);
    insertKeys(0, RECORDS);
    compact(index);
    assertOnlyTheOpenPageHoldsEntries(index);

    final ConcurrentCommit commit = new ConcurrentCommit("late", key(7));
    LSMTreeIndexCompactor.setCompactionTestHook(commit::runWhileTheCompactionWaits);
    compact(index);
    commit.awaitEnd();

    assertCommitKept(commit, RECORDS);
  }

  @Test
  void commitOnTheOpenPageDuringAFullCompactionIsKept() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 1);
    final LSMTreeIndex index = createTypeAndIndex(Schema.INDEX_TYPE.LSM_TREE);
    insertKeys(0, RECORDS);
    compact(index);
    assertOnlyTheOpenPageHoldsEntries(index);

    final ConcurrentCommit commit = new ConcurrentCommit("late", key(7));
    LSMTreeIndexCompactor.setCompactionTestHook(commit::runWhileTheCompactionWaits);
    compact(index); // a compacted sub-index exists and the threshold is 1: this round is a full one
    commit.awaitEnd();

    assertThat(index.getMutableIndex().getSubIndex().getSeriesCount()).as("the full compaction ran").isEqualTo(1);
    assertCommitKept(commit, RECORDS);
  }

  /**
   * The case the merge was right about: with a sealed page in the mutable file, only the pages up to it are merged and the open page
   * is copied by the swap, so a commit is neither held up nor lost.
   */
  @Test
  void commitOnTheOpenPageBesideASealedPageIsNotHeldUp() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex(Schema.INDEX_TYPE.LSM_TREE);
    insertKeys(0, RECORDS);
    compact(index);
    insertKeys(RECORDS, 2 * RECORDS);
    assertThat(sealedPages(index)).as("the mutable file holds sealed pages").isPositive();

    final ConcurrentCommit commit = new ConcurrentCommit("late", key(7));
    LSMTreeIndexCompactor.setCompactionTestHook(commit::runWhileTheCompactionWaits);
    compact(index);
    commit.awaitEnd();

    assertThat(commit.committedBeforeTheSwap.get()).as("the commit ran while the compaction was held").isTrue();
    assertCommitKept(commit, 2 * RECORDS);
  }

  /** A full-text index is an LSM-tree index underneath and goes through the same compactor. */
  @Test
  void fullTextCommitOnTheOpenPageDuringACompactionIsKept() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex(Schema.INDEX_TYPE.FULL_TEXT);
    insertKeys(0, RECORDS);
    compact(index);
    assertOnlyTheOpenPageHoldsEntries(index);

    final ConcurrentCommit commit = new ConcurrentCommit("latecomer", key(7));
    LSMTreeIndexCompactor.setCompactionTestHook(commit::runWhileTheCompactionWaits);
    compact(index);
    commit.awaitEnd();

    assertThat(commit.error.get()).isNull();
    assertThat(searchFullText("latecomer")).as("the word of the record committed during the compaction").hasSize(1);
    assertThat(searchFullText(key(7))).as("the word of the record deleted during the compaction").isEmpty();
    assertThat(searchFullText(key(8))).hasSize(1);
  }

  /**
   * Inserts a record with key {@code newKey} and deletes the record with key {@code deletedKey} from another thread, as soon as
   * the compaction has written its merged series and before it swaps the files in.
   */
  private class ConcurrentCommit {
    private final String                     newKey;
    private final String                     deletedKey;
    private final AtomicReference<Throwable> error                  = new AtomicReference<>();
    private final AtomicBoolean              committed              = new AtomicBoolean();
    private final AtomicBoolean              committedBeforeTheSwap = new AtomicBoolean();
    private final AtomicReference<RID>       insertedRid            = new AtomicReference<>();
    private final RID                        deletedRid;
    private       Thread                     writer;

    ConcurrentCommit(final String newKey, final String deletedKey) {
      this.newKey = newKey;
      this.deletedKey = deletedKey;
      this.deletedRid = findRecord(deletedKey);
    }

    void runWhileTheCompactionWaits(final LSMTreeIndexCompacted ignored) {
      if (writer != null)
        return;

      writer = new Thread(() -> {
        try {
          database.transaction(() -> {
            insertedRid.set(database.newDocument(TYPE_NAME).set("k", newKey).save().getIdentity());
            database.deleteRecord(deletedRid.getRecord());
          });
          committed.set(true);
        } catch (final Throwable e) {
          error.set(e);
        }
      }, "issue-12206-writer");
      writer.start();

      // The commit either completes here, or waits for the lock the compaction holds on the file it writes to and
      // completes once the swap has released it
      Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(5))
          .until(() -> committed.get() || error.get() != null || waitsForAFileLock(writer));
      committedBeforeTheSwap.set(committed.get());
    }

    void awaitEnd() throws InterruptedException {
      assertThat(writer).as("the compaction reached the point before its swap").isNotNull();
      writer.join(60_000);
      assertThat(writer.isAlive()).isFalse();
    }
  }

  private void assertCommitKept(final ConcurrentCommit commit, final int recordsBefore) {
    assertThat(commit.error.get()).isNull();
    assertThat(commit.committed.get()).isTrue();

    database.transaction(() -> {
      final TypeIndex typeIndex = typeIndex();
      assertThat(ridsOf(typeIndex.get(new Object[] { commit.newKey })))
          .as("the record inserted while the compaction ran").containsExactly(commit.insertedRid.get());
      assertThat(ridsOf(typeIndex.get(new Object[] { commit.deletedKey })))
          .as("the record deleted while the compaction ran").isEmpty();
    });

    assertIndexMatchesRecords(recordsBefore);

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).set("k", commit.newKey).save()))
        .as("the UNIQUE index still refuses the key").isInstanceOf(DuplicatedKeyException.class);
  }

  /** Every record has exactly its entry in the index, and the index has no other entry. */
  private void assertIndexMatchesRecords(final int recordsBefore) {
    final Set<String> records = new HashSet<>();
    final Set<String> entries = new HashSet<>();
    database.transaction(() -> {
      for (final Iterator<Record> it = database.iterateType(TYPE_NAME, false); it.hasNext(); ) {
        final Document doc = (Document) it.next();
        records.add(doc.getString("k") + "|" + doc.getIdentity());
      }
      final IndexCursor cursor = typeIndex().iterator(true);
      while (cursor.hasNext()) {
        final RID rid = cursor.next().getIdentity();
        assertThat(entries.add(cursor.getKeys()[0] + "|" + rid)).as("the index returns %s once", rid).isTrue();
      }
    });
    assertThat(records).hasSize(recordsBefore);
    assertThat(entries).isEqualTo(records);
  }

  private void assertOnlyTheOpenPageHoldsEntries(final LSMTreeIndex index) throws IOException {
    final LSMTreeIndexMutable mutable = index.getMutableIndex();
    assertThat(mutable.getTotalPages()).as("the empty first page of the swap and the open page it carried over").isEqualTo(2);
    assertThat(sealedPages(index)).as("no page of the mutable file is sealed").isZero();
  }

  private int sealedPages(final LSMTreeIndex index) throws IOException {
    final LSMTreeIndexMutable mutable = index.getMutableIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    int sealed = 0;
    for (int p = 0; p < mutable.getTotalPages(); ++p) {
      final ImmutablePage page = db.getPageManager()
          .getImmutablePage(new PageId(db, mutable.getFileId(), p), mutable.getPageSize(), false, false);
      if (!mutable.isMutable(page))
        ++sealed;
    }
    return sealed;
  }

  private static boolean waitsForAFileLock(final Thread thread) {
    final Thread.State state = thread.getState();
    if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING)
      return false;
    for (final StackTraceElement frame : thread.getStackTrace())
      if (frame.getClassName().endsWith(".LockManager") && frame.getMethodName().equals("tryLock"))
        return true;
    return false;
  }

  private LSMTreeIndex createTypeAndIndex(final Schema.INDEX_TYPE indexType) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("k", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(indexType)
          .withUnique(indexType == Schema.INDEX_TYPE.LSM_TREE).withPageSize(PAGE_SIZE).create();
    });
    final IndexInternal bucketIndex = typeIndex().getIndexesOnBuckets()[0];
    return (LSMTreeIndex) ((LSMTreeIndexMutable) bucketIndex.getComponent()).getMainIndex();
  }

  private TypeIndex typeIndex() {
    return database.getSchema().getType(TYPE_NAME).getIndexesByProperties("k").getFirst();
  }

  private void insertKeys(final int fromInclusive, final int toExclusive) {
    database.transaction(() -> {
      for (int i = fromInclusive; i < toExclusive; ++i)
        database.newDocument(TYPE_NAME).set("k", key(i)).save();
    });
  }

  private RID findRecord(final String key) {
    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE k = ?", key)) {
        rid.set(rs.next().getIdentity().orElseThrow());
      }
    });
    return rid.get();
  }

  private Set<RID> searchFullText(final String word) {
    final Set<RID> found = new HashSet<>();
    database.transaction(() -> found.addAll(ridsOf(typeIndex().get(new Object[] { word }))));
    return found;
  }

  private static Set<RID> ridsOf(final IndexCursor cursor) {
    final Set<RID> rids = new HashSet<>();
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      assertThat(rids.add(rid)).as("the lookup returns %s once", rid).isTrue();
    }
    return rids;
  }

  private static String key(final int i) {
    return "k%05d".formatted(i);
  }

  private static void compact(final LSMTreeIndex index) throws Exception {
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isTrue();
  }
}
