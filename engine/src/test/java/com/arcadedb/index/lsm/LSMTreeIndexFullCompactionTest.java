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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Full ("2nd level") compaction: when the compacted sub-index accumulates
 * {@link GlobalConfiguration#INDEX_COMPACTION_FULL_SERIES} series, compact() merges ALL existing
 * series together with the mutable immutable pages into a single fresh series in a NEW compacted
 * file, resolves each key's tombstone history (a key-wide tombstone clears everything before it, a
 * per-RID tombstone kills only its target, the newest operation on a RID wins) and drops dead
 * entries entirely. Without it, tombstones survive every compaction round forever - delete-heavy
 * workloads accumulate unbounded dead-key runs that every range scan must skip - and the series
 * count grows monotonically, slowing every cursor construction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexFullCompactionTest extends TestHelper {
  private static final String TYPE_NAME = "Doc";

  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    // any compaction finding an existing compacted sub-index goes full
    GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES.setValue(1);
  }

  @Test
  void fullCompactionDropsTombstonedKeys() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    insert(0, 10_000);
    compact(index); // incremental: creates the compacted file with series 1

    deleteRange(0, 9_000);
    final long filesBefore = registeredFiles();
    final int pagesBefore = index.getMutableIndex().getSubIndex().getTotalPages();

    compact(index); // full: merges series 1 + the tombstoned mutable pages, drops dead keys

    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
    assertThat(compacted.getSeriesCount()).as("all series must collapse into one").isEqualTo(1);
    assertThat(compacted.getTotalPages())
        .as("dropping 90%% of the keys must shrink the compacted file")
        .isLessThan(pagesBefore);
    assertThat(registeredFiles()).as("the old compacted file must be reclaimed").isEqualTo(filesBefore);

    assertThat(countRange(0, 10_000)).isEqualTo(1_000);
    database.transaction(() -> {
      assertThat(index.get(new Object[] { 5 }).hasNext()).as("deleted key").isFalse();
      assertThat(index.get(new Object[] { 9_500 }).hasNext()).as("live key").isTrue();
    });
  }

  @Test
  void fullCompactionKeepsReAddedKeys() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    insert(0, 1_000);
    compact(index);

    deleteRange(0, 1_000);
    insert(500, 1_000); // re-add the upper half AFTER the delete: it must survive

    compact(index); // full

    assertThat(countRange(0, 1_000)).isEqualTo(500);
    database.transaction(() -> {
      assertThat(index.get(new Object[] { 499 }).hasNext()).isFalse();
      assertThat(index.get(new Object[] { 500 }).hasNext()).isTrue();
    });
  }

  @Test
  void fullCompactionResolvesPerRidTombstones() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    // two records share the same key
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      victim.set(database.newDocument(TYPE_NAME).set("id", 42).save().getIdentity());
      database.newDocument(TYPE_NAME).set("id", 42).save();
    });
    insert(1_000, 2_000); // filler so compaction has pages to work with
    compact(index);

    database.transaction(() -> database.deleteRecord(victim.get().getRecord()));

    compact(index); // full

    database.transaction(() -> {
      final IndexCursor cursor = index.get(new Object[] { 42 });
      final List<RID> found = new ArrayList<>();
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
      assertThat(found).as("only the surviving record must remain").hasSize(1);
      assertThat(found.getFirst()).isNotEqualTo(victim.get());
    });
  }

  @Test
  void fullCompactionResolvesKeyWideTombstones() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    insert(0, 2_000);
    compact(index);

    // key-wide removal (remove(keys) without rid): everything before it dies
    database.transaction(() -> index.remove(new Object[] { 77 }));

    compact(index); // full

    database.transaction(() -> assertThat(index.get(new Object[] { 77 }).hasNext()).isFalse());
    assertThat(countRange(0, 2_000)).isEqualTo(1_999);
  }

  @Test
  void thresholdZeroDisablesFullCompaction() throws Exception {
    GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES.setValue(0);
    final LSMTreeIndex index = createTypeAndIndex();
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 0);

    insert(0, 5_000);
    compact(index);
    deleteRange(0, 4_000);
    compact(index); // incremental: appends series 2, keeps the tombstones

    assertThat(index.getMutableIndex().getSubIndex().getSeriesCount()).isEqualTo(2);
    assertThat(countRange(0, 5_000)).isEqualTo(1_000);
  }

  @Test
  void failedFullCompactionLeavesIndexIntact() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    insert(0, 5_000);
    compact(index);
    deleteRange(0, 4_000);

    final long filesBefore = registeredFiles();
    final int seriesBefore = index.getMutableIndex().getSubIndex().getSeriesCount();

    LSMTreeIndexCompactor.setCompactionTestHook(compactedIndex -> {
      throw new IOException("injected failure during full compaction");
    });
    try {
      assertThat(index.scheduleCompaction()).isTrue();
      assertThatThrownBy(index::compact).isInstanceOf(IOException.class)
          .hasMessageContaining("injected failure during full compaction");
    } finally {
      LSMTreeIndexCompactor.setCompactionTestHook(null);
    }

    assertThat(registeredFiles()).as("the aborted new compacted file must not leak").isEqualTo(filesBefore);
    assertThat(index.getMutableIndex().getSubIndex().getSeriesCount()).isEqualTo(seriesBefore);
    assertThat(countRange(0, 5_000)).isEqualTo(1_000);

    // and a subsequent full compaction succeeds
    compact(index);
    assertThat(index.getMutableIndex().getSubIndex().getSeriesCount()).isEqualTo(1);
    assertThat(countRange(0, 5_000)).isEqualTo(1_000);
  }

  @Test
  void inFlightScanSurvivesFullCompaction() throws Exception {
    final LSMTreeIndex index = createTypeAndIndex();

    insert(0, 5_000);
    compact(index);
    deleteRange(0, 2_000);

    final long filesBefore = registeredFiles();

    database.begin();
    try {
      // the scan captures cursors over the CURRENT files, including the compacted series it loads lazily
      final IndexCursor cursor = index.range(true, new Object[] { 0 }, true, new Object[] { 5_000 }, true);
      int count = 0;
      // consume a first chunk before the compaction swaps the files
      while (count < 500 && cursor.hasNext()) {
        cursor.next();
        ++count;
      }

      // full compaction on another thread (this thread holds an open transaction)
      final AtomicReference<Throwable> error = new AtomicReference<>();
      final Thread compactor = new Thread(() -> {
        try {
          compact(index);
        } catch (final Throwable e) {
          error.set(e);
        }
      }, "full-compactor");
      compactor.start();
      compactor.join(120_000);
      assertThat(error.get()).as("full compaction must complete").isNull();

      // the scan must finish correctly on its snapshot: the old compacted file cannot be dropped
      // while this cursor still reads it
      while (cursor.hasNext())
        if (cursor.next() != null)
          ++count;
      assertThat(count).isEqualTo(3_000);
      cursor.close();
    } finally {
      database.commit();
    }

    // with the reader gone, the next compaction reclaims the retired file
    insert(10_000, 11_000);
    compact(index);
    assertThat(registeredFiles()).isEqualTo(filesBefore);
    assertThat(countRange(0, 11_000)).isEqualTo(4_000);
  }

  // ---------------------------------------------------------------------------------------------

  private LSMTreeIndex createTypeAndIndex() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_FULL_SERIES, 1);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(4_096).create();

    if (database.isTransactionActive())
      database.commit();

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private void insert(final int fromInclusive, final int toExclusive) {
    database.transaction(() -> {
      for (int i = fromInclusive; i < toExclusive; ++i)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });
  }

  private void deleteRange(final int fromInclusive, final int toExclusive) {
    final List<RID> toDelete = new ArrayList<>();
    database.transaction(() -> {
      for (final Iterator<Record> it = database.iterateType(TYPE_NAME, false); it.hasNext(); ) {
        final Document doc = (Document) it.next();
        final Integer id = doc.getInteger("id");
        if (id >= fromInclusive && id < toExclusive)
          toDelete.add(doc.getIdentity());
      }
    });
    database.transaction(() -> {
      for (final RID rid : toDelete)
        database.deleteRecord(rid.getRecord());
    });
  }

  private void compact(final LSMTreeIndex index) throws Exception {
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isTrue();
  }

  private int countRange(final int fromInclusive, final int toExclusive) {
    final int[] count = { 0 };
    database.transaction(() -> {
      final IndexCursor cursor = index().range(true, new Object[] { fromInclusive }, true, new Object[] { toExclusive }, true);
      while (cursor.hasNext())
        if (cursor.next() != null)
          count[0]++;
    });
    return count[0];
  }

  private LSMTreeIndex index() {
    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }

  private long registeredFiles() {
    return ((DatabaseInternal) database).getFileManager().getFiles().stream().filter(Objects::nonNull).count();
  }
}
