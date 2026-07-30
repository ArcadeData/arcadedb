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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A backup has to carry the bloom filter component (#5517), and a restore has to give it back attached.
 * <p>
 * The filter file is a {@code PaginatedComponent} so the backup enumerates it with everything else - but "it is a
 * component, so it travels" is an argument, and this is the case where being wrong is quiet. Two distinct ways it
 * could fail: the {@code .bfidx} is simply not in the archive, so the restored index reads every series (slower, still
 * correct); or it is restored but no longer resolves to its compacted index, in which case the orphan sweep at open
 * DELETES it as unreferenced - a file silently thrown away on every restore.
 * <p>
 * Neither shows up in a comparison of the two databases' contents, which is what the existing backup tests check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5517BloomFilterBackupIT {
  private static final String SOURCE_PATH   = "target/databases/Issue5517BloomFilterBackup";
  private static final String RESTORED_PATH = "target/databases/Issue5517BloomFilterBackup_restored";
  private static final String BACKUP_FILE   = "target/Issue5517BloomFilterBackup.zip";
  private static final String TYPE_NAME     = "Doc";
  private static final int    TOTAL_KEYS    = 40_000;

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.reset();
  }

  @Test
  void aRestoredDatabaseKeepsItsFiltersAndUsesThem() throws Exception {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.setValue(1L);
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(0.01f);

    final int series;
    final int publishedFilters;
    final String filterFileName;

    try (final DatabaseFactory factory = new DatabaseFactory(SOURCE_PATH); final Database database = factory.create()) {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("uid", String.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "uid" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

      database.transaction(() -> {
        for (int i = 0; i < TOTAL_KEYS; i++)
          database.newDocument(TYPE_NAME).set("uid", uid(i)).save();
      });

      final TypeIndex index = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("uid").getFirst();
      assertThat(((IndexInternal) index).scheduleCompaction()).isTrue();
      assertThat(((IndexInternal) index).compact()).isTrue();

      final LSMTreeIndexCompacted compacted = compactedOf(index);
      final LSMTreeIndexBloomFilter filter = compacted.getBloomFilter();
      assertThat(filter).as("the source database must have filters to begin with").isNotNull();

      series = compacted.getSeriesCount();
      publishedFilters = filter.getPublishedFilters();
      filterFileName = filter.getOSFile().getName();
      assertThat(publishedFilters).isEqualTo(series);

      new Backup(database, BACKUP_FILE).backupDatabase();
    }

    assertThat(new File(BACKUP_FILE)).exists();

    new Restore(BACKUP_FILE, RESTORED_PATH).restoreDatabase();

    // Opened READ_WRITE on purpose: that is the mode that runs the orphan sweep, so a filter whose owner no longer
    // resolves after the restore would be deleted here rather than merely ignored.
    try (final DatabaseFactory factory = new DatabaseFactory(RESTORED_PATH); final Database restored = factory.open()) {
      assertThat(new File(RESTORED_PATH, filterFileName))
          .as("the backup must carry the bloom filter file, and the restore must keep it").exists();

      final TypeIndex index = restored.getSchema().getType(TYPE_NAME).getIndexesByProperties("uid").getFirst();
      final LSMTreeIndexCompacted compacted = compactedOf(index);

      assertThat(compacted.getSeriesCount()).as("the restored index must have the same series").isEqualTo(series);
      assertThat(compacted.getBloomFilter())
          .as("the restored filter file must resolve back to its compacted index, not be swept as an orphan")
          .isNotNull();
      assertThat(compacted.getBloomFilter().getPublishedFilters())
          .as("the restored directory must describe every series").isEqualTo(publishedFilters);

      // The gate: every key still answerable THROUGH the restored filters.
      for (int i = 0; i < TOTAL_KEYS; i++)
        assertThat(get(index, uid(i))).as("key %d must be findable after the restore", i).isTrue();

      // ... and they must be doing something, or the assertion above would hold just as well without them.
      final long skippedBefore = compacted.getBloomSkippedSeries();
      for (int i = 0; i < 2_000; i++)
        assertThat(get(index, absentUid(i))).as("absent key %d must not be found", i).isFalse();

      assertThat(compacted.getBloomSkippedSeries() - skippedBefore)
          .as("the restored filters must actually be skipping series").isGreaterThan(0);
    }
  }

  private static boolean get(final TypeIndex index, final String key) {
    final IndexCursor cursor = index.get(new Object[] { key });
    try {
      return cursor.hasNext();
    } finally {
      cursor.close();
    }
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex index) {
    return ((LSMTreeIndex) index.getIndexesOnBuckets()[0]).getMutableIndex().getSubIndex();
  }

  /** High-entropy key so the compacted index does not compress away to a trivial size. */
  private static String uid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-" + i;
  }

  /** Never inserted, but sorts AMONG the stored keys, so only a filter can rule it out. */
  private static String absentUid(final int i) {
    final long mixed = i * 0x9E3779B97F4A7C15L;
    return Long.toHexString(mixed) + "-" + Long.toHexString(Long.reverse(mixed) ^ i) + "-absent-" + i;
  }
}
