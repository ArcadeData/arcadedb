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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4219.
 * <p>
 * On a Raft follower under heavy bulk insertion, every LSM compaction on the leader fires a
 * SCHEMA_ENTRY that the follower applies via {@code LocalSchema.load(MODE.READ_WRITE, true)}.
 * Before the fix, that load() path destroyed every in-memory bucket/index instance and the
 * load-time {@link LocalBucket} constructor called {@code gatherPageStatistics()} eagerly.
 * For buckets whose pages were mostly full (typical under bulk insert) the scan walked every
 * single page of every bucket on every reload, pulling them all through the page cache and
 * the active TransactionContext. Across thousands of compaction events, this exhausted the
 * follower heap and surfaced as a Java heap space OOM in
 * {@code com.arcadedb.engine.PageManager.loadPage}, followed by cascading "Bucket with id X
 * was not found" SchemaExceptions on subsequent TX_ENTRY applies (the schema reload had
 * already cleared bucketMap before the OOM aborted it).
 * <p>
 * After the fix, the load-time bucket constructor no longer pre-warms statistics.
 * {@link LocalBucket#findAvailableSpace} already calls {@code gatherPageStatistics()} lazily
 * on the first allocation, so a leader still gets reuse behavior for free; a follower that
 * only applies leader-shipped page bytes via the state machine never triggers the scan at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaReloadDoesNotEagerScanBucketsTest {

  private static final String DB_PATH    = "./target/databases/SchemaReloadDoesNotEagerScanBuckets";
  private static final String TYPE       = "BulkRow";
  private static final int    REC_COUNT  = 5_000;
  private static final int    PAYLOAD_KB = 1;

  @Test
  void schemaReloadDoesNotEagerlyScanBucketPages() throws Exception {
    final String previousMode = GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString();
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      // Step 1: create the database and fill the bucket with many pages. Default page size is
      // ~64KB; 5_000 records with a 1 KB payload span dozens of pages, enough to make a full
      // bucket-page scan during schema reload measurable.
      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType(TYPE, 1);
        final String payload = "x".repeat(PAYLOAD_KB * 1024);
        db.transaction(() -> {
          for (int i = 0; i < REC_COUNT; i++)
            db.newDocument(TYPE).set("idx", i).set("payload", payload).save();
        });
      }

      try (final Database db = factory.open()) {
        final DatabaseInternal internal = (DatabaseInternal) db;
        final PageManager pageManager = internal.getPageManager();

        // The open path warmed schema state. Evict everything we cached for this database so the
        // next reload has to go to disk - the same situation a Raft follower hits during a
        // SCHEMA_ENTRY apply after a long bulk-load run that has churned the page cache.
        pageManager.removeAllReadPagesOfDatabase(db);

        final long pagesReadBaseline = pageManager.getStats().pagesRead;

        // Simulate the follower SCHEMA_ENTRY path: full schema reload from disk.
        ((LocalSchema) internal.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);

        final long pagesReadDelta = pageManager.getStats().pagesRead - pagesReadBaseline;

        // Before the fix, gatherPageStatistics() ran from the bucket constructor on every load
        // and read every full page (often hundreds for a 5_000-record bucket). After the fix,
        // schema reload touches only the dictionary, schema.json, and bucket headers - a small,
        // fixed amount of I/O independent of bucket size.
        assertThat(pagesReadDelta)
            .as("Schema reload must not eagerly scan bucket data pages (issue #4219)")
            .isLessThan(30L);
      }
    } finally {
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue(previousMode);
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  @Test
  void lazyPageStatisticsStillWorkAfterReload() throws Exception {
    // Sanity check that removing the eager warmup did not regress the lazy path: in HIGH mode,
    // the first allocation after a reload must still trigger gatherPageStatistics() (so writes
    // continue to recycle freed space on a leader). Measured indirectly via pagesRead delta.
    final String previousMode = GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString();
    GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue("high");

    FileUtils.deleteRecursively(new File(DB_PATH));
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.getSchema().createDocumentType(TYPE, 1);
        final String payload = "y".repeat(PAYLOAD_KB * 1024);
        db.transaction(() -> {
          for (int i = 0; i < REC_COUNT; i++)
            db.newDocument(TYPE).set("idx", i).set("payload", payload).save();
        });
      }

      try (final Database db = factory.open()) {
        final DatabaseInternal internal = (DatabaseInternal) db;
        final PageManager pageManager = internal.getPageManager();

        // Force a fresh schema reload and drop cached pages so the lazy scan we want to trigger
        // below has to read from disk (which is what bumps pagesRead).
        ((LocalSchema) internal.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);
        pageManager.removeAllReadPagesOfDatabase(db);

        final long pagesReadBeforeAlloc = pageManager.getStats().pagesRead;

        // First write after the reload: the allocator must run gatherPageStatistics lazily,
        // which scans pages and increases the page-read counter (proving the lazy path fires).
        db.transaction(() -> db.newDocument(TYPE).set("idx", -1).set("payload", "trigger").save());

        final long pagesReadAfterAlloc = pageManager.getStats().pagesRead;
        assertThat(pagesReadAfterAlloc - pagesReadBeforeAlloc)
            .as("First allocation after a reload must trigger lazy gatherPageStatistics")
            .isGreaterThan(0L);
      }
    } finally {
      GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.setValue(previousMode);
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
