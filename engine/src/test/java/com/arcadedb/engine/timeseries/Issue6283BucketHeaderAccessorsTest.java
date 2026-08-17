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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageId;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6283 (item 2): the header accessors of {@link TimeSeriesBucket} used to disagree with each other -
 * {@code getSampleCount()} and {@code getDataPageCount()} short-circuited on {@code getTotalPages() == 0} while
 * the timestamp and compaction readers went straight to {@code tx.getPage()}, which resolves with
 * {@code createIfNotExists=true} and therefore FABRICATES a zero-filled page 0, caches it, and reads zeros out
 * of it. Zero is a legal timestamp, so that answer is indistinguishable from a real one.
 * <p>
 * All of them now go through one non-fabricating header read that is authoritative on the magic in page 0, the
 * way {@code TimeSeriesTagDictionary.readStoredHeader()} is (issue #6198).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6283BucketHeaderAccessorsTest extends TestHelper {

  private List<ColumnDefinition> columns() {
    return List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("temperature", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
  }

  /**
   * A bucket whose file carries no header page yet: the constructor deliberately does not write one, the shard
   * calls {@code initHeaderPage()} afterwards. Every reader must report emptiness rather than invent a page.
   */
  @Test
  void aBucketWithoutAHeaderPageReportsEmptyAndLeavesNoPhantomPage() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    final TimeSeriesBucket bucket = new TimeSeriesBucket(db, "issue6283_noheader",
        db.getDatabasePath() + "/issue6283_noheader", columns());
    ((LocalSchema) db.getSchema()).registerFile(bucket);

    assertThat(bucket.getSampleCount()).isZero();
    assertThat(bucket.getDataPageCount()).isZero();
    // The empty markers initHeaderPage() writes, NOT the 0 a fabricated page would have read back.
    assertThat(bucket.getMinTimestamp()).isEqualTo(Long.MAX_VALUE);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(Long.MIN_VALUE);
    assertThat(bucket.isCompactionInProgress()).isFalse();
    assertThat(bucket.getCompactionWatermark()).isZero();

    // Nothing was cached for a page that does not exist: asking the page manager without createIfNotExists
    // still finds nothing.
    assertThat(db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), 0), bucket.getPageSize(), true, false))
        .isNull();
    database.rollback();

    dropBucket(db, bucket);
  }

  /**
   * The counters written inside the current transaction - by {@code initHeaderPage()}, which adds page 0, and by
   * the append that follows - must be the ones the readers see, before any commit.
   */
  @Test
  void headerWrittenInTheCurrentTransactionIsVisibleToTheReaders() throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.begin();
    final TimeSeriesBucket bucket = new TimeSeriesBucket(db, "issue6283_txlocal",
        db.getDatabasePath() + "/issue6283_txlocal", columns());
    ((LocalSchema) db.getSchema()).registerFile(bucket);
    bucket.initHeaderPage();

    assertThat(bucket.getSampleCount()).isZero();
    assertThat(bucket.getMinTimestamp()).isEqualTo(Long.MAX_VALUE);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(Long.MIN_VALUE);

    bucket.appendSamples(new long[] { 1000L, 3000L }, new Object[] { 20.0, 22.0 });
    assertThat(bucket.getSampleCount()).isEqualTo(2);
    assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(3000L);
    assertThat(bucket.getDataPageCount()).isEqualTo(1);

    bucket.setCompactionInProgress(true);
    bucket.setCompactionWatermark(4096L);
    assertThat(bucket.isCompactionInProgress()).isTrue();
    assertThat(bucket.getCompactionWatermark()).isEqualTo(4096L);
    database.commit();

    // ...and again once committed, when the readers resolve the page through the page manager instead.
    database.begin();
    assertThat(bucket.getSampleCount()).isEqualTo(2);
    assertThat(bucket.getMinTimestamp()).isEqualTo(1000L);
    assertThat(bucket.getMaxTimestamp()).isEqualTo(3000L);
    assertThat(bucket.isCompactionInProgress()).isTrue();
    assertThat(bucket.getCompactionWatermark()).isEqualTo(4096L);
    database.commit();

    dropBucket(db, bucket);
  }

  /**
   * The buckets built here are registered by hand and belong to no TimeSeries type, so they have to be removed
   * before the integrity check that closes the test, which reports any file the schema does not claim.
   */
  private void dropBucket(final DatabaseInternal db, final TimeSeriesBucket bucket) throws IOException {
    ((LocalSchema) db.getSchema()).removeFile(bucket.getFileId());
    db.getPageManager().deleteFile(db, bucket.getFileId());
    db.getFileManager().dropFile(bucket.getFileId());
  }
}
