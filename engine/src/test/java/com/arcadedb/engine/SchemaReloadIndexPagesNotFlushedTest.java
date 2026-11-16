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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.schema.LocalSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full schema reload of a LIVE database builds a new component on every file, and a component built from a file counts its
 * pages from the file's size, which lags a commit until the flush thread writes the page. The mutable file of an LSM index created
 * just before the reload has its header page committed and still queued for flushing: the new instance counted no page, took the
 * file for the header-less leftover of a compaction (issue #10046), and the reload left that bucket's index out of the schema. That
 * is issue #12101, the flaky {@code ColumnarIndexManagerLifecycleTest.registerIsIdempotentPerIndexFileId}: a COLUMNAR index keeps
 * its schema face in such an LSM file, so it lost the sub-index of a bucket, or the whole index when no page had reached the disk.
 * <p>
 * Made deterministic by suspending the page flush across the index creation and the reload, so every page the creation commits
 * stays off the disk, as {@link SchemaReloadDictionaryPagesNotFlushedTest} does for the dictionary.
 */
class SchemaReloadIndexPagesNotFlushedTest extends TestHelper {

  @Test
  void aReloadKeepsTheLsmIndexWhoseHeaderPagesTheFlushHasNotWrittenYet() throws IOException {
    assertTheReloadKeepsEveryBucketIndex("NOTUNIQUE");
  }

  @Test
  void aReloadKeepsTheColumnarIndexWhoseHeaderPagesTheFlushHasNotWrittenYet() throws IOException {
    assertTheReloadKeepsEveryBucketIndex("COLUMNAR");
  }

  private void assertTheReloadKeepsEveryBucketIndex(final String indexType) throws IOException {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    final DatabaseInternal db = (DatabaseInternal) database;
    // everything created so far reaches the disk, so only the index creation below has pages the flush holds back
    assertThat(db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

    final LocalSchema schema = db.getSchema().getEmbedded();
    assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(database, true))
        .as("flush suspension must engage or the new pages could reach disk, invalidating the test").isTrue();
    try {
      database.command("sql", "CREATE INDEX ON Asset (brand) " + indexType);
      final String indexName = database.getSchema().getType("Asset").getIndexesByProperties("brand").getFirst().getName();
      final List<IndexInternal> before = typeIndex(indexName).getSubIndexes();
      assertThat(before).hasSize(2);
      for (final IndexInternal sub : before) {
        final PaginatedComponent file = (PaginatedComponent) schema.getFileById(sub.getFileId());
        assertThat(file.getCommittedPageCount()).as("the premise: the header page of '%s' is committed", file.getName()).isPositive();
        assertThat(file.getComponentFile().getSize()).as("the premise: the header page of '%s' is not on disk", file.getName())
            .isZero();
      }

      schema.load(ComponentFile.MODE.READ_WRITE, true);

      final List<IndexInternal> after = typeIndex(indexName).getSubIndexes();
      assertThat(after).as("the reload keeps the index of every bucket").hasSize(2);
      for (int i = 0; i < after.size(); i++) {
        assertThat(after.get(i)).as("the reload built a new instance").isNotSameAs(before.get(i));
        assertThat(after.get(i).getAssociatedBucketId()).isEqualTo(before.get(i).getAssociatedBucketId());
        final PaginatedComponent file = (PaginatedComponent) schema.getFileById(after.get(i).getFileId());
        assertThat(file.getCommittedPageCount()).as("the reloaded '%s' counts its committed header page", file.getName())
            .isPositive();
        if (after.get(i) instanceof ColumnarIndex columnar)
          assertThat(columnar.isUsable()).isTrue();
      }
      assertThat(typeIndex(indexName).isValid()).isTrue();
    } finally {
      PageManager.INSTANCE.getFlushThread().setSuspended(database, false);
    }

    // the records written after the reload reach the reloaded index, as the integrity check of the teardown verifies
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Asset").set("brand", "b" + i).save();
    });
  }

  private TypeIndex typeIndex(final String name) {
    final Index index = database.getSchema().getIndexByName(name);
    assertThat(index).isInstanceOf(TypeIndex.class);
    return (TypeIndex) index;
  }
}
