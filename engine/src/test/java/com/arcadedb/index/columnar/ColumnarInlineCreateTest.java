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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A COLUMNAR index added to the open projection of an EMPTY bucket publishes its column on the DDL's thread, through the scan and
 * publish of the {@code create} rebuild, so a FACETS query right after the DDL answers from the index (final review A, answer 2b, and
 * the F2 investigation of plan 6). A projection whose maintenance slot is busy falls back to the background {@code create}.
 */
class ColumnarInlineCreateTest extends TestHelper {
  private static final String QUERY = "SELECT @rid FROM Asset FACETS brand TERMS TOP 10, fileSize TERMS TOP 10 LIMIT 1";

  @Test
  void aSecondColumnOfAnEmptyTypeIsPublishedByTheDdl() throws InterruptedException {
    createAssetWithBrand();
    final ColumnarProjection projection = projection();
    final long rebuildsBefore = manager().getStats().rebuilds("Asset", ColumnarRebuilder.CREATE);

    database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");

    assertThat(projection.isPublishedWith(fileSizeIndexFileId())).as("published when the DDL returns").isTrue();
    assertThat(projection.hasUnpublishedColumn()).isFalse();
    final long scansBefore = manager().getStats().queryFallbackScansCount();
    final FacetResults facets = facets();
    assertThat(facets.get("fileSize").source()).isEqualTo("index");
    assertThat(facets.get("brand").source()).isEqualTo("index");
    assertThat(manager().getStats().queryFallbackScansCount()).as("no record scan in a create window").isEqualTo(scansBefore);
    assertThat(manager().getStats().rebuilds("Asset", ColumnarRebuilder.CREATE)).as("the empty publish of an open, done late: not a rebuild")
        .isEqualTo(rebuildsBefore);

    // the inline publish is a real state: records committed afterwards are applied with the new column
    database.transaction(() -> database.newDocument("Asset").set("brand", "acme").set("fileSize", 7L).save());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    final FacetResults after = facets();
    assertThat(after.get("fileSize").source()).isEqualTo("index");
    assertThat(after.get("fileSize").buckets()).hasSize(1);
  }

  @Test
  void aBusyMaintenanceSlotFallsBackToTheBackgroundCreate() throws InterruptedException {
    createAssetWithBrand();
    final ColumnarProjection projection = projection();

    assertThat(projection.tryBeginMaintenance()).isTrue();
    try {
      // no spin on the DDL's thread: the DDL returns with the column left to the background create
      database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");
      assertThat(projection.isPublishedWith(fileSizeIndexFileId())).isFalse();
      assertThat(ColumnarRebuilder.isQueuedOrRunning(projection)).isTrue();
      // the create window: the bucket is read from its records, counted under its own reason
      try (final LogCapture log = new LogCapture()) {
        assertThat(facets().get("fileSize").source()).isEqualTo("scan");
        assertThat(manager().getStats().queryFallbackScansCount("not_published")).isEqualTo(1);
        assertThat(manager().getStats().queryFallbackScansCount()).isEqualTo(1);
        assertThat(log.messages(Level.FINE, "answered by a record scan")).containsExactly(
            "FACETS facet 'fileSize' of bucket 'Asset_0' (type 'Asset') answered by a record scan: column not published (create or schema "
                + "window)");
      }
    } finally {
      projection.endMaintenance();
    }

    for (int i = 0; i < 10_000 && !projection.isPublishedWith(fileSizeIndexFileId()); i++)
      Thread.sleep(1);
    assertThat(projection.isPublishedWith(fileSizeIndexFileId())).as("published by the background create").isTrue();
    assertThat(facets().get("fileSize").source()).isEqualTo("index");
  }

  @Test
  void anUnusableProjectionKeepsTheBackgroundRebuild() throws InterruptedException {
    // final review fix 1, m2: a projection waiting for a failed rebuild's retry is not published inline, so the retry runs and counts
    createAssetWithBrand();
    final ColumnarProjection projection = projection();
    projection.markUnusableWithoutRequest(ColumnarProjection.CORRUPT);

    // no rebuild may start meanwhile: the queued one waits for a permit
    final int held = ColumnarRebuilder.REBUILD_SEMAPHORE.drainPermits();
    try {
      database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");
      assertThat(projection.isPublishedWith(fileSizeIndexFileId())).as("not published on the DDL's thread").isFalse();
      assertThat(ColumnarRebuilder.isQueuedOrRunning(projection)).isTrue();
    } finally {
      ColumnarRebuilder.REBUILD_SEMAPHORE.release(held);
      ColumnarRebuilder.drainWaiting();
    }
    for (int i = 0; i < 10_000 && !projection.isPublishedWith(fileSizeIndexFileId()); i++)
      Thread.sleep(1);
    assertThat(projection.isPublishedWith(fileSizeIndexFileId())).as("published by the background rebuild").isTrue();
  }

  @Test
  void aNonEmptyBucketKeepsTheBackgroundCreate() throws InterruptedException {
    createAssetWithBrand();
    database.transaction(() -> database.newDocument("Asset").set("brand", "acme").set("fileSize", 7L).save());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    final ColumnarProjection projection = projection();
    final long rebuildsBefore = manager().getStats().rebuilds("Asset", ColumnarRebuilder.CREATE);

    database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");

    for (int i = 0; i < 10_000 && !projection.isPublishedWith(fileSizeIndexFileId()); i++)
      Thread.sleep(1);
    assertThat(projection.isPublishedWith(fileSizeIndexFileId())).isTrue();
    assertThat(manager().getStats().rebuilds("Asset", ColumnarRebuilder.CREATE)).as("a create with records is a counted rebuild").isEqualTo(rebuildsBefore + 1);
  }

  private void createAssetWithBrand() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.fileSize LONG");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    assertThat(projection().isPublishedWith(indexFileId("Asset[brand]_columnar"))).as("an empty bucket is published at open").isTrue();
  }

  private FacetResults facets() {
    try (final ResultSet rs = database.query("sql", QUERY)) {
      while (rs.hasNext())
        rs.next();
      return rs.getFacets();
    }
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarProjection projection() {
    return manager().projection(database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId)
        .findFirst().orElseThrow());
  }

  private int fileSizeIndexFileId() {
    return indexFileId("Asset[fileSize]_columnar");
  }

  private int indexFileId(final String typeIndexName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(typeIndexName);
    return ((ColumnarIndex) typeIndex.getIndexesOnBuckets()[0]).columnDefinition().indexFileId();
  }
}
