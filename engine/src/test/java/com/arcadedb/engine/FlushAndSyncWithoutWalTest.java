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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link PageManager#flushAndSyncWithoutWAL} makes ONE file durable: the vector graph build calls it on the graph file it
 * committed without the WAL, while holding the build lock concurrent searches of the index wait on. It used to force every
 * file of the database written since its last fsync, so each small-graph rebuild a search triggered paid one fsync per
 * file that writers of other types had touched: with a second indexed type taking inserts, searches of a 128-dimension
 * index with 800 vectors went from 2.4 ms to 8.4 s at the median.
 */
class FlushAndSyncWithoutWalTest extends TestHelper {

  @Test
  void forcesTheFileItIsAskedForAndNoOther() {
    database.getSchema().createDocumentType("Asked", 1);
    database.getSchema().createDocumentType("Other", 1);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newDocument("Asked").set("i", i).save();
        database.newDocument("Other").set("i", i).save();
      }
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final PageManager pageManager = db.getPageManager();
    assertThat(pageManager.waitPagesPendingNowOfDatabaseAreFlushed(db, 30_000)).isTrue();

    final LocalBucket askedBucket = (LocalBucket) db.getSchema().getType("Asked").getBuckets(false).getFirst();
    final int askedFileId = askedBucket.getFileId();
    final int otherFileId = db.getSchema().getType("Other").getBuckets(false).getFirst().getFileId();
    final PaginatedComponentFile asked = (PaginatedComponentFile) db.getFileManager().getFile(askedFileId);
    final PaginatedComponentFile other = (PaginatedComponentFile) db.getFileManager().getFile(otherFileId);
    assertThat(asked.isModifiedSinceLastSync()).isTrue();
    assertThat(other.isModifiedSinceLastSync()).isTrue();

    final int[] notOnDisk = pageManager.flushAndSyncWithoutWAL(db, askedFileId, askedBucket.getTotalPages(), 30_000);

    assertThat(notOnDisk).isEmpty();
    assertThat(asked.isModifiedSinceLastSync()).as("the file asked for is forced").isFalse();
    assertThat(other.isModifiedSinceLastSync()).as("a file nobody asked for is left to the next full sync").isTrue();
  }
}
