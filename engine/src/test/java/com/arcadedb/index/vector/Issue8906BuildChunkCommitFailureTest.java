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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8906: the bulk load of an LSM_VECTOR build commits its transaction at each chunk boundary from inside the
 * {@code scanBucket} callback, and the scan logs and swallows whatever its callback throws. A failed chunk commit
 * therefore never reached {@code build()}, which went on to report a partial index as READY.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue8906BuildChunkCommitFailureTest extends TestHelper {
  private static final int DIMENSIONS = 512;
  private static final int DOCS       = 700;

  @Test
  void aFailedChunkCommitFailsTheBuildInsteadOfReportingAPartialIndexAsReady() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB, 1L);
    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_COMMIT_LOCK_TIMEOUT, 50L);

    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      ((TypeLSMVectorIndexBuilder) database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType())
          .withDimensions(DIMENSIONS).withStoreVectorsInGraph(true).create();
    });

    final Random random = new Random(8906L);
    database.transaction(() -> {
      for (int i = 0; i < DOCS; i++) {
        final float[] v = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          v[j] = (float) random.nextGaussian();
        database.newDocument("Doc").set("embedding", v).save();
      }
    });

    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]"))
        .getIndexesOnBuckets()[0];
    final int graphFileId = index.getGraphFile().getFileId();

    // another committer keeps the vecgraph file, which every commit touching the index locks, so the chunk commit times out
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Object holder = new Object();
    final Thread blocker = new Thread(() -> {
      db.getTransactionManager().tryLockFiles(List.of(graphFileId), 0, holder);
      held.countDown();
      try {
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        db.getTransactionManager().unlockFilesInOrder(List.of(graphFileId), holder);
      }
    }, "issue8906-blocker");
    blocker.setDaemon(true);
    blocker.start();
    assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();

    try {
      assertThatThrownBy(() -> index.build(null, null)).isInstanceOf(IndexException.class)
          .hasMessageContaining("chunk commit failed");
    } finally {
      release.countDown();
      blocker.join(TimeUnit.SECONDS.toMillis(30));
    }

    assertThat(index.isValid()).as("a partial build must not be left looking usable").isFalse();
    assertThat(database.isTransactionActive()).isFalse();
  }
}
