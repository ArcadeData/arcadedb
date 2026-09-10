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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7361: a vector graph build that had cost 27 minutes was thrown away because ONE of its persist chunk
 * commits could not take the vecgraph file lock inside the interactive 5s default:
 * <pre>
 * Timeout on locking file 6 (..._vecgraph...) during commit (timeout=5000ms, heldBy='Thread[#1,main]' for 7271ms)
 * PERSIST: Failed to persist graph for ... (nodes=1600000): IndexException - Error writing graph to pages
 * </pre>
 * <b>Why anything else holds that file.</b> {@code LSMVectorIndex.getFileIds()} puts the companion graph file in
 * the lock set of EVERY commit that touched the index (issue #4937), so an ordinary insert commit - which writes no
 * graph page at all - takes it. That is deliberate and stays: it is what keeps a transaction that does write graph
 * pages from passing the commit version checks without the file lock held. The consequence is that a graph persist
 * running alongside a live ingest queues behind the loader as a matter of course.
 * <p>
 * <b>Why giving up after 5s is the wrong answer there.</b> {@code arcadedb.commitLockTimeout} is sized for an
 * interactive transaction, where a fast failure is right because the caller retries cheaply. A graph persist is the
 * opposite: it commits once per {@code arcadedb.index.buildChunkSizeMB}, every one of those commits is a step of a
 * build that already cost minutes, and losing any one of them discards the build and marks the pages unusable -
 * to spare a wait of about seven seconds. The persist now waits on
 * {@code arcadedb.index.buildCommitLockTimeout} instead, which bounds only how long it WAITS and never how long it
 * HOLDS, so no other transaction is slowed by it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7361GraphPersistCommitLockTimeoutTest extends TestHelper {
  private static final int DIMENSIONS = 32;
  private static final int LIVE       = 150;

  /**
   * The plumbing, on its own: a transaction can be given a commit lock budget of its own, and it is that budget -
   * not the configured one - that bounds its wait.
   */
  @Test
  void aTransactionCanBeGivenACommitLockBudgetOfItsOwn() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    database.getSchema().createDocumentType("Doc");

    final int fileId = database.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();

    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Object holder = new Object();

    final Thread contender = new Thread(() -> {
      db.getTransactionManager().tryLockFiles(List.of(fileId), 0, holder);
      held.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        db.getTransactionManager().unlockFilesInOrder(List.of(fileId), holder);
      }
    }, "issue7361-lock-holder");
    contender.setDaemon(true);
    contender.start();

    assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();
    try {
      database.begin();
      db.getTransaction().setCommitLockTimeout(50L);
      database.newDocument("Doc").set("v", 1).save();

      assertThatThrownBy(database::commit)
          .as("50ms is the budget this transaction asked for, whatever the configured one is")
          .isInstanceOf(LockTimeoutException.class)
          .hasMessageContaining("timeout=50ms");
    } finally {
      release.countDown();
      contender.join(TimeUnit.SECONDS.toMillis(30));
    }

    assertThat(database.isTransactionActive()).isFalse();
  }

  /**
   * The behaviour the issue is about: a persist whose chunk commit queues behind an ordinary commit that holds the
   * vecgraph file for longer than the interactive default must wait for it, not discard the build.
   * <p>
   * The interactive budget is set far below the hold time so a persist running on it is certain to fail, and the
   * bulk budget far above it so one running on the bulk budget is certain to succeed. What separates the two is
   * which budget the persist uses, which is what this asserts.
   */
  @Test
  void aPersistChunkWaitsOnTheBulkBudgetRatherThanDiscardingTheBuild() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 100L);
    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_COMMIT_LOCK_TIMEOUT, 30_000L);

    createSchema();
    insertDocs(LIVE);

    final int graphFileId = graphFileOf(vectorIndex()).getFileId();

    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Object holder = new Object();

    // Stands in for the importer thread of the report: it holds the vecgraph file - which every commit touching
    // this index locks (#4937) - for far longer than the interactive budget, and far less than the bulk one.
    final Thread importer = new Thread(() -> {
      db.getTransactionManager().tryLockFiles(List.of(graphFileId), 0, holder);
      held.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        db.getTransactionManager().unlockFilesInOrder(List.of(graphFileId), holder);
      }
    }, "issue7361-importer");
    importer.setDaemon(true);
    importer.start();

    assertThat(held.await(30, TimeUnit.SECONDS)).isTrue();

    // Releases the file a beat after the persist has started queueing for it, so the persist genuinely waits past
    // the interactive budget instead of never contending at all.
    final Thread releaser = new Thread(() -> {
      try {
        Thread.sleep(1_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      release.countDown();
    }, "issue7361-releaser");
    releaser.setDaemon(true);
    releaser.start();

    try {
      vectorIndex().buildVectorGraphNow();
    } finally {
      release.countDown();
      importer.join(TimeUnit.SECONDS.toMillis(30));
      releaser.join(TimeUnit.SECONDS.toMillis(30));
    }

    final LSMVectorIndexGraphFile graphFile = graphFileOf(vectorIndex());
    assertThat(graphFile.getLastWrittenGraphBytes())
        .as("the persist waited for the file instead of discarding a completed build").isGreaterThan(0L);
    assertThat(graphFile.getManifest().read()).isNotNull();
    assertThat(graphFile.getManifest().read().vectorCount())
        .as("and its manifest vouches for the pages rather than refusing them").isEqualTo(LIVE);
  }

  /**
   * The ordinary bulk build path - {@code build()}, what {@code CREATE INDEX ... TYPE LSM_VECTOR} runs - and not
   * only the rebuild one. {@code build()} opens the transaction the whole build runs in before anything under it
   * gets a say, so a budget applied only by a nested method that thinks it opened its own transaction never
   * applies at all: the first chunk commit, and a whole single-chunk persist, stayed on the interactive default.
   * <p>
   * Asserted on the transaction itself rather than through contention, because that is the fact that decides it
   * and it holds for every commit of the build, not only for one that happens to be contended.
   */
  @Test
  void theOrdinaryBulkBuildRunsItsCommitsOnTheBulkBudgetToo() {
    final DatabaseInternal db = (DatabaseInternal) database;

    db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 100L);
    db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_COMMIT_LOCK_TIMEOUT, 30_000L);

    createSchema();
    insertDocs(LIVE);

    final List<Long> observed = new ArrayList<>();

    database.begin();
    try {
      // Every record of the bulk load is indexed inside the transaction build() itself opened and commits.
      vectorIndex().build((document, totalIndexed) -> observed.add(db.getTransaction().getCommitLockTimeout()), null);
    } finally {
      if (database.isTransactionActive())
        database.commit();
    }

    assertThat(observed).as("the build has to have indexed something for this to say anything").isNotEmpty();
    assertThat(observed)
        .as("every commit of a bulk build waits on the bulk budget, from the first one: the transaction it all "
            + "runs in is opened by build() itself, so nothing under it can be the thing that sets this")
        .containsOnly(30_000L);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private void createSchema() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withStoreVectorsInGraph(true).create();
    });
  }

  private void insertDocs(final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });
  }

  private static float[] embedding(final int doc) {
    final Random random = new Random(0x7361L * 31 + doc);
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) random.nextGaussian();
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return vectorIndexOf(database);
  }

  private static LSMVectorIndex vectorIndexOf(final Database db) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private static LSMVectorIndexGraphFile graphFileOf(final LSMVectorIndex index) {
    final LSMVectorIndexGraphFile graphFile = index.getGraphFile();
    assertThat(graphFile).as("the index must have a graph file").isNotNull();
    return graphFile;
  }
}
