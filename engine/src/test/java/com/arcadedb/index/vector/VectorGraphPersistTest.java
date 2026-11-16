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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LockManager;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Persistence, build commits and side files of the vector graph. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorGraphPersistTest {

  /**
   * Regression for #7361: a graph persist chunk commit waits for the vecgraph file lock.
   * <p>
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
  @Nested
  @Tag("vector")
  class GraphPersistCommitLockTimeout extends TestHelper {
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
      final AtomicBoolean contended = new AtomicBoolean();

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

      // Releases the file only once the persist is OBSERVABLY queued behind it, so the test cannot pass for the
      // wrong reason - a sleep long enough on a fast machine is not long enough on a loaded CI runner, and the
      // persist would then take an uncontended lock and prove nothing.
      final Thread releaser = new Thread(() -> {
        while (waitersOn(db, graphFileId) == 0) {
          if (release.getCount() == 0)
            return;
          try {
            Thread.sleep(5);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        contended.set(true);
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
      assertThat(contended.get())
          .as("the persist has to have actually queued behind the holder, or this proves nothing").isTrue();
    }

    /**
     * The other branch of the budget: {@code 0} is the lock manager's "wait indefinitely", and it has to survive
     * the clamp against {@code arcadedb.commitLockTimeout} rather than being read as "smaller, so use the other
     * one". The clamp exists so the budget can never make a build give up SOONER than today, and waiting forever
     * is the opposite of sooner.
     */
    @Test
    void anIndefiniteBulkBudgetIsNotClampedAwayByTheCommitDefault() {
      final DatabaseInternal db = (DatabaseInternal) database;

      db.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 5_000L);
      db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_COMMIT_LOCK_TIMEOUT, 0L);

      createSchema();
      insertDocs(8);

      final List<Long> observed = new ArrayList<>();

      database.begin();
      try {
        vectorIndex().build((document, totalIndexed) -> observed.add(db.getTransaction().getCommitLockTimeout()), null);
      } finally {
        if (database.isTransactionActive())
          database.commit();
      }

      assertThat(observed).isNotEmpty();
      assertThat(observed).as("0 means wait indefinitely, and no positive commit default outranks it").containsOnly(0L);

      // And a negative value, which the lock manager reads the same way, is carried through just as it is.
      db.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_COMMIT_LOCK_TIMEOUT, -1L);
      observed.clear();

      database.begin();
      try {
        vectorIndex().build((document, totalIndexed) -> observed.add(db.getTransaction().getCommitLockTimeout()), null);
      } finally {
        if (database.isTransactionActive())
          database.commit();
      }

      assertThat(observed).isNotEmpty();
      assertThat(observed).as("anything <= 0 is the same 'wait indefinitely' to the lock manager").containsOnly(-1L);
    }

    /**
     * @return how many requesters are queued behind whoever holds {@code fileId} right now
     */
    private static int waitersOn(final DatabaseInternal db, final int fileId) {
      for (final LockManager.LockStats stats : db.getTransactionManager().getLockStats())
        if (String.valueOf(fileId).equals(stats.resource()))
          return stats.waiters();
      return 0;
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

      final AtomicReference<Long> budgetAfterBuild = new AtomicReference<>(-1L);

      database.begin();
      try {
        // Every record of the bulk load is indexed inside the transaction build() itself opened and commits.
        vectorIndex().build((document, totalIndexed) -> observed.add(db.getTransaction().getCommitLockTimeout()), null);
        budgetAfterBuild.set(db.getTransaction().getCommitLockTimeout());
      } finally {
        if (database.isTransactionActive())
          database.commit();
      }

      assertThat(observed).as("the build has to have indexed something for this to say anything").isNotEmpty();
      assertThat(observed)
          .as("every commit of a bulk build waits on the bulk budget, from the first one: the transaction it all "
              + "runs in is opened by build() itself, so nothing under it can be the thing that sets this")
          .containsOnly(30_000L);
      assertThat(budgetAfterBuild.get())
          .as("and the budget is put back the way the WAL setting is: build() is public, so a caller that already "
              + "had a transaction open must not have its own later commit inherit a bulk build's budget")
          .isNull();
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

  /**
   * Regression for #7362: the persisted graph length used to measure the footer.
   * <p>
   * Issue #7362: a persisted vector graph could not be reloaded as an {@code OnDiskGraphIndex}, because the length
   * the load measured its JVector footer back from was re-derived from the component's page count instead of being
   * the length the write actually ended on.
   * <p>
   * Two independent defects fed that, and both are covered here.
   * <p>
   * <b>The page count did not count the pages.</b> {@code ContiguousPageWriter} appended pages through
   * {@code TransactionContext.getPage() + getPageToModify()}. That never fails past the end of a file - {@code getPage}
   * asks the page manager with {@code createIfNotExists}, so it invents a zero-filled page - which made the
   * {@code addPage()} fallback next to it unreachable. {@code addPage} is the only thing that raises the transaction's
   * page counter, so no counter moved, and the component's count was left to be raised one page at a time, later, by
   * the async flush thread. A graph written and reloaded in the same breath therefore measured itself against
   * whatever the flusher had got through: in the report, 7,601 pages for 3,148,792,924 bytes actually written, a
   * length short by more than a gigabyte, and a footer magic read off the middle of the payload.
   * <p>
   * <b>The page count could not have answered it anyway.</b> {@code updatePageCount} is monotonic, so once a file has
   * held a large generation of the graph its count never comes back down: a smaller generation written over it is
   * measured with its predecessor's size. The length is now recorded by the write, in the manifest, and read back
   * from there.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class PersistedGraphLength extends TestHelper {
    private static final int DIMENSIONS = 32;
    private static final int LIVE       = 200;
    private static final int PAGE_SIZE  = 65536;

    /**
     * The root defect, in isolation and without a graph in sight: pages appended to a component file must be visible
     * in its page count as soon as they are written, and not only once the flush thread has caught up.
     */
    @Test
    void appendedPagesAreCountedBeforeTheCommitAndNotOnlyAfterTheAsyncFlush() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final int usablePageSize = PAGE_SIZE - BasePage.PAGE_HEADER_SIZE;
      final int pagesToWrite = 5;

      database.begin();
      try {
        final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(db, "test-7362-counting",
            db.getDatabasePath() + "/test-7362-counting", ComponentFile.MODE.READ_WRITE, PAGE_SIZE);
        db.getSchema().getEmbedded().registerFile(graphFile);

        final ContiguousPageWriter writer = new ContiguousPageWriter(db, graphFile.getFileId(), PAGE_SIZE);

        // usablePageSize is a multiple of 4, so this lands exactly on a page boundary: pages [0, pagesToWrite).
        final long target = (long) usablePageSize * pagesToWrite;
        while (writer.position() < target)
          writer.writeInt(0xC0FFEE);
        assertThat(writer.position()).isEqualTo(target);
        writer.close();

        assertThat(graphFile.getTotalPages())
            .as("the %d pages just appended must be counted while the transaction that wrote them is still open: "
                + "everything that asks how big the persisted graph is asks this", pagesToWrite)
            .isEqualTo(pagesToWrite);

        database.commit();

        assertThat(graphFile.getTotalPages())
            .as("and still be counted right after the commit, with the async flush thread not necessarily done")
            .isEqualTo(pagesToWrite);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    /**
     * The write records the length it ended on, and the load uses that rather than re-deriving one. Verified against
     * a page count deliberately inflated past the truth - which is the state a file that once held a larger
     * generation of the graph is permanently in, {@code updatePageCount} being monotonic.
     */
    @Test
    void theGraphIsLoadedWithTheLengthTheWriteRecordedNotWithOneDerivedFromThePageCount() throws Exception {
      createSchema();
      insertDocs(LIVE);
      vectorIndex().buildVectorGraphNow();

      final LSMVectorIndexGraphFile graphFile = graphFileOf(vectorIndex());
      final long written = graphFile.getLastWrittenGraphBytes();

      assertThat(written).as("the persist must report the length it ended on").isGreaterThan(0L);
      assertThat(graphFile.getManifest().read()).isNotNull();
      assertThat(graphFile.getManifest().read().graphBytes())
          .as("and record it next to the pages, so the next session does not have to re-derive it")
          .isEqualTo(written);

      // What a file that once held a bigger graph looks like forever after: a page count that no longer describes
      // its contents, and that cannot be lowered.
      graphFile.updatePageCount(graphFile.getTotalPages() * 4 + 128);

      try (final OnDiskGraphIndex reloaded = graphFile.loadGraph()) {
        assertThat(reloaded)
            .as("the recorded length is a fact about the write, so an inflated page count cannot break the reload")
            .isNotNull();
        assertThat(reloaded.getIdUpperBound()).isEqualTo(LIVE);
      }
    }

    /**
     * The same length has to survive the session that wrote it: on a reopen there is no in-memory record of the
     * write left, only the manifest.
     */
    @Test
    void theRecordedLengthSurvivesAReopen() throws Exception {
      createSchema();
      insertDocs(LIVE);
      vectorIndex().buildVectorGraphNow();

      final long written = graphFileOf(vectorIndex()).getLastWrittenGraphBytes();
      assertThat(written).isGreaterThan(0L);

      reopenDatabase();

      final LSMVectorIndexGraphFile graphFile = graphFileOf(vectorIndex());
      assertThat(graphFile.getLastWrittenGraphBytes())
          .as("nothing in this session wrote the graph, so the length can only come from the manifest").isEqualTo(-1L);
      assertThat(graphFile.getManifest().read().graphBytes()).isEqualTo(written);

      graphFile.updatePageCount(graphFile.getTotalPages() * 4 + 128);

      try (final OnDiskGraphIndex reloaded = graphFile.loadGraph()) {
        assertThat(reloaded).isNotNull();
        assertThat(reloaded.getIdUpperBound()).isEqualTo(LIVE);
      }
    }

    /**
     * A manifest written before issue #7362 carries no length, and must keep being read - and keep falling back to
     * the page-count derivation - rather than being refused wholesale.
     */
    @Test
    void aManifestWithoutARecordedLengthStillReads() throws Exception {
      createSchema();
      insertDocs(LIVE);
      vectorIndex().buildVectorGraphNow();

      final LSMVectorIndexGraphManifest manifest = graphFileOf(vectorIndex()).getManifest();
      final LSMVectorIndexGraphManifest.Content before = manifest.read();
      assertThat(before).isNotNull();

      // Exactly what an older build wrote: same manifest, without the key.
      manifest.write(before.vectorCount(), before.fingerprint(), before.unreachableOrdinals(), 0L);

      final LSMVectorIndexGraphManifest.Content after = manifest.read();
      assertThat(after).as("an older manifest is still a readable one").isNotNull();
      assertThat(after.vectorCount()).isEqualTo(before.vectorCount());
      assertThat(after.fingerprint()).isEqualTo(before.fingerprint());
      assertThat(after.graphBytes()).as("it simply records no length").isZero();
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void createSchema() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");

        final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) database.getSchema()
            .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
        // Inline vectors are what makes the persisted graph big enough for its length to matter at all, and they are
        // the configuration the report ran with.
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
      final Random random = new Random(0x7362L * 31 + doc);
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
      assertThat(graphFile).as("the index must have persisted its graph").isNotNull();
      return graphFile;
    }
  }

  /**
   * Regression for #8129: building a vector index leaves the WAL setting of the thread transaction alone.
   * <p>
   * Regression test for issue #8129: building an {@code LSM_VECTOR} index switched the WAL off for bulk loading on the
   * thread's own {@link TransactionContext}, which is reused across begin()/commit() cycles, and did not put it back.
   * Every later transaction on that thread then committed without writing the WAL at all, so {@code txWalFlush=2} cost
   * nothing because there was nothing left to flush.
   * <p>
   * The durability a session asked for belongs to the session: a vector build or graph persist must leave it exactly
   * as it found it, whether that is the configured default or a value the caller set on purpose.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorBuildLeavesWalOn {
    private static final String DB_PATH    = "./target/databases/VectorBuildLeavesWalOn";
    private static final int    SEED_ROWS  = 200;
    private static final int    DIMENSIONS = 16;

    private DatabaseFactory factory;
    private Database        database;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
      factory = new DatabaseFactory(DB_PATH);
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
      factory.close();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void transactionsAfterAVectorIndexBuildStillWriteTheWal() {
      database.setWALFlush(WALFile.FlushType.YES_FULL);
      seedAndIndex();

      assertSessionDurability(true, WALFile.FlushType.YES_FULL, "after CREATE INDEX ... LSM_VECTOR");
      assertCommitWritesTheWal();

      // A search persists the graph in a transaction of its own: that path must leave the session alone as well.
      searchEveryBucket(new Random(1));
      assertSessionDurability(true, WALFile.FlushType.YES_FULL, "after a search that persisted the vector graph");
      assertCommitWritesTheWal();
    }

    @Test
    void anExplicitRebuildLeavesTheWalOn() {
      seedAndIndex();

      database.command("sql", "REBUILD INDEX `P[embedding]`");
      searchEveryBucket(new Random(2));

      assertSessionDurability(true, currentWalFlush(), "after REBUILD INDEX");
      assertCommitWritesTheWal();
    }

    @Test
    void aSessionThatDisabledTheWalOnPurposeKeepsItDisabled() {
      database.setUseWAL(false);
      database.setWALFlush(WALFile.FlushType.NO);
      seedAndIndex();
      searchEveryBucket(new Random(3));

      // The build used to restore the CONFIGURED value rather than the one it found, re-enabling the WAL on a session
      // that had switched it off for its own bulk load.
      assertSessionDurability(false, WALFile.FlushType.NO, "after a vector build on a WAL-less session");
    }

    @Test
    void theTransactionOverrideDoesNotOutliveItsTransaction() {
      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.setUseWALForThisTransaction(false);
      assertThat(tx.isUseWAL()).isFalse();
      database.commit();

      assertSessionDurability(true, currentWalFlush(), "after a commit whose transaction skipped the WAL");

      database.begin();
      ((DatabaseInternal) database).getTransaction().setUseWALForThisTransaction(false);
      database.rollback();

      assertSessionDurability(true, currentWalFlush(), "after a rollback whose transaction skipped the WAL");
    }

    @Test
    void aBuildSharingTheCallersTransactionLeavesItsOverrideAsItFoundIt() {
      seedAndIndex();
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("P[embedding]");

      // A build that joins a transaction somebody else opened: its finally is the only thing standing between the
      // build's WAL-less override and the caller's own commit, and it must hand back what the caller had set.
      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.setUseWALForThisTransaction(true);
      typeIndex.build(IndexBuilder.BUILD_BATCH_SIZE, true, null);

      assertThat(((DatabaseInternal) database).getTransaction()).isSameAs(tx);
      assertThat(tx.getUseWALForThisTransaction()).as("the caller's override after a build that shared its transaction")
          .isTrue();
      database.commit();

      assertSessionDurability(true, currentWalFlush(), "after a build that shared the caller's transaction");
      assertCommitWritesTheWal();
    }

    private void seedAndIndex() {
      seedRows();
      createVectorIndex();
    }

    private void seedRows() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE PROPERTY P.pid INTEGER");
      database.command("sql", "CREATE PROPERTY P.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON P (pid) UNIQUE");

      final Random rnd = new Random(8129);
      database.transaction(() -> {
        for (int i = 0; i < SEED_ROWS; i++)
          database.newVertex("P").set("pid", i).set("views", 0).set("embedding", randomVector(rnd)).save();
      });
    }

    private void createVectorIndex() {
      database.command("sql", "CREATE INDEX ON P (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    }

    private void assertSessionDurability(final boolean useWAL, final WALFile.FlushType walFlush, final String when) {
      database.begin();
      try {
        final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
        assertThat(tx.isUseWAL()).as("useWAL of a new transaction " + when).isEqualTo(useWAL);
        assertThat(tx.getWALFlush()).as("walFlush of a new transaction " + when).isEqualTo(walFlush);
      } finally {
        database.rollback();
      }
    }

    private void assertCommitWritesTheWal() {
      final long before = walBytesWritten();
      database.transaction(() -> database.command("sql", "UPDATE P SET views = views + 1 WHERE pid < 6"));
      assertThat(walBytesWritten()).as("a committed update must be written to the WAL").isGreaterThan(before);
    }

    private WALFile.FlushType currentWalFlush() {
      database.begin();
      try {
        return ((DatabaseInternal) database).getTransaction().getWALFlush();
      } finally {
        database.rollback();
      }
    }

    private long walBytesWritten() {
      return (Long) ((DatabaseInternal) database).getTransactionManager().getStats().get("bytesWritten");
    }

    private void searchEveryBucket(final Random rnd) {
      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("P[embedding]");
      for (final var bucketIndex : typeIndex.getIndexesOnBuckets())
        ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(randomVector(rnd), 5, 64);
    }

    private static float[] randomVector(final Random rnd) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = rnd.nextFloat();
      return v;
    }
  }

  /**
   * Regression for #8905: build commits go through the wrapper on a replicated database.
   * <p>
   * Issue #8905: two gaps of {@code LSMVectorIndex.build()} on a replicated database.
   * <ol>
   *   <li>PHASE 3, {@code buildGraphWithChunking()}, committed its graph persist chunks on the inner
   *   {@link LocalDatabase} while the rest of {@code build()} commits through the wrapper. On an HA node that wrapper is
   *   the Raft-replicated database, so those chunks - and whatever bulk-loaded vector pages the build's transaction held
   *   at that point - landed on the building node's disk only. The graph file is node-local (every node persists its own
   *   graph, #8292), so routing those commits through the wrapper is no answer either: the graph pages have different
   *   versions on every node. PHASE 3 is now skipped on a replicated database, and the graph is built node-locally on
   *   first use, as every other node of the cluster builds it.</li>
   *   <li>The build's chunk size ({@code arcadedb.index.buildChunkSizeMB}, 50MB by default) was not bounded by the
   *   replicated entry cap ({@code GlobalConfiguration.maxReplicatedRaftEntrySize}, 32MB by default), and on the ordinary arm one chunk is one replicated entry.</li>
   * </ol>
   * The wrapper is a delegating proxy installed the way the HA plugin installs {@code RaftReplicatedDatabase}
   * ({@link LocalDatabase#setWrappedDatabaseInstance}); it counts the {@code commit()} calls it receives and can claim to
   * be replicated. The end-to-end behaviour on a real cluster is covered by {@code Issue8905VectorBuildHATest} in the
   * ha-raft module.
   */
  @Nested
  class VectorBuildCommitsReachTheWrapper extends TestHelper {

    // TWO DIMENSIONS ARE ESTIMATED AT 2 * 4 + 32 = 40 BYTES A VECTOR, SO 20_000 OF THEM ARE ~780KB: UNDER THE 1MB CHUNK
    // SET BELOW, SO THE BULK LOAD COMMITS NO CHUNK AND THE GRAPH STAYS UNLOADED (PHASE 3 IS ONLY REACHED THEN). THE GRAPH
    // OF 20_000 NODES, WITH ITS VECTORS INLINE, IS WELL PAST 1MB, SO ITS PERSIST CROSSES AT LEAST ONE CHUNK BOUNDARY
    private static final int    GRAPH_RECORDS    = 20_000;
    private static final int    GRAPH_DIMENSIONS = 2;
    private static final String GRAPH_TYPE       = "Issue8905Graph";

    // 3_000 VECTORS OF 32 DIMENSIONS ARE ESTIMATED AT 3_000 * 160 = ~470KB: ONE CHUNK AT THE 50MB DEFAULT
    private static final int    CAPPED_RECORDS    = 3_000;
    private static final int    CAPPED_DIMENSIONS = 32;
    private static final String CAPPED_TYPE       = "Issue8905Capped";

    @AfterEach
    void resetConfiguration() {
      GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.reset();
      GlobalConfiguration.HA_APPEND_BUFFER_SIZE.reset();
    }

    /**
     * Finding 1. The index is reopened from disk so its graph is not loaded, and the bulk load fits one chunk so nothing
     * loads it before PHASE 3: the only path into {@code buildGraphWithChunking()}, whose persist would cross a chunk
     * boundary and commit the build's transaction on the inner instance. On a replicated database the build has to skip
     * it, so its whole transaction - one chunk here - commits once, through the wrapper. The index still answers a search
     * afterwards: its graph is built on first use.
     */
    @Test
    void aReplicatedBuildCommitsOnlyThroughTheWrapperAndLeavesTheGraphToTheNode() {
      GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
      createVectorType(GRAPH_TYPE, GRAPH_RECORDS, GRAPH_DIMENSIONS, true);

      reopenDatabase();
      final LSMVectorIndex index = bucketIndex(GRAPH_TYPE);

      final AtomicInteger wrapperCommits = new AtomicInteger();
      final AtomicBoolean graphBuilt = new AtomicBoolean();
      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, true));
      try {
        index.build(null, (phase, processed, total, insertsInProgress) -> graphBuilt.set(true));
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(graphBuilt.get()).as("PHASE 3 must not run on a replicated database").isFalse();
      assertThat(wrapperCommits.get()).as("the build's one chunk commits once, through the wrapper").isEqualTo(1);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, 5)).hasSize(5);
    }

    /**
     * A guard for the same path off HA: a database that is not replicated still builds and persists the graph inside
     * the build, in chunks. The skip must cost a single node nothing.
     */
    @Test
    void aBuildThatIsNotReplicatedStillPersistsTheGraphInChunks() {
      GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB.setValue(1L);
      createVectorType(GRAPH_TYPE, GRAPH_RECORDS, GRAPH_DIMENSIONS, true);

      reopenDatabase();
      final LSMVectorIndex index = bucketIndex(GRAPH_TYPE);

      final AtomicBoolean graphBuilt = new AtomicBoolean();
      index.build(null, (phase, processed, total, insertsInProgress) -> graphBuilt.set(true));

      assertThat(graphBuilt.get()).as("the build has to reach PHASE 3, or this proves nothing").isTrue();
      assertThat(index.getGraphFile().getLastWrittenGraphBytes())
          .as("the graph is past the 1MB chunk, so its persist crossed a chunk boundary").isGreaterThan(1024L * 1024);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, 5)).hasSize(5);
    }

    /**
     * Finding 2. A build over ~470KB of estimated vectors fits one chunk at the 50MB default. On a database whose wrapper
     * is replicated with a 256KB entry cap, the chunk is clamped below that cap, so the same build commits more than
     * once.
     */
    @Test
    void aReplicatedBuildChunksBelowTheReplicatedEntryCap() {
      createVectorType(CAPPED_TYPE, 0, CAPPED_DIMENSIONS, false);
      createRecords(CAPPED_TYPE, CAPPED_RECORDS, CAPPED_DIMENSIONS);
      final LSMVectorIndex index = bucketIndex(CAPPED_TYPE);

      GlobalConfiguration.HA_APPEND_BUFFER_SIZE.setValue("256KB");

      final AtomicInteger wrapperCommits = new AtomicInteger();
      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, true));
      try {
        assertThat(index.build(null, null)).isEqualTo(CAPPED_RECORDS);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      // HALF OF THE 256KB CAP IS 128KB OF ESTIMATED VECTORS PER CHUNK: ~470KB IS THREE OF THEM, AND THE FINAL COMMIT
      assertThat(wrapperCommits.get()).as("the build has to commit in chunks below the replicated entry cap")
          .isGreaterThanOrEqualTo(4);
    }

    /**
     * A guard: the same build on a database that is not replicated keeps the configured chunk size, whatever the HA
     * settings say. The clamp costs a replicated database Raft round trips; it must cost a single node nothing.
     */
    @Test
    void aBuildThatIsNotReplicatedKeepsTheConfiguredChunk() {
      createVectorType(CAPPED_TYPE, 0, CAPPED_DIMENSIONS, false);
      createRecords(CAPPED_TYPE, CAPPED_RECORDS, CAPPED_DIMENSIONS);
      final LSMVectorIndex index = bucketIndex(CAPPED_TYPE);

      GlobalConfiguration.HA_APPEND_BUFFER_SIZE.setValue("256KB");

      final AtomicInteger wrapperCommits = new AtomicInteger();
      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local, wrapperCommits, false));
      try {
        assertThat(index.build(null, null)).isEqualTo(CAPPED_RECORDS);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(wrapperCommits.get()).as("one chunk at the 50MB default: the final commit only").isEqualTo(1);
    }

    private void createVectorType(final String typeName, final int records, final int dimensions,
        final boolean storeVectorsInGraph) {
      final VertexType type = database.getSchema().buildVertexType().withName(typeName).withTotalBuckets(1).create();
      type.createProperty("vector", float[].class);
      createRecords(typeName, records, dimensions);

      final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(typeName, new String[] { "vector" })
          .withLSMVectorType();
      builder.withDimensions(dimensions);
      builder.withStoreVectorsInGraph(storeVectorsInGraph);
      builder.create();
    }

    private void createRecords(final String typeName, final int records, final int dimensions) {
      database.transaction(() -> {
        for (int i = 0; i < records; i++) {
          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++)
            vector[j] = ((i * 31 + j * 7) % 1_009 + 1) / 1_009f;
          database.newVertex(typeName).set("vector", vector).save();
        }
      });
    }

    private LSMVectorIndex bucketIndex(final String typeName) {
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName(typeName + "[vector]");
      return (LSMVectorIndex) index.getIndexesOnBuckets()[0];
    }

    private static DatabaseInternal countingWrapper(final DatabaseInternal delegate, final AtomicInteger commits,
        final boolean replicated) {
      return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("commit".equals(method.getName()) && (args == null || args.length == 0))
              commits.incrementAndGet();
            else if ("isReplicated".equals(method.getName()) && (args == null || args.length == 0))
              return replicated;
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  /**
   * Regression for #8906: a failed chunk commit during the bulk load fails the build.
   * <p>
   * Issue #8906: the bulk load of an LSM_VECTOR build commits its transaction at each chunk boundary from inside the
   * {@code scanBucket} callback, and the scan logs and swallows whatever its callback throws. A failed chunk commit
   * therefore never reached {@code build()}, which went on to report a partial index as READY.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class BuildChunkCommitFailure extends TestHelper {
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
            .hasMessageContaining("chunk commit");
      } finally {
        release.countDown();
        blocker.join(TimeUnit.SECONDS.toMillis(30));
      }

      assertThat(index.isValid()).as("a partial build must not be left looking usable").isFalse();
      assertThat(database.isTransactionActive()).isFalse();
    }
  }

  /**
   * Regression for #9233: graph build vector lookups by ordinal.
   * <p>
   * Regression tests for issue #9233: a graph build whose cache holds the whole corpus answers jvector's vector lookups
   * from a flat by-ordinal array instead of walking ordinal -> vector id -> cache slot -> entry. The answer must be the
   * very vector the cache holds, and a build whose cache is smaller than the corpus must keep going through the cache so
   * the array never pins vectors the cache's budget evicted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BuildVectorsByOrdinal {
    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int                DIMS = 4;

    private static VectorFloat<?> vector(final int seed) {
      final float[] data = new float[DIMS];
      for (int i = 0; i < DIMS; i++)
        data[i] = seed + i;
      return VTS.createFloatVector(data);
    }

    @Test
    void buildWithWholeCorpusInCacheServesTheCachedVectors() {
      final int n = 1000;
      final int[] ordinalToVectorId = new int[n];
      final VectorCache cache = new VectorCache(n);
      final VectorFloat<?>[] expected = new VectorFloat<?>[n];
      for (int i = 0; i < n; i++) {
        // Ids are not the ordinals: the mapping has to be honoured
        ordinalToVectorId[i] = n - 1 - i;
        expected[i] = vector(i);
        cache.put(ordinalToVectorId[i], expected[i]);
      }

      final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
          cache);

      for (int i = 0; i < n; i++)
        assertThat(values.getVector(i)).as("ordinal %d", i).isSameAs(expected[i]);

      // Served by the ordinal array, not the cache: the vector is still there once the cache has dropped it
      cache.remove(ordinalToVectorId[0]);
      assertThat(values.getVector(0)).isSameAs(expected[0]);
    }

    @Test
    void buildWithSmallerCacheStillGoesThroughTheCache() {
      final int n = 1000;
      final int[] ordinalToVectorId = new int[n];
      for (int i = 0; i < n; i++)
        ordinalToVectorId[i] = i;
      final VectorCache cache = new VectorCache(64);
      final VectorFloat<?> first = vector(1);
      cache.put(0, first);

      final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
          cache);
      assertThat(values.getVector(0)).isSameAs(first);

      // The cache is the only owner: once it drops the vector, the build reader does not keep serving it
      cache.remove(0);
      assertThat(values.getVector(0)).isNotSameAs(first);
    }

    @Test
    void ordinalOutOfRangeYieldsTheSentinel() {
      final VectorCache cache = new VectorCache(8);
      final int[] ordinalToVectorId = { 0, 1 };
      cache.put(0, vector(0));
      cache.put(1, vector(1));
      final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
          cache);

      assertThat(values.isDeletedSentinel(values.getVector(2))).isTrue();
      assertThat(values.isDeletedSentinel(values.getVector(-1))).isTrue();
    }

    @Test
    void capacityExactlyEqualToCorpusStillServesEveryVector() {
      final int n = 1024;
      final int[] ordinalToVectorId = new int[n];
      final VectorCache cache = new VectorCache(n);
      assertThat(cache.capacity()).isEqualTo(n);
      final VectorFloat<?>[] expected = new VectorFloat<?>[n];
      for (int i = 0; i < n; i++) {
        ordinalToVectorId[i] = i;
        expected[i] = vector(i);
        cache.put(i, expected[i]);
      }

      final ArcadePageVectorValues values = ArcadePageVectorValues.forGraphBuild(null, DIMS, "v", null, ordinalToVectorId, null,
          cache);
      for (int i = 0; i < n; i++)
        assertThat(values.getVector(i)).as("ordinal %d", i).isSameAs(expected[i]);
    }
  }

  /**
   * Regression for #9506: build() over an index that already holds entries replaces the vector ids.
   * <p>
   * Issue #9506: {@code LSMVectorIndex.build()} over an index that already holds entries appended a second vector id per
   * record instead of replacing the existing one, so {@code countEntries()} doubled, a later delete tombstoned two ids per
   * record, and a search, which builds the graph keyed by RID, then disagreed with the count. A dense vector index keeps
   * ONE live vector per record, and a build over a populated index must preserve that.
   */
  @Nested
  class BuildOverPopulatedIndex extends TestHelper {
    private static final String TYPE    = "Probe";
    private static final int    RECORDS = 10;

    @Test
    void aBuildOverAPopulatedIndexKeepsOneEntryPerRecord() {
      final List<RID> rids = populate();
      final LSMVectorIndex index = bucketIndex();
      assertThat(index.countEntries()).isEqualTo(RECORDS);
      final Set<Integer> beforeBuild = liveIds(index, rids);

      assertThat(index.build(null, null)).isEqualTo(RECORDS);
      final Set<Integer> afterFirstBuild = assertOneLiveEntryPerRecord(index, rids);
      // REPLACED, not kept: every record got a fresh id and the ids it had before the build are tombstoned
      assertThat(afterFirstBuild).doesNotContainAnyElementsOf(beforeBuild);
      for (final int id : beforeBuild)
        assertThat(index.getVectorIndex().isLive(id)).as("vector id %d held before the build", id).isFalse();

      // A second build is no different: the count does not grow with the number of builds
      index.build(null, null);
      assertThat(assertOneLiveEntryPerRecord(index, rids)).doesNotContainAnyElementsOf(afterFirstBuild);
    }

    /**
     * A build that commits in several chunks: each record's {@code REMOVE} and {@code ADD} are queued in the same chunk,
     * so no chunk commit can leave a record with its old id tombstoned and no new one, or with both live.
     */
    @Test
    void aBuildOverAPopulatedIndexAcrossSeveralChunksKeepsOneEntryPerRecord() {
      // 512 DIMENSIONS ARE ESTIMATED AT 512 * 4 + 32 = 2_080 BYTES A RECORD, SO 1_500 RECORDS ARE ~3MB: THREE CHUNKS OF 1MB
      final int dimensions = 512;
      final int records = 1_500;
      ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_CHUNK_SIZE_MB, 1L);

      database.command("sql", "CREATE DOCUMENT TYPE Wide BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Wide.vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Wide (vector) LSM_VECTOR METADATA { \"dimensions\": " + dimensions + " }");

      final Random random = new Random(9506L);
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < records; i++) {
          final float[] vector = new float[dimensions];
          for (int j = 0; j < dimensions; j++)
            vector[j] = (float) random.nextGaussian();
          rids.add(database.newDocument("Wide").set("vector", vector).save().getIdentity());
        }
      });

      final LSMVectorIndex index =
          (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Wide[vector]")).getIndexesOnBuckets()[0];
      assertThat(index.build(null, null)).isEqualTo(records);

      assertThat(index.countEntries()).isEqualTo(records);
      for (final RID rid : rids)
        assertThat(index.getVectorIndex().getVectorIdsForRid(rid)).as("live vector ids of %s", rid).hasSize(1);
    }

    /**
     * A build sharing a transaction its caller opened (the {@code CREATE INDEX} inside an open transaction path, issue
     * #6324) never commits a chunk of its own, so the replacement has to land in the caller's commit.
     */
    @Test
    void aBuildSharingTheCallersTransactionKeepsOneEntryPerRecord() {
      final List<RID> rids = populate();
      final LSMVectorIndex index = bucketIndex();

      database.transaction(() -> index.build(0, true, null));
      assertOneLiveEntryPerRecord(index, rids);
    }

    @Test
    void aDeleteAfterABuildOverAPopulatedIndexRemovesExactlyOneEntry() {
      final List<RID> rids = populate();
      final LSMVectorIndex index = bucketIndex();
      index.build(null, null);

      database.transaction(() -> rids.getFirst().asDocument().delete());

      assertThat(index.countEntries()).isEqualTo(RECORDS - 1);
      assertThat(index.getVectorIndex().getVectorIdsForRid(rids.getFirst())).isEmpty();
    }

    @Test
    void aSearchAfterABuildOverAPopulatedIndexReturnsEveryRecordOnceAndLeavesTheCountAlone() {
      final List<RID> rids = populate();
      final LSMVectorIndex index = bucketIndex();
      index.build(null, null);
      // Before the search too: the graph a search builds keeps one entry per RID, which used to hide the duplicates
      assertThat(index.countEntries()).isEqualTo(RECORDS);

      final List<Pair<RID, Float>> neighbors = index.findNeighborsFromVector(new float[] { 0.5f, 0.5f }, RECORDS * 3);
      final List<RID> found = new ArrayList<>();
      for (final Pair<RID, Float> neighbor : neighbors)
        found.add(neighbor.getFirst());

      assertThat(found).doesNotHaveDuplicates();
      assertThat(new HashSet<>(found)).isEqualTo(new HashSet<>(rids));
      assertThat(index.countEntries()).isEqualTo(RECORDS);
    }

    private List<RID> populate() {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + TYPE + ".vector ARRAY_OF_FLOATS");
      // EUCLIDEAN, so that the all-zero vector below is indexed and returned like any other (issue #8962)
      database.command("sql",
          "CREATE INDEX ON " + TYPE + " (vector) LSM_VECTOR METADATA { \"dimensions\": 2, \"similarity\": \"EUCLIDEAN\" }");

      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        // The origin included on purpose: a removal keyed by an all-zero placeholder would coincide with its key
        rids.add(database.newDocument(TYPE).set("vector", new float[] { 0f, 0f }).save().getIdentity());
        for (int i = 1; i < RECORDS; i++)
          rids.add(database.newDocument(TYPE).set("vector", new float[] { i / 10f, (RECORDS - i) / 10f }).save().getIdentity());
      });
      return rids;
    }

    private LSMVectorIndex bucketIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(TYPE + "[vector]")).getIndexesOnBuckets()[0];
    }

    private static Set<Integer> assertOneLiveEntryPerRecord(final LSMVectorIndex index, final List<RID> rids) {
      assertThat(index.countEntries()).as("live entries after a build over a populated index").isEqualTo(RECORDS);
      for (final RID rid : rids)
        assertThat(index.getVectorIndex().getVectorIdsForRid(rid)).as("live vector ids of %s", rid).hasSize(1);
      final Set<Integer> ids = liveIds(index, rids);
      assertThat(ids).hasSize(RECORDS);
      return ids;
    }

    private static Set<Integer> liveIds(final LSMVectorIndex index, final List<RID> rids) {
      final Set<Integer> ids = new HashSet<>();
      for (final RID rid : rids)
        for (final int id : index.getVectorIndex().getVectorIdsForRid(rid))
          ids.add(id);
      return ids;
    }
  }

  /**
   * Regression for #9917: side files of a persisted graph are observed consistently.
   * <p>
   * Issue #9917: the side files of a persisted vector graph are observed before a point in time and accepted after it only
   * when nothing replaced them in between, so a full backup can archive them with the graph's pages and the restored
   * database can verify and reuse the graph instead of rebuilding it. See {@link LSMVectorIndexGraphSideFiles} for why
   * that pairing is sound; these tests drive each rule of it through the real writers.
   */
  @Nested
  @Tag("vector")
  class GraphSideFiles extends TestHelper {
    private static final int DIMENSIONS = 16;
    private static final int VECTORS    = 400;

    @Test
    void anIdleGraphIsVouchedForByItsManifestItsOrdinalMapAndItsPQ() throws Exception {
      createVectorIndex("Doc", "NONE");
      createVectorIndex("PqDoc", "PRODUCT");

      final Map<Integer, LSMVectorIndexGraphSideFiles> captured = LSMVectorIndexGraphSideFiles.capture((DatabaseInternal) database);
      assertThat(captured).as("both graphs are idle, so both are vouched for")
          .containsOnlyKeys(graphFile("Doc").getFileId(), graphFile("PqDoc").getFileId());

      try (final LSMVectorIndexGraphSideFiles.Opened opened = captured.get(graphFile("Doc").getFileId()).openIfUnchanged()) {
        assertThat(opened).isNotNull();
        assertThat(names(opened)).as("a non-quantized index carries no PQ, and the manifest comes last")
            .containsExactly(ordinalMapName("Doc"), manifestName("Doc"));
      }

      final Path pq = vectorIndex("PqDoc").getProductQuantizationFilePath();
      assertThat(pq).as("the fixture really is PRODUCT-quantized, with its PQ on disk").isNotNull();
      assertThat(Files.exists(pq)).isTrue();
      try (final LSMVectorIndexGraphSideFiles.Opened opened = captured.get(graphFile("PqDoc").getFileId()).openIfUnchanged()) {
        assertThat(opened).isNotNull();
        assertThat(names(opened)).as("a PRODUCT-quantized index carries its ordinal map and its PQ, and the manifest comes last")
            .containsExactly(ordinalMapName("PqDoc"), pq.getFileName().toString(), manifestName("PqDoc"));
      }
    }

    @Test
    void aPersistBetweenTheCaptureAndTheOpenRefusesTheSideFiles() throws Exception {
      createVectorIndex("Doc", "NONE");
      final LSMVectorIndexGraphSideFiles sideFiles = LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"));
      assertThat(sideFiles).isNotNull();

      // THE RACE THE PROTOCOL EXISTS FOR: the pages captured at t0 are no longer the ones the manifest on disk describes
      vectorIndex("Doc").buildVectorGraphNow();

      assertThat(sideFiles.openIfUnchanged()).as("a manifest replaced since the capture cannot vouch for the pages at t0")
          .isNull();
    }

    @Test
    void aGraphWithNoManifestIsNotVouchedFor() {
      createVectorIndex("Doc", "NONE");
      // THE STATE OF A PERSIST IN FLIGHT: writeGraph drops the manifest before it touches the first page
      graphFile("Doc").getManifest().invalidate();

      assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"))).isNull();
      assertThat(LSMVectorIndexGraphSideFiles.capture((DatabaseInternal) database)).isEmpty();
    }

    @Test
    void aGraphPersistDropsThePQBeforeItRewritesThePages() {
      createVectorIndex("PqDoc", "PRODUCT");
      final Path pq = vectorIndex("PqDoc").getProductQuantizationFilePath();
      assertThat(Files.exists(pq)).as("the fixture: the first build persisted a PQ").isTrue();

      // REPORTED AFTER THE PAGES ARE WRITTEN AND BEFORE THE MANIFEST IS: the window in which the pages on disk are the
      // new graph's and the PQ, if it were still there, the previous graph's
      final boolean[] observed = { false, false, false };
      vectorIndex("PqDoc").buildVectorGraphNow((phase, processed, total, accesses) -> {
        if ("persisting".equals(phase) && processed == total && total > 0) {
          observed[0] = true;
          observed[1] = Files.exists(pq);
          observed[2] = graphFile("PqDoc").getManifest().exists();
        }
      });

      assertThat(observed[0]).as("the fixture: the persist reported its pages written").isTrue();
      assertThat(observed[2]).as("the manifest is gone while the pages change").isFalse();
      assertThat(observed[1]).as("and so is the PQ written for the previous graph, so nothing can pair it with the new one")
          .isFalse();
      assertThat(Files.exists(pq)).as("the persist writes the new graph's PQ once it is certified").isTrue();
      assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc"))).isNotNull();
    }

    @Test
    void aPQThatCannotBeDroppedBeforeAPersistIsNotVouchedFor() throws Exception {
      createVectorIndex("PqDoc", "PRODUCT");
      final Path pq = vectorIndex("PqDoc").getProductQuantizationFilePath();

      // A PQ THAT NEITHER THE DROP BEFORE THE PERSIST NOR THE RENAME AFTER IT CAN REPLACE: a non-empty directory under its
      // name. The persist still completes and certifies its graph, so the manifest is new and unchanged - and the thing
      // under the PQ's name is not the new graph's PQ
      assertThat(pq.toFile().delete()).isTrue();
      Files.createDirectories(pq.resolve("blocker"));
      try {
        vectorIndex("PqDoc").buildVectorGraphNow();
        assertThat(graphFile("PqDoc").getManifest().exists()).as("the fixture: the persist certified its graph").isTrue();
        assertThat(Files.exists(pq)).as("the fixture: the stale entry survived the persist").isTrue();
        assertThat(vectorIndex("PqDoc").isPersistedPQKnownStale()).isTrue();

        assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc"))).isNull();
      } finally {
        Files.deleteIfExists(pq.resolve("blocker"));
        Files.deleteIfExists(pq);
      }
    }

    @Test
    void aManifestRewrittenWithoutNewPagesStillPairsWithItsPQ() {
      createVectorIndex("PqDoc", "PRODUCT");

      // markCloseDeferred rewrites the manifest - so it is now NEWER than the PQ - without touching the pages: the PQ
      // still belongs to them, and the pair is vouched for without comparing the two files' modification times
      graphFile("PqDoc").getManifest().markCloseDeferred();

      assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc"))).isNotNull();
    }

    @Test
    void aGraphComponentThatIsNotItsIndexsCurrentGraphIsNotVouchedFor() throws Exception {
      createVectorIndex("Doc", "NONE");
      final DatabaseInternal db = (DatabaseInternal) database;

      // THE SHAPE A COMPACT INDEX LEAVES BEHIND: the pre-compaction graph component still registered and still pointing
      // at its index, while the index has moved on to a replacement. Its side files are not the index's to vouch for
      final LSMVectorIndexGraphFile stale = new LSMVectorIndexGraphFile(db, "issue9917-stale", db.getDatabasePath() + "/issue9917-stale",
          ComponentFile.MODE.READ_WRITE, graphFile("Doc").getPageSize());
      try {
        stale.setMainIndex(vectorIndex("Doc"));
        assertThat(vectorIndex("Doc").getGraphFile()).isNotSameAs(stale);

        assertThat(LSMVectorIndexGraphSideFiles.capture(stale)).isNull();
        assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"))).as("the index's own graph still is").isNotNull();
      } finally {
        stale.close();
      }
    }

    @Test
    void aPQMissingFromAProductQuantizedIndexIsRefused() {
      createVectorIndex("PqDoc", "PRODUCT");
      assertThat(vectorIndex("PqDoc").getProductQuantizationFilePath().toFile().delete()).isTrue();

      assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc")))
          .as("the restored index would rebuild its graph for want of a PQ anyway").isNull();
    }

    @Test
    void filesOpenedWhileUnchangedKeepTheirBytesWhenAPersistReplacesThemDuringTheCopy() throws Exception {
      // BOTH SHAPES: the non-quantized index has an ordinal map, the PRODUCT-quantized one an ordinal map and a PQ
      createVectorIndex("Doc", "NONE");
      createVectorIndex("PqDoc", "PRODUCT");
      final Path directory = Path.of(database.getDatabasePath());
      for (final String type : new String[] { "Doc", "PqDoc" }) {
        final LSMVectorIndexGraphSideFiles sideFiles = LSMVectorIndexGraphSideFiles.capture(graphFile(type));
        assertThat(sideFiles).isNotNull();

        final Map<String, byte[]> before = new HashMap<>();
        try (final LSMVectorIndexGraphSideFiles.Opened opened = sideFiles.openIfUnchanged()) {
          assertThat(opened).isNotNull();
          assertThat(opened.entries()).hasSize(type.equals("PqDoc") ? 3 : 2);
          for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
            before.put(entry.fileName(), Files.readAllBytes(directory.resolve(entry.fileName())));

          // EVERY SIDE FILE IS REPLACED WHILE THE STREAMS ARE OPEN, by a persist over MORE vectors, so that what replaces
          // them cannot be byte-identical to what they held
          insertVectors(type, VECTORS, 50);
          vectorIndex(type).buildVectorGraphNow();
          for (final String name : before.keySet())
            assertThat(Files.readAllBytes(directory.resolve(name)))
                .as("the fixture: the persist really did replace %s", name).isNotEqualTo(before.get(name));
          assertThat(sideFiles.openIfUnchanged()).as("and the old observation no longer matches").isNull();

          for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
            try (final InputStream input = entry.input()) {
              assertThat(input.readAllBytes()).as("%s: '%s' is read as it was when it was opened", type, entry.fileName())
                  .isEqualTo(before.get(entry.fileName()));
            }
        }
      }
    }

    // ------------------------------------------------------------------------------------------------------- HELPERS

    private void createVectorIndex(final String type, final String quantization) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
      });
      database.command("sql", "CREATE INDEX ON `" + type + "` (`embedding`) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
          + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
      insertVectors(type, 0, VECTORS);
      vectorIndex(type).buildVectorGraphNow();
    }

    private void insertVectors(final String type, final int from, final int count) {
      database.transaction(() -> {
        for (int i = from; i < from + count; i++) {
          final float[] vector = new float[DIMENSIONS];
          for (int j = 0; j < DIMENSIONS; j++)
            vector[j] = (float) Math.sin(i * 0.37 + j * 1.13) + 1.5f;
          database.newDocument(type).set("embedding", vector).save();
        }
      });
    }

    private LSMVectorIndex vectorIndex(final String type) {
      return vectorIndexOf(database, type);
    }

    private static LSMVectorIndex vectorIndexOf(final Database db, final String type) {
      return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0];
    }

    private LSMVectorIndexGraphFile graphFile(final String type) {
      final LSMVectorIndexGraphFile graphFile = vectorIndex(type).getGraphFile();
      assertThat(graphFile).as("the index must have persisted its graph").isNotNull();
      return graphFile;
    }

    private String manifestName(final String type) {
      return graphFile(type).getManifest().getFilePath().getFileName().toString();
    }

    private String ordinalMapName(final String type) {
      return graphFile(type).getManifest().getOrdinalMapFilePath().getFileName().toString();
    }

    private static List<String> names(final LSMVectorIndexGraphSideFiles.Opened opened) {
      final List<String> names = new ArrayList<>();
      for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
        names.add(entry.fileName());
      return names;
    }
  }
}
