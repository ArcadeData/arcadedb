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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
@Tag("vector")
class Issue7362PersistedGraphLengthTest extends TestHelper {
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
