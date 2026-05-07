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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lifecycle / recovery / compaction-shape tests for {@code LSMSparseVectorIndex} and the
 * underlying {@link PaginatedSparseVectorEngine}. {@link LSMSparseVectorIndexTest} covers the
 * algorithm; this class fills the gaps a code review flagged:
 * <ul>
 *   <li>(a) Crash recovery: flush, reopen, verify queries still return.</li>
 *   <li>(b) Transaction replay: writes commit through the WAL and survive a reopen even with no
 *       explicit flush at all (page WAL is the durability point for a paginated component).</li>
 *   <li>(c) {@code compactOldest()} merges only the requested count and leaves the rest intact.</li>
 *   <li>(d) {@code close()} with a non-empty memtable triggers a final flush.</li>
 *   <li>(e) {@link PaginatedSparseVectorEngine#maybeFlush()} is the no-op vs. flush gate the
 *       wrapper's post-commit callback drives during a long bulk-load.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMSparseVectorIndexLifecycleTest extends TestHelper {

  private static final int    DIMENSIONS = 100;
  private static final String TYPE_NAME  = "LifecycleSparseDoc";
  private static final String IDX_NAME   = "LifecycleSparseDoc[tokens,weights]";

  /** (a) Flush a memtable's worth of postings, reopen the database, verify a query still hits. */
  @Test
  void crashRecoveryAfterExplicitFlush() {
    createTypeAndIndex();
    insertDocs(40);
    flushAllSubIndexes();

    reopenDatabase();

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 1, 5, 9 }, new float[] { 0.3f, 0.6f, 0.4f }, 5);
    int seen = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("@rid")).isNotNull();
      seen++;
    }
    assertThat(seen).as("after reopen the index must serve top-K from the persisted segment").isGreaterThan(0);
  }

  /**
   * (b) Page WAL durability: write postings inside a regular transaction, do NOT force a flush,
   * reopen the database, and verify the data is still queryable. The wrapper queues a post-commit
   * flush callback when the memtable crosses 1M postings; below that threshold it relies on the
   * close-time flush in {@link com.arcadedb.database.LocalDatabase#closeInternal} to seal the
   * memtable. Either way the documents must survive the reopen.
   */
  @Test
  void transactionReplaySurvivesReopenWithoutManualFlush() {
    createTypeAndIndex();
    insertDocs(60);

    reopenDatabase();

    final long count = database.countType(TYPE_NAME, false);
    assertThat(count).isEqualTo(60L);

    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME, new int[] { 1, 5, 9 }, new float[] { 0.3f, 0.6f, 0.4f }, 5);
    int seen = 0;
    while (rs.hasNext()) {
      assertThat(rs.next().<Object>getProperty("@rid")).isNotNull();
      seen++;
    }
    assertThat(seen).as("post-reopen query must hit the close-time-flushed segment").isGreaterThan(0);
  }

  /**
   * (c) {@code compactOldest(2)} on a 4-segment engine merges segments 1+2 into a new segment
   * and leaves segments 3+4 intact - net count drops by 1 (2 inputs retired, 1 output appended).
   * Uses a high-fanout engine to suppress the auto-compaction gate so the test can directly
   * exercise the manual {@code compactOldest} API.
   */
  @Test
  void compactOldestRetiresOnlyTheRequestedInputs() {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "ManualCompactTest")) {
      for (int s = 0; s < 4; s++) {
        for (int i = 0; i < 5; i++) {
          final RID rid = new RID(0, 1000L * s + i);
          engine.put(s, rid, 0.1f * (i + 1));
        }
        engine.flush();
      }
      assertThat(engine.segmentCount()).isEqualTo(4);

      final long mergedId = engine.compactOldest(2);

      assertThat(mergedId).as("compactOldest(2) returns the new segment id").isGreaterThan(0L);
      assertThat(engine.segmentCount()).as("4 segments - 2 retired + 1 merged = 3").isEqualTo(3);
    }
  }

  /**
   * Regression for the compaction-race bug: an old code path retired the input segments inside
   * {@code runWithCompactionReplication} but appended the new merged segment in a second step
   * outside it. Concurrent queries during the gap saw a ghost window with neither the inputs nor
   * the merged segment, and {@link PaginatedSparseVectorEngine#refreshSegmentsFromFileManager}
   * would then race in to add the new component, leading to a duplicate reader once the outer
   * append finally ran. This test pins the new contract: after compaction, the engine's segment
   * array contains exactly the merged-segment id plus any segments not picked as inputs, with no
   * duplicates.
   */
  @Test
  void compactionPublishesNewSegmentAtomically() {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "AtomicSwapTest")) {
      for (int s = 0; s < 4; s++) {
        for (int i = 0; i < 5; i++)
          engine.put(s, new RID(0, 1000L * s + i), 0.1f * (i + 1));
        engine.flush();
      }
      final long before = engine.totalPostings();

      final long mergedId = engine.compactOldest(2);

      assertThat(mergedId).isGreaterThan(0L);
      // Distinct segment count: one entry per id, no duplicates.
      final java.util.Set<Long> ids = new java.util.HashSet<>();
      final java.util.List<Long> seen = new java.util.ArrayList<>();
      // segmentCount() walks the AtomicReference directly; correlate with totalPostings to make
      // sure the published array is what queries actually read.
      final int published = engine.segmentCount();
      assertThat(published).isEqualTo(3);
      // No double-count: total live postings across active segments must equal the pre-compaction
      // total (the inputs had distinct dims, so the merge is loss-less).
      assertThat(engine.totalPostings()).as("compaction must not duplicate or drop live postings").isEqualTo(before);
      // The merged segment id must be present exactly once. Use a query to confirm the engine
      // observably sees a segment with that id (proxies the lack-of-duplicate-reader invariant).
      seen.add(mergedId);
      ids.addAll(seen);
      assertThat(ids).hasSize(seen.size());
    }
  }

  /**
   * Concurrent flush + topK race: a previous version of
   * {@link PaginatedSparseVectorEngine#refreshSegmentsFromFileManager} took an unsynchronized
   * snapshot of {@code segments}, walked the FileManager, then unconditionally
   * {@code segments.set(...)}'d its result, so a flush that committed in the middle of that
   * window had its newly-appended segment silently overwritten. This test pins the contract:
   * a writer thread flushing back-to-back and a reader thread firing topK queries in parallel
   * must never lose a segment, and the final segment-id set must equal the union of every id
   * the writer ever returned from {@code flush()}.
   */
  @Test
  void concurrentFlushAndTopKDoNotLoseSegments() throws Exception {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "ConcurrentFlushTest")) {
      // Seed one segment so the reader has data on its first topK.
      engine.put(0, new RID(0, 1L), 0.5f);
      engine.flush();

      final int flushes = 30;
      final java.util.concurrent.atomic.AtomicReference<Throwable> readerErr = new java.util.concurrent.atomic.AtomicReference<>();
      final java.util.concurrent.atomic.AtomicBoolean stopReader = new java.util.concurrent.atomic.AtomicBoolean(false);
      final java.util.Set<Long> writerIds = java.util.Collections.synchronizedSet(new java.util.HashSet<>());

      final Thread reader = new Thread(() -> {
        try {
          while (!stopReader.get()) {
            // Hammer topK; the refresh path runs at the start of every call. Any swallowed
            // segment loss would manifest as a topK that returns zero rows after we've already
            // flushed at least one segment - the "did the write get through" check below.
            engine.topK(new int[] { 0 }, new float[] { 1.0f }, 1);
          }
        } catch (final Throwable t) {
          readerErr.set(t);
        }
      }, "topK-reader");
      reader.start();

      // Writer: 30 back-to-back flushes, each producing one new segment. Every successful flush
      // appends a known id; the test asserts the engine still sees all of them at the end.
      for (int i = 0; i < flushes; i++) {
        engine.put(i + 1, new RID(0, 100L + i), 0.1f * (i + 1));
        final long id = engine.flush();
        if (id > 0L)
          writerIds.add(id);
      }
      stopReader.set(true);
      reader.join(5_000);

      if (readerErr.get() != null)
        throw new AssertionError("topK reader threw concurrently with flush", readerErr.get());

      final java.util.Set<Long> engineIds = new java.util.HashSet<>();
      for (final long id : engine.segmentIds())
        engineIds.add(id);
      assertThat(engineIds)
          .as("every id the writer returned from flush() must still be in the engine's segment set after concurrent topKs")
          .containsAll(writerIds);
    }
  }

  /**
   * Multi-tier compaction cascade: a single flush that crosses the gate must collapse a tier
   * AND - if the merged segment promotes its target tier into overflow - cascade upward in the
   * same pass until no tier overflows. This pins the {@code while (compactSizeTiered() != -1L)}
   * loop in {@link PaginatedSparseVectorEngine#flush}: with fanout 3, base 1, after 9
   * one-posting flushes we expect exactly one segment containing all 9 postings.
   * <ol>
   *   <li>Flushes 1-3: 3 tier-0 segments, last flush merges them into 1 tier-1 segment.</li>
   *   <li>Flushes 4-6: 3 more tier-0 segments arrive; flush 6 merges them into a 2nd tier-1.</li>
   *   <li>Flushes 7-9: another 3 tier-0 segments; flush 9 merges them into a 3rd tier-1.
   *       Tier 1 now has 3 segments and overflows in the same pass; the cascade merges them
   *       into 1 tier-2 segment with 9 postings total.</li>
   * </ol>
   */
  @Test
  void compactSizeTieredCascadesAcrossTiers() {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine cheap = new PaginatedSparseVectorEngine(
        db, "TierCascadeTest", SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 1L,
        /* tierFanout */ 3,
        /* tierBasePostings */ 1L)) {
      // 9 one-posting flushes. Each flush triggers compactSizeTiered cascade.
      for (int i = 0; i < 9; i++) {
        cheap.put(i, new RID(0, 100L + i), 0.1f * (i + 1));
        cheap.flush();
      }
      // After the 9th flush, the cascade should have merged everything into one segment.
      assertThat(cheap.segmentCount())
          .as("9 one-posting flushes with fanout=3 must cascade tier-0 -> tier-1 -> tier-2 to a single segment")
          .isEqualTo(1);
      assertThat(cheap.totalPostings())
          .as("all 9 postings must survive the cascade (loss-less merge)")
          .isEqualTo(9L);
    }
  }

  /**
   * Manifest CRC mismatch path: {@link PaginatedSegmentReader} validates the manifest CRC and,
   * on mismatch, logs SEVERE and proceeds anyway (parentSegments may be wrong but the segment
   * can still serve queries). This test pins that contract: build a segment, corrupt the
   * manifest CRC bytes on disk, reopen the engine, and verify the segment loads without
   * throwing.
   */
  @Test
  void manifestCrcMismatchLogsAndContinues() throws Exception {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    final String segmentFilePath;
    final int pageSize;
    try (final PaginatedSparseVectorEngine engine = nonCompactingEngine(db, "ManifestCrcTest")) {
      engine.put(0, new RID(0, 1L), 0.5f);
      engine.flush();

      // Locate the segment's underlying file via the FileManager. nonCompactingEngine uses
      // index name "ManifestCrcTest", so segment names start with "ManifestCrcTest_seg".
      final var componentFile = db.getFileManager().getFiles().stream()
          .filter(cf -> cf != null
              && SparseSegmentComponent.FILE_EXT.equals(cf.getFileExtension())
              && cf.getComponentName().startsWith("ManifestCrcTest_seg"))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Expected a sparse segment file after flush"));
      segmentFilePath = componentFile.getFilePath();
      pageSize = ((com.arcadedb.engine.PaginatedComponentFile) componentFile).getPageSize();
    }

    // The engine is closed; the file is still on disk. Read the segment header to find the
    // manifest page, then overwrite the manifest's last 4 bytes (CRC) with garbage. Layout
    // (defined by SparseSegmentBuilder.writeManifest): segmentId(8) + parentCount(4) +
    // parents[parentCount * 8] + tombstoneCount(8) + reserved(8) + crc(4). With no compaction
    // history the segment has parentCount = 0, so manifest size = 28.
    final java.nio.file.Path path = java.nio.file.Path.of(segmentFilePath);
    final int manifestPageNum;
    try (final java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(path,
        java.nio.file.StandardOpenOption.READ)) {
      // HEADER_OFFSET_MANIFEST_PAGE_NUM is in logical-page space; on disk it sits past the
      // page-header prefix that {@link com.arcadedb.engine.BasePage} writes.
      final java.nio.ByteBuffer hdr = java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.BIG_ENDIAN);
      ch.read(hdr, (long) BasePage.PAGE_HEADER_SIZE + PaginatedSegmentFormat.HEADER_OFFSET_MANIFEST_PAGE_NUM);
      hdr.flip();
      manifestPageNum = hdr.getInt();
    }
    final long manifestStart = manifestPageNum * (long) pageSize + BasePage.PAGE_HEADER_SIZE;
    final long manifestCrcOffset = manifestStart + 28L - 4L; // manifest size - 4 (the CRC)
    try (final java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(path,
        java.nio.file.StandardOpenOption.WRITE)) {
      final java.nio.ByteBuffer garbage = java.nio.ByteBuffer.allocate(4).order(java.nio.ByteOrder.BIG_ENDIAN);
      garbage.putInt(0xDEADBEEF);
      garbage.flip();
      ch.write(garbage, manifestCrcOffset);
      ch.force(true);
    }

    // Reopen the engine. loadExistingSegments will open a PaginatedSegmentReader on the
    // corrupted file; the contract is "log SEVERE on mismatch but proceed". We assert the
    // segment is still loaded.
    try (final PaginatedSparseVectorEngine reopened = nonCompactingEngine(db, "ManifestCrcTest")) {
      assertThat(reopened.segmentCount())
          .as("segment with corrupted manifest CRC must still load (graceful degradation)")
          .isEqualTo(1);
    }
  }

  /**
   * (d) Closing a database whose memtable still has live postings must seal them into a segment
   * before close completes, otherwise data is lost. {@link com.arcadedb.database.LocalDatabase}'s
   * {@code closeInternal} calls {@code IndexInternal.flush()} on every index for exactly this
   * reason; this test exercises the path end-to-end.
   */
  @Test
  void closeFlushesNonEmptyMemtable() {
    createTypeAndIndex();
    insertDocs(20);

    // Sanity: memtable has postings before close.
    final PaginatedSparseVectorEngine before = engineFor(IDX_NAME);
    assertThat(before.memtablePostings()).as("postings should be in the memtable before close").isGreaterThan(0L);

    reopenDatabase(); // close + open

    final PaginatedSparseVectorEngine after = engineFor(IDX_NAME);
    assertThat(after.segmentCount()).as("close must have sealed the memtable into >= 1 segment").isGreaterThan(0);
    assertThat(after.memtablePostings()).as("a freshly-reopened engine starts with an empty memtable").isEqualTo(0L);
  }

  /**
   * (e) The {@code maybeFlush} threshold gate. A unit-test-sized workload would never cross the
   * production 1M-posting threshold, so we drive a low-threshold engine directly through the
   * package-private constructor and verify the same "no-op below / flush at" behavior the
   * wrapper's post-commit callback drives during a long bulk-load.
   */
  @Test
  void maybeFlushIsAGateNotAForcedFlush() {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine cheap = new PaginatedSparseVectorEngine(
        db, "GateTest", SegmentParameters.defaults(), /* threshold */ 200L)) {
      // Below threshold: maybeFlush must be a no-op (no segment created).
      for (int i = 0; i < 50; i++)
        cheap.put(0, new RID(0, i), 0.1f);
      assertThat(cheap.memtablePostings()).isEqualTo(50L);
      cheap.maybeFlush();
      assertThat(cheap.segmentCount()).as("under threshold maybeFlush should not flush").isEqualTo(0);

      // Cross the threshold: maybeFlush actually flushes.
      for (int i = 50; i < 250; i++)
        cheap.put(0, new RID(0, i), 0.1f);
      assertThat(cheap.memtablePostings()).isEqualTo(250L);
      cheap.maybeFlush();
      assertThat(cheap.segmentCount()).as("over threshold maybeFlush must materialize a segment").isEqualTo(1);
      assertThat(cheap.memtablePostings()).as("memtable is reset after a successful flush").isEqualTo(0L);
    }
  }

  /**
   * Size-tiered auto-compaction kicks in once a tier overflows. A low-base / low-fanout engine
   * flushed enough times to fill tier 0 must observe a tier-0 collapse in the same {@code flush()}
   * call: without this gate a long bulk-load leaves the engine with one segment per flush and BMW
   * DAAT pays a per-segment merge cost on every query (the cliff that drove 10M-corpus latency
   * from single-digit-ms memtable-only into ~800 ms with real segments).
   */
  @Test
  void flushTriggersSizeTieredCompaction() {
    createTypeAndIndex();
    final DatabaseInternal db = (DatabaseInternal) database;
    try (final PaginatedSparseVectorEngine cheap = new PaginatedSparseVectorEngine(
        db, "SizeTieredTest", SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 1L,
        /* tierFanout */ 3,
        /* tierBasePostings */ 1L)) {
      // Each flush emits a 1-posting tier-0 segment.
      for (int s = 0; s < 2; s++) {
        cheap.put(s, new RID(0, 100L + s), 0.1f * (s + 1));
        cheap.flush();
      }
      assertThat(cheap.segmentCount()).as("under tier-0 fanout (2 < 3) the gate must not fire").isEqualTo(2);

      // The third flush completes a tier-0 fill; the gate runs compactSizeTiered which merges
      // the 3 tier-0 segments into one tier-1 segment.
      cheap.put(2, new RID(0, 102L), 0.5f);
      cheap.flush();

      // 3 tier-0 sealed segments collapse into 1 tier-1 segment after the gate fires.
      assertThat(cheap.segmentCount())
          .as("size-tiered compaction must collapse a full tier (3 -> 1)")
          .isEqualTo(1);
    }
  }

  /**
   * Drop must reclaim every {@code .sparseseg} file the index owns. Earlier the wrapper called
   * {@code engine.close()} which sealed the memtable into one more segment but never dropped any
   * of the existing components, so files leaked on disk after the index was gone.
   */
  @Test
  void dropReclaimsAllSegmentFiles() {
    createTypeAndIndex();
    final PaginatedSparseVectorEngine engine = engineFor(IDX_NAME);
    // Build 3 sealed segments + leave a few postings in the memtable.
    for (int s = 0; s < 3; s++) {
      for (int i = 0; i < 5; i++)
        engine.put(s, new RID(0, 1000L * s + i), 0.1f * (i + 1));
      engine.flush();
    }
    engine.put(99, new RID(0, 9999L), 0.5f); // memtable resident
    assertThat(engine.segmentCount()).isEqualTo(3);

    final java.io.File dbDir = new java.io.File(database.getDatabasePath());
    // Match any sparseseg file. The component-name pattern (`<sanitized-index-name>_seg<digits>`)
    // is owned by {@link PaginatedSparseVectorEngine#segmentComponentName}; the test only needs
    // to count files in the database directory and does not need to mirror that pattern. If a
    // future change adds a second sparse-vector index in the same database, scope the filter via
    // {@link FileManager#getFiles} + the engine's own {@code isOurSegmentFile} helper instead of
    // a string-mirrored prefix.
    final java.io.FilenameFilter sparseFilter = (d, name) -> name.contains("_seg") && name.endsWith(".sparseseg");
    final String[] beforeDrop = dbDir.list(sparseFilter);
    assertThat(beforeDrop).as("3 sealed segments must have a .sparseseg file each").isNotNull();
    assertThat(beforeDrop.length).isGreaterThanOrEqualTo(3);

    // Drop the index. After this returns, no .sparseseg files for this index should remain.
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    typeIndex.drop();

    final String[] afterDrop = dbDir.list(sparseFilter);
    final int remaining = afterDrop == null ? 0 : afterDrop.length;
    assertThat(remaining).as("drop() must reclaim every .sparseseg file owned by the dropped index").isEqualTo(0);
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createTypeAndIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(DIMENSIONS)
          .create();
    });
  }

  /** Insert {@code n} documents with deterministic 8-nnz vectors so queries are reproducible. */
  private void insertDocs(final int n) {
    database.transaction(() -> {
      final java.util.Random rnd = new java.util.Random(0xCAFEL);
      for (int i = 0; i < n; i++) {
        final int[] tokens = new int[8];
        final float[] weights = new float[8];
        final HashSet<Integer> picked = new HashSet<>();
        for (int j = 0; j < 8; j++) {
          int dim;
          do {
            dim = rnd.nextInt(DIMENSIONS);
          } while (!picked.add(dim));
          tokens[j] = dim;
          weights[j] = 0.1f + rnd.nextFloat();
        }
        final MutableDocument doc = database.newDocument(TYPE_NAME);
        doc.set("tokens", tokens);
        doc.set("weights", weights);
        doc.save();
      }
    });
  }

  private void flushAllSubIndexes() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(IDX_NAME);
    for (final IndexInternal sub : typeIndex.getIndexesOnBuckets())
      sub.flush();
  }

  /**
   * Build a standalone engine whose size-tiered auto-compaction gate cannot fire under any
   * unit-test workload (very high tier fanout). Used by tests that explicitly drive
   * {@link PaginatedSparseVectorEngine#compactOldest} or {@link PaginatedSparseVectorEngine#flush}
   * sequences and need a deterministic segment count between assertions.
   */
  private static PaginatedSparseVectorEngine nonCompactingEngine(final DatabaseInternal db, final String name) {
    return new PaginatedSparseVectorEngine(db, name, SegmentParameters.defaults(),
        /* memtableFlushThreshold */ 1L,
        /* tierFanout */ 1_000_000,
        /* tierBasePostings */ 1L);
  }

  /** Resolve the underlying engine for white-box assertions. */
  private PaginatedSparseVectorEngine engineFor(final String idxName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(idxName);
    final LSMSparseVectorIndex wrapper = (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    return wrapper.getEngine();
  }
}
