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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6566: whether a sparse-vector index still holds a segment whose
 * precedence predates the recency-epoch fix (issue #6379) used to be observable only as a
 * once-per-instance {@code WARNING} log line - nothing could alert on it, and once a node had been
 * up for a while there was no way to ask "is this index still affected" without restarting it, nor
 * to confirm a {@code REBUILD INDEX} actually cleared it.
 * <p>
 * {@link PaginatedSparseVectorEngine#untrustedSegmentCount()} re-derives the same condition
 * {@code warnOnPreRecencyEpochSegments} logs, but from the live segment snapshot on every call
 * instead of latching it once - so it is what {@link LSMSparseVectorIndex#getStats()},
 * {@link LSMSparseVectorIndex#getUpgradeWarning()} (the same surface {@code schema:indexes},
 * {@code schema:index:<name>}, Studio and the HTTP admin API already read for every other index
 * upgrade warning) and {@link LSMSparseVectorIndexMetrics#buildJSON} now expose.
 * <p>
 * "Legacy-shaped" segments are synthesised the way a pre-#6379 build actually left them on disk:
 * parents recorded, recency epoch left at 0 - the same fixture
 * {@code PaginatedSparseVectorEngineCompactionRecencyTest#preRecencyEpochMergedSegmentIsReportedAtOpen}
 * uses to pin the WARNING log line itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6566UntrustedSegmentSurfaceTest extends TestHelper {

  private static final int DIM = 0;

  @Test
  void untrustedSegmentCountReportsALegacyShapedSegmentAndConfirmsARebuildClearsIt() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    // A legacy merge: parents recorded, epoch never set - the on-disk shape a pre-#6379 build left behind.
    database.transaction(() -> {
      final SparseSegmentComponent component = newComponent(db, "UntrustedSurfaceTest_seg99");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
        b.setSegmentId(99L);
        b.setRecencyEpoch(0L); // what a pre-fix builder left in the slot
        b.setParentSegments(new long[] { 90L, 91L });
        b.startDim(DIM);
        b.appendPosting(new RID(0, 500L), 0.5f);
        b.endDim();
        b.finish();
      }
    });

    try (final PaginatedSparseVectorEngine engine = new PaginatedSparseVectorEngine(db, "UntrustedSurfaceTest",
        SegmentParameters.defaults(), /* memtableFlushThreshold */ 100_000L, /* tierFanout */ 1_000_000,
        /* tierBasePostings */ 1L)) {
      assertThat(engine.segmentCount()).isEqualTo(1);
      assertThat(engine.untrustedSegmentCount())
          .as("the legacy-shaped segment must be counted, not only logged").isEqualTo(1);
    }

    // The remedy: drop the legacy segment and rebuild the index content from scratch, exactly what
    // REBUILD INDEX does at the storage level - a fresh segment with no parents.
    final var legacyComponent = ((LocalSchema) db.getSchema().getEmbedded()).getFileByName("UntrustedSurfaceTest_seg99");
    assertThat(legacyComponent).isNotNull();
    db.getFileManager().dropFile(legacyComponent.getFileId());

    try (final PaginatedSparseVectorEngine rebuilt = new PaginatedSparseVectorEngine(db, "UntrustedSurfaceTest",
        SegmentParameters.defaults(), /* memtableFlushThreshold */ 100_000L, /* tierFanout */ 1_000_000,
        /* tierBasePostings */ 1L)) {
      assertThat(rebuilt.segmentCount())
          .as("the legacy segment must be gone, not merely outnumbered").isZero();
      rebuilt.put(DIM, new RID(0, 501L), 0.7f);
      rebuilt.flush();
      assertThat(rebuilt.untrustedSegmentCount())
          .as("a queryable stat, not only a latched log line: it must be able to go back to zero and say so, "
              + "confirming the remedy actually worked").isZero();
    }
  }

  /**
   * End-to-end through the {@link LSMSparseVectorIndex} an ordinary {@code CREATE INDEX ... LSM_SPARSE_VECTOR}
   * produces: the surface a monitoring system or Studio would actually read, not only the engine underneath it.
   */
  @Test
  void upgradeWarningAndStatsSurfaceTheUntrustedSegmentThroughTheRealIndex() throws Exception {
    final String typeName = "UntrustedSurfaceDoc";
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex(typeName, new String[] { "tokens", "weights" })
          .withSparseVectorType().withDimensions(100).create();

      final MutableDocument doc = database.newDocument(typeName);
      doc.set("tokens", new int[] { 1, 5 });
      doc.set("weights", new float[] { 0.3f, 0.7f });
      doc.save();
    });

    final LSMSparseVectorIndex sparse = sparseSubIndex(typeName);
    sparse.getEngine().flush();
    final String engineName = sparse.getName(); // the per-bucket physical index name the engine's segments are keyed under

    assertThat(sparse.getUpgradeWarning())
        .as("nothing has been merged from a pre-#6379 segment yet").isNull();
    assertThat(sparse.getStats().get("untrustedSegments")).isZero();

    final DatabaseInternal db = (DatabaseInternal) database;
    database.transaction(() -> {
      final SparseSegmentComponent component = newComponent(db, engineName + "_seg999");
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, SegmentParameters.defaults())) {
        b.setSegmentId(999L);
        b.setRecencyEpoch(0L);
        b.setParentSegments(new long[] { 900L, 901L });
        b.startDim(DIM);
        b.appendPosting(new RID(0, 500L), 0.5f);
        b.endDim();
        b.finish();
      }
    });

    // Capture what the schema-load warning actually says on reopen (PR #6720 review): getUpgradeWarning() must
    // describe the problem only, and LocalSchema.reportUpgradeWarning() - the sole caller that knows the LOGICAL
    // TypeIndex name - must be the one and only place "REBUILD INDEX" is appended, naming that logical index and
    // not the physical per-bucket sub-index name this test's own engineName variable holds.
    final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
    final Logger originalLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings, originalLogger));
    try {
      reopenDatabase();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }

    final LSMSparseVectorIndex reopened = sparseSubIndex(typeName);
    assertThat(reopened.getStats().get("untrustedSegments"))
        .as("the reopened index must count the legacy segment now on disk").isEqualTo(1L);
    assertThat(reopened.getUpgradeWarning())
        .as("getUpgradeWarning() itself must say what is lost, not what to type - the physical sub-index name "
            + "must never appear in a 'REBUILD INDEX' clause it assembles")
        .contains("#6379").doesNotContain("REBUILD INDEX");

    final String logicalName = typeName + "[tokens,weights]";
    final String remedyLine = "REBUILD INDEX `" + logicalName + "`";
    assertThat(warnings)
        .as("the schema-load WARNING must name the LOGICAL index for REBUILD INDEX exactly once, never the "
            + "physical per-bucket sub-index name (%s) this fix used to embed a second time", engineName)
        .anyMatch(w -> w.contains("#6379") && w.contains(remedyLine) && countOccurrences(w, "REBUILD INDEX") == 1
            && !w.contains(engineName));
  }

  private LSMSparseVectorIndex sparseSubIndex(final String typeName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(typeName + "[tokens,weights]");
    return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static SparseSegmentComponent newComponent(final DatabaseInternal db, final String name) {
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(db, name, db.getDatabasePath() + "/" + name,
          ComponentFile.MODE.READ_WRITE, SparseSegmentComponent.DEFAULT_PAGE_SIZE);
      ((LocalSchema) db.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new RuntimeException("failed to create sparse segment component '" + name + "'", e);
    }
  }

  private static int countOccurrences(final String haystack, final String needle) {
    int count = 0;
    for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length()))
      count++;
    return count;
  }

  /**
   * Captures WARNING-and-above messages while forwarding everything to the real logger, matching the pattern
   * {@code PaginatedSparseVectorEngineCompactionRecencyTest} uses for the same purpose. The fixed-arity overload
   * must NOT delegate to the varargs one: the fixed arity is the exact match, so it would recurse until the stack
   * dies.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;
    private final Logger       delegate;

    CapturingLogger(final List<String> warnings, final Logger delegate) {
      this.warnings = warnings;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Raw template is good enough for the substring matching the assertions do.
        }
      }
      warnings.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
