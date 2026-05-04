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

import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 verification: {@link FlushWorker} writes memtable contents into a sealed segment
 * that the reader can open and replay deterministically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FlushWorkerTest {

  @Test
  void flushedSegmentReadsBackInRidOrder(@TempDir final Path tmp) throws IOException {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 30), 0.3f);
    m.put(0, new RID(0, 10), 0.1f);
    m.put(0, new RID(0, 20), 0.2f);
    m.put(2, new RID(0, 5), 0.5f);
    m.put(2, new RID(0, 15), 0.7f);

    final Path target = tmp.resolve("seg.sparseseg");
    try (final SparseSegmentReader r = FlushWorker.flush(m, target, 42L, SegmentParameters.defaults())) {
      assertThat(r).isNotNull();
      assertThat(r.segmentId()).isEqualTo(42L);
      assertThat(r.totalPostings()).isEqualTo(5L);
      assertThat(r.totalDims()).isEqualTo(2);

      try (final SegmentDimCursor c = r.openCursor(0)) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 10));
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 20));
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 30));
        assertThat(c.advance()).isFalse();
      }

      try (final SegmentDimCursor c = r.openCursor(2)) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 5));
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 15));
        assertThat(c.advance()).isFalse();
      }
    }
    // The .tmp staging file must not survive a successful flush.
    assertThat(Files.exists(target.resolveSibling(target.getFileName() + ".tmp"))).isFalse();
  }

  @Test
  void flushOfEmptyMemtableReturnsNull(@TempDir final Path tmp) throws IOException {
    final Memtable m = new Memtable();
    final Path target = tmp.resolve("seg.sparseseg");
    final SparseSegmentReader r = FlushWorker.flush(m, target, 1L, SegmentParameters.defaults());
    assertThat(r).isNull();
    assertThat(Files.exists(target)).isFalse();
  }

  @Test
  void tombstonesPropagateToSegment(@TempDir final Path tmp) throws IOException {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 1), 0.5f);
    m.remove(0, new RID(0, 2));
    m.put(0, new RID(0, 3), 0.7f);

    final Path target = tmp.resolve("tomb.sparseseg");
    try (final SparseSegmentReader r = FlushWorker.flush(m, target, 1L, SegmentParameters.defaults())) {
      try (final SegmentDimCursor c = r.openCursor(0)) {
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 1));
        assertThat(c.isTombstone()).isFalse();
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 2));
        assertThat(c.isTombstone()).isTrue();
        assertThat(c.advance()).isTrue();
        assertThat(c.currentRid()).isEqualTo(new RID(0, 3));
        assertThat(c.isTombstone()).isFalse();
      }
    }
  }

  @Test
  void atomicMoveLeavesNoStagingFile(@TempDir final Path tmp) throws IOException {
    final Memtable m = new Memtable();
    m.put(0, new RID(0, 1), 0.5f);
    final Path target = tmp.resolve("atomic.sparseseg");
    try (final SparseSegmentReader r = FlushWorker.flush(m, target, 1L, SegmentParameters.defaults())) {
      assertThat(Files.exists(target)).isTrue();
    }
    assertThat(Files.list(target.getParent()).filter(p -> p.toString().endsWith(".tmp")).count()).isZero();
  }
}
