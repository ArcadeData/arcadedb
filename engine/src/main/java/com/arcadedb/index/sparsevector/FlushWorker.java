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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Serializes a {@link Memtable} snapshot to a sealed {@code .sparseseg} file. This is the
 * write path's flush operation: when the memtable's heap or posting count exceeds its
 * threshold, the engine snapshots it (atomic swap with a fresh empty memtable) and hands the
 * snapshot to this worker for durable serialization.
 * <p>
 * The output file is written atomically: data is staged under a {@code .tmp} suffix, fsynced,
 * then renamed into place. A reader opening the final file therefore observes either the full
 * segment or no file at all. If the flush throws, the partial {@code .tmp} file is removed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FlushWorker {

  private FlushWorker() {
    // utility class
  }

  /**
   * Flush {@code memtable} into a new {@code .sparseseg} file at {@code targetFile}. Returns an
   * open reader on the new segment ready to join the active segment set, or {@code null} if the
   * memtable was empty (no segment created).
   */
  public static SparseSegmentReader flush(final Memtable memtable, final Path targetFile, final long segmentId,
      final SegmentParameters params) throws IOException {
    if (memtable.isEmpty())
      return null;

    final Path tmpFile = targetFile.resolveSibling(targetFile.getFileName().toString() + ".tmp");
    try {
      try (final SparseSegmentWriter w = new SparseSegmentWriter(tmpFile, params)) {
        w.setSegmentId(segmentId);
        for (final int dim : memtable.sortedDims()) {
          final Iterator<MemtablePosting> it = memtable.iterateDim(dim);
          if (!it.hasNext())
            continue;
          w.startDim(dim);
          while (it.hasNext()) {
            final MemtablePosting p = it.next();
            if (p.tombstone())
              w.appendTombstone(p.rid());
            else
              w.appendPosting(p.rid(), p.weight());
          }
          w.endDim();
        }
        w.finish();
      }
      // Atomic move into place; the writer already fsynced the file contents.
      Files.move(tmpFile, targetFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    } catch (final RuntimeException | IOException e) {
      Files.deleteIfExists(tmpFile);
      throw e;
    }
    return new SparseSegmentReader(targetFile);
  }
}
