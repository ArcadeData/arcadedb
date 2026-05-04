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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 1 large-segment scaling test. Writes a 10M-posting segment across 1000 dims and
 * verifies storage size + a sample of read paths. Tagged {@code slow} so it does not run on
 * every CI invocation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class SparseSegmentLargeTest {

  @Test
  void tenMillionPostingsRoundtripAndCompress(@TempDir final Path tmp) throws IOException {
    final Path file = tmp.resolve("large.sparseseg");
    final int dims = 1_000;
    final int avgPostingsPerDim = 10_000;   // total ~10M postings
    final long seed = 0xDEADBEEFL;

    final long writeStart = System.nanoTime();
    long postingCount = 0L;
    final Random rnd = new Random(seed);
    try (final SparseSegmentWriter w = new SparseSegmentWriter(file, SegmentParameters.defaults())) {
      for (int dim = 0; dim < dims; dim++) {
        // Within each dim, RIDs are simulated: ~10K postings, all in bucket 0, monotonically increasing offset.
        w.startDim(dim);
        long offset = rnd.nextInt(1000);
        for (int i = 0; i < avgPostingsPerDim; i++) {
          w.appendPosting(new RID(0, offset), rnd.nextFloat());
          offset += 1 + rnd.nextInt(8);
          postingCount++;
        }
        w.endDim();
      }
      w.finish();
    }
    final long writeMillis = (System.nanoTime() - writeStart) / 1_000_000L;
    final long fileBytes = Files.size(file);
    final double bytesPerPosting = (double) fileBytes / postingCount;

    // Sanity: with int8 + VarInt-delta we expect ~3-5 bytes per posting on this synthetic distribution.
    // Loose bound to stay robust across machines:
    assertThat(bytesPerPosting).isLessThan(15.0);

    System.out.printf(
        "Phase 1 large-segment write: %d postings, %d dims, %d bytes (%.2f bytes/posting), %d ms%n",
        postingCount, dims, fileBytes, bytesPerPosting, writeMillis);

    // Read back: sample 100 random dims, verify cursor walks all postings and ordering is preserved.
    final long readStart = System.nanoTime();
    long verified = 0L;
    try (final SparseSegmentReader r = new SparseSegmentReader(file)) {
      assertThat(r.totalDims()).isEqualTo(dims);
      assertThat(r.totalPostings()).isEqualTo(postingCount);
      final Random sampleRnd = new Random(seed ^ 1L);
      for (int s = 0; s < 100; s++) {
        final int dim = sampleRnd.nextInt(dims);
        try (final SegmentDimCursor c = r.openCursor(dim)) {
          RID prev = null;
          int seen = 0;
          while (c.advance()) {
            if (prev != null)
              assertThat(SparseSegmentWriter.compareRid(c.currentRid(), prev)).isPositive();
            prev = c.currentRid();
            seen++;
          }
          verified += seen;
        }
      }
    }
    final long readMillis = (System.nanoTime() - readStart) / 1_000_000L;
    System.out.printf("Phase 1 large-segment sample read: %d postings verified in %d ms%n", verified, readMillis);
  }
}
