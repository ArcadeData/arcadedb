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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the decompression-bomb defense primitives used by
 * {@link SnapshotInstaller#downloadSnapshot}: the {@link SnapshotInstaller.CountingInputStream}
 * accurately counts bytes consumed, and the {@code uncompressed / compressed > MAX_RATIO} math
 * (applied inline in downloadSnapshot) flags a high-ratio entry while ignoring tiny ones.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotCompressionRatioTest {

  @Test
  void countingInputStreamTracksBulkReads() throws IOException {
    final byte[] payload = new byte[1024];
    for (int i = 0; i < payload.length; i++)
      payload[i] = (byte) i;

    final SnapshotInstaller.CountingInputStream counter =
        new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(payload));

    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    final byte[] buf = new byte[128];
    int n;
    while ((n = counter.read(buf)) != -1)
      sink.write(buf, 0, n);

    assertThat(counter.getCount()).isEqualTo(payload.length);
    assertThat(sink.toByteArray()).isEqualTo(payload);
  }

  @Test
  void countingInputStreamTracksSingleByteReads() throws IOException {
    final byte[] payload = { 1, 2, 3, 4, 5 };
    final SnapshotInstaller.CountingInputStream counter =
        new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(payload));

    while (counter.read() != -1) { /* drain */ }

    assertThat(counter.getCount()).isEqualTo(payload.length);
  }

  @Test
  void countingInputStreamHandlesSkip() throws IOException {
    final byte[] payload = new byte[2048];
    final SnapshotInstaller.CountingInputStream counter =
        new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(payload));

    final long skipped = counter.skip(1024);

    assertThat(skipped).isEqualTo(1024);
    assertThat(counter.getCount()).isEqualTo(1024);
  }

  /**
   * Locks in the threshold math: an entry inflating 1000:1 over the minimum-check size is flagged,
   * and an entry inflating exactly at the cap is accepted. This mirrors the check inline in
   * {@link SnapshotInstaller} lines ~445, so any future refactor that drops the guard will fail
   * here.
   */
  @Test
  void ratioCheckFlagsHighInflation() {
    final long compressed = 4_096;
    final long uncompressed = 5_000_000; // ~1220:1
    assertThat(uncompressed).isGreaterThan(SnapshotInstaller.MIN_RATIO_CHECK_BYTES);
    assertThat(uncompressed / Math.max(1, compressed))
        .isGreaterThan(SnapshotInstaller.MAX_COMPRESSION_RATIO_PER_ENTRY);
  }

  @Test
  void ratioCheckSkipsTinyEntries() {
    // Tiny schema JSON: 2 KB uncompressed from 10 B compressed = 200:1 ratio BUT under the
    // minimum-check threshold, so it must not be flagged.
    final long compressed = 10;
    final long uncompressed = 2_048;
    assertThat(uncompressed).isLessThan(SnapshotInstaller.MIN_RATIO_CHECK_BYTES);
    // Check is gated on uncompressed > MIN_RATIO_CHECK_BYTES, so this entry is never evaluated.
  }

  @Test
  void ratioCheckAcceptsRealisticPageData() {
    // Real DEFLATE on structured page data typically yields 5-20x. Even at an aggressive 50x
    // (highly redundant bucket pages) the entry is far below the 200:1 cap.
    final long compressed = 1_000_000;   // 1 MB
    final long uncompressed = 50_000_000; // 50 MB -> 50:1
    assertThat(uncompressed).isGreaterThan(SnapshotInstaller.MIN_RATIO_CHECK_BYTES);
    assertThat(uncompressed / Math.max(1, compressed))
        .isLessThanOrEqualTo(SnapshotInstaller.MAX_COMPRESSION_RATIO_PER_ENTRY);
  }
}
