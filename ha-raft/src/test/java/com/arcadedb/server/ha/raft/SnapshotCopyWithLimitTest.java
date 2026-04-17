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
import java.io.InputStream;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks in the no-silent-truncate contract of {@link SnapshotInstaller#copyWithLimit}. An entry
 * that exceeds the per-entry byte cap MUST throw before the over-limit chunk reaches the output
 * stream, otherwise a partial file would land in the extracted snapshot.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotCopyWithLimitTest {

  /** Content exactly at the limit is copied in full without throwing. */
  @Test
  void copiesExactlyAtLimit() throws IOException {
    final byte[] payload = randomBytes(2048);
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    SnapshotInstaller.copyWithLimit(new ByteArrayInputStream(payload), sink, payload.length, "entry");

    assertThat(sink.toByteArray()).isEqualTo(payload);
  }

  /**
   * Content one byte over the limit throws. The output is empty because a single read() returns
   * the whole payload (below the 512 KB internal buffer), the size check fires, and write() is
   * never reached - proving the throw happens BEFORE any write rather than after a partial one.
   */
  @Test
  void throwsOnSingleReadOverflowWithoutWriting() {
    final byte[] payload = randomBytes(2048);
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    assertThatThrownBy(() ->
        SnapshotInstaller.copyWithLimit(new ByteArrayInputStream(payload), sink, payload.length - 1L, "entry"))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("exceeds size limit");

    assertThat(sink.size()).isZero();
  }

  /**
   * Content that needs multiple reads to cross the limit writes only the chunks that stayed
   * within the cap, then throws on the first chunk that pushes the total over. Critically, the
   * last (over-limit) chunk must NOT be visible in the output - that would be a silent truncation
   * of the caller's extracted file.
   */
  @Test
  void throwsMidStreamAndDoesNotWriteOverLimitChunk() {
    // 128-byte reads force multiple iterations. Limit allows the first two reads but not the third.
    final byte[] payload = randomBytes(384);
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    assertThatThrownBy(() ->
        SnapshotInstaller.copyWithLimit(new FixedChunkInputStream(payload, 128), sink, 256, "entry"))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("entry");

    // First two 128-byte reads (256 bytes total) made it through. The third read is rejected
    // BEFORE write() runs, so exactly 256 bytes are in the sink.
    assertThat(sink.size()).isEqualTo(256);
    assertThat(sink.toByteArray()).isEqualTo(Arrays.copyOfRange(payload, 0, 256));
  }

  /** An empty stream with a positive limit copies nothing and does not throw. */
  @Test
  void emptyStreamIsNoOp() throws IOException {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    SnapshotInstaller.copyWithLimit(new ByteArrayInputStream(new byte[0]), sink, 1024, "entry");

    assertThat(sink.size()).isZero();
  }

  /**
   * Zero limit with non-empty input throws on the first read. Guards against a regression where
   * maxBytes=0 might be treated as "unlimited".
   */
  @Test
  void zeroLimitRejectsAnyContent() {
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    assertThatThrownBy(() ->
        SnapshotInstaller.copyWithLimit(new ByteArrayInputStream(new byte[] { 0x01 }), sink, 0L, "entry"))
        .isInstanceOf(ReplicationException.class);

    assertThat(sink.size()).isZero();
  }

  private static byte[] randomBytes(final int n) {
    final byte[] b = new byte[n];
    for (int i = 0; i < n; i++)
      b[i] = (byte) (i & 0xFF);
    return b;
  }

  /**
   * Yields fixed-size chunks regardless of the caller's buffer capacity, so the test can
   * deterministically drive the copy loop through multiple iterations.
   */
  private static final class FixedChunkInputStream extends InputStream {
    private final byte[] data;
    private final int    chunkSize;
    private       int    pos;

    FixedChunkInputStream(final byte[] data, final int chunkSize) {
      this.data = data;
      this.chunkSize = chunkSize;
    }

    @Override
    public int read() {
      if (pos >= data.length) return -1;
      return data[pos++] & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
      if (pos >= data.length) return -1;
      final int toCopy = Math.min(Math.min(len, chunkSize), data.length - pos);
      System.arraycopy(data, pos, b, off, toCopy);
      pos += toCopy;
      return toCopy;
    }
  }
}
