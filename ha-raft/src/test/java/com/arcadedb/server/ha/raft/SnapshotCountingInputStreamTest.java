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
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SnapshotInstaller.CountingInputStream}. The compression-ratio bomb
 * defense in {@code downloadSnapshot} depends on this class under-counting compressed bytes
 * by a safe margin, so the counter's behavior across read modes is worth pinning.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotCountingInputStreamTest {

  @Test
  void singleByteReadsAreCounted() throws IOException {
    final byte[] payload = { 1, 2, 3 };
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(payload))) {
      assertThat(in.getCount()).isZero();
      assertThat(in.read()).isEqualTo(1);
      assertThat(in.getCount()).isEqualTo(1);
      assertThat(in.read()).isEqualTo(2);
      assertThat(in.read()).isEqualTo(3);
      assertThat(in.getCount()).isEqualTo(3);
    }
  }

  @Test
  void eofSingleReadDoesNotIncrementCount() throws IOException {
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(new byte[] { 42 }))) {
      assertThat(in.read()).isEqualTo(42);
      assertThat(in.getCount()).isEqualTo(1);
      assertThat(in.read()).isEqualTo(-1);
      assertThat(in.getCount()).as("EOF must not increment").isEqualTo(1);
      assertThat(in.read()).isEqualTo(-1);
      assertThat(in.getCount()).isEqualTo(1);
    }
  }

  @Test
  void bulkReadIsCountedByBytesTransferred() throws IOException {
    final byte[] payload = new byte[100];
    for (int i = 0; i < payload.length; i++)
      payload[i] = (byte) i;
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(payload))) {
      final byte[] buf = new byte[40];
      final int n = in.read(buf, 0, buf.length);
      assertThat(n).isEqualTo(40);
      assertThat(in.getCount()).isEqualTo(40);

      final int m = in.read(buf, 5, 10);
      assertThat(m).isEqualTo(10);
      assertThat(in.getCount()).isEqualTo(50);
    }
  }

  @Test
  void bulkReadEofDoesNotIncrementCount() throws IOException {
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(new byte[0]))) {
      final byte[] buf = new byte[16];
      assertThat(in.read(buf, 0, buf.length)).isEqualTo(-1);
      assertThat(in.getCount()).as("EOF on bulk read must not increment").isZero();
    }
  }

  @Test
  void skipIsCountedWhenStreamHonorsIt() throws IOException {
    final byte[] payload = new byte[64];
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(payload))) {
      final long skipped = in.skip(20);
      assertThat(skipped).isEqualTo(20);
      assertThat(in.getCount()).isEqualTo(20);
    }
  }

  @Test
  void skipZeroBytesDoesNotChangeCount() throws IOException {
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(new byte[16]))) {
      assertThat(in.skip(0)).isZero();
      assertThat(in.getCount()).isZero();
    }
  }

  @Test
  void markSupportedReturnsFalse() throws IOException {
    // Even when the wrapped stream supports mark/reset, the counter explicitly opts out so that
    // ZipInputStream (which can probe markSupported) never assumes it can rewind past counted bytes.
    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(new InputStream() {
      @Override
      public int read() {
        return -1;
      }

      @Override
      public boolean markSupported() {
        return true; // upstream says yes
      }
    })) {
      assertThat(in.markSupported()).as("counter must report false regardless of upstream").isFalse();
    }
  }

  @Test
  void mixedReadModesAccumulateCorrectly() throws IOException {
    // Simulate the ZipInputStream access pattern: a few single-byte reads (header inspection)
    // followed by bulk reads (body decode). Total count must equal total bytes consumed.
    final byte[] payload = new byte[128];
    for (int i = 0; i < payload.length; i++)
      payload[i] = (byte) (i + 1);

    try (final SnapshotInstaller.CountingInputStream in = new SnapshotInstaller.CountingInputStream(
        new ByteArrayInputStream(payload))) {
      in.read();
      in.read();
      assertThat(in.getCount()).isEqualTo(2);

      final byte[] buf = new byte[30];
      in.read(buf, 0, buf.length);
      assertThat(in.getCount()).isEqualTo(32);

      in.skip(10);
      assertThat(in.getCount()).isEqualTo(42);

      // Remaining 86 bytes in one bulk.
      in.read(new byte[200]);
      assertThat(in.getCount()).isEqualTo(128);

      // EOF path stops the counter.
      assertThat(in.read()).isEqualTo(-1);
      assertThat(in.getCount()).isEqualTo(128);
    }
  }
}
