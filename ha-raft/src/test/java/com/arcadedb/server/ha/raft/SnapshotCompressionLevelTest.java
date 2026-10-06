/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The compression level of the snapshot ZIP stream is configurable ({@link GlobalConfiguration#HA_SNAPSHOT_COMPRESSION_LEVEL}),
 * so a leader on a fast private network can stop spending a core on DEFLATE.
 */
class SnapshotCompressionLevelTest {

  private static byte[] compressiblePayload() {
    // Page-like content: runs of zeros with scattered pseudo-random bytes
    final byte[] data = new byte[2 * 1024 * 1024];
    final Random random = new Random(42);
    for (int i = 0; i < data.length; i += 64)
      data[i] = (byte) random.nextInt(256);
    return data;
  }

  private static byte[] zip(final byte[] payload, final int level) throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (final ZipOutputStream zos = SnapshotHttpHandler.newSnapshotZipStream(baos, level)) {
      zos.putNextEntry(new ZipEntry("file.bin"));
      zos.write(payload);
      zos.closeEntry();
      zos.finish();
    }
    return baos.toByteArray();
  }

  private static byte[] unzip(final byte[] zipped) throws Exception {
    try (final ZipInputStream in = new ZipInputStream(new ByteArrayInputStream(zipped))) {
      assertThat(in.getNextEntry()).isNotNull();
      return in.readAllBytes();
    }
  }

  @Test
  void defaultIsTheJdkDefaultLevel() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_COMPRESSION_LEVEL.getDefValue()).isEqualTo(-1);
  }

  @Test
  void levelZeroStoresAndLevelNineCompressesAndBothRoundTrip() throws Exception {
    final byte[] payload = compressiblePayload();

    final byte[] level0 = zip(payload, 0);
    final byte[] level1 = zip(payload, 1);
    final byte[] level9 = zip(payload, 9);

    assertThat(level0.length).isGreaterThanOrEqualTo(payload.length);
    assertThat(level1.length).isLessThan(payload.length / 2);
    assertThat(level9.length).isLessThanOrEqualTo(level1.length);

    assertThat(unzip(level0)).isEqualTo(payload);
    assertThat(unzip(level1)).isEqualTo(payload);
    assertThat(unzip(level9)).isEqualTo(payload);
  }

  @Test
  void outOfRangeLevelFallsBackToTheDefault() throws Exception {
    final byte[] payload = compressiblePayload();
    final byte[] expected = zip(payload, -1);

    assertThat(zip(payload, 10)).hasSameSizeAs(expected);
    assertThat(zip(payload, -5)).hasSameSizeAs(expected);
    assertThat(unzip(zip(payload, 10))).isEqualTo(payload);
  }
}
