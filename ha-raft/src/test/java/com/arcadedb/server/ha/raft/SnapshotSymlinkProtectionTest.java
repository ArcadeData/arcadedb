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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotSymlinkProtectionTest {

  @Test
  void copyWithLimitRejectsOversizedEntry() {
    final byte[] data = new byte[100];
    final ByteArrayInputStream in = new ByteArrayInputStream(data);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThatThrownBy(() ->
        SnapshotInstaller.copyWithLimit(in, out, 50, "test-entry"))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("exceeds size limit");
  }

  @Test
  void copyWithLimitAllowsEntryWithinBounds() throws Exception {
    final byte[] data = new byte[50];
    final ByteArrayInputStream in = new ByteArrayInputStream(data);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    final long copied = SnapshotInstaller.copyWithLimit(in, out, 100, "test-entry");

    assertThat(copied).isEqualTo(50);
    assertThat(out.toByteArray()).isEqualTo(data);
  }

  @Test
  void countingInputStreamTracksBytes() throws Exception {
    final byte[] data = new byte[] { 1, 2, 3, 4, 5 };
    final SnapshotInstaller.CountingInputStream cis =
        new SnapshotInstaller.CountingInputStream(new ByteArrayInputStream(data));

    assertThat(cis.getCount()).isZero();

    cis.read();
    assertThat(cis.getCount()).isEqualTo(1);

    final byte[] buf = new byte[3];
    cis.read(buf, 0, 3);
    assertThat(cis.getCount()).isEqualTo(4);

    cis.close();
  }

  @Test
  void maxZipEntryConstantIsTenGigabytes() {
    assertThat(SnapshotInstaller.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES).isEqualTo(10L * 1024 * 1024 * 1024);
  }
}
