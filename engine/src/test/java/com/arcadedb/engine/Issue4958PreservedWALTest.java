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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the preserved-WAL findings grouped in issue #4958:
 * <ul>
 *   <li>{@code TransactionManager.createWALFilePool} started {@code logFileCounter} from 0 on every open
 *   and {@code WALFile} opens its path in "rw" mode WITHOUT truncation. A WAL file preserved by a previous
 *   open (corrupt-gap detection or a #4928 crash-equivalent close) was therefore silently reused as an
 *   active WAL: new transactions were appended AFTER the old (possibly corrupt) content, and the old
 *   records replayed again on the next recovery.</li>
 *   <li>When recovery aborted on a corrupt-gap, the WAL files were preserved under their original
 *   {@code .wal} names, so beside the reuse above the next recovery re-scanned and re-flagged them
 *   forever. They are now renamed to {@code .wal.corrupt} for manual inspection.</li>
 * </ul>
 */
class Issue4958PreservedWALTest extends TestHelper {

  // Layout constants mirroring the private statics in WALFile.
  private static final int TX_HEADER_SIZE   = 24; // txId(8) + timestamp(8) + pages(4) + segmentSize(4)
  private static final int PAGE_HEADER_SIZE = 24; // fileId(4) + pageNumber(4) + from(4) + to(4) + version(4) + size(4)
  private static final int TX_FOOTER_SIZE   = 12; // segmentSize(4) + MAGIC_NUMBER(8)

  @Override
  protected void beginTest() {
    database.getSchema().getOrCreateDocumentType("PreservedWALType");
  }

  @Test
  void preservedWalFileIsNeverReusedAsActiveWal() throws Exception {
    final String dbPath = database.getDatabasePath();
    database.close();

    // Simulate a WAL file preserved by a previous open (e.g. after corrupt-gap detection): a stray
    // txlog_0.wal that the new WAL pool must NOT adopt and append to.
    final byte[] preservedContent = new byte[64];
    Arrays.fill(preservedContent, (byte) 0x7F);
    final File preserved = new File(dbPath, "txlog_0.wal");
    Files.write(preserved.toPath(), preservedContent);

    // A single-file pool makes the test deterministic: every commit maps to pool slot 0, the exact
    // slot that (before the fix) reopened the preserved txlog_0.wal and appended after its content.
    final Object previousWalFiles = GlobalConfiguration.TX_WAL_FILES.getValue();
    GlobalConfiguration.TX_WAL_FILES.setValue(1);
    try {
      database = factory.open();

      database.transaction(() -> database.newDocument("PreservedWALType").set("k", 1).save());

      assertThat(preserved.length())
          .as("the preserved WAL file must not be appended to by the new active WAL pool")
          .isEqualTo((long) preservedContent.length);
      assertThat(Files.readAllBytes(preserved.toPath())).isEqualTo(preservedContent);
    } finally {
      GlobalConfiguration.TX_WAL_FILES.setValue(previousWalFiles);
    }
  }

  @Test
  void corruptGapWalFilesAreRenamedForManualInspection() throws Exception {
    final String dbPath = database.getDatabasePath();

    // Craft a WAL with a corrupt first record FOLLOWED by a valid transaction record: exactly the
    // pattern the #4508 gap detector aborts recovery on and preserves the files for.
    final byte[] garbage = new byte[32];
    Arrays.fill(garbage, (byte) 0x7F);

    final byte[] delta = { 1, 2, 3, 4, 5 };
    final int segmentSize = PAGE_HEADER_SIZE + delta.length;
    final ByteBuffer buf = ByteBuffer.allocate(garbage.length + TX_HEADER_SIZE + segmentSize + TX_FOOTER_SIZE);
    buf.put(garbage);
    buf.putLong(42L);              // txId
    buf.putLong(123456L);          // timestamp
    buf.putInt(1);                 // page count
    buf.putInt(segmentSize);
    buf.putInt(3);                 // fileId
    buf.putInt(0);                 // pageNumber
    buf.putInt(0);                 // changesFrom
    buf.putInt(delta.length - 1);  // changesTo
    buf.putInt(1);                 // currentPageVersion
    buf.putInt(1024);              // currentPageSize
    buf.put(delta);
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final File corrupt = new File(dbPath, "txlog_777.wal");
    Files.write(corrupt.toPath(), buf.array());

    ((DatabaseInternal) database).getTransactionManager().checkIntegrity();

    assertThat(corrupt)
        .as("a WAL flagged by the corrupt-gap detector must not survive under its active .wal name")
        .doesNotExist();
    assertThat(new File(dbPath, "txlog_777.wal.corrupt"))
        .as("the corrupt WAL must be preserved for manual inspection under the .corrupt suffix")
        .exists();
  }
}
