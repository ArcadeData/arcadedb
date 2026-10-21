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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10507: closing a database that was opened {@code READ_ONLY} must not write to its directory.
 * <p>
 * The close of a read-write handle persists {@code statistics.json} (the cached record counts) and {@code last-tx-id.bin}
 * (the recency marker of the HA bootstrap) from the handle's own memory. A {@code READ_ONLY} handle holds the state of its
 * open, and the engine lets a read-write process of the same directory commit while it is open, so writing that state back
 * at close rewound the writer's counters: {@code count(*)} under-reported and {@code CHECK DATABASE} reported an error.
 * <p>
 * The test plays the writer: while the {@code READ_ONLY} handle is open it replaces both files with content the handle never
 * loaded, with an old modification time, and expects the close to leave them byte for byte and timestamp for timestamp as
 * they were.
 */
class Issue10507ReadOnlyCloseTest extends TestHelper {
  private static final String TYPE = "Issue10507Type";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
  }

  @Test
  void closingAReadOnlyHandleLeavesTheStatisticsAndTheLastTransactionIdAlone() throws IOException {
    database.close();

    final File statistics = new File(getDatabasePath(), "statistics.json");
    final File lastTxId = new File(getDatabasePath(), "last-tx-id.bin");
    assertThat(statistics).as("the read-write close writes the statistics").exists();
    assertThat(lastTxId).as("the read-write close writes the last transaction id").exists();

    final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
    try {
      assertThat(readOnly.countType(TYPE, true)).isEqualTo(100);

      // WHAT A READ-WRITE PROCESS THAT COMMITTED SINCE THE OPEN LEAVES BEHIND: THE HANDLE NEVER LOADED THIS CONTENT
      final String writerStatistics = new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8) + "\n";
      Files.write(statistics.toPath(), writerStatistics.getBytes(StandardCharsets.UTF_8));
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (final DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeLong(123_456_789L);
      }
      Files.write(lastTxId.toPath(), bytes.toByteArray());

      // AN OLD TIMESTAMP, SO THAT ANY REWRITE MOVES IT WHATEVER THE RESOLUTION OF THE FILE SYSTEM
      final long oldTimestamp = System.currentTimeMillis() - 3_600_000L;
      assertThat(statistics.setLastModified(oldTimestamp)).isTrue();
      assertThat(lastTxId.setLastModified(oldTimestamp)).isTrue();

      final String statisticsBefore = new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8);
      final byte[] lastTxIdBefore = Files.readAllBytes(lastTxId.toPath());
      final long statisticsTimeBefore = Files.getLastModifiedTime(statistics.toPath()).toMillis();
      final long lastTxIdTimeBefore = Files.getLastModifiedTime(lastTxId.toPath()).toMillis();

      readOnly.close();

      assertThat(new String(Files.readAllBytes(statistics.toPath()), StandardCharsets.UTF_8)).as("statistics.json content")
          .isEqualTo(statisticsBefore);
      assertThat(Files.readAllBytes(lastTxId.toPath())).as("last-tx-id.bin content").isEqualTo(lastTxIdBefore);
      assertThat(Files.getLastModifiedTime(statistics.toPath()).toMillis()).as("statistics.json modification time")
          .isEqualTo(statisticsTimeBefore);
      assertThat(Files.getLastModifiedTime(lastTxId.toPath()).toMillis()).as("last-tx-id.bin modification time")
          .isEqualTo(lastTxIdTimeBefore);
    } finally {
      if (readOnly.isOpen())
        readOnly.close();
    }
  }
}
