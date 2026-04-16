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
package com.arcadedb.index.vector;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #3869: BufferOverflowException during chunked graph persistence.
 * <p>
 * Same root cause as issue #3867 (stale page reference after chunk commit), but with a different
 * failure mode: when the async flush thread writes the stale page to disk via
 * {@code channel.write(buffer)}, it advances the ByteBuffer position to the limit. If the writer
 * then tries to put data via {@code buffer.put()}, the remaining capacity is zero and a
 * {@code BufferOverflowException} is thrown.
 * <p>
 * The fix (applied in #3867) invalidates the stale page reference after each chunk commit by
 * resetting {@code currentPageNum = -1} and {@code currentPage = null}, forcing
 * {@code ensurePageLoaded()} to re-acquire the page in the new transaction.
 * <p>
 * This test verifies data integrity across multiple chunk boundaries with a larger write volume
 * that exercises more chunk commits and page transitions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3869ChunkedWriteBufferOverflowTest {
  private static final String DB_PATH = "databases/test-issue-3869-buffer-overflow";

  @AfterEach
  void cleanup() {
    if (new DatabaseFactory(DB_PATH).exists())
      new DatabaseFactory(DB_PATH).open().drop();
  }

  /**
   * Writes a large volume of data through ContiguousPageWriter with chunked commits,
   * then verifies every byte can be read back correctly. This exercises both the mid-page
   * chunk boundary (which caused #3867/#3869) and the page-boundary crossing logic.
   * <p>
   * The write pattern mixes different primitive types (int, long, float, short) to stress
   * the page writer across various write sizes and alignment scenarios.
   */
  @Test
  void mixedTypeChunkedWriteShouldNotCorruptOrOverflow() throws Exception {
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    Database database = factory.create();

    try (database) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      final int pageSize = 65536;

      final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
          dbInternal, "test-overflow-graph",
          dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE, pageSize);

      dbInternal.getSchema().getEmbedded().registerFile(graphFile);

      database.begin();
      dbInternal.getTransaction().setUseWAL(false);

      final ChunkCommitCallback chunkCallback = (bytesWritten) -> {
        database.commit();
        database.begin();
        dbInternal.getTransaction().setUseWAL(false);
      };

      // Use 1 MB chunks - same as the test for #3867 but with mixed types
      final ContiguousPageWriter writer = new ContiguousPageWriter(
          dbInternal, graphFile.getFileId(), pageSize,
          1, // 1 MB chunks
          chunkCallback);

      // Write a pattern of mixed types (int, long, float) to stress different write sizes
      // and alignment. Each iteration writes 16 bytes (4+8+4).
      // Total: 200,000 * 16 = 3,200,000 bytes (~3 MB) -> triggers 3 chunk commits at 1 MB each
      final int iterations = 200_000;
      for (int i = 0; i < iterations; i++) {
        writer.writeInt(i);           // 4 bytes
        writer.writeLong(i * 100L);   // 8 bytes
        writer.writeFloat(i * 0.5f);  // 4 bytes
      }

      final long totalBytes = writer.position();
      writer.close();
      database.commit();

      // Read back and verify all values
      final ContiguousPageReader reader = new ContiguousPageReader(
          dbInternal, graphFile.getFileId(), pageSize, totalBytes, 0L);

      reader.seek(0);
      for (int i = 0; i < iterations; i++) {
        assertThat(reader.readInt())
            .as("int at iteration %d", i).isEqualTo(i);
        assertThat(reader.readLong())
            .as("long at iteration %d", i).isEqualTo(i * 100L);
        assertThat(reader.readFloat())
            .as("float at iteration %d", i).isEqualTo(i * 0.5f);
      }

      reader.close();
    }
  }
}
