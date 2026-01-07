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
 * Tests write/read symmetry for ContiguousPageWriter and ContiguousPageReader.
 * Verifies that data written at specific positions can be read back correctly.
 */
class ContiguousPageIOTest {
  private static final String DB_PATH = "databases/test-contiguous-io";

  @AfterEach
  void cleanup() {
    if (new DatabaseFactory(DB_PATH).exists()) {
      new DatabaseFactory(DB_PATH).open().drop();
    }
  }

  @Test
  void writeReadSymmetry() throws Exception {
    // Create database
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists()) {
      factory.open().drop();
    }

    Database database = factory.create();

    try (database) {
      DatabaseInternal dbInternal = (DatabaseInternal) database;
      database.begin();
      // Create a graph file to test with
      final String fileName = "test-io";
      final int pageSize = 65536;  // Standard page size
      final int usablePageSize = pageSize - BasePage.PAGE_HEADER_SIZE;

      LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
          dbInternal,
          fileName,
          dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE,
          pageSize
      );

      // Write phase: write known magic values sequentially
      ContiguousPageWriter writer = new ContiguousPageWriter(
          dbInternal,
          graphFile.getFileId(),
          pageSize
      );

      // Record positions where we write magic values
      // We'll write some padding between them and record where each magic value starts
      long[] magicPositions = new long[7];
      int[] magicValues = {
          0xDEADBEEF,  // pos 0
          0xCAFEBABE,  // after some padding
          0xFEEDFACE,  // near page boundary
          0xDEADC0DE,  // just before page boundary
          0xBAADF00D,  // first byte of page 1
          0xFEEDBEEF,  // well into page 1
          0xCAFED00D   // page 2
      };

      // Write padding to position 0
      magicPositions[0] = writer.position();
      writer.writeInt(magicValues[0]);

      // Write padding to get to position ~100
      for (int i = 0; i < 24; i++)
        writer.writeInt(0);  // 96 bytes padding
      magicPositions[1] = writer.position();
      writer.writeInt(magicValues[1]);

      // Write padding to get near page boundary (65527 - 4 = 65523 usable bytes before boundary)
      long target = 65520; // A few bytes before boundary
      while (writer.position() < target) {
        writer.writeInt(0);
      }
      magicPositions[2] = writer.position();
      writer.writeInt(magicValues[2]);

      // Write one more int, this should be just before boundary
      magicPositions[3] = writer.position();
      writer.writeInt(magicValues[3]);

      // Next write should go to page 1
      magicPositions[4] = writer.position();
      writer.writeInt(magicValues[4]);

      // Write more padding into page 1
      for (int i = 0; i < 100; i++)
        writer.writeInt(0);  // 400 bytes into page 1
      magicPositions[5] = writer.position();
      writer.writeInt(magicValues[5]);

      // Write padding to get into page 2
      target = (long) usablePageSize * 2 + 100;
      while (writer.position() < target) {
        writer.writeInt(0);
      }
      magicPositions[6] = writer.position();
      writer.writeInt(magicValues[6]);

      final long totalBytesWritten = writer.position();
      writer.close();

      for (int i = 0; i < magicPositions.length; i++) {
        long pos = magicPositions[i];
        int pageNum = (int) (pos / usablePageSize);
        int pageOffset = (int) (pos % usablePageSize);
      }

      database.commit();

      ContiguousPageReader reader = new ContiguousPageReader(
          dbInternal,
          graphFile.getFileId(),
          pageSize,
          totalBytesWritten,
          0L);

      for (int i = 0; i < magicPositions.length; i++) {
        long pos = magicPositions[i];
        reader.seek(pos);
        int actual = reader.readInt();
        int expected = magicValues[i];

        int pageNum = (int) (pos / usablePageSize);
        int pageOffset = (int) (pos % usablePageSize);

        assertThat(actual)
            .as("Value at position %d (page %d, offset %d)", pos, pageNum, pageOffset)
            .isEqualTo(expected);
      }

      reader.close();

    }
  }
}
