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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests write/read symmetry for ContiguousPageWriter and ContiguousPageReader.
 * Verifies that data written at specific positions can be read back correctly.
 */
class ContiguousPageIOTest {
  private static final String DB_PATH = "target/databases/test-contiguous-io";

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

  /**
   * Issue #11003: the bulk reads ({@code read(int[])}, {@code read(float[])}, {@code readFully(long[])}) read a run of values
   * off each page in one go, and the single-value reads take a fast path inside the page held. Pages of 256 bytes and a
   * one-byte prefix put a value across every page boundary, so each bulk read has to split its run at every page and read the
   * straddling value byte by byte, and must still return exactly what the writer wrote.
   */
  @Test
  void bulkAndSingleReadsAgreeWithTheWriterAcrossPageBoundaries() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    try (Database database = factory.create()) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      database.begin();

      final int pageSize = 256;
      final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(dbInternal, "test-bulk-io", dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE, pageSize);
      final ContiguousPageWriter writer = new ContiguousPageWriter(dbInternal, graphFile.getFileId(), pageSize);

      final int[] ints = new int[300];
      final float[] floats = new float[300];
      final long[] longs = new long[100];
      for (int i = 0; i < ints.length; i++)
        ints[i] = i * 0x01010101 - 7;
      for (int i = 0; i < floats.length; i++)
        floats[i] = i * -1.25f + 0.5f;
      for (int i = 0; i < longs.length; i++)
        longs[i] = i * 0x0102030405060708L + 3;

      writer.write(0x5A);
      final long intsAt = writer.position();
      for (final int v : ints)
        writer.writeInt(v);
      final long floatsAt = writer.position();
      for (final float v : floats)
        writer.writeFloat(v);
      final long longsAt = writer.position();
      for (final long v : longs)
        writer.writeLong(v);
      final long totalBytes = writer.position();
      writer.close();
      database.commit();

      final ContiguousPageReader reader = new ContiguousPageReader(dbInternal, graphFile.getFileId(), pageSize, totalBytes, 0L);

      // Bulk, from the start of each run, and from an offset into the destination array
      final int[] readInts = new int[ints.length + 3];
      reader.seek(intsAt);
      reader.read(readInts, 3, ints.length);
      for (int i = 0; i < ints.length; i++)
        assertThat(readInts[i + 3]).as("int %d", i).isEqualTo(ints[i]);
      assertThat(reader.getPosition()).isEqualTo(floatsAt);

      final float[] readFloats = new float[floats.length];
      reader.read(readFloats, 0, floats.length);
      assertThat(readFloats).containsExactly(floats);

      final long[] readLongs = new long[longs.length];
      reader.readFully(readLongs);
      assertThat(readLongs).containsExactly(longs);
      assertThat(reader.getPosition()).isEqualTo(totalBytes);

      // One at a time, in reverse order so every read seeks
      for (int i = ints.length - 1; i >= 0; i--) {
        reader.seek(intsAt + (long) i * Integer.BYTES);
        assertThat(reader.readInt()).as("int %d", i).isEqualTo(ints[i]);
      }
      for (int i = floats.length - 1; i >= 0; i--) {
        reader.seek(floatsAt + (long) i * Float.BYTES);
        assertThat(reader.readFloat()).as("float %d", i).isEqualTo(floats[i]);
      }
      for (int i = longs.length - 1; i >= 0; i--) {
        reader.seek(longsAt + (long) i * Long.BYTES);
        assertThat(reader.readLong()).as("long %d", i).isEqualTo(longs[i]);
      }

      // The byte reads agree with each other, into an array and into heap and direct buffers
      final byte[] all = new byte[(int) totalBytes];
      reader.seek(0);
      reader.readFully(all);
      assertThat(all[0]).isEqualTo((byte) 0x5A);
      final ByteBuffer heap = ByteBuffer.allocate(all.length + 2);
      heap.position(2);
      reader.seek(0);
      reader.readFully(heap);
      assertThat(heap.position()).isEqualTo(all.length + 2);
      final ByteBuffer direct = ByteBuffer.allocateDirect(all.length);
      reader.seek(0);
      reader.readFully(direct);
      for (int i = 0; i < all.length; i++) {
        assertThat(heap.get(i + 2)).as("heap byte %d", i).isEqualTo(all[i]);
        assertThat(direct.get(i)).as("direct byte %d", i).isEqualTo(all[i]);
      }

      // Past the end of the data fails, whether the value would fit the page or not
      reader.seek(totalBytes - 2);
      assertThatThrownBy(reader::readInt).isInstanceOf(IOException.class);
      reader.seek(longsAt);
      assertThatThrownBy(() -> reader.read(new int[longs.length * 2 + 1], 0, longs.length * 2 + 1)).isInstanceOf(IOException.class);

      reader.close();
    }
  }

  /**
   * Regression test for issue #4584: ContiguousPageReader.seek(n) must apply the file
   * offset for every position, not only for seek(0). With a non-zero offset the logical
   * address mapping must stay monotonic (seek(0) -> offset, seek(1) -> offset+1, ...).
   */
  @Test
  void readWithNonZeroOffset() throws Exception {
    DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    Database database = factory.create();

    try (database) {
      final DatabaseInternal dbInternal = (DatabaseInternal) database;
      database.begin();

      final int pageSize = 65536;

      final LSMVectorIndexGraphFile graphFile = new LSMVectorIndexGraphFile(
          dbInternal,
          "test-offset-io",
          dbInternal.getDatabasePath(),
          ComponentFile.MODE.READ_WRITE,
          pageSize
      );

      final ContiguousPageWriter writer = new ContiguousPageWriter(
          dbInternal,
          graphFile.getFileId(),
          pageSize
      );

      // Simulate a 64-byte header region that the caller addresses past via the offset.
      final long offset = 64L;
      while (writer.position() < offset)
        writer.writeInt(0);

      // Write known magic values right after the header.
      final int[] magic = { 0xDEADBEEF, 0xCAFEBABE, 0xFEEDFACE };
      for (final int v : magic)
        writer.writeInt(v);

      final long totalBytesWritten = writer.position();
      writer.close();

      database.commit();

      final ContiguousPageReader reader = new ContiguousPageReader(
          dbInternal,
          graphFile.getFileId(),
          pageSize,
          totalBytesWritten,
          offset);

      // The caller's logical length excludes the header region.
      assertThat(reader.length()).isEqualTo(totalBytesWritten - offset);

      // Every seek must land at physical position (logical + offset).
      for (int i = 0; i < magic.length; i++) {
        reader.seek((long) i * Binary.INT_SERIALIZED_SIZE);
        assertThat(reader.readInt())
            .as("Value at logical position %d (physical %d)", i * Binary.INT_SERIALIZED_SIZE,
                offset + (long) i * Binary.INT_SERIALIZED_SIZE)
            .isEqualTo(magic[i]);
      }

      // Caller-space round-trip: getPosition() mirrors the value passed to seek().
      reader.seek(0);
      assertThat(reader.getPosition()).isEqualTo(0L);
      reader.seek(1);
      assertThat(reader.getPosition()).isEqualTo(1L);

      // Seeking past the caller-visible length (which excludes the header) must fail.
      assertThatThrownBy(() -> reader.seek(totalBytesWritten - offset + 1))
          .isInstanceOf(IOException.class);

      reader.close();
    }
  }
}
