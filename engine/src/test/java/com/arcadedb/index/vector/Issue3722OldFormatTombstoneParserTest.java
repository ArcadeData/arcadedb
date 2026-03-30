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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the page parser correctly handles old-format tombstones that were written
 * WITHOUT the quantization type byte (before the fix for issue #3722 reopened).
 * <p>
 * This test directly manipulates page data to simulate the old tombstone format, then
 * verifies the parser can still read all entries (entries before AND after the tombstone).
 * <p>
 * Root cause: The old persistDeletionTombstones() did not write the quantization type byte.
 * The parser's skipQuantizationData() would read the next entry's vectorId byte as the
 * quantization type, corrupting all subsequent entries on the same page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3722OldFormatTombstoneParserTest extends TestHelper {

  private static final int DIMENSIONS = 64;

  /**
   * Simulate old-format tombstone by writing entries to a page where the tombstone
   * is missing the quantization type byte, then verify the parser handles it.
   */
  @Test
  void parserShouldHandleOldFormatTombstoneWithoutQuantTypeByte() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Create schema with vector index
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Vec");
      type.createProperty("name", Type.STRING);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);

      database.getSchema().buildTypeIndex("Vec", new String[] { "vector" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withMaxConnections(16)
          .withBeamWidth(100)
          .create();
    });

    // Insert 66 initial vectors, then delete one, then insert 480 more
    final RID[] rids = new RID[546];
    database.transaction(() -> {
      for (int i = 0; i < 66; i++) {
        final MutableVertex v = database.newVertex("Vec");
        v.set("name", "v" + i);
        v.set("vector", createVector(i));
        v.save();
        rids[i] = v.getIdentity();
      }
    });

    // Force graph build
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getSubIndexes().iterator().next();
    index.buildVectorGraphNow();

    // Delete one vector to create a tombstone
    database.transaction(() -> rids[30].asVertex().delete());

    // Insert 480 more vectors
    database.transaction(() -> {
      for (int i = 66; i < 546; i++) {
        final MutableVertex v = database.newVertex("Vec");
        v.set("name", "v" + i);
        v.set("vector", createVector(i));
        v.save();
        rids[i] = v.getIdentity();
      }
    });

    // Now patch the page to simulate old-format tombstone:
    // Find the tombstone entry and remove the quantization type byte
    final int fileId = index.getFileId();
    final int pageSize = index.getPageSize();
    final DatabaseInternal db = (DatabaseInternal) database;

    database.transaction(() -> {
      try {
        // Read page 0 (all entries should be on one page for small vectors)
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, fileId, 0), pageSize, false);

        final int numEntries = page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES);
        final int offsetFreeContent = page.readInt(LSMVectorIndex.OFFSET_FREE_CONTENT);

        // Find the tombstone entry and record its position
        int offset = LSMVectorIndex.HEADER_BASE_SIZE;
        int tombstoneQuantTypeOffset = -1;
        for (int i = 0; i < numEntries; i++) {
          // Skip vectorId
          final long[] vid = page.readNumberAndSize(offset);
          offset += (int) vid[1];
          // Skip bucketId
          final long[] bid = page.readNumberAndSize(offset);
          offset += (int) bid[1];
          // Skip position
          final long[] pos = page.readNumberAndSize(offset);
          offset += (int) pos[1];
          // Read deleted flag
          final boolean deleted = page.readByte(offset) == 1;
          offset += 1;

          if (deleted) {
            // This is the tombstone - record the position of the quantType byte
            tombstoneQuantTypeOffset = offset;
          }

          // Skip quantization data
          final byte quantType = page.readByte(offset);
          offset += 1;
          if (quantType == 1) { // INT8
            final int vecLen = page.readInt(offset);
            offset += 4 + vecLen + 8;
          } else if (quantType == 2) { // BINARY
            final int origLen = page.readInt(offset);
            offset += 4 + ((origLen + 7) / 8) + 4;
          }
        }

        assertThat(tombstoneQuantTypeOffset).as("Should have found a tombstone entry").isGreaterThan(0);

        // Simulate old format: shift all data after the tombstone's quantType byte back by 1 byte
        // This effectively removes the quantType byte from the tombstone entry
        final int shiftStart = tombstoneQuantTypeOffset + 1; // byte after the quantType
        final int shiftEnd = offsetFreeContent;
        final int shiftLength = shiftEnd - shiftStart;

        if (shiftLength > 0) {
          // Read the data after the quantType byte
          final byte[] dataAfter = new byte[shiftLength];
          for (int i = 0; i < shiftLength; i++)
            dataAfter[i] = page.readByte(shiftStart + i);

          // Write it back 1 byte earlier (overwriting the quantType byte)
          for (int i = 0; i < shiftLength; i++)
            page.writeByte(tombstoneQuantTypeOffset + i, dataAfter[i]);

          // Update offsetFreeContent (one less byte)
          page.writeInt(LSMVectorIndex.OFFSET_FREE_CONTENT, offsetFreeContent - 1);
        }
      } catch (final Exception e) {
        throw new RuntimeException("Failed to patch page", e);
      }
    });

    // Now reopen the database - the page has an old-format tombstone
    reopenDatabase();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

    // Verify that the parser handles the old-format tombstone
    database.transaction(() -> {
      final TypeIndex ti = (TypeIndex) database.getSchema().getIndexByName("Vec[vector]");
      final LSMVectorIndex idx = (LSMVectorIndex) ti.getSubIndexes().iterator().next();

      // Parse all entries from pages
      final List<LSMVectorIndexPageParser.VectorEntry> entries =
          LSMVectorIndexPageParser.parseAllEntries(
              (DatabaseInternal) database, idx.getFileId(), idx.getTotalPages(), idx.getPageSize(), false);

      // Count regular and tombstone entries
      int regularCount = 0;
      int tombstoneCount = 0;
      for (final LSMVectorIndexPageParser.VectorEntry entry : entries) {
        if (entry.deleted)
          tombstoneCount++;
        else
          regularCount++;
      }

      // Before fix: entries after the old-format tombstone would be lost (~66 entries only)
      // After fix: all entries should be parsed. The original vector entry still exists on the page
      // alongside the tombstone (LSM merge-on-read: tombstone overrides at query time, not at page level).
      // Total: 66 initial + 1 tombstone + 480 additional = 547 entries
      assertThat(tombstoneCount)
          .as("Should have 1 tombstone entry (old format without quantType byte)")
          .isEqualTo(1);

      // Note: due to the 1-byte shift, the last entry might not parse correctly (data truncated).
      // The key assertion is: we must find SIGNIFICANTLY more than 66 entries (the pre-tombstone count).
      assertThat(regularCount)
          .as("After fix: most regular entries should be parsed despite old-format tombstone. " +
              "Before fix, only ~65 entries would be found due to page corruption.")
          .isGreaterThan(500);

      // Also verify vectorNeighbors returns most results
      final float[] queryVector = new float[DIMENSIONS];
      Arrays.fill(queryVector, 0.5f);
      final ResultSet rs = database.query("sql",
          "SELECT vectorNeighbors('Vec[vector]', ?, ?) AS neighbors",
          queryVector, 500);

      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final List<?> neighbors = result.getProperty("neighbors");
      assertThat(neighbors).isNotNull();

      assertThat(neighbors.size())
          .as("After fix: should find close to 500 results. " +
              "Before fix: would only find ~65 (entries before the old-format tombstone).")
          .isGreaterThan(400);

      rs.close();
    });
  }

  private float[] createVector(final int index) {
    final float[] vector = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      vector[j] = (float) Math.sin(index * 0.1 + j * 0.3) * 0.5f + 0.5f;
    return vector;
  }
}
