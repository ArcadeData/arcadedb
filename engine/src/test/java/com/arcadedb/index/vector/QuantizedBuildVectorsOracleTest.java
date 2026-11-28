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

import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Oracle tests of the quantized build store of an INT8 (and BINARY) index and of the reader jvector's builder walks it with:
 * the heap the store reports is the heap its arrays hold, a full store refuses another row, and the reader answers a resident
 * row, a row of the fallback and a placeholder exactly where each belongs, whatever the ordinal.
 */
class QuantizedBuildVectorsOracleTest {
  private static final int INT8_DIMENSIONS = 4096; // 2048 rows per 8 MiB chunk

  @Test
  void ramBytesIsTheHeapTheArraysHold() {
    for (final VectorQuantizationType type : new VectorQuantizationType[] { VectorQuantizationType.INT8, VectorQuantizationType.BINARY })
      for (final int dimensions : new int[] { 8, 100, 4096 })
        for (final int capacity : new int[] { 0, 1, 7, 1000, 5000 }) {
          final QuantizedBuildVectors store = QuantizedBuildVectors.create(type, dimensions, capacity);
          final int rowBytes = QuantizedBuildVectors.rowBytes(type, dimensions);
          final long scales = (type == VectorQuantizationType.INT8 ? 2L : 1L) * capacity * Float.BYTES;
          assertThat(store.capacity()).isEqualTo(capacity);
          assertThat(store.ramBytes()).as("%s %d x %d, nothing written: the scales only", type, dimensions, capacity).isEqualTo(scales);

          // Rows are allocated a chunk at a time, the last chunk only as long as the rows it will ever hold
          final int rowsPerChunk = Math.max(1, Integer.highestOneBit(Math.max(1, 8 * 1024 * 1024 / rowBytes)));
          long chunkBytes = 0;
          for (int i = 0; i < capacity; i++) {
            store.writableChunk()[store.writableOffset()] = (byte) i;
            store.commit(0f, 1f);
            if (i % rowsPerChunk == 0)
              chunkBytes += (long) Math.min(rowsPerChunk, capacity - i) * rowBytes;
            assertThat(store.ramBytes()).as("%s %d x %d after row %d", type, dimensions, capacity, i).isEqualTo(scales + chunkBytes);
          }
          store.release();
          assertThat(store.ramBytes()).as("released").isZero();
          assertThat(store.size()).isZero();
        }
  }

  @Test
  void aFullStoreRefusesAnotherRowAndKeepsItsRows() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 2, 2);
    // Two distinguishable rows: codes (10, -20) and (30, 40), dequantized over [0, 255] as code + 128
    final byte[][] rows = { { 10, -20 }, { 30, 40 } };
    for (final byte[] row : rows) {
      System.arraycopy(row, 0, store.writableChunk(), store.writableOffset(), row.length);
      store.commit(0f, 255f);
    }
    assertThat(store.hasRoom()).isFalse();
    assertThatThrownBy(() -> store.commit(0f, 1f)).isInstanceOf(IllegalStateException.class).hasMessageContaining("full");
    assertThat(store.size()).isEqualTo(2);
    assertThat(store.isResident(1)).isTrue();
    assertThat(store.isResident(2)).isFalse();
    final float[] out = new float[2];
    for (int ordinal = 0; ordinal < rows.length; ordinal++) {
      store.dequantizeInto(ordinal, out);
      assertThat(out).as("row %d is untouched by the refused commit", ordinal)
          .containsExactly(rows[ordinal][0] + 128.0f, rows[ordinal][1] + 128.0f);
    }

    assertThat(QuantizedBuildVectors.create(VectorQuantizationType.BINARY, 8, 0).hasRoom()).isFalse();
    assertThatThrownBy(() -> QuantizedBuildVectors.create(VectorQuantizationType.INT8, 8, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> QuantizedBuildVectors.rowBytes(VectorQuantizationType.NONE, 8))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void everyRowOfEveryChunkComesBackAsItsOwnBytes() {
    final int count = 5000; // three chunks, the last one partial
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, INT8_DIMENSIONS, count);
    for (int i = 0; i < count; i++) {
      final byte[] chunk = store.writableChunk();
      final int offset = store.writableOffset();
      // The first and the last byte of the row carry the ordinal, so a row written one slot off is seen
      chunk[offset] = (byte) (i % 251 - 125);
      chunk[offset + INT8_DIMENSIONS - 1] = (byte) ((i >> 3) % 251 - 125);
      store.commit(-3f + i % 5, 7f + i % 3);
    }
    final float[] out = new float[INT8_DIMENSIONS];
    for (final int ordinal : new int[] { 0, 1, 2047, 2048, 2049, 4095, 4096, 4097, count - 1 }) {
      store.dequantizeInto(ordinal, out);
      final float min = -3f + ordinal % 5;
      final float max = 7f + ordinal % 3;
      assertThat(out[0]).as("first of %d", ordinal).isEqualTo(((ordinal % 251 - 125) + 128.0f) * ((max - min) / 255.0f) + min);
      assertThat(out[INT8_DIMENSIONS - 1]).as("last of %d", ordinal)
          .isEqualTo((((ordinal >> 3) % 251 - 125) + 128.0f) * ((max - min) / 255.0f) + min);
    }
  }

  @Test
  void readerAsksTheFallbackOnlyForOrdinalsOfTheCorpusTheStoreDoesNotHold() {
    final int dimensions = 4;
    final int size = 10;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, 3);
    for (int i = 0; i < 3; i++) {
      Arrays.fill(store.writableChunk(), store.writableOffset(), store.writableOffset() + dimensions, (byte) (i * 10));
      store.commit(0f, 255f);
    }
    final AtomicInteger asked = new AtomicInteger(-100);
    final QuantizedBuildVectorValues.NonResidentRows fallback = (ordinal, row, scales) -> {
      asked.set(ordinal);
      Arrays.fill(row, (byte) 5);
      scales[0] = 0f;
      scales[1] = 255f;
      return true;
    };
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.create(store, VectorQuantizationType.INT8, size, dimensions,
        fallback);

    // Resident: from the store, the fallback is not asked
    VectorFloat<?> vector = values.getVector(2);
    assertThat(vector.get(0)).isEqualTo((20 + 128.0f) * 1.0f);
    assertThat(asked.get()).isEqualTo(-100);

    // The first ordinal past the store and the last ordinal of the corpus: the fallback answers
    for (final int ordinal : new int[] { 3, 4, size - 1 }) {
      vector = values.getVector(ordinal);
      assertThat(asked.get()).as("ordinal %d asked the fallback", ordinal).isEqualTo(ordinal);
      assertThat(vector.get(0)).isEqualTo(133.0f);
      assertThat(values.isDeletedSentinel(vector)).isFalse();
      asked.set(-100);
    }

    // Outside the corpus: the placeholder, and the fallback is not asked about an ordinal that does not exist
    for (final int ordinal : new int[] { -1, size, size + 1, Integer.MAX_VALUE, Integer.MIN_VALUE }) {
      vector = values.getVector(ordinal);
      assertThat(values.isDeletedSentinel(vector)).as("ordinal %d is the placeholder", ordinal).isTrue();
      assertThat(asked.get()).as("ordinal %d: the fallback is not asked", ordinal).isEqualTo(-100);
    }
    assertThat(values.getVector(-1).get(0)).as("the placeholder is a vector of ones").isEqualTo(1.0f);
    assertThat(values.getVector(-1).length()).isEqualTo(dimensions);

    // A fallback that cannot read the row: the placeholder as well
    final QuantizedBuildVectorValues refusing = QuantizedBuildVectorValues.create(store, VectorQuantizationType.INT8, size, dimensions,
        (ordinal, row, scales) -> false);
    assertThat(refusing.isDeletedSentinel(refusing.getVector(5))).isTrue();
    assertThat(refusing.isDeletedSentinel(refusing.getVector(1))).as("resident rows do not need it").isFalse();
  }

  @Test
  void binaryFallbackRowsAreDequantizedAsBinary() {
    final int dimensions = 16;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.BINARY, dimensions, 0);
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.create(store, VectorQuantizationType.BINARY, 4, dimensions,
        (ordinal, row, scales) -> {
          row[0] = (byte) 0b0000_0101;
          row[1] = (byte) 0xFF;
          scales[0] = 0.25f;
          return true;
        });
    final VectorFloat<?> vector = values.getVector(1);
    // The first ordinal of the corpus is an ordinal like the others: a store that holds none of the rows hands it to the fallback
    assertThat(values.isDeletedSentinel(values.getVector(0))).as("ordinal 0 is read by the fallback").isFalse();
    final float magnitude = (float) (1.0 / Math.sqrt(dimensions));
    for (int i = 0; i < dimensions; i++) {
      final boolean set = i < 8 ? (i == 0 || i == 2) : true;
      assertThat(vector.get(i)).as("bit %d", i).isEqualTo(set ? magnitude : -magnitude);
    }
  }
}
