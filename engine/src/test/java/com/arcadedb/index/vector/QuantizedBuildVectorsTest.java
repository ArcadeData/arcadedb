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

import com.arcadedb.utility.VectorApiSupport;
import com.sun.management.ThreadMXBean;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10543: the quantized rows a graph build keeps on the heap instead of the float vectors they stand for. These
 * tests pin the contract the build relies on: the bytes go in and come out as the same floats the index pages would
 * have produced, a row can be read from any thread through a reader of its own, and the reader never hands out the
 * scratch of another one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QuantizedBuildVectorsTest {
  private static final VectorTypeSupport VTS = VectorApiSupport.jvectorProvider().getVectorTypeSupport();

  @Test
  void rowSizeFollowsTheQuantization() {
    assertThat(QuantizedBuildVectors.rowBytes(VectorQuantizationType.INT8, 768)).isEqualTo(768);
    assertThat(QuantizedBuildVectors.rowBytes(VectorQuantizationType.BINARY, 768)).isEqualTo(96);
    assertThat(QuantizedBuildVectors.rowBytes(VectorQuantizationType.BINARY, 10)).isEqualTo(2);

    // a row plus the floats that scale it
    assertThat(QuantizedBuildVectors.bytesPerVector(VectorQuantizationType.INT8, 768)).isEqualTo(768 + 8);
    assertThat(QuantizedBuildVectors.bytesPerVector(VectorQuantizationType.BINARY, 768)).isEqualTo(96 + 4);

    assertThatThrownBy(() -> QuantizedBuildVectors.rowBytes(VectorQuantizationType.NONE, 8))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> QuantizedBuildVectors.rowBytes(VectorQuantizationType.PRODUCT, 8))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aQuantizedRowIsFourTimesSmallerThanTheFloatVectorTheCacheWouldHold() {
    // This is the whole point of the store: the same heap budget covers four times as many INT8 vectors
    final long floatBytes = VectorHeapBudget.bytesPerCachedVector(768);
    final long quantizedBytes = QuantizedBuildVectors.bytesPerVector(VectorQuantizationType.INT8, 768);
    assertThat(floatBytes / quantizedBytes).isGreaterThanOrEqualTo(4L);
  }

  @Test
  void int8RowsComeBackAsTheFloatsTheIndexPagesWouldHaveProduced() {
    final int dimensions = 37; // deliberately not a multiple of anything
    final int count = 300;
    final Random rng = new Random(1);
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, count);

    final byte[][] rows = new byte[count][];
    final float[] mins = new float[count];
    final float[] maxs = new float[count];
    for (int i = 0; i < count; i++) {
      rows[i] = new byte[dimensions];
      rng.nextBytes(rows[i]);
      mins[i] = rng.nextFloat() * -2f;
      maxs[i] = rng.nextFloat() * 2f;
      append(store, rows[i], mins[i], maxs[i]);
    }

    assertThat(store.size()).isEqualTo(count);
    final float[] out = new float[dimensions];
    for (int i = 0; i < count; i++) {
      store.dequantizeInto(i, out);
      for (int d = 0; d < dimensions; d++)
        assertThat(out[d]).as("row %d dim %d", i, d).isCloseTo(referenceInt8(rows[i][d], mins[i], maxs[i]), within(tolerance(mins[i], maxs[i])));
    }
  }

  @Test
  void aConstantInt8RowDequantizesToItsValue() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 4, 1);
    append(store, new byte[4], 0.25f, 0.25f);

    final float[] out = new float[4];
    store.dequantizeInto(0, out);
    assertThat(out).containsExactly(0.25f, 0.25f, 0.25f, 0.25f);
  }

  @Test
  void binaryRowsComeBackAsTheSignsOfTheirBits() {
    final int dimensions = 10;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.BINARY, dimensions, 2);

    // bits 0, 3 and 9 set
    final byte[] packed = new byte[] { (byte) 0b0000_1001, (byte) 0b0000_0010 };
    append(store, packed, 0.5f, 0f);
    // every bit set: the constant vector, whose sign is the median's
    append(store, new byte[] { (byte) 0xFF, (byte) 0b0000_0011 }, -3f, 0f);

    final float magnitude = (float) (1.0 / Math.sqrt(dimensions));
    final float[] out = new float[dimensions];
    store.dequantizeInto(0, out);
    for (int d = 0; d < dimensions; d++)
      assertThat(out[d]).as("dim %d", d).isEqualTo(d == 0 || d == 3 || d == 9 ? magnitude : -magnitude);

    store.dequantizeInto(1, out);
    for (int d = 0; d < dimensions; d++)
      assertThat(out[d]).as("constant, dim %d", d).isEqualTo(-magnitude);
  }

  @Test
  void rowsSpanningSeveralChunksAreAllAddressable() {
    // wide rows so a chunk holds few of them, and many rows so the store needs a lot of chunks
    final int dimensions = 4096;
    final int count = 3000;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, count);

    final byte[] row = new byte[dimensions];
    for (int i = 0; i < count; i++) {
      Arrays.fill(row, (byte) (i % 100 - 50));
      append(store, row, -1f, 1f);
    }
    assertThat(store.size()).isEqualTo(count);

    final float[] out = new float[dimensions];
    for (final int ordinal : new int[] { 0, 1, 511, 512, 513, 1023, 1024, 2047, 2048, count - 1 }) {
      store.dequantizeInto(ordinal, out);
      final float expected = referenceInt8((byte) (ordinal % 100 - 50), -1f, 1f);
      assertThat(out[0]).as("first of row %d", ordinal).isCloseTo(expected, within(tolerance(-1f, 1f)));
      assertThat(out[dimensions - 1]).as("last of row %d", ordinal).isCloseTo(expected, within(tolerance(-1f, 1f)));
    }
  }

  @Test
  void aRowThatWasNotCommittedIsNeitherResidentNorKept() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 4, 3);
    append(store, new byte[] { 1, 2, 3, 4 }, 0f, 1f);

    // A candidate that failed validation is written into the next slot and simply not committed: the slot is reused
    final byte[] chunk = store.writableChunk();
    final int offset = store.writableOffset();
    Arrays.fill(chunk, offset, offset + 4, (byte) 99);
    assertThat(store.size()).isEqualTo(1);
    assertThat(store.isResident(1)).isFalse();

    append(store, new byte[] { 5, 6, 7, 8 }, 0f, 1f);
    final float[] out = new float[4];
    store.dequantizeInto(1, out);
    assertThat(out[0]).isCloseTo(referenceInt8((byte) 5, 0f, 1f), within(tolerance(0f, 1f)));
  }

  @Test
  void residencyStopsAtTheCapacity() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 2, 2);
    append(store, new byte[] { 1, 1 }, 0f, 1f);
    assertThat(store.hasRoom()).isTrue();
    append(store, new byte[] { 2, 2 }, 0f, 1f);
    assertThat(store.hasRoom()).isFalse();

    assertThat(store.isResident(0)).isTrue();
    assertThat(store.isResident(1)).isTrue();
    assertThat(store.isResident(2)).isFalse();
    assertThat(store.isResident(-1)).isFalse();
  }

  @Test
  void releaseDropsTheRows() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 4, 4);
    append(store, new byte[] { 1, 2, 3, 4 }, 0f, 1f);

    store.release();

    assertThat(store.isResident(0)).isFalse();
    assertThat(store.size()).isZero();
  }

  @Test
  void theReaderServesTheStoredRowThroughASharedScratchVector() {
    final int dimensions = 8;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, 2);
    append(store, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0f, 1f);
    append(store, new byte[] { 10, 10, 10, 10, 10, 10, 10, 10 }, 0f, 1f);

    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.ofStoreOnly(store, 2, dimensions);
    assertThat(values.size()).isEqualTo(2);
    assertThat(values.dimension()).isEqualTo(dimensions);
    // jvector uses this to give every worker thread (and the second operand of a distance) a reader of its own
    assertThat(values.isValueShared()).isTrue();

    final VectorFloat<?> first = values.getVector(0);
    assertThat(first.get(0)).isCloseTo(referenceInt8((byte) 0, 0f, 1f), within(tolerance(0f, 1f)));

    final VectorFloat<?> second = values.getVector(1);
    assertThat(second.get(0)).isCloseTo(referenceInt8((byte) 10, 0f, 1f), within(tolerance(0f, 1f)));
    // Same object, overwritten: that is what "shared" promises, and what makes a read allocation-free
    assertThat(second).isSameAs(first);
    assertThat(first.get(0)).isCloseTo(referenceInt8((byte) 10, 0f, 1f), within(tolerance(0f, 1f)));
  }

  @Test
  void aCopyOfTheReaderHasAScratchOfItsOwn() {
    final int dimensions = 4;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, 2);
    append(store, new byte[] { -128, -128, -128, -128 }, 0f, 1f);
    append(store, new byte[] { 127, 127, 127, 127 }, 0f, 1f);

    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.ofStoreOnly(store, 2, dimensions);
    final QuantizedBuildVectorValues copy = (QuantizedBuildVectorValues) values.copy();

    final VectorFloat<?> a = values.getVector(0);
    final VectorFloat<?> b = copy.getVector(1);

    assertThat(b).isNotSameAs(a);
    // reading through the copy did not disturb the original's vector
    assertThat(a.get(0)).isEqualTo(0f);
    assertThat(b.get(0)).isEqualTo(1f);
  }

  @Test
  void readingAVectorAllocatesNothingWhetherItIsResidentOrReadFromTheFallback() {
    final int dimensions = 768;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, dimensions, 4);
    final byte[] row = new byte[dimensions];
    for (int i = 0; i < 4; i++)
      append(store, row, -1f, 1f);
    // ordinal 4 is not resident: it comes from the fallback, which fills the reader's row without allocating either
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.create(store, VectorQuantizationType.INT8, 5, dimensions,
        (ordinal, target, scales) -> {
          scales[0] = -1f;
          scales[1] = 1f;
          return true;
        });

    final Object managementBean = ManagementFactory.getThreadMXBean();
    assumeTrue(managementBean instanceof ThreadMXBean, "per-thread allocation counters are unavailable on this JVM");
    final ThreadMXBean threads = (ThreadMXBean) managementBean;
    assumeTrue(threads.isThreadAllocatedMemorySupported(), "per-thread allocation counters are unsupported on this JVM");
    threads.setThreadAllocatedMemoryEnabled(true);
    assumeTrue(threads.isThreadAllocatedMemoryEnabled(), "per-thread allocation counters are disabled on this JVM");
    final int rounds = 20_000;
    float sink = 0;
    for (int i = 0; i < rounds; i++) // warm up: let the JIT settle so the measurement is the steady state
      sink += values.getVector(i % 5).get(0);

    final long before = threads.getCurrentThreadAllocatedBytes();
    for (int i = 0; i < rounds; i++)
      sink += values.getVector(i % 5).get(0);
    final long perRead = (threads.getCurrentThreadAllocatedBytes() - before) / rounds;

    // A float[768] alone is 3,088 bytes: the old path allocated that, a wrapper, a cache entry and the page row, per miss
    assertThat(perRead).as("bytes allocated per vector read (sink %s)", sink).isLessThan(64L);
  }

  @Test
  void anInterruptedThreadIsToldToStop() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 2, 1);
    append(store, new byte[] { 1, 1 }, 0f, 1f);
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.ofStoreOnly(store, 1, 2);

    Thread.currentThread().interrupt();
    try {
      // A build is cancelled by interrupting its workers, and a read served from memory has no I/O to notice it on
      assertThatThrownBy(() -> values.getVector(0)).isInstanceOf(CancellationException.class);
    } finally {
      Thread.interrupted(); // leave the thread as found
    }
  }

  @Test
  void anOrdinalTheStoreDoesNotHoldAndNoFallbackCanReadIsThePlaceholder() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 3, 1);
    append(store, new byte[] { 1, 2, 3 }, 0f, 1f);
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.ofStoreOnly(store, 5, 3);

    final VectorFloat<?> placeholder = values.getVector(4);
    assertThat(placeholder).isNotNull();
    assertThat(placeholder.length()).isEqualTo(3);
    assertThat(values.isDeletedSentinel(placeholder)).isTrue();
    assertThat(values.isDeletedSentinel(values.getVector(0))).isFalse();
  }

  @Test
  void theSharedScratchIsAJvectorVectorOverTheArrayTheStoreWritesInto() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 3, 1);
    append(store, new byte[] { 1, 2, 3 }, 0f, 1f);
    final QuantizedBuildVectorValues values = QuantizedBuildVectorValues.ofStoreOnly(store, 1, 3);

    final VectorFloat<?> vector = values.getVector(0);
    // whatever provider jvector picked, the vector it reads must agree with what was dequantized
    final VectorFloat<?> expected = VTS.createFloatVector(new float[] { referenceInt8((byte) 1, 0f, 1f),
        referenceInt8((byte) 2, 0f, 1f), referenceInt8((byte) 3, 0f, 1f) });
    for (int d = 0; d < 3; d++)
      assertThat(vector.get(d)).isCloseTo(expected.get(d), within(tolerance(0f, 1f)));
  }

  @Test
  void theLowestCodeIsTheMinimumExactlyAndTheHighestIsTheMaximumToWithinAStepOfFloatRounding() {
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 2, 1);
    append(store, new byte[] { -128, 127 }, -0.3f, 0.7f);

    final float[] out = new float[2];
    store.dequantizeInto(0, out);
    assertThat(out[0]).isEqualTo(-0.3f);
    assertThat(out[1]).isCloseTo(0.7f, within(tolerance(-0.3f, 0.7f)));
  }

  @Test
  void aVectorFarFromTheOriginWithATinyRangeKeepsItsRange() {
    // The shape that rules out the cheaper q * step + base: a term of the size of the range added to one a million times
    // bigger loses the range, and every component comes back as (nearly) the same number.
    final float min = 1000.000f;
    final float max = 1000.001f;
    final QuantizedBuildVectors store = QuantizedBuildVectors.create(VectorQuantizationType.INT8, 3, 1);
    append(store, new byte[] { -128, 0, 127 }, min, max);

    final float[] out = new float[3];
    store.dequantizeInto(0, out);
    final float range = max - min;
    assertThat(out[0]).isEqualTo(min);
    assertThat(out[1]).isCloseTo(min + range * 128 / 255f, within(tolerance(min, max)));
    assertThat(out[2]).isCloseTo(max, within(tolerance(min, max)));
    assertThat(out[2] - out[0]).isCloseTo(range, within(tolerance(min, max)));
  }

  /** What the fast reconstruction may differ from the documented one by: a small fraction of one quantization step. */
  private static float tolerance(final float min, final float max) {
    return 1e-4f * Math.max(Math.abs(max - min), Math.ulp(Math.max(Math.abs(min), Math.abs(max))));
  }

  /** The documented INT8 reconstruction: {@code ((q + 128) / 255) * (max - min) + min}, a constant vector when max == min. */
  private static float referenceInt8(final byte q, final float min, final float max) {
    final float range = max - min;
    if (range == 0.0f)
      return min;
    return ((q + 128) / 255.0f) * range + min;
  }

  private static void append(final QuantizedBuildVectors store, final byte[] row, final float a, final float b) {
    System.arraycopy(row, 0, store.writableChunk(), store.writableOffset(), row.length);
    store.commit(a, b);
  }
}
