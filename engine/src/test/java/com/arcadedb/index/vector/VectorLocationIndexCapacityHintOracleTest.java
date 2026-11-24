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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The capacity hint of {@link VectorLocationIndex}: "at least 0", and it pre-sizes only the directory (one reference per chunk of
 * {@value VectorLocationIndex#CHUNK_SIZE} ids), never a chunk: "a hint larger than the set that materializes costs 4 bytes per chunk
 * instead of a whole chunk". Read through {@code estimatedRetainedBytes()} of fresh instances whose reverse tables have the same size, so
 * that the difference is the directory and nothing else.
 */
class VectorLocationIndexCapacityHintOracleTest {

  @Test
  void aHintOfZeroIsAcceptedAndANegativeOneIsRefused() {
    assertThatCode(() -> new VectorLocationIndex(0)).doesNotThrowAnyException();
    assertThatCode(() -> new VectorLocationIndex(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aHintPreSizesOneDirectorySlotPerChunkAndAllocatesNoChunk() {
    final int slot = 4; // one compressed reference

    // 1 id needs one chunk and 0 ids none; both reverse tables are the minimum size
    assertThat(new VectorLocationIndex(1).estimatedRetainedBytes() - new VectorLocationIndex(0).estimatedRetainedBytes()).isEqualTo(slot);

    // 257 ids need 3 chunks, 512 need 4: the same reverse table (1024 slots), one slot more in the directory
    assertThat(new VectorLocationIndex(512).estimatedRetainedBytes() - new VectorLocationIndex(257).estimatedRetainedBytes()).isEqualTo(slot);

    // 513 ids need 5 chunks, 1024 need 8: the same reverse table (2048 slots), three slots more
    assertThat(new VectorLocationIndex(1024).estimatedRetainedBytes() - new VectorLocationIndex(513).estimatedRetainedBytes()).isEqualTo(3 * slot);

    for (final int hint : new int[] { 0, 1, 128, 129, 10_000 }) {
      final VectorLocationIndex index = new VectorLocationIndex(hint);
      assertThat(index.chunkCount()).as("hint %d allocates no chunk", hint).isZero();
      assertThat(index.size()).isZero();
    }
  }
}
