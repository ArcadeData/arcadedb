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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The hand-rolled owner table behind the shared vector id check, against a {@link HashMap} oracle: dense ids, stray
 * large ids that go to the open-addressing table, and the migration of those into the dense arrays when they grow.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class IdOwnersTest {
  @Test
  void matchesAHashMapOverDenseAndSparseIdsAcrossManyGrowths() {
    final LSMVectorIndex.IdOwners owners = new LSMVectorIndex.IdOwners();
    final Map<Integer, long[]> oracle = new HashMap<>();
    final Random random = new Random(42);

    for (int i = 0; i < 200_000; i++) {
      // Mostly a dense run, with strays far above it that later fall inside the grown dense range.
      final int id = random.nextInt(10) == 0 ? random.nextInt(2_000_000) : i / 2;
      final int bucket = random.nextInt(20) == 0 ? LSMVectorIndex.IdOwners.DELETED : 1 + random.nextInt(8);
      final long position = bucket == LSMVectorIndex.IdOwners.DELETED ? 0L : random.nextInt(1_000_000);
      owners.put(id, bucket, position);
      oracle.put(id, new long[] { bucket, position });
    }

    for (final Map.Entry<Integer, long[]> entry : oracle.entrySet()) {
      assertThat(owners.bucket(entry.getKey())).as("bucket of id %d", entry.getKey())
          .isEqualTo((int) entry.getValue()[0]);
      assertThat(owners.position(entry.getKey())).as("position of id %d", entry.getKey())
          .isEqualTo(entry.getValue()[1]);
    }
  }

  @Test
  void anIdNobodyWroteIsAbsentWhetherDenseOrSparse() {
    final LSMVectorIndex.IdOwners owners = new LSMVectorIndex.IdOwners();
    owners.put(3, 1, 7L);
    owners.put(5_000_000, 2, 9L);

    assertThat(owners.bucket(4)).isZero();
    assertThat(owners.bucket(4_999_999)).isZero();
    assertThat(owners.position(4_999_999)).isZero();
    assertThat(owners.bucket(Integer.MAX_VALUE)).isZero();
  }
}
