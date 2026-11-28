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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The candidates of a delta scan come out of {@link ScoredCandidateCursor} nearest first, each once, whatever the order and the
 * ties they went in with and whether the cursor spans the whole of its arrays or a prefix of them. A sort of the same pairs is the
 * oracle.
 */
class ScoredCandidateCursorOracleTest {

  @Test
  void everyPollIsTheNextNearestCandidateAndEachCandidateComesOutOnce() {
    final Random rnd = new Random(77);
    for (int round = 0; round < 500; round++) {
      final int size = rnd.nextInt(70);
      final int length = size + (rnd.nextBoolean() ? 0 : rnd.nextInt(5));
      final float[] distance = new float[length];
      final int[] payload = new int[length];
      // few distinct distances in some rounds, so that ties are common
      final int distinct = rnd.nextBoolean() ? 4 : 1000;
      for (int i = 0; i < length; i++) {
        distance[i] = rnd.nextInt(distinct) / 10f;
        payload[i] = 100 + i;
      }
      final float[] tailDistance = Arrays.copyOfRange(distance, size, length);
      final int[] tailPayload = Arrays.copyOfRange(payload, size, length);
      final List<Float> sorted = new ArrayList<>();
      final Set<Integer> expectedPayloads = new HashSet<>();
      final float[] byPayload = distance.clone();
      for (int i = 0; i < size; i++) {
        sorted.add(distance[i]);
        expectedPayloads.add(payload[i]);
      }
      sorted.sort(Float::compare);

      final ScoredCandidateCursor cursor = new ScoredCandidateCursor(distance, payload, size);
      assertThat(cursor.size()).isEqualTo(size);
      assertThat(cursor.isEmpty()).isEqualTo(size == 0);
      final Set<Integer> polled = new HashSet<>();
      for (int i = 0; i < size; i++) {
        final float nearest = cursor.peekDistance();
        assertThat(nearest).as("round %d: peek before poll %d is the nearest left", round, i).isEqualTo(sorted.get(i));
        final int head = cursor.poll();
        assertThat(polled.add(head)).as("round %d: candidate %d once", round, head).isTrue();
        assertThat(expectedPayloads).as("round %d: only candidates that went in", round).contains(head);
        assertThat(byPayload[head - 100]).as("round %d: poll %d comes with the distance it went in with", round, i).isEqualTo(nearest);
        assertThat(cursor.size()).isEqualTo(size - i - 1);
      }
      assertThat(polled).hasSameSizeAs(expectedPayloads);
      assertThat(cursor.isEmpty()).isTrue();
      // the entries past the cursor are not touched
      assertThat(Arrays.copyOfRange(distance, size, length)).containsExactly(tailDistance);
      assertThat(Arrays.copyOfRange(payload, size, length)).containsExactly(tailPayload);
    }
  }
}
