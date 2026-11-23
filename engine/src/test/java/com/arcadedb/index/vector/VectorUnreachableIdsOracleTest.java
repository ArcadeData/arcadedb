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

import com.arcadedb.utility.IntHashSet;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle test of the translation of a persisted graph's unreachable ordinals (issue #7190) into the vector ids the delta buffer
 * has to take over: {@code LSMVectorIndex.unreachableVectorIdsOf}, a pure function on its arguments, held to a
 * straightforward model over random manifests, including the ordinals at and past the graph's size, the ids a compaction
 * turned into {@code -1}, and the ids the delta buffer already holds.
 */
class VectorUnreachableIdsOracleTest {
  @Test
  void answersTheListedOrdinalsThatAreInTheGraphAreStillLiveAndAreNotQueuedYet() throws Exception {
    final Random random = new Random(53);
    for (int round = 0; round < 5_000; round++) {
      final int mapLength = random.nextInt(40);
      final int[] ordinalToVectorId = new int[mapLength];
      for (int i = 0; i < mapLength; i++)
        ordinalToVectorId[i] = random.nextInt(8) == 0 ? -1 : random.nextInt(60);
      // The graph covers a prefix of the map, or (a manifest that does not match its map) more or fewer entries than it
      final int graphNodes = random.nextInt(mapLength + 6);
      final int count = random.nextInt(30);
      final int[] ordinals = new int[count];
      for (int i = 0; i < count; i++)
        ordinals[i] = random.nextInt(mapLength + 10) - 2; // negative, inside, at and past the end of the map
      final IntHashSet queued = new IntHashSet();
      final Set<Integer> queuedModel = new HashSet<>();
      for (int i = random.nextInt(10); i > 0; i--) {
        final int id = random.nextInt(60);
        queued.add(id);
        queuedModel.add(id);
      }

      final List<Integer> expected = new ArrayList<>();
      for (final int ordinal : ordinals)
        if (ordinal >= 0 && ordinal < graphNodes && ordinal < mapLength && ordinalToVectorId[ordinal] >= 0
            && !queuedModel.contains(ordinalToVectorId[ordinal]))
          expected.add(ordinalToVectorId[ordinal]);

      final int[] actual = unreachableVectorIdsOf(ordinals, ordinalToVectorId, graphNodes, queued);
      assertThat(actual).as("round %d: ordinals=%s map=%s graphNodes=%d", round, Arrays.toString(ordinals),
          Arrays.toString(ordinalToVectorId), graphNodes).containsExactly(expected.stream().mapToInt(Integer::intValue).toArray());
    }
  }

  @Test
  void nothingListedIsNothingToQueue() throws Exception {
    final IntHashSet none = new IntHashSet();
    assertThat(unreachableVectorIdsOf(null, new int[] { 1, 2 }, 2, none)).isEmpty();
    assertThat(unreachableVectorIdsOf(new int[0], new int[] { 1, 2 }, 2, none)).isEmpty();
    // The last ordinal of the graph is in it, the first one past it is not
    assertThat(unreachableVectorIdsOf(new int[] { 1 }, new int[] { 10, 11, 12 }, 2, none)).containsExactly(11);
    assertThat(unreachableVectorIdsOf(new int[] { 2 }, new int[] { 10, 11, 12 }, 2, none)).isEmpty();
    // ...and the map's own end bounds it when the graph claims more nodes than the map has
    assertThat(unreachableVectorIdsOf(new int[] { 2 }, new int[] { 10, 11, 12 }, 9, none)).containsExactly(12);
    assertThat(unreachableVectorIdsOf(new int[] { 3 }, new int[] { 10, 11, 12 }, 9, none)).isEmpty();
    // Ordinal zero is an ordinal; negative ones are not
    assertThat(unreachableVectorIdsOf(new int[] { -1, 0 }, new int[] { 10, 11 }, 2, none)).containsExactly(10);
    // A vector id of zero is a vector id; -1 is a vector that is gone
    assertThat(unreachableVectorIdsOf(new int[] { 0, 1 }, new int[] { 0, -1 }, 2, none)).containsExactly(0);
  }

  private static int[] unreachableVectorIdsOf(final int[] ordinals, final int[] ordinalToVectorId, final int graphNodes,
      final IntHashSet alreadyQueued) throws Exception {
    final Method method = LSMVectorIndex.class.getDeclaredMethod("unreachableVectorIdsOf", int[].class, int[].class, int.class,
        IntHashSet.class);
    method.setAccessible(true);
    try {
      return (int[]) method.invoke(null, ordinals, ordinalToVectorId, graphNodes, alreadyQueued);
    } catch (final InvocationTargetException e) {
      throw e.getCause() instanceof Exception cause ? cause : e;
    }
  }
}
