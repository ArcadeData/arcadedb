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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the ordering device the issue #6501 delta merge rests on. The grouped search reads correctness off
 * this class alone: {@link GroupAdmissionState} is first-come-first-served, so a cursor that returns candidates even
 * slightly out of rank order hands a group slot to a row that did not earn it, and the answer is wrong in a way no
 * exception marks. Worth pinning here rather than only through a search, where the graph walk's own approximation
 * would blur what failed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ScoredCandidateCursorTest {

  @Test
  void anEmptyCursorIsExhaustedFromTheStart() {
    final ScoredCandidateCursor cursor = new ScoredCandidateCursor(new float[0], new int[0], 0);

    assertThat(cursor.isEmpty()).isTrue();
    assertThat(cursor.size()).isZero();
    assertThatThrownBy(cursor::peekDistance).isInstanceOf(NoSuchElementException.class);
    assertThatThrownBy(cursor::poll).isInstanceOf(NoSuchElementException.class);
  }

  @Test
  void aSingleCandidateComesBackOnceAndThenTheCursorIsDone() {
    final ScoredCandidateCursor cursor = new ScoredCandidateCursor(new float[] { 0.5f }, new int[] { 7 }, 1);

    assertThat(cursor.peekDistance()).isEqualTo(0.5f);
    assertThat(cursor.poll()).isEqualTo(7);
    assertThat(cursor.isEmpty()).isTrue();
  }

  /**
   * The payload has to travel with its own distance through every sift, not just end up in the same multiset - the
   * caller looks its candidate up by payload, so a pair that comes apart returns one row's identity at another
   * row's distance.
   */
  @Test
  void payloadsStayPairedWithTheirDistances() {
    final float[] distances = { 9f, 1f, 7f, 3f, 5f };
    final int[] payloads = { 90, 10, 70, 30, 50 };

    final ScoredCandidateCursor cursor = new ScoredCandidateCursor(distances, payloads, distances.length);

    final List<Integer> drained = new ArrayList<>();
    while (!cursor.isEmpty()) {
      final float distance = cursor.peekDistance();
      final int payload = cursor.poll();
      assertThat(payload).as("payload %d does not belong to distance %s", payload, distance)
          .isEqualTo(Math.round(distance * 10));
      drained.add(payload);
    }
    assertThat(drained).containsExactly(10, 30, 50, 70, 90);
  }

  /**
   * The caller scores into over-allocated arrays and compacts as it filters, so everything past {@code size} is
   * whatever the previous entry left there. It must never surface.
   */
  @Test
  void entriesPastTheDeclaredSizeAreInvisible() {
    final float[] distances = { 4f, 2f, 6f, -1f, -1f };
    final int[] payloads = { 40, 20, 60, 999, 999 };

    final ScoredCandidateCursor cursor = new ScoredCandidateCursor(distances, payloads, 3);

    assertThat(cursor.size()).isEqualTo(3);
    final List<Integer> drained = new ArrayList<>();
    while (!cursor.isEmpty())
      drained.add(cursor.poll());
    assertThat(drained).as("a candidate the caller filtered out must not reappear").containsExactly(20, 40, 60);
  }

  @Test
  void aSizeTheArraysCannotHoldIsRejected() {
    assertThatThrownBy(() -> new ScoredCandidateCursor(new float[2], new int[2], 3))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ScoredCandidateCursor(new float[4], new int[2], 3))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ScoredCandidateCursor(new float[2], new int[2], -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Duplicate distances are the norm, not the exception - a delta buffer full of copies of one vector scores them
   * all identically - and a comparison that mishandles ties either loses candidates or spins.
   */
  @Test
  void tiedDistancesAllComeBack() {
    final float[] distances = new float[64];
    final int[] payloads = new int[64];
    for (int i = 0; i < 64; i++) {
      distances[i] = 0.25f;
      payloads[i] = i;
    }

    final ScoredCandidateCursor cursor = new ScoredCandidateCursor(distances, payloads, 64);

    final List<Integer> drained = new ArrayList<>();
    while (!cursor.isEmpty()) {
      assertThat(cursor.peekDistance()).isEqualTo(0.25f);
      drained.add(cursor.poll());
    }
    assertThat(drained).hasSize(64).doesNotHaveDuplicates();
  }

  /**
   * Randomised, against the answer a sort gives, over sizes that straddle every shape of the heap - odd and even
   * node counts, a last level that is full and one that is not, a single-child parent.
   */
  @Test
  void anyInputDrainsInAscendingOrder() {
    final Random random = new Random(6501);
    for (int size = 0; size <= 200; size++) {
      final float[] distances = new float[size];
      final int[] payloads = new int[size];
      final float[] expected = new float[size];
      for (int i = 0; i < size; i++) {
        distances[i] = random.nextFloat() * 100f;
        expected[i] = distances[i];
        payloads[i] = i;
      }
      Arrays.sort(expected);

      final ScoredCandidateCursor cursor = new ScoredCandidateCursor(distances, payloads, size);

      for (int i = 0; i < size; i++) {
        assertThat(cursor.size()).as("size after %d of %d polls", i, size).isEqualTo(size - i);
        final float distance = cursor.peekDistance();
        cursor.poll();
        assertThat(distance).as("candidate %d of %d is out of rank order", i, size).isEqualTo(expected[i]);
      }
      assertThat(cursor.isEmpty()).isTrue();
    }
  }
}
