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
package com.arcadedb.query.facet;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AncestorExpanderTest {
  // tree: 0 root; 1,2 children of 0; 3 child of 1; 4 child of 1
  private static final int[] OFFSETS = { 0, 0, 1, 2, 3, 4 };
  private static final int[] PARENTS = { 0, 0, 1, 1 };

  @Test
  void twoTagsInOneSubtreeCountAncestorsOnce() {
    final AncestorExpander e = new AncestorExpander(5);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 3, 4 }, 2);
    assertThat(n).isEqualTo(4);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 4, 1, 0);
  }

  @Test
  void cycleTerminatesAndCountsEachMemberOnce() {
    // 0 -> 1 -> 2 -> 0 (parent of 0 is 1, parent of 1 is 2, parent of 2 is 0)
    final int[] offsets = { 0, 1, 2, 3 };
    final int[] parents = { 1, 2, 0 };
    final AncestorExpander e = new AncestorExpander(3);
    final int n = e.expand(offsets, parents, 3, new int[] { 2 }, 1);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(2, 1, 0);
  }

  @Test
  void dagSharedAncestorOnce() {
    // 3 has parents 1 and 2, both under 0
    final int[] offsets = { 0, 0, 1, 2, 4 };
    final int[] parents = { 0, 0, 1, 2 };
    final AncestorExpander e = new AncestorExpander(4);
    final int n = e.expand(offsets, parents, 4, new int[] { 3 }, 1);
    assertThat(n).isEqualTo(4);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 1, 2, 0);
  }

  @Test
  void stateIsResetBetweenDocuments() {
    final AncestorExpander e = new AncestorExpander(5);
    e.expand(OFFSETS, PARENTS, 5, new int[] { 3 }, 1);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 2 }, 1);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(2, 0);
  }

  @Test
  void growsWhenViewGrew() {
    final AncestorExpander e = new AncestorExpander(2);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 3, 4 }, 2);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 4, 1, 0);
  }

  @Test
  void growsPastTheFirstBitsetWord() {
    // chain of 200 nodes, node i has parent i-1
    final int count = 200;
    final int[] offsets = new int[count + 1];
    final int[] parents = new int[count - 1];
    for (int i = 1; i < count; i++) {
      offsets[i + 1] = i;
      parents[i - 1] = i - 1;
    }
    final AncestorExpander e = new AncestorExpander(1);
    assertThat(e.expand(offsets, parents, count, new int[] { count - 1 }, 1)).isEqualTo(count);
    assertThat(e.expand(offsets, parents, count, new int[] { 64 }, 1)).isEqualTo(65);
    assertThat(e.expand(offsets, parents, count, new int[] { 63 }, 1)).isEqualTo(64);
  }

  @Test
  void minusOneStartIsIgnored() {
    final AncestorExpander e = new AncestorExpander(5);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { -1, 3, -1 }, 3);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 1, 0);
    assertThat(e.expand(OFFSETS, PARENTS, 5, new int[] { -1 }, 1)).isZero();
  }

  @Test
  void duplicateStartsCountOnce() {
    final AncestorExpander e = new AncestorExpander(5);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 2, 2 }, 2);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(2, 0);
  }

  @Test
  void outOfRangeStartIsABuilderBug() {
    final AncestorExpander e = new AncestorExpander(5);
    assertThatThrownBy(() -> e.expand(OFFSETS, PARENTS, 5, new int[] { 5 }, 1)).isInstanceOf(AssertionError.class);
  }

  @Test
  void outOfRangeParentIsABuilderBug() {
    final AncestorExpander e = new AncestorExpander(5);
    assertThatThrownBy(() -> e.expand(OFFSETS, new int[] { 0, 0, 1, 5 }, 5, new int[] { 4 }, 1)).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> e.expand(OFFSETS, new int[] { 0, 0, 1, -2 }, 5, new int[] { 4 }, 1)).isInstanceOf(AssertionError.class);
  }

  @Test
  void stateIsResetAfterAnExceptionMidExpand() {
    final AncestorExpander e = new AncestorExpander(5);
    // offsets too short: reading the parents of node 3 throws after the starts 3 and 4 were marked
    assertThatThrownBy(() -> e.expand(new int[] { 0, 0, 1, 2 }, PARENTS, 5, new int[] { 3, 4 }, 2)).isInstanceOf(
        ArrayIndexOutOfBoundsException.class);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 3 }, 1);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 1, 0);
  }

  @Test
  void nodeCountShrinkingAfterALargerExpand() {
    final int count = 200;
    final int[] offsets = new int[count + 1];
    final int[] parents = new int[count - 1];
    for (int i = 1; i < count; i++) {
      offsets[i + 1] = i;
      parents[i - 1] = i - 1;
    }
    final AncestorExpander e = new AncestorExpander(5);
    assertThat(e.expand(offsets, parents, count, new int[] { count - 1 }, 1)).isEqualTo(count);
    final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 3 }, 1);
    assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 1, 0);
  }
}
