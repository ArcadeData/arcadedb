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
package com.arcadedb.bench.workload.rag;

import com.arcadedb.bench.workload.VectorRecall;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

class FilteredExactTest {
  private static float[][] base(final int n) {
    final float[][] base = new float[n][];
    for (int i = 0; i < n; i++)
      base[i] = new float[] { (float) Math.cos(i * 0.1), (float) Math.sin(i * 0.1) };
    return base;
  }

  @Test
  void topKOnlyReturnsVisibleIds() {
    final float[][] base = base(100);
    final boolean[] visible = new boolean[100];
    for (int i = 0; i < 100; i += 3)
      visible[i] = true;
    final int[] top = FilteredExact.topK(base, VectorRecall.norms(base), visible, new float[] { 1, 0 }, 10);
    assertThat(top).hasSize(10);
    for (final int id : top)
      assertThat(visible[id]).isTrue();
    assertThat(top[0]).isEqualTo(0);
  }

  @Test
  void withEverythingVisibleItMatchesTheUnfilteredAnswer() {
    final float[][] base = base(100);
    final boolean[] visible = new boolean[100];
    Arrays.fill(visible, true);
    final float[] q = { 0.3f, 0.9f };
    assertThat(FilteredExact.topK(base, VectorRecall.norms(base), visible, q, 7)).containsExactly(VectorRecall.exactTopK(base, q, 7));
  }

  @Test
  void recallDividesByTheVisibleCountWhenItIsBelowK() {
    final float[][] base = base(100);
    final boolean[] visible = new boolean[100];
    visible[5] = true;
    visible[40] = true;
    visible[77] = true;
    final int[] exact = FilteredExact.topK(base, VectorRecall.norms(base), visible, new float[] { 1, 0 }, 10);
    assertThat(exact).hasSize(3);
    assertThat(FilteredExact.recall(new int[] { 77, 5, 40 }, exact)).isEqualTo(1.0);
    assertThat(FilteredExact.recall(new int[] { 77 }, exact)).isCloseTo(1.0 / 3, offset(1e-9));
  }

  @Test
  void nothingVisibleGivesNothing() {
    final float[][] base = base(10);
    assertThat(FilteredExact.topK(base, VectorRecall.norms(base), new boolean[10], new float[] { 1, 0 }, 5)).isEmpty();
  }
}
