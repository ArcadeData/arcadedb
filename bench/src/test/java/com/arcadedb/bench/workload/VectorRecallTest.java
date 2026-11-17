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
package com.arcadedb.bench.workload;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VectorRecallTest {
  @Test
  void exactTopKOnTinySet() {
    final float[][] base = { { 1, 0 }, { 0, 1 }, { 0.7f, 0.7f } };
    assertThat(VectorRecall.exactTopK(base, new float[] { 1, 0 }, 2)).containsExactly(0, 2);
  }

  @Test
  void recallFraction() {
    assertThat(VectorRecall.recall(new int[] { 1, 2, 3, 4 }, new int[] { 1, 2, 5, 6 })).isEqualTo(0.5);
  }

  @Test
  void nonUnitInputsAreRankedByCosine() {
    final float[][] base = { { 10, 0 }, { 0, 1 }, { 0.2f, 0.2f } };
    assertThat(VectorRecall.exactTopK(base, new float[] { 1, 1 }, 1)).containsExactly(2);
  }

  @Test
  void zeroNeighboursIsAnEmptyAnswer() {
    final float[][] base = { { 1, 0 }, { 0, 1 } };
    assertThat(VectorRecall.exactTopK(base, new float[] { 1, 0 }, 0)).isEmpty();
  }
}
