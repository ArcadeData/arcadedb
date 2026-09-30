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
package com.arcadedb.graph;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test of the chunk-chain cycle detection behind issue #8713, for cycles long enough to re-arm the checkpoint.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ChainCycleGuardTest {

  @Test
  void detectsACycleOfAnyLength() {
    for (int length = 1; length <= 300; length++) {
      final ChainCycleGuard guard = new ChainCycleGuard(rid(0));
      boolean detected = false;
      // a tail of 5 chunks leading into the cycle, then the cycle itself
      for (int step = 1; step <= 20 * (length + 5) && !detected; step++)
        detected = guard.revisits(rid(step <= 5 ? step : 5 + (step - 5) % length));
      assertThat(detected).as("cycle of length %d", length).isTrue();
    }
  }

  @Test
  void neverTripsOnALongAcyclicChain() {
    final ChainCycleGuard guard = new ChainCycleGuard(rid(0));
    for (int i = 1; i <= 100_000; i++)
      assertThat(guard.revisits(rid(i))).isFalse();
  }

  @Test
  void exactGuardReportsTheFirstRevisit() {
    final ChainCycleGuard guard = ChainCycleGuard.exact(rid(0));
    assertThat(guard.revisits(rid(1))).isFalse();
    assertThat(guard.revisits(rid(2))).isFalse();
    assertThat(guard.revisits(rid(0))).isTrue();
  }

  private static RID rid(final int position) {
    return new RID(1, position);
  }
}
