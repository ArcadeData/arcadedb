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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.utility.HeapUsage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #11011: the default working-memory budget of an algorithm call was a fixed eighth of the maximum heap, so on a
 * 2 GB heap {@code algo.wcc()} was refused an adjacency of 280 MB that the heap had room for. Left at its default (0) the
 * budget is what the heap can give the call: half of the heap free after the last collection plus the page cache, which
 * the call takes back before it allocates, and never less than the fixed budget it replaces. A configured value is still
 * a fixed budget.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11011AlgoWorkingMemoryAutoBudgetTest extends TestHelper {

  @AfterEach
  void restoreDefault() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
        GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
  }

  @Test
  void theDefaultIsSizedFromTheHeap() {
    assertThat(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue()).isEqualTo(0L);
  }

  @Test
  void theDefaultAdmitsAWorkingSetPastTheFixedBudgetWhenTheHeapHasRoom() {
    final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
    final long free = HeapUsage.availableHeapBytes();
    assumeTrue(free > 4 * floor, "the test JVM's heap has less room than the reservation needs");

    // Reserving allocates nothing: the budget only decides
    final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
    budget.reserve(floor + 1024 * 1024, "the adjacency list", "test");
  }

  @Test
  void theDefaultStillRefusesWhatTheHeapCannotHold() {
    final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
    assertThatThrownBy(() -> budget.reserve(HeapUsage.maxHeapBytes() * 2, "the adjacency list", "test"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the adjacency list would need")
        .hasMessageContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void aConfiguredValueIsAFixedBudget() {
    final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, floor);
    final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
    budget.reserve(floor, "the adjacency list", "test");
    assertThatThrownBy(() -> budget.reserve(1, "the degree buffer", "test"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than the " + floor + " bytes allowed.");
  }

  @Test
  void theFloorIsAnEighthOfTheHeapAndNeverBelow64MB() {
    assertThat(AbstractAlgoProcedure.fixedBudgetFloor()).isEqualTo(
        Math.max(64L * 1024 * 1024, HeapUsage.maxHeapBytes() / 8));
  }

  @Test
  void aWorkingSetPastTheFreeHeapTakesItsShareBackFromThePageCache() {
    final long mb = 1024 * 1024;
    // Fits without the cache: nothing to give up
    assertThat(AbstractAlgoProcedure.pageCacheToReclaim(300 * mb, 1000 * mb, 500 * mb, 0)).isZero();
    // 700 MB on 1000 MB free: twice the working set is 1400 MB, so 400 MB of cache goes first
    assertThat(AbstractAlgoProcedure.pageCacheToReclaim(700 * mb, 1000 * mb, 500 * mb, 0)).isEqualTo(400 * mb);
    // A later reservation of the same call only takes what the earlier ones did not
    assertThat(AbstractAlgoProcedure.pageCacheToReclaim(800 * mb, 1000 * mb, 500 * mb, 400 * mb)).isEqualTo(200 * mb);
    // An unknown heap reading reclaims nothing: the budget stayed at its floor
    assertThat(AbstractAlgoProcedure.pageCacheToReclaim(800 * mb, -1, Long.MAX_VALUE, 0)).isZero();
  }
}
