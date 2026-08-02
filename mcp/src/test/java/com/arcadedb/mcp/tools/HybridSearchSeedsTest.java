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
package com.arcadedb.mcp.tools;

import com.arcadedb.database.RID;
import com.arcadedb.mcp.tools.HybridSearchTool.LegRow;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link HybridSearchTool#collectSeeds} directly against synthetic rows, with no database
 * involved: the seed-collection rule (cap, interleaving, de-duplication) is pure list arithmetic.
 */
class HybridSearchSeedsTest {

  private static List<LegRow> rowsFor(final int bucketId, final int count) {
    final List<LegRow> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++)
      rows.add(new LegRow(new RID(bucketId, i), (double) -i));
    return rows;
  }

  @Test
  void seedListFillsExactlyToTheCap() {
    final List<LegRow> vectorLeg = rowsFor(1, HybridSearchTool.MAX_SEEDS * 2);
    final List<LegRow> fullTextLeg = rowsFor(2, HybridSearchTool.MAX_SEEDS * 2);

    final List<RID> seeds = HybridSearchTool.collectSeeds(vectorLeg, fullTextLeg);

    // Both legs offer far more than the cap, so the result must sit exactly on it. Asserting only an
    // upper bound would hold just as well for an empty list and prove nothing about the cap.
    assertThat(seeds.size()).isEqualTo(HybridSearchTool.MAX_SEEDS);
  }

  @Test
  void bothLegsContributeEvenWhenTheVectorLegAloneWouldFillTheCap() {
    // The vector leg alone has more rows than the cap, so a naive vector-first-then-fulltext
    // concatenation would starve the full-text leg out of the seed set entirely.
    final List<LegRow> vectorLeg = rowsFor(1, HybridSearchTool.MAX_SEEDS * 2);
    final List<LegRow> fullTextLeg = rowsFor(2, 10);

    final List<RID> seeds = HybridSearchTool.collectSeeds(vectorLeg, fullTextLeg);
    final Set<RID> seedSet = new HashSet<>(seeds);

    for (final LegRow row : fullTextLeg)
      assertThat(seedSet).contains(row.rid());
  }

  @Test
  void aRidPresentInBothLegsAppearsExactlyOnce() {
    final RID shared = new RID(1, 0);
    final List<LegRow> vectorLeg = List.of(new LegRow(shared, -0.1), new LegRow(new RID(1, 1), -0.2));
    final List<LegRow> fullTextLeg = List.of(new LegRow(shared, 5.0), new LegRow(new RID(2, 1), 4.0));

    final List<RID> seeds = HybridSearchTool.collectSeeds(vectorLeg, fullTextLeg);

    assertThat(seeds).filteredOn(shared::equals).hasSize(1);
  }
}
