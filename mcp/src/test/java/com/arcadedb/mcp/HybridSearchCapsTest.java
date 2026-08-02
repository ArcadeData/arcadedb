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
package com.arcadedb.mcp;

import com.arcadedb.mcp.tools.HybridSearchTool;
import com.arcadedb.mcp.tools.MCPVectorLeg;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cap arithmetic, verified without a running server. These bounds are the only thing standing
 * between a wide request and an unbounded traversal, so they are pinned independently of the
 * end-to-end tests that exercise them.
 */
class HybridSearchCapsTest {

  @Test
  void legLimitOverFetchesRelativeToK() {
    assertThat(HybridSearchTool.legLimit(1)).isEqualTo(4);
    assertThat(HybridSearchTool.legLimit(10)).isEqualTo(40);
  }

  @Test
  void legLimitIsBoundedRegardlessOfK() {
    assertThat(HybridSearchTool.legLimit(MCPVectorLeg.MAX_K))
        .isEqualTo(HybridSearchTool.MAX_LEG_CANDIDATES);
    // The multiplication must not overflow into a negative or tiny limit at the top of the range.
    assertThat(HybridSearchTool.legLimit(Integer.MAX_VALUE))
        .isEqualTo(HybridSearchTool.MAX_LEG_CANDIDATES);
  }

  @Test
  void capsAreOrderedSoTheExpansionBudgetExceedsTheSeedBudget() {
    assertThat(HybridSearchTool.MAX_SEEDS).isLessThan(HybridSearchTool.MAX_EXPANSION);
    assertThat(HybridSearchTool.MAX_DEPTH).isEqualTo(3);
  }

  @Test
  void toolDefinitionDeclaresTheDepthCap() {
    final var expand = HybridSearchTool.getDefinition()
        .getJSONObject("inputSchema").getJSONObject("properties").getJSONObject("expand")
        .getJSONObject("properties").getJSONObject("maxDepth");
    assertThat(expand.getInt("maximum")).isEqualTo(HybridSearchTool.MAX_DEPTH);
  }
}
