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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RagDatasetTest {
  @Test
  void theSameSeedGivesTheSameData() {
    final RagDataset a = new RagDataset(42, 10_000);
    final RagDataset b = new RagDataset(42, 10_000);
    for (int c = 0; c < 1_000; c++) {
      assertThat(a.visibility(c)).isEqualTo(b.visibility(c));
      assertThat(a.tenant(c)).isEqualTo(b.tenant(c));
      assertThat(a.mentions(c)).containsExactly(b.mentions(c));
      assertThat(a.tenant(c)).isBetween(0, 9);
    }
  }

  @Test
  void aDifferentSeedGivesDifferentData() {
    final RagDataset a = new RagDataset(42, 10_000);
    final RagDataset b = new RagDataset(43, 10_000);
    int differing = 0;
    for (int c = 0; c < 1_000; c++)
      if (a.tenant(c) != b.tenant(c))
        differing++;
    assertThat(differing).isGreaterThan(500);
  }

  @Test
  void visibilitySharesAreCloseToTheirTargets() {
    final int n = 50_000;
    final RagDataset d = new RagDataset(7, n);
    int pub = 0, priv = 0, restricted = 0;
    for (int c = 0; c < n; c++) {
      switch (d.visibility(c)) {
      case "public" -> pub++;
      case "private" -> priv++;
      case "restricted" -> restricted++;
      default -> throw new AssertionError("unexpected visibility");
      }
    }
    assertThat(pub / (double) n).isBetween(0.48, 0.52);
    assertThat(priv / (double) n).isBetween(0.38, 0.42);
    assertThat(restricted / (double) n).isBetween(0.08, 0.12);
  }

  @Test
  void mentionsAreDistinctAndInRange() {
    final RagDataset d = new RagDataset(1, 100);
    assertThat(d.entities()).isEqualTo(5);
    for (int c = 0; c < 2_000; c++) {
      final int[] m = d.mentions(c);
      assertThat(m).hasSize(3).doesNotHaveDuplicates();
      for (final int e : m)
        assertThat(e).isBetween(0, d.entities() - 1);
    }
  }

  @Test
  void mentionsAreSkewedTowardsHubs() {
    final int n = 50_000;
    final RagDataset d = new RagDataset(3, n);
    final int entities = d.entities();
    final int hubs = entities / 100;
    long hubMentions = 0, total = 0;
    for (int c = 0; c < n; c++)
      for (final int e : d.mentions(c)) {
        total++;
        if (e < hubs)
          hubMentions++;
      }
    assertThat(hubMentions / (double) total).isGreaterThanOrEqualTo(0.20);
  }

  @Test
  void relatedNeverPointsAtItself() {
    final RagDataset d = new RagDataset(5, 2_000);
    for (int e = 0; e < d.entities(); e++) {
      final int[] r = d.related(e);
      assertThat(r).hasSize(4).doesNotHaveDuplicates().doesNotContain(e);
      for (final int o : r)
        assertThat(o).isBetween(0, d.entities() - 1);
    }
  }

  @Test
  void relatedWorksAtTheMinimumChunkCount() {
    final RagDataset d = new RagDataset(5, RagDataset.MIN_CHUNKS);
    for (int e = 0; e < d.entities(); e++)
      assertThat(d.related(e)).hasSize(4).doesNotHaveDuplicates().doesNotContain(e);
  }

  @Test
  void tooFewChunksAreRejected() {
    assertThatThrownBy(() -> new RagDataset(1, RagDataset.MIN_CHUNKS - 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void visibleFollowsTheTenantRule() {
    assertThat(RagDataset.visible("public", 1, 2)).isTrue();
    assertThat(RagDataset.visible("private", 3, 3)).isTrue();
    assertThat(RagDataset.visible("private", 3, 4)).isFalse();
    assertThat(RagDataset.visible("restricted", 3, 3)).isFalse();
  }
}
