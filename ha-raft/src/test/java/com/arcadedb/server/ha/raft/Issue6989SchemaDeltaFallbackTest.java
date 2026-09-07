/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
 * <p>
 * These are the branches whose failure mode is silent. Neither of the two defects the integration test caught -
 * the setting read off the per-database configuration instead of the server's, and a staleness guard keyed on
 * the schema version instead of the Raft term - broke replication or failed a test: they just stopped the delta
 * path from ever engaging. The term arm in particular is otherwise reachable only by forcing a re-election
 * mid-test, which is expensive and flaky, so the decision is pinned here instead.
 */
class Issue6989SchemaDeltaFallbackTest {

  private static final long TERM = 7L;

  @Test
  void theSettingBeingOffShipsTheWholeDocument() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(false, true, TERM, TERM)).isFalse();
  }

  @Test
  void noBaseYetShipsTheWholeDocument() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, false, -1L, TERM)).isFalse();
  }

  @Test
  void aTermThatMovedShipsTheWholeDocument() {
    // Another node was leader in between, so what the followers hold is whatever IT shipped.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM + 1)).isFalse();
    // And the same node regaining leadership does not make the cache valid again: a new term is a new term.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM + 2)).isFalse();
  }

  @Test
  void anUnreadableTermShipsTheWholeDocument() {
    // A division restarting in place reports -1: unknown cannot show the cache is still valid.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, -1L)).isFalse();
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, -1L, -1L))
        .as("-1 must not match -1 into a usable base")
        .isFalse();
  }

  @Test
  void anUnchangedTermWithTheSettingOnUsesTheBase() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM)).isTrue();
  }

  @Test
  void aDeltaWorthMostOfTheDocumentIsNotShipped() {
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(600, 1000)).isFalse();
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(500, 1000))
        .as("exactly half is not smaller enough to be worth the key sets")
        .isFalse();
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(499, 1000)).isTrue();
  }

  @Test
  void aDeltaIsShippedWhenNoWholeDocumentLengthIsKnownYet() {
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(10_000, 0)).isTrue();
  }

  @Test
  void theSizeBarDoesNotOverflow() {
    // The doubling is done in long arithmetic: an int-sized delta must not wrap into "worth shipping".
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(Integer.MAX_VALUE, 1_000)).isFalse();
  }
}
