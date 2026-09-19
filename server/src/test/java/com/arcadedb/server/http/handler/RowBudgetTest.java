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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.http.handler.TimeSeriesHandlerUtils.RowBudget;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7663: the hard row ceiling spread across a response that answers SEVERAL reads - one Grafana target per
 * panel query, one Prometheus {@code Query} per selector.
 * <p>
 * {@code Issue7663GrafanaPrometheusRowCeilingIT} pins the same arithmetic end to end, over HTTP. This class exists
 * for the edges that are awkward to reach through a running server: a ceiling of exactly
 * {@link Integer#MAX_VALUE}, and the exhausted-budget boundary between the last read that fits and the first that
 * does not.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7663">issue #7663</a>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class RowBudgetTest {

  /**
   * The fetch asks for one row more than the response can carry: that row is never served, it only tells a cut
   * answer from a complete one.
   */
  @Test
  void theFetchAsksForOneRowPastWhatTheResponseCanCarry() {
    final RowBudget budget = new RowBudget(20);

    assertThat(budget.ceiling()).isEqualTo(20);
    assertThat(budget.fetchLimit()).isEqualTo(21);

    assertThat(budget.charge(15)).isTrue();
    assertThat(budget.fetchLimit()).as("five rows left, plus the one that proves there were more").isEqualTo(6);
  }

  /**
   * The boundary the whole mechanism turns on: exactly the ceiling is served, one row more is refused.
   */
  @Test
  void exactlyTheCeilingIsServedAndOneMoreIsRefused() {
    assertThat(new RowBudget(20).charge(20)).isTrue();
    assertThat(new RowBudget(20).charge(21)).isFalse();

    // Split across two reads, the answer is the same: the budget belongs to the response, not to the read.
    final RowBudget split = new RowBudget(20);
    assertThat(split.charge(12)).isTrue();
    assertThat(split.charge(8)).as("twenty rows in total still fit").isTrue();

    final RowBudget over = new RowBudget(20);
    assertThat(over.charge(12)).isTrue();
    assertThat(over.charge(9)).as("twenty-one in total does not, however it is split").isFalse();
  }

  /**
   * A read that matches nothing neither consumes budget nor refuses a response that is exactly full.
   */
  @Test
  void anEmptyReadNeitherConsumesBudgetNorRefusesAFullResponse() {
    final RowBudget budget = new RowBudget(20);

    assertThat(budget.charge(20)).isTrue();
    assertThat(budget.fetchLimit()).isEqualTo(1);
    assertThat(budget.charge(0)).as("nothing more to carry is not more than the ceiling").isTrue();
  }

  /**
   * A non-positive ceiling disables the budget, exactly as everywhere else this setting is read - and the fetch it
   * hands out is the unlimited one {@code queryAscending} understands.
   */
  @Test
  void aNonPositiveCeilingDisablesTheBudget() {
    for (final int disabled : new int[] { 0, -1 }) {
      final RowBudget budget = new RowBudget(disabled);
      assertThat(budget.fetchLimit()).as("ceiling %d means unlimited", disabled).isZero();
      assertThat(budget.charge(Integer.MAX_VALUE)).isTrue();
      assertThat(budget.fetchLimit()).isZero();
    }
  }

  /**
   * The arithmetic edge the code review found on PR #7720: {@code ceiling - used + 1} at a ceiling of
   * {@link Integer#MAX_VALUE} would wrap to {@link Integer#MIN_VALUE}, which {@code queryAscending} reads as
   * unlimited - the right answer reached by an overflow. It is answered directly instead, and the budget still
   * counts: such a ceiling is unlimited in practice because no {@code List} can hold that many rows.
   */
  @Test
  void aCeilingAtIntegerMaxValueIsUnlimitedRatherThanOverflowed() {
    final RowBudget budget = new RowBudget(Integer.MAX_VALUE);

    assertThat(budget.fetchLimit()).as("never negative, which would be an overflow read as unlimited").isZero();
    assertThat(budget.charge(1_000_000)).isTrue();
    // Once anything is charged the subtraction is safe again, and the extra row comes back.
    assertThat(budget.fetchLimit()).isEqualTo(Integer.MAX_VALUE - 1_000_000 + 1);
  }

  /**
   * One below the overflow edge is an ordinary ceiling and must not be special-cased with it.
   */
  @Test
  void oneBelowTheOverflowEdgeIsAnOrdinaryCeiling() {
    final RowBudget budget = new RowBudget(Integer.MAX_VALUE - 1);

    assertThat(budget.fetchLimit()).isEqualTo(Integer.MAX_VALUE);
    assertThat(budget.charge(10)).isTrue();
  }
}
