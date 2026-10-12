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
package com.arcadedb.function.sql.vector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9697: the termination rules of {@link SQLFunctionVectorAbstract#searchGroupedWithStaleTopUp}, driven by a
 * scripted pass so each rule is pinned without a database or a concurrent writer.
 */
class Issue9697GroupedStaleTopUpLoopTest {

  /** One scripted pass: the rows it answers and the stale candidates it reports. */
  private record Step(int rows, int stale) {
  }

  /** Replays the steps in order and records the budgets each pass was asked for. */
  private static final class ScriptedPass implements SQLFunctionVectorAbstract.GroupedSearchPass {
    private final Step[]      steps;
    private final List<int[]> asked = new ArrayList<>();

    private ScriptedPass(final Step... steps) {
      this.steps = steps;
    }

    @Override
    public ArrayList<Object> run(final int fetchGroups, final int fetchGroupSize, final int[] staleSkipped) {
      final Step step = steps[asked.size()];
      asked.add(new int[] { fetchGroups, fetchGroupSize });
      staleSkipped[0] += step.stale();
      final ArrayList<Object> rows = new ArrayList<>(step.rows());
      for (int i = 0; i < step.rows(); i++)
        rows.add("pass" + asked.size() + "-row" + i);
      return rows;
    }
  }

  @Test
  void aFullFirstAnswerRunsOnePass() {
    final ScriptedPass pass = new ScriptedPass(new Step(6, 1));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
    assertThat(pass.asked).hasSize(1);
  }

  @Test
  void aShortAnswerWithoutStaleRowsRunsOnePass() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(4);
    assertThat(pass.asked).hasSize(1);
  }

  @Test
  void eachPassWidensBothBudgetsByTheStaleCount() {
    final ScriptedPass pass = new ScriptedPass(new Step(3, 2), new Step(5, 3), new Step(6, 3));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
    assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 8, 7 });
  }

  @Test
  void aPassThatDoesNotGrowStopsOnTheFresherAnswer() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 2), new Step(4, 4));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass2-"));
    assertThat(pass.asked).hasSize(2);
  }

  @Test
  void aPassThatShrinksStopsOnTheLargerEarlierAnswer() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 2), new Step(3, 4));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass1-"));
    assertThat(pass.asked).hasSize(2);
  }

  @Test
  void aWiderBudgetPastTheCapIsNeverAsked() {
    // groups=3, groupSize=2 costs 30 candidates; widening by 1 would cost 4 * 3 * 5 = 60, past a cap of 50.
    final ScriptedPass pass = new ScriptedPass(new Step(4, 1));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 50, pass)).hasSize(4);
    assertThat(pass.asked).hasSize(1);
  }
}
