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

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9697: the termination rules of {@link SQLFunctionVectorAbstract#searchGroupedWithStaleTopUp}, driven by a
 * scripted pass so each rule is pinned without a database or a concurrent writer. Issue #10002 added the rule that a
 * wider pass which did not grow keeps widening while it meets stale candidates no earlier pass met.
 */
class Issue9697GroupedStaleTopUpLoopTest {

  /** One scripted pass: the rows it answers and the stale candidates it reports, by position in bucket 1. */
  private record Step(int rows, int... stale) {
  }

  /** Replays the steps in order and records the budgets each pass was asked for. */
  private static class ScriptedPass implements SQLFunctionVectorAbstract.GroupedSearchPass {
    private final Step[]      steps;
    final         List<int[]> asked = new ArrayList<>();

    private ScriptedPass(final Step... steps) {
      this.steps = steps;
    }

    @Override
    public ArrayList<Object> run(final int fetchGroups, final int fetchGroupSize, final Set<RID> staleRids) {
      final Step step = steps[asked.size()];
      asked.add(new int[] { fetchGroups, fetchGroupSize });
      for (final int position : step.stale())
        staleRids.add(new RID(1, position));
      final ArrayList<Object> rows = new ArrayList<>(step.rows());
      for (int i = 0; i < step.rows(); i++)
        rows.add("pass" + asked.size() + "-row" + i);
      return rows;
    }
  }

  @Test
  void aFullFirstAnswerRunsOnePass() {
    final ScriptedPass pass = new ScriptedPass(new Step(6, 0));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
    assertThat(pass.asked).hasSize(1);
  }

  @Test
  void aShortAnswerWithoutStaleRowsRunsOnePass() {
    final ScriptedPass pass = new ScriptedPass(new Step(4));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(4);
    assertThat(pass.asked).hasSize(1);
  }

  @Test
  void eachPassWidensBothBudgetsByTheStaleCount() {
    final ScriptedPass pass = new ScriptedPass(new Step(3, 0, 1), new Step(5, 0, 1, 2), new Step(6, 0, 1, 2));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass)).hasSize(6);
    assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 8, 7 });
  }

  @Test
  void aPassThatDoesNotGrowAndMeetsNoNewStaleCandidateStopsOnTheFresherAnswer() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(4, 0, 1));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass2-"));
    assertThat(pass.asked).hasSize(2);
  }

  @Test
  void aPassThatShrinksAndMeetsNoNewStaleCandidateStopsOnTheLargerEarlierAnswer() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(3, 0, 1));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass1-"));
    assertThat(pass.asked).hasSize(2);
  }

  /**
   * Issue #10002: under churn a wider pass meets the records deleted while it ran, and those can leave it no larger than
   * the pass before. That is not a corpus with nothing more to give, so the loop widens again - and since it is racing a
   * writer, it at least doubles both budgets: (5, 4) becomes (5 + 5, 4 + 4).
   */
  @Test
  void aPassThatDoesNotGrowButMeetsNewStaleCandidatesWidensAgain() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(4, 2, 3, 4), new Step(6));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(6).allMatch(row -> row.toString().startsWith("pass3-"));
    assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 10, 8 });
  }

  /** A shrinking pass that met new stale candidates widens again too, and the larger answer wins if nothing better comes. */
  @Test
  void aShrinkingPassThatMeetsNewStaleCandidatesWidensAgainAndKeepsTheLargerAnswer() {
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0, 1), new Step(3, 2), new Step(3, 2));
    final List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 100_000, pass);
    assertThat(answer).hasSize(4).allMatch(row -> row.toString().startsWith("pass1-"));
    assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 5, 4 }, new int[] { 10, 8 });
  }

  /**
   * Issue #10002: a writer that keeps deleting what the searches rank hands every pass stale candidates no earlier pass met,
   * and never a full answer. The loop keeps widening, doubling once a pass stops growing, and the candidate budget is what
   * ends it - after a number of passes that grows with the log of the budget, not with the budget.
   */
  @Test
  void anEndlessStreamOfFreshStaleCandidatesStopsAtTheBudget() {
    final ScriptedPass pass = new ScriptedPass() {
      @Override
      public ArrayList<Object> run(final int fetchGroups, final int fetchGroupSize, final Set<RID> staleRids) {
        final int passNumber = asked.size();
        asked.add(new int[] { fetchGroups, fetchGroupSize });
        for (int i = 0; i < 3; i++)
          staleRids.add(new RID(1, passNumber * 3L + i));
        final ArrayList<Object> rows = new ArrayList<>();
        for (int i = 0; i < 4; i++)
          rows.add("pass" + asked.size() + "-row" + i);
        return rows;
      }
    };
    // (3,2) grows from nothing and widens by 3 to (6,5); from there no pass grows, so each doubles: (12,10) costs 600, the
    // next, (24,20), would cost 2,400 > 1,000
    List<Object> answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 1_000, pass);
    assertThat(answer).hasSize(4);
    assertThat(pass.asked).containsExactly(new int[] { 3, 2 }, new int[] { 6, 5 }, new int[] { 12, 10 });

    // Against the default budget the race costs a handful of passes, not one per stale candidate
    pass.asked.clear();
    answer = SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(5, 3, 100_000, pass);
    assertThat(answer).hasSize(4);
    assertThat(pass.asked.size()).isLessThanOrEqualTo(8);
  }

  @Test
  void aWiderBudgetPastTheCapIsNeverAsked() {
    // groups=3, groupSize=2 costs 30 candidates; widening by 1 would cost 4 * 3 * 5 = 60, past a cap of 50.
    final ScriptedPass pass = new ScriptedPass(new Step(4, 0));
    assertThat(SQLFunctionVectorAbstract.searchGroupedWithStaleTopUp(3, 2, 50, pass)).hasSize(4);
    assertThat(pass.asked).hasSize(1);
  }
}
