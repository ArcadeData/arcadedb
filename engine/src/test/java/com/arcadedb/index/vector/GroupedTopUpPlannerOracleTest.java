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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Oracle tests of {@link GroupedTopUpPlanner}: the answer merged from several sources (each one answering the
 * {@code groupBy}/{@code groupSize} rule on its own rows) plus the group-restricted searches the planner asks for must be
 * the answer of the rule applied to the union of the rows. The union is the oracle, the hand-made cases pin when the
 * planner asks nobody (the single-phase common case) and what it asks otherwise.
 */
class GroupedTopUpPlannerOracleTest {
  private record Row(RID rid, int source, int key, float value) {
  }

  @Test
  void anEmptyPlannerAsksNobody() {
    assertThat(new GroupedTopUpPlanner(2, 2).plan()).isEmpty();
  }

  @Test
  void aSourceThatReturnedFewerThanLimitGroupsIsNeverAsked() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(3, 1);
    add(planner, true, row(1, 'A', 10f), row(2, 'B', 8f));
    add(planner, true, row(3, 'C', 9f), row(4, 'D', 7f), row(5, 'E', 6f));
    // source 0 holds two groups of three asked: it answered everything it has, source 1 cannot hide a winner
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void anUncappedSourceIsNeverAsked() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 5f));
    // source 1 would be asked about winner A (5 < its cut-off 9) if it could have cut members; it returned every row it holds
    add(planner, false, row(3, 'B', 9f), row(4, 'B', 8f));
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void strongWinnersFilledEverywhereTakeASinglePhase() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(2, 1);
    add(planner, true, row(1, 'A', 10f), row(2, 'B', 8f));
    add(planner, true, row(3, 'C', 9f), row(4, 'D', 7f));
    // each winner already holds its single member above the other source's second best peak
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void aWinnerWithMembersBelowTheCutOffIsAskedAboutWithItsFloor() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(2, 2);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 5f), row(3, 'B', 8f), row(4, 'B', 4f));
    add(planner, true, row(5, 'C', 9f), row(6, 'C', 6f), row(7, 'D', 7f), row(8, 'D', 3f));
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    assertThat(plan).hasSize(2);
    assertThat(plan.get(0).source()).isEqualTo(0);
    assertThat(plan.get(0).groupKeys()).containsExactly((Object) 'C');
    assertThat(plan.get(0).floor()).isEqualTo(6f);
    assertThat(plan.get(1).source()).isEqualTo(1);
    assertThat(plan.get(1).groupKeys()).containsExactly((Object) 'A');
    assertThat(plan.get(1).floor()).isEqualTo(5f);
  }

  @Test
  void aWinnerHoldingFewerThanGroupSizeMembersTakesAnyValue() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 3);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 9f));
    add(planner, true, row(3, 'B', 5f));
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    assertThat(plan).hasSize(1);
    assertThat(plan.get(0).source()).isEqualTo(1);
    assertThat(plan.get(0).groupKeys()).containsExactly((Object) 'A');
    assertThat(plan.get(0).floor()).isEqualTo(Float.NEGATIVE_INFINITY);
  }

  @Test
  void aRecordOfferedByTwoSourcesCountsOnce() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    final RID shared = new RID(1, 1);
    planner.addSource(List.of(shared), new float[] { 10f }, List.of('A'), true);
    planner.addSource(List.of(shared), new float[] { 4f }, List.of('A'), true);
    planner.addSource(List.of(new RID(1, 2)), new float[] { 3f }, List.of('B'), true);
    // group A holds ONE record: it is short of groupSize 2 and takes any value, so the source that does not have it is asked.
    // Counted twice it would hold 10 and 4, and 4 would not be below that source's cut-off of 3
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    assertThat(plan).hasSize(1);
    assertThat(plan.get(0).source()).isEqualTo(2);
    assertThat(plan.get(0).groupKeys()).containsExactly((Object) 'A');
    assertThat(plan.get(0).floor()).isEqualTo(Float.NEGATIVE_INFINITY);
  }

  @Test
  void aRecordOfferedTwiceIsRankedByItsBestValue() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 1);
    final RID shared = new RID(1, 1);
    planner.addSource(List.of(shared), new float[] { 10f }, List.of('A'), true);
    planner.addSource(List.of(shared), new float[] { 4f }, List.of('A'), true);
    planner.addSource(List.of(new RID(1, 2)), new float[] { 6f }, List.of('B'), true);
    // A peaks at 10 (its best sighting), so B at 6 is no winner and nobody is asked about it
    assertThat(planner.plan()).isEmpty();

    // the better sighting may come last
    final GroupedTopUpPlanner lastBest = new GroupedTopUpPlanner(1, 1);
    assertThat(lastBest.addSource(List.of(shared), new float[] { 4f }, List.of('A'), true)).isEqualTo(0);
    assertThat(lastBest.addSource(List.of(shared), new float[] { 10f }, List.of('A'), true)).isEqualTo(1);
    assertThat(lastBest.addSource(List.of(new RID(1, 2)), new float[] { 6f }, List.of('B'), true)).isEqualTo(2);
    assertThat(lastBest.plan()).isEmpty();
  }

  @Test
  void aRowWhoseRecordCouldNotBeReadTakesNoPartInThePlan() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 1);
    planner.addSource(List.of(new RID(1, 1)), new float[] { 100f }, List.<Object>of(GroupAdmissionState.UNRESOLVED), true);
    assertThat(planner.plan()).isEmpty();
    planner.addSource(List.of(new RID(1, 2)), new float[] { 1f }, List.of('B'), true);
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void aWinnerWhoseFloorEqualsTheCutOffIsNotAsked() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 5f));
    add(planner, true, row(3, 'B', 5f));
    // what source 1 left out peaks at most at its cut-off 5, and a member only counts strictly above the floor 5
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void theCutOffIsTheLimitthBestPeakOfASourceThatReturnedMoreGroups() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(2, 2);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 6f), row(3, 'B', 8f), row(4, 'B', 7f), row(5, 'C', 1f));
    add(planner, true, row(6, 'D', 9f), row(7, 'D', 5f), row(8, 'E', 7f), row(9, 'E', 3f));
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    // source 0: cut-off is its 2nd best peak 8 (not its lowest, 1), so winner D (floor 5) may have members it cut
    assertThat(plan).hasSize(2);
    assertThat(plan.get(0).source()).isEqualTo(0);
    assertThat(plan.get(0).groupKeys()).containsExactly((Object) 'D');
    assertThat(plan.get(0).floor()).isEqualTo(5f);
    assertThat(plan.get(1).source()).isEqualTo(1);
    assertThat(plan.get(1).groupKeys()).containsExactly((Object) 'A');
    assertThat(plan.get(1).floor()).isEqualTo(6f);
  }

  @Test
  void anUnresolvedRowCanNeitherWinAGroupNorFillOne() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    planner.addSource(List.of(new RID(1, 1)), new float[] { 100f }, List.<Object>of(GroupAdmissionState.UNRESOLVED), true);
    add(planner, true, row(2, 'B', 5f));
    // B is the only group: it is complete in its own source, and the unresolved row's source holds no group at all
    assertThat(planner.plan()).isEmpty();
  }

  @Test
  void aGroupTiedWithTheLastWinningPeakArrivesCompleteToo() {
    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(1, 2);
    add(planner, true, row(1, 'A', 10f), row(2, 'A', 2f));
    add(planner, true, row(3, 'B', 10f), row(4, 'B', 9f));
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    // both groups peak at 10: whichever the admission opens must be complete, so A is asked of source 1 and B of source 0
    final Set<Object> asked = new HashSet<>();
    for (final GroupedTopUpPlanner.TopUp t : plan)
      asked.addAll(t.groupKeys());
    assertThat(asked).containsExactlyInAnyOrder('A', 'B');
  }

  @Test
  void theMergedAnswerOfTheSourcesAndTheTopUpsIsTheAnswerOverTheUnion() {
    int topUps = 0;
    for (int seed = 1; seed <= 6000; seed++)
      topUps += checkCorpus(seed, false);
    // the corpora have to exercise the second phase, or this proves nothing
    assertThat(topUps).isGreaterThan(1000);
  }

  @Test
  void theMergedAnswerStaysAnAnswerOverTheUnionUnderTiedValues() {
    int topUps = 0;
    for (int seed = 1; seed <= 6000; seed++)
      topUps += checkCorpus(seed, true);
    assertThat(topUps).isGreaterThan(1000);
  }

  /** One random corpus spread over several sources; returns 1 when the planner asked for a second phase. */
  private static int checkCorpus(final int seed, final boolean ties) {
    final Random r = new Random(seed);
    final int sources = 2 + r.nextInt(3);
    final int groups = 1 + r.nextInt(12);
    final int limit = 1 + r.nextInt(6);
    final int groupSize = 1 + r.nextInt(4);
    final int rowCount = 1 + r.nextInt(120);
    final List<Row> rows = new ArrayList<>();
    final Set<Float> used = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      float value;
      do
        value = ties ? r.nextInt(30) : r.nextFloat();
      while (!ties && !used.add(value));
      rows.add(new Row(new RID(1, i), r.nextInt(sources), r.nextInt(groups), value));
    }

    final List<Row> exact = admit(rows, limit, groupSize);

    final GroupedTopUpPlanner planner = new GroupedTopUpPlanner(limit, groupSize);
    final List<Row> merged = new ArrayList<>();
    final List<List<Row>> perSource = new ArrayList<>();
    for (int s = 0; s < sources; s++) {
      final List<Row> mine = new ArrayList<>();
      for (final Row row : rows)
        if (row.source == s)
          mine.add(row);
      perSource.add(mine);
      final List<Row> answer = admit(mine, limit, groupSize);
      merged.addAll(answer);
      final List<RID> rids = new ArrayList<>();
      final float[] values = new float[answer.size()];
      final List<Object> keys = new ArrayList<>();
      for (int i = 0; i < answer.size(); i++) {
        rids.add(answer.get(i).rid);
        values[i] = answer.get(i).value;
        keys.add(answer.get(i).key);
      }
      planner.addSource(rids, values, keys, true);
    }
    final List<GroupedTopUpPlanner.TopUp> plan = planner.plan();
    for (final GroupedTopUpPlanner.TopUp topUp : plan) {
      final List<Row> restricted = new ArrayList<>();
      for (final Row row : perSource.get(topUp.source()))
        if (topUp.groupKeys().contains(row.key) && row.value > topUp.floor())
          restricted.add(row);
      merged.addAll(admit(restricted, Integer.MAX_VALUE, groupSize));
    }
    final List<Row> answer = admit(merged, limit, groupSize);

    assertThat(isAnswerOver(rows, answer, exact, groupSize)).as("seed %d (ties=%s) limit=%d groupSize=%d plan=%s", seed, ties, limit,
        groupSize, plan).isTrue();
    return plan.isEmpty() ? 0 : 1;
  }

  /** The rule applied in score order, as the caller does with its merged candidates. */
  private static List<Row> admit(final Collection<Row> in, final int limit, final int groupSize) {
    final List<Row> sorted = new ArrayList<>(in);
    sorted.sort((a, b) -> Float.compare(b.value, a.value));
    final GroupAdmissionState state = new GroupAdmissionState(limit, groupSize);
    final List<Row> out = new ArrayList<>();
    final Set<RID> seen = new HashSet<>();
    for (final Row row : sorted) {
      if (state.isFull())
        break;
      if (seen.contains(row.rid))
        continue;
      if (state.admit(row.key)) {
        out.add(row);
        seen.add(row.rid);
      }
    }
    return out;
  }

  /**
   * Whether {@code answer} is a valid answer over the corpus: its group peaks are the exact group peaks (a multiset, groups
   * tied at the last winning peak may differ) and each group it holds has that group's {@code groupSize} best values.
   */
  private static boolean isAnswerOver(final List<Row> corpus, final List<Row> answer, final List<Row> exact, final int groupSize) {
    final Map<Integer, List<Float>> all = new HashMap<>();
    for (final Row row : corpus)
      all.computeIfAbsent(row.key, k -> new ArrayList<>()).add(row.value);
    for (final List<Float> values : all.values())
      values.sort(Collections.reverseOrder());
    final Map<Integer, List<Float>> got = new HashMap<>();
    for (final Row row : answer)
      got.computeIfAbsent(row.key, k -> new ArrayList<>()).add(row.value);
    final List<Float> gotPeaks = new ArrayList<>();
    for (final Map.Entry<Integer, List<Float>> e : got.entrySet()) {
      e.getValue().sort(Collections.reverseOrder());
      final List<Float> best = all.get(e.getKey());
      if (!best.subList(0, Math.min(groupSize, best.size())).equals(e.getValue()))
        return false;
      gotPeaks.add(e.getValue().get(0));
    }
    final Map<Integer, Float> exactPeaks = new HashMap<>();
    for (final Row row : exact)
      exactPeaks.merge(row.key, row.value, Math::max);
    final List<Float> expectedPeaks = new ArrayList<>(exactPeaks.values());
    Collections.sort(gotPeaks);
    Collections.sort(expectedPeaks);
    return gotPeaks.equals(expectedPeaks);
  }

  private static Row row(final int rid, final char key, final float value) {
    return new Row(new RID(1, rid), 0, key, value);
  }

  private static void add(final GroupedTopUpPlanner planner, final boolean capped, final Row... rows) {
    final List<RID> rids = new ArrayList<>();
    final float[] values = new float[rows.length];
    final List<Object> keys = new ArrayList<>();
    for (int i = 0; i < rows.length; i++) {
      rids.add(rows[i].rid);
      values[i] = rows[i].value;
      keys.add((char) rows[i].key);
    }
    planner.addSource(rids, values, keys, capped);
  }
}
