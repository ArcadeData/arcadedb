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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.test.support.FacetSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class FacetCheckTest {
  private static final int      NODES    = 3;
  /**
   * Deadline of the cases expected to pass or to fail on sight: they return at once, so a generous bound costs nothing and keeps a
   * JVM stall in the unit lane from turning a case that needs two polls into a FACET_STALL.
   */
  private static final Duration GENEROUS = Duration.ofSeconds(60);

  private static SortedMap<String, Long> counts(final long nike) {
    final SortedMap<String, Long> counts = new TreeMap<>();
    counts.put("brand=Nike", nike);
    counts.put("brand=Adidas", 2L);
    counts.put("size=small", 3L);
    return counts;
  }

  private static FakeFacetReader reader() {
    final FakeFacetReader reader = new FakeFacetReader();
    for (int i = 0; i < NODES; i++)
      reader.data.put(i, counts(5));
    return reader;
  }

  private static void script(final FakeFacetReader reader, final int node, final FacetSnapshot... samples) {
    final ArrayDeque<FacetSnapshot> deque = new ArrayDeque<>();
    for (final FacetSnapshot sample : samples)
      deque.add(sample);
    reader.script.put(node, deque);
  }

  private static List<Violation> check(final FakeFacetReader reader, final Duration timeout) throws InterruptedException {
    return new FacetCheck(reader, NODES, timeout, Duration.ofMillis(5)).run();
  }

  @Test
  void identicalNodesWithExactFacetsPass() throws Exception {
    assertThat(check(reader(), GENEROUS)).isEmpty();
  }

  @Test
  void facetsThatDisagreeWithGroupByAreFacetDrift() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(true, 0, "index", counts(4)));
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(v.getFirst().message()).contains("node 1").contains("where=''");
    assertThat(v.getFirst().details()).containsExactly("brand=Nike expected=5 actual=4");
  }

  @Test
  void driftWhilePatchingIsFacetDrift() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 0, FakeFacetReader.sample(true, 7, 40, "index", counts(4)), FakeFacetReader.sample(true, 0, "index", counts(5)));
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
    assertThat(v.getFirst().message()).contains("patched=7").contains("lagMs=40");
  }

  @Test
  void driftOnlyAtLagZeroIsFacetDrift() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 2, FakeFacetReader.sample(true, 7, 40, "index", counts(5)), FakeFacetReader.sample(true, 0, "index", counts(4)));
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
    assertThat(v.getFirst().message()).contains("at lag 0").contains("node 2");
  }

  @Test
  void nonExactSamplesAreNotCompared() throws Exception {
    final FakeFacetReader reader = reader();
    final FacetSnapshot wrong = FakeFacetReader.sample(false, 50, "index", counts(1));
    script(reader, 1, wrong, wrong, wrong, FakeFacetReader.sample(true, 0, "index", counts(5)));
    assertThat(check(reader, GENEROUS)).isEmpty();
  }

  @Test
  void scanSourceIsComparedAndIndexAwaited() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(true, 0, "unavailable", new TreeMap<>()), FakeFacetReader.sample(true, 0, "scan", counts(5)),
        FakeFacetReader.sample(true, 0, "index", counts(5)));
    assertThat(check(reader, GENEROUS)).isEmpty();
  }

  @Test
  void scanSourceWithWrongCountsIsFacetDrift() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(true, 0, "scan", counts(4)));
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
    assertThat(v.getFirst().details()).containsExactly("brand=Nike expected=5 actual=4");
  }

  @Test
  void projectionThatNeverSettlesIsFacetStall() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(true, 0, 30, "index", counts(5)));
    assertThat(check(reader, Duration.ofMillis(300))).singleElement().satisfies(x -> {
      assertThat(x.invariant()).isEqualTo("FACET_STALL");
      assertThat(x.kind()).isEqualTo(ResultKind.AVAILABILITY);
    });
  }

  @Test
  void sourceThatNeverReturnsToIndexIsFacetStall() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 2, FakeFacetReader.sample(true, 0, "scan", counts(5)));
    assertThat(check(reader, Duration.ofMillis(300))).singleElement().satisfies(x -> {
      assertThat(x.invariant()).isEqualTo("FACET_STALL");
      assertThat(x.kind()).isEqualTo(ResultKind.AVAILABILITY);
    });
  }

  @Test
  void exactFalseWithoutLagIsAWarningNotAViolation() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(false, 0, "index", counts(5)));
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    assertThat(check.run()).isEmpty();
    assertThat(check.warnings()).hasSize(1);
    assertThat(check.warnings().getFirst()).contains("FACET_META").contains("node 1").contains("exact=false").contains("lagMs=0");
  }

  @Test
  void exactFalseWithoutLagAndWrongCountsIsStillFacetDrift() throws Exception {
    final FakeFacetReader reader = reader();
    script(reader, 1, FakeFacetReader.sample(false, 0, "index", counts(4)));
    assertThat(check(reader, GENEROUS)).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
  }

  @Test
  void groupByIsPolledTwiceBeforeItIsTrusted() throws Exception {
    final FakeFacetReader reader = reader();
    assertThat(check(reader, GENEROUS)).isEmpty();
    assertThat(reader.groupByCallsAtFirstFacetRead).isGreaterThanOrEqualTo(2 * NODES);
  }

  @Test
  void groupByThatNeverHoldsStillIsReportedAtTheDeadline() throws Exception {
    final FakeFacetReader reader = reader();
    final int[] round = { 5 };
    reader.afterEveryFacetRead = () -> {
      ++round[0];
      for (int i = 0; i < NODES; i++)
        reader.data.put(i, counts(round[0]));
    };
    final List<Violation> v = check(reader, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DIVERGENCE");
  }

  @Test
  void transientGroupByErrorOnTheReReadIsRetried() throws Exception {
    final FakeFacetReader reader = reader();
    reader.afterFirstFacetRead = () -> reader.groupByFailures.put(1, 1);
    assertThat(check(reader, GENEROUS)).isEmpty();
  }

  @Test
  void identicalNodesPastTheDeadlineAreNotDivergence() throws Exception {
    assertThat(check(reader(), Duration.ZERO)).isEmpty();
  }

  @Test
  void serverErrorOnTheFacetQueryIsFacetError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.facetsServerError.add(1);
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    final List<Violation> v = check.run();
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_ERROR");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    // a 500 is not retried: node 0 answered once, node 1 failed on its first read
    assertThat(reader.facetCalls).isEqualTo(2);
    assertThat(check.warnings()).isEmpty();
  }

  /** A replica can read a chunk chain a replicated apply is still writing: the server answers 503 and the read is retried. */
  @Test
  void retryable503OnTheFacetQueryIsRetriedAndWarned() throws Exception {
    final FakeFacetReader reader = reader();
    FakeFacetReader.failNext(reader.facetsThrows, 1, new HttpNodeReader.ServerErrorException(1, 503, "ConcurrentModificationException"));
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    assertThat(check.run()).isEmpty();
    assertThat(check.warnings()).singleElement().asString().contains("FACET_RETRY").contains("node 1").contains("HTTP 503");
  }

  @Test
  void retryable503ThatOutlastsTheDeadlineIsFacetError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.serverStatus = 503;
    reader.facetsServerError.add(1);
    final FacetCheck check = new FacetCheck(reader, NODES, Duration.ofMillis(300), Duration.ofMillis(5));
    final List<Violation> v = check.run();
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_ERROR");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(v.getFirst().message()).contains("HTTP 503");
    assertThat(reader.facetCalls).isGreaterThan(2);
    assertThat(check.warnings()).anySatisfy(w -> assertThat(w).contains("FACET_RETRY"));
  }

  @Test
  void retryable503OnGroupByIsRetried() throws Exception {
    final FakeFacetReader reader = reader();
    FakeFacetReader.failNext(reader.groupByThrows, 2, new HttpNodeReader.ServerErrorException(2, 503, "retry"),
        new HttpNodeReader.ServerErrorException(2, 503, "retry"));
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    assertThat(check.run()).isEmpty();
    assertThat(check.warnings()).singleElement().asString().contains("FACET_RETRY").contains("node 2");
  }

  @Test
  void retryable503OnGroupByUntilTheDeadlineIsFacetError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.serverStatus = 503;
    reader.serverError.add(2);
    final List<Violation> v = check(reader, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_ERROR");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
  }

  @Test
  void retryable503OnTheReReadIsRetried() throws Exception {
    final FakeFacetReader reader = reader();
    reader.afterFirstFacetRead = () -> FakeFacetReader.failNext(reader.groupByThrows, 0,
        new HttpNodeReader.ServerErrorException(0, 503, "retry"));
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    assertThat(check.run()).isEmpty();
    assertThat(check.warnings()).singleElement().asString().contains("FACET_RETRY").contains("node 0");
  }

  @Test
  void retryable503OnTheReReadUntilTheDeadlineIsFacetError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.afterFirstFacetRead = () -> {
      reader.serverStatus = 503;
      reader.serverError.add(0);
    };
    assertThat(check(reader, Duration.ofMillis(300))).extracting(Violation::invariant).containsExactly("FACET_ERROR");
  }

  /** An authentication lockout (a stranger on the port) is transient: retried until the deadline, then a HARNESS result. */
  @Test
  void authenticationErrorIsRetriedUntilTheDeadline() throws Exception {
    final FakeFacetReader reader = reader();
    FakeFacetReader.failNext(reader.facetsThrows, 1, new HttpNodeReader.ClientErrorException(1, 401, "locked"));
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    assertThat(check.run()).isEmpty();
    assertThat(check.warnings()).singleElement().asString().contains("FACET_RETRY").contains("HTTP 401");

    final FakeFacetReader locked = reader();
    locked.clientStatus = 403;
    locked.clientError.add(1);
    final List<Violation> v = check(locked, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::kind).containsExactly(ResultKind.HARNESS);
    assertThat(v.getFirst().message()).contains("HTTP 403");
  }

  @Test
  void unreadableFacetQueryUntilTheDeadlineIsFacetScan() throws Exception {
    final FakeFacetReader reader = reader();
    reader.facetsUnreadable.add(1);
    final List<Violation> v = check(reader, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_SCAN");
    assertThat(v.getFirst().message()).contains("facets");
  }

  @Test
  void aStallIsNotMislabelledByAnEarlierGroupByError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.groupByFailures.put(2, 1);
    script(reader, 1, FakeFacetReader.sample(true, 0, 30, "index", counts(5)));
    assertThat(check(reader, Duration.ofMillis(300))).extracting(Violation::invariant).containsExactly("FACET_STALL");
  }

  @Test
  void clientErrorIsAHarnessResult() throws Exception {
    final FakeFacetReader reader = reader();
    reader.clientError.add(1);
    final FacetCheck check = new FacetCheck(reader, NODES, GENEROUS, Duration.ofMillis(5));
    final List<Violation> v = check.run();
    assertThat(v).extracting(Violation::kind).containsExactly(ResultKind.HARNESS);
    // a 400 is not retried
    assertThat(check.warnings()).isEmpty();
  }

  @Test
  void nodesHoldingDifferentDataAreFacetDivergence() throws Exception {
    final FakeFacetReader reader = reader();
    reader.data.put(2, counts(4));
    final List<Violation> v = check(reader, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DIVERGENCE");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(v.getFirst().details()).containsExactly("node 2: brand=Nike expected=5 actual=4");
  }

  @Test
  void lateCommitBetweenReadsIsRetriedNotReported() throws Exception {
    final FakeFacetReader reader = reader();
    reader.afterFirstFacetRead = () -> {
      for (int i = 0; i < NODES; i++)
        reader.data.put(i, counts(6));
    };
    assertThat(check(reader, GENEROUS)).isEmpty();
  }

  @Test
  void serverErrorIsFacetError() throws Exception {
    final FakeFacetReader reader = reader();
    reader.serverError.add(1);
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_ERROR");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
  }

  @Test
  void unreadableNodeUntilTheDeadlineIsFacetScan() throws Exception {
    final FakeFacetReader reader = reader();
    reader.unreadable.add(2);
    final List<Violation> v = check(reader, Duration.ofMillis(300));
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_SCAN");
    assertThat(v.getFirst().kind()).isEqualTo(ResultKind.AVAILABILITY);
  }

  @Test
  void secondFilterIsCheckedToo() throws Exception {
    final FakeFacetReader reader = reader();
    reader.filteredFacets.put(1, counts(4));
    final List<Violation> v = check(reader, GENEROUS);
    assertThat(v).extracting(Violation::invariant).containsExactly("FACET_DRIFT");
    assertThat(v.getFirst().message()).contains("fileSize >= 1024");
  }
}
