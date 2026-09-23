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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class InvariantCheckerIT {
  private final Ledger           ledger  = new Ledger(2);
  private final InvariantChecker checker = new InvariantChecker(ledger);

  private long op(final int writer, final boolean pair, final byte outcome) {
    final long key = ledger.reserve(writer, pair);
    ledger.record(key, outcome);
    return key;
  }

  /** A snapshot holding the given keys, each with the edge count its ledger entry expects. */
  private NodeSnapshot snapshot(final long... keys) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    for (final long key : keys)
      snapshot.add(key, ledger.isPair(key) ? 1 : 0);
    return snapshot;
  }

  @Test
  void consistentClusterPassesAndResolvesUnknowns() {
    final long acked = op(0, false, Ledger.ACKED);
    final long pair = op(1, true, Ledger.ACKED);
    op(0, false, Ledger.FAILED);
    final long unknownPresent = op(0, false, Ledger.UNKNOWN);
    final long unknownAbsent = op(1, false, Ledger.UNKNOWN);

    assertThat(checker.check(snapshot(acked, pair, unknownPresent))).isEmpty();
    assertThat(ledger.outcome(unknownPresent)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(ledger.outcome(unknownAbsent)).isEqualTo(Ledger.LOST_UNKNOWN);
  }

  @Test
  void lostAcknowledgedWriteIsI1() {
    final long acked = op(0, false, Ledger.ACKED);
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).hasSize(1);
    assertThat(violations.getFirst().invariant()).isEqualTo("I1");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(violations.getFirst().keys()).containsExactly(acked);
  }

  @Test
  void rowsNoOperationProducedAreI2() {
    op(0, false, Ledger.ACKED);
    final NodeSnapshot snapshot = snapshot(Ledger.key(0, 0));
    snapshot.add(Ledger.key(0, 999), 0);
    snapshot.add(Ledger.key(7, 0), 0);
    final List<Violation> violations = checker.check(snapshot);
    assertThat(violations).extracting(Violation::invariant).containsExactly("I2");
    assertThat(violations.getFirst().keys()).containsExactlyInAnyOrder(Ledger.key(0, 999), Ledger.key(7, 0));
  }

  @Test
  void duplicateRowIsI2() {
    final long acked = op(0, false, Ledger.ACKED);
    final List<Violation> violations = checker.check(snapshot(acked, acked));
    assertThat(violations).extracting(Violation::invariant).containsExactly("I2");
    assertThat(violations.getFirst().keys()).containsExactly(acked);
  }

  @Test
  void resurrectedFailureIsI3() {
    final long failed = op(0, false, Ledger.FAILED);
    final List<Violation> violations = checker.check(snapshot(failed));
    assertThat(violations).extracting(Violation::invariant).containsExactly("I3");
  }

  @Test
  void aKeyOnceSeenMustNeverDisappear() {
    final long unknown = op(0, false, Ledger.UNKNOWN);
    assertThat(checker.check(snapshot(unknown))).isEmpty();
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).extracting(Violation::invariant).containsExactly("I1");
    assertThat(violations.getFirst().keys()).containsExactly(unknown);
  }

  @Test
  void lateCommitIsNotAViolation() {
    final long unknown = op(0, false, Ledger.UNKNOWN);
    assertThat(checker.check(snapshot())).isEmpty();
    assertThat(ledger.outcome(unknown)).isEqualTo(Ledger.LOST_UNKNOWN);
    assertThat(checker.check(snapshot(unknown))).isEmpty();
    assertThat(ledger.outcome(unknown)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(checker.lateCommits()).isEqualTo(1);
  }

  @Test
  void partialTransactionsAreI5() {
    final long pairWithoutEdge = op(0, true, Ledger.ACKED);
    final long singleWithEdge = op(0, false, Ledger.ACKED);
    final long pairWithTwoEdges = op(1, true, Ledger.ACKED);
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    snapshot.add(pairWithoutEdge, 0);
    snapshot.add(singleWithEdge, 1);
    snapshot.add(pairWithTwoEdges, 2);
    final List<Violation> violations = checker.check(snapshot);
    assertThat(violations).extracting(Violation::invariant).containsExactly("I5");
    assertThat(violations.getFirst().message()).contains("applied partially").contains("stale read");
    assertThat(violations.getFirst().keys()).containsExactlyInAnyOrder(pairWithTwoEdges, pairWithoutEdge, singleWithEdge);
  }

  @Test
  void operationStillInFlightIsAHarnessError() {
    ledger.reserve(0, false);
    final List<Violation> violations = checker.check(snapshot());
    assertThat(violations).extracting(Violation::invariant).containsExactly("QUIESCE");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.HARNESS);
  }

  @Test
  void reportedKeysAreCappedButTheCountIsNot() {
    for (int i = 0; i < 150; i++)
      op(0, false, Ledger.ACKED);
    final Violation violation = checker.check(snapshot()).getFirst();
    assertThat(violation.keys()).hasSize(InvariantChecker.MAX_KEYS);
    assertThat(violation.message()).startsWith("150 ");
  }

  @Test
  void diffFindsPresenceAndEdgeDifferences() {
    final long a = op(0, false, Ledger.ACKED);
    final long b = op(1, false, Ledger.ACKED);
    assertThat(snapshot(a, b).diff(snapshot(a), 10)).containsExactly(b);
    assertThat(snapshot(a, b).diff(snapshot(a, b), 10)).isEmpty();
    final NodeSnapshot withEdge = new NodeSnapshot(ledger);
    withEdge.add(a, 1);
    withEdge.add(b, 0);
    assertThat(snapshot(a, b).diff(withEdge, 10)).containsExactly(a);
  }
}
