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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("chaos")
class CheckpointIT {
  private final Ledger ledger = new Ledger(1);

  private long acked() {
    final long key = ledger.reserve(0, false);
    ledger.record(key, Ledger.ACKED);
    return key;
  }

  /** Node contents as {key, edges} rows; a node can lag (report stale counts) or be unreadable. */
  private static final class ScriptedNodeReader implements NodeReader {
    final Map<Integer, List<long[]>> rows       = new HashMap<>();
    final Map<Integer, Integer>      lagPolls   = new HashMap<>();
    final Set<Integer>               unreadable = new HashSet<>();
    final Map<Integer, IOException>  scanErrors = new HashMap<>();
    Runnable                         beforeScan;

    @Override
    public long[] counts(final int node) throws IOException {
      if (unreadable.contains(node))
        throw new IOException("node " + node + " down");
      final int lag = lagPolls.getOrDefault(node, 0);
      if (lag > 0) {
        lagPolls.put(node, lag - 1);
        return new long[] { -1, -1 };
      }
      final List<long[]> nodeRows = rows.get(node);
      return new long[] { nodeRows.size(), nodeRows.stream().mapToLong(row -> row[1]).sum() };
    }

    @Override
    public void scan(final int node, final NodeSnapshot sink) throws IOException {
      if (beforeScan != null) {
        final Runnable action = beforeScan;
        beforeScan = null;
        action.run();
      }
      if (scanErrors.containsKey(node))
        throw scanErrors.get(node);
      for (final long[] row : rows.get(node))
        sink.add(row[0], (int) row[1]);
    }
  }

  private Checkpoint checkpoint(final NodeReader reader, final Duration timeout) {
    return new Checkpoint(reader, ledger, new InvariantChecker(ledger), 3, timeout, Duration.ofMillis(20));
  }

  @Test
  void identicalNodesPass() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final Checkpoint.Result result = checkpoint(reader, Duration.ofSeconds(5)).run();
    assertThat(result.violations()).isEmpty();
    assertThat(result.counts()).containsExactly(2, 0, 2, 0, 2, 0);
  }

  @Test
  void laggingNodeConverges() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.lagPolls.put(2, 3);
    assertThat(checkpoint(reader, Duration.ofSeconds(5)).run().violations()).isEmpty();
  }

  @Test
  void neverConvergingIsASafetyViolation() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    reader.rows.put(0, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    reader.rows.put(1, List.of(new long[] { a, 0 }));
    reader.rows.put(2, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofMillis(300)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("CONVERGENCE");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(violations.getFirst().message()).contains("[2, 0, 1, 0, 2, 0]");
  }

  @Test
  void unreadableNodeTimesOutWithTheReadError() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.unreadable.add(1);
    final List<Violation> violations = checkpoint(reader, Duration.ofMillis(300)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("CONVERGENCE");
    assertThat(violations.getFirst().message()).contains("node 1 down");
  }

  @Test
  void sameCountsDifferentKeysIsDivergence() throws InterruptedException {
    final long a = acked();
    final long b = acked();
    final long c = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    reader.rows.put(0, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    reader.rows.put(1, List.of(new long[] { a, 0 }, new long[] { c, 0 }));
    reader.rows.put(2, List.of(new long[] { a, 0 }, new long[] { b, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).contains("DIVERGENCE");
    final Violation divergence = violations.stream().filter(v -> v.invariant().equals("DIVERGENCE")).findFirst().orElseThrow();
    assertThat(divergence.keys()).containsExactlyInAnyOrder(b, c);
  }

  @Test
  void invariantViolationsSurface() throws InterruptedException {
    final long a = acked();
    final long lost = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("I1");
    assertThat(violations.getFirst().keys()).containsExactly(lost);
  }

  @Test
  void serverErrorDuringScanIsASafetyViolation() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.scanErrors.put(1, new HttpNodeReader.ServerErrorException(1, 500, "duplicate key"));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("SCAN_ERROR");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.SAFETY);
    assertThat(violations.getFirst().message()).contains("node 1").contains("500").contains("duplicate key");
  }

  @Test
  void connectionErrorDuringScanIsAnAvailabilityViolation() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, List.of(new long[] { a, 0 }));
    reader.scanErrors.put(2, new IOException("connection reset"));
    final List<Violation> violations = checkpoint(reader, Duration.ofSeconds(5)).run().violations();
    assertThat(violations).extracting(Violation::invariant).containsExactly("SCAN");
    assertThat(violations.getFirst().kind()).isEqualTo(ResultKind.AVAILABILITY);
    assertThat(violations.getFirst().message()).contains("connection reset");
  }

  @Test
  void commitLandingDuringTheScanIsNotDivergence() throws InterruptedException {
    final long a = acked();
    final ScriptedNodeReader reader = new ScriptedNodeReader();
    for (int node = 0; node < 3; node++)
      reader.rows.put(node, new ArrayList<>(List.of(new long[] { a, 0 })));
    final long late = acked();
    // node 0 is scanned before the late commit reaches every node, nodes 1 and 2 after it
    reader.beforeScan = () -> reader.beforeScan = () -> {
      for (int node = 0; node < 3; node++)
        reader.rows.get(node).add(new long[] { late, 0 });
    };
    final Checkpoint.Result result = checkpoint(reader, Duration.ofSeconds(5)).run();
    assertThat(result.violations()).isEmpty();
    assertThat(result.counts()).containsExactly(2, 0, 2, 0, 2, 0);
  }
}
