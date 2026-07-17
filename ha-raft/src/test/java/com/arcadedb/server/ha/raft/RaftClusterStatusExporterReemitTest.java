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

import com.arcadedb.server.ha.raft.RaftClusterStatusExporter.ConfigSnapshot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5304: the leader emitted the CLUSTER CONFIGURATION table exactly once, at
 * the leader-change notification, deduplicated on the hash of the FULL rendered text (including the
 * volatile LAG/LATENCY columns). A membership change committed right after that single emission (e.g. a
 * third node finishing its join 112 ms later during a parallel Kubernetes bootstrap) never re-triggered
 * it, so the log permanently showed a transient 2-of-3 member set - and a follower frozen at
 * {@code STATUS UNKNOWN} / stale lag - while {@code /api/v1/cluster} was correct.
 * <p>
 * The fix re-emits the table from the periodic lag-monitor tick, deduplicated on a STABLE signature
 * (term, member ids, addresses, roles, replica statuses) that excludes the volatile LAG/LATENCY columns
 * and the commit index: the logged view converges within one tick of any committed membership change,
 * role change or replica-status transition, without flooding the log every 5 seconds just because the
 * lag numbers moved. While fewer members than the configured server count are present, the table also
 * carries an explicit "not yet converged" note so a bootstrap-window snapshot is never read as
 * authoritative.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftClusterStatusExporterReemitTest {

  /**
   * Exporter with the data-collection and log-emission seams overridden: snapshots are injected and
   * emissions are captured, so the re-emit decision logic runs exactly as in production.
   */
  private static final class TestExporter extends RaftClusterStatusExporter {
    private ConfigSnapshot     snapshot;
    private final List<String> emitted = new ArrayList<>();

    TestExporter() {
      super(null, null);
    }

    @Override
    ConfigSnapshot collectSnapshot(final boolean leaderOnly) {
      return snapshot;
    }

    @Override
    void emit(final String output) {
      emitted.add(output);
    }
  }

  // Columns since issue #5314: SERVER, ADDRESS, ROLE, LAG, RTT, LAST CONTACT, STATUS. The pre-existing
  // tests below do not exercise RTT (they cover the re-emit logic), so this helper leaves it empty and
  // maps the old "latency" argument to its honest home, the LAST CONTACT column.
  private static String[] row(final String id, final String address, final String role, final String lag,
      final String lastContact, final String status) {
    return row(id, address, role, lag, "", lastContact, status);
  }

  private static String[] row(final String id, final String address, final String role, final String lag,
      final String rtt, final String lastContact, final String status) {
    return new String[] { id, address, role, lag, rtt, lastContact, status };
  }

  private static ConfigSnapshot snapshot(final long term, final long commitIndex, final int configuredServers,
      final String[]... rows) {
    return new ConfigSnapshot(term, commitIndex, List.of(rows), List.of(), configuredServers);
  }

  @Test
  void firstEmissionPrintsFullTable() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 3,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "914", "", "UNKNOWN"));

    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(1);
    final String table = exporter.emitted.get(0);
    assertThat(table).contains("CLUSTER CONFIGURATION (term=20, commitIndex=913)");
    assertThat(table).contains("arcadedb-1");
    assertThat(table).contains("arcadedb-2");
  }

  @Test
  void lagAndLatencyChangesAloneDoNotReemit() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "914", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    // Next lag-monitor tick: same members/roles/statuses, only the volatile numbers moved.
    exporter.snapshot = snapshot(20, 954, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "3 ms", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(1);
  }

  @Test
  void identicalSnapshotDoesNotReemit() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));

    exporter.printClusterConfiguration();
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(1);
  }

  @Test
  void committedMemberJoinReemits() {
    // The issue #5304 timeline: the only emission raced arcadedb-0's join by 112 ms and froze on a
    // 2-of-3 member set forever. With the fix, the tick after the join sees a third row and re-emits.
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 3,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "914", "", "UNKNOWN"));
    exporter.printClusterConfiguration();

    exporter.snapshot = snapshot(20, 954, 3,
        row("arcadedb-0", "arcadedb-0:2424", "Follower", "0", "", "HEALTHY"),
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(2);
    assertThat(exporter.emitted.get(1)).contains("arcadedb-0");
  }

  @Test
  void replicaStatusTransitionReemits() {
    // Second facet reported on the issue: the frozen row showed STATUS UNKNOWN / LAG 914 forever while
    // the follower was actually HEALTHY at lag 0. A status transition must refresh the logged table.
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "914", "", "UNKNOWN"));
    exporter.printClusterConfiguration();

    exporter.snapshot = snapshot(20, 954, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(2);
    assertThat(exporter.emitted.get(1)).contains("HEALTHY");
  }

  @Test
  void roleChangeReemits() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    exporter.snapshot = snapshot(21, 954, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Follower", "0", "", "HEALTHY"),
        row("arcadedb-2", "arcadedb-2:2424", "Leader", "", "", ""));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(2);
  }

  @Test
  void termChangeReemits() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    exporter.snapshot = snapshot(22, 954, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(2);
  }

  @Test
  void nonLeaderEmitsNothing() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = null; // collectSnapshot() returns null on non-leaders

    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).isEmpty();
  }

  @Test
  void convergenceNoteShownWhileConfiguredMembersAreMissing() {
    // While fewer members than arcadedb.ha.serverList are present, the table must say so explicitly
    // instead of presenting a transient bootstrap-window state as authoritative.
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 3,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "914", "", "UNKNOWN"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted.get(0)).contains("2 of 3 configured servers");

    exporter.snapshot = snapshot(20, 954, 3,
        row("arcadedb-0", "arcadedb-0:2424", "Follower", "0", "", "HEALTHY"),
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(2);
    assertThat(exporter.emitted.get(1)).doesNotContain("configured servers");
  }

  @Test
  void headerRenamesLatencyToLastContactAndAddsRtt() {
    // Issue #5314: the old "LATENCY" header actually showed time-since-last-RPC, not the round trip.
    // It is renamed to the honest "LAST CONTACT", and a real "RTT" column is added alongside it.
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "2408 ms", "HEALTHY"));

    exporter.printClusterConfiguration();

    final String table = exporter.emitted.get(0);
    assertThat(table).contains("LAST CONTACT");
    assertThat(table).contains("RTT");
    assertThat(table).doesNotContain("LATENCY");
    // The staleness figure is now under LAST CONTACT, always shown (it was previously suppressed when it
    // exceeded the heartbeat window - exactly when it matters most).
    assertThat(table).contains("2408 ms");
  }

  @Test
  void rttColumnRendersMeasuredRoundTrip() {
    // The RTT column carries the real, sub-millisecond replication round-trip even while LAST CONTACT
    // sits at multiple seconds on an idle cluster - the two are independent quantities (issue #5314).
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "0.65 ms", "2408 ms", "HEALTHY"));

    exporter.printClusterConfiguration();

    final String table = exporter.emitted.get(0);
    assertThat(table).contains("0.65 ms"); // RTT
    assertThat(table).contains("2408 ms"); // LAST CONTACT
  }

  @Test
  void rttChangeAloneDoesNotReemit() {
    // RTT is as volatile as LAG/LAST CONTACT and must be excluded from the stable signature, or the
    // table would re-flood the log on every lag-monitor tick (issue #5304 treatment, issue #5314 scope).
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = snapshot(20, 913, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "0.61 ms", "2400 ms", "HEALTHY"));
    exporter.printClusterConfiguration();

    exporter.snapshot = snapshot(20, 954, 2,
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", "", ""),
        row("arcadedb-2", "arcadedb-2:2424", "Follower", "0", "0.72 ms", "2412 ms", "HEALTHY"));
    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(1);
  }

  @Test
  void bootstrapBaselinesAreRendered() {
    final TestExporter exporter = new TestExporter();
    exporter.snapshot = new ConfigSnapshot(20, 913, List.<String[]>of(
        row("arcadedb-1", "arcadedb-1:2424", "Leader", "", "", "")),
        List.<String[]>of(new String[] { "mydb", "42", "0123abcd...89efcdab" }), 1);

    exporter.printClusterConfiguration();

    assertThat(exporter.emitted).hasSize(1);
    assertThat(exporter.emitted.get(0)).contains("BOOTSTRAP_LAST_TX_ID");
    assertThat(exporter.emitted.get(0)).contains("mydb");
  }
}
