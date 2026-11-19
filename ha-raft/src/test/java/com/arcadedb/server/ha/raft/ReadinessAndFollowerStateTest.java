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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * What a node publishes about readiness and about its followers: degraded followers, unreadable divisions, halt and log failures,
 * the notify loop wiring and the replication path state.
 */
class ReadinessAndFollowerStateTest {

  /**
   * Regression test for issue #7135: {@code getLivePeers()} and {@code isReadyForTraffic()} caught only
   * {@link IOException}, while Ratis throws an {@link IllegalStateException}
   * ("stateMachineUpdater is uninitialized") for the whole window in which an in-place division restart
   * re-initializes (issue #5271). Both methods therefore propagated an unchecked exception instead of
   * degrading to the fallback their javadoc promises.
   * <p>
   * That window is opened by the health monitor's {@code restartRatisIfNeeded()} on a CLOSED division, i.e.
   * exactly during the pod churn the readers are supposed to survive. After the #7040 fix
   * {@code configuredPeers()} - and with it {@code getStats()}, {@code getReplicaAddresses()} and the
   * Bolt/gRPC routing table - all read {@code getLivePeers()}, so one unguarded read 500s
   * {@code /api/v1/cluster} and aborts the health tick that is driving the recovery.

   */
  @Nested
  class UnreadableDivisionDegrades {

    private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";

    /** The exact exception Ratis raises while a division is being re-initialized in place. */
    private static IllegalStateException ratisRestartWindow() {
      return new IllegalStateException("stateMachineUpdater is uninitialized");
    }

    // Regression for #7135: UnreadableDivisionDegrades
    @Test
    void livePeersFallsBackToTheDeclaredListWhenTheDivisionThrowsUnchecked() throws Exception {
      final RaftHAServer raft = detachedServerWithUnreadableDivision();

      assertThat(raft.getLivePeers())
          .as("an unreadable division must degrade to the declared server list, not propagate")
          .extracting(peer -> ((RaftPeer) peer).getId().toString())
          .hasSize(3);
    }

    /**
     * The distinction {@code getCommittedPeersOrNull} exists for (issue #7136) must survive the widened catch:
     * "cannot read" is still no information, never the declared list dressed up as a committed membership.
     */
    // Regression for #7135: UnreadableDivisionDegrades
    @Test
    void committedPeersStillReportsNoInformationRatherThanTheDeclaredList() throws Exception {
      final RaftHAServer raft = detachedServerWithUnreadableDivision();

      assertThat(raft.getCommittedPeersOrNull()).isNull();
    }

    // Regression for #7135: UnreadableDivisionDegrades
    @Test
    void readinessFailsClosedWhenTheDivisionThrowsUnchecked() throws Exception {
      final RaftHAServer raft = detachedServerWithUnreadableDivision();

      assertThat(raft.isReadyForTraffic(10L))
          .as("the probe must answer NOT_READY, not 500")
          .isFalse();
    }

    /**
     * The blast radius #7135 is really about: every membership view behind {@code /api/v1/cluster} and the
     * client routing tables reads {@code getLivePeers()} through {@code configuredPeers()} since #7040.
     */
    // Regression for #7135: UnreadableDivisionDegrades
    @Test
    void theClusterStatusPayloadIsStillBuiltDuringTheRestartWindow() throws Exception {
      final RaftHAServer raft = detachedServerWithUnreadableDivision();

      assertThat(raft.getStats()).containsKey("replicas");
      assertThat(raft.getReplicaAddresses()).isNotNull();
    }

    /**
     * A {@link RaftHAServer} built from the static server list, with a Ratis server whose division cannot be
     * resolved because it is mid-restart. Ratis is never started; the mock stands in for the exact throw the
     * three hardened sibling readers ({@code isLeader}, {@code getLeaderId}, {@code getLastAppliedIndex})
     * already document and guard.
     */
    private static RaftHAServer detachedServerWithUnreadableDivision() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");

      final RaftHAServer raft = new RaftHAServer(arcadeServer, config);

      final RaftServer ratis = mock(RaftServer.class);
      when(ratis.getDivision(any())).thenThrow(ratisRestartWindow());

      final Field field = RaftHAServer.class.getDeclaredField("raftServer");
      field.setAccessible(true);
      field.set(raft, ratis);

      return raft;
    }
  }

  /**
   * Regression test for issue #7872: two readiness inputs the #7136 invariant never published.
   * <p>
   * #7136 wrote the invariant into both the handler and the OpenAPI contract - <em>anything that makes
   * {@code /api/v1/ready} answer 503 is visible in {@code GET /api/v1/cluster}</em> - and then delivered it for the
   * resync / WAL-gap inputs only. Two were left:
   * <ol>
   *   <li>{@code haltedAfterCriticalError}, the node-wide halt {@code triggerCriticalHalt()} trips, checked before
   *       every other readiness gate;</li>
   *   <li>the persistent Raft log-writer failure ({@code getRaftLogFailure()}), the #7118 gate, checked earlier
   *       still - before the {@code readinessRequiresHA} switch.</li>
   * </ol>
   * On a node in either state the status document answered {@code 200} with {@code alerts: []},
   * {@code raftState: RUNNING} and {@code localResync.inProgress: false} while the probe was pinned at 503. A
   * monitoring rule or rolling-upgrade controller built on the documented invariant read a perfectly healthy node
   * and waited forever, and the one place the halt was recorded was a SEVERE log line.
   * <p>
   * The halt is the worse of the two because its own remediation can fail silently: {@code triggerCriticalHalt()}
   * sets the flag and runs {@code server.stop()} on a DAEMON thread whose only failure handling is a log line. If
   * that stop does not complete, the process stays up and the HTTP endpoints keep answering - with nothing
   * machine-readable anywhere to say the state machine is dead.

   */
  @Nested
  class HaltAndLogFailureArePublished {

    private static JSONObject alertWithId(final JSONArray alerts, final String id) {
      for (int i = 0; i < alerts.length(); i++) {
        final JSONObject alert = alerts.getJSONObject(i);
        if (id.equals(alert.getString("id", null)))
          return alert;
      }
      return null;
    }

    /**
     * The halt alert carries the index and the reason, because that is the difference between "upgrade this node"
     * and "file a bug" - and neither is "wait", which is what the generic readiness message used to say.
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void aHaltedStateMachineRaisesAnAlertNamingWhatTrippedIt() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addCriticalHaltAlert(
          new ArcadeStateMachine.CriticalHalt(4711L, "unknown Raft log entry type", 1_700_000_000_000L), true, alerts);

      final JSONObject halt = alertWithId(alerts, "halted-after-critical-error");
      assertThat(halt).as("the condition the #7136 invariant promised would be visible here").isNotNull();
      assertThat(halt.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(halt.getJSONObject("details").getLong("index")).isEqualTo(4711L);
      assertThat(halt.getJSONObject("details").getString("reason")).contains("unknown Raft log entry type");
      assertThat(halt.getJSONObject("details").getLong("timestamp")).isEqualTo(1_700_000_000_000L);
      assertThat(halt.getString("recommendation"))
          .as("the halt does not clear in place, so the remedy is the restart and not waiting")
          .contains("Restart this node");
    }

    /** And a healthy state machine raises nothing, which is what keeps the assertion above meaningful. */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void anApplyingStateMachineRaisesNoHaltAlert() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addCriticalHaltAlert(null, true, alerts);
      assertThat(alerts).isEmpty();
    }

    /**
     * The log-writer failure, published for the first time here although the #7118 readiness gate has been reading
     * it since. Unlike the halt it IS recoverable in place, so the recommendation is about the storage volume
     * rather than about the process - the two conditions are not interchangeable and their alerts must not read as
     * though they were.
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void aFailedLogWriterRaisesAnAlertAboutTheVolume() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addRaftLogFailureAlert(
          new ArcadeStateMachine.RaftLogFailure(99L, "java.io.IOException: No space left on device", 1_700_000_000_001L),
          true, alerts);

      final JSONObject failure = alertWithId(alerts, "raft-log-writer-failed");
      assertThat(failure).isNotNull();
      assertThat(failure.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(failure.getJSONObject("details").getLong("index")).isEqualTo(99L);
      assertThat(failure.getJSONObject("details").getString("cause")).contains("No space left on device");
      assertThat(failure.getString("recommendation")).contains("Free space on the Raft storage volume");
    }

    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void aHealthyLogWriterRaisesNoAlert() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addRaftLogFailureAlert(null, true, alerts);
      assertThat(alerts).isEmpty();
    }

    /**
     * The liveness counterpart (issue #7622), equally invisible here until now. It is what fails
     * {@code /api/v1/health}, and a deployment without a liveness probe had nothing but a SEVERE line to read.
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void anEscalatedCrashLoopRaisesAnAlert() {
      final JSONArray raised = new JSONArray();
      ClusterAlerts.addCrashLoopEscalatedAlert(true, raised);
      assertThat(alertWithId(raised, "crash-loop-escalated")).isNotNull();

      final JSONArray quiet = new JSONArray();
      ClusterAlerts.addCrashLoopEscalatedAlert(false, quiet);
      assertThat(quiet).isEmpty();
    }

    /**
     * None of the three is scoped by the authorization filter, and that is the point rather than an oversight: a
     * halted state machine applies nothing for any database, a failed log writer appends nothing for any database,
     * and an escalated crash loop is about the node. There is no tenant for whom any of them is untrue, and no
     * database name in any payload to scope. Same reasoning as {@code localResync.inProgress}.
     * <p>
     * Driven through the real {@code scan} rather than the builders, so this asserts what the endpoint answers.
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void theThreeNodeLevelConditionsAreNotSuppressedForAScopedCaller() {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();

      // A caller authorized on no database at all: the strictest filter the endpoint can build.
      final JSONArray alerts = ClusterAlerts.scan(server, null, List.of(), Set.of(), null, null, null,
          new ClusterAlerts.NodeStatus(null, null, true, false), false);

      assertThat(alertWithId(alerts, "crash-loop-escalated"))
          .as("whether this node's HA layer has given up is not a per-tenant fact")
          .isNotNull();
    }

    /**
     * And the contract declares all three, so a client generated from it can read them. The invariant lives in the
     * published document, not only in the handler, and #7872 is the second time a member reached the response
     * without reaching the document (#7741 was the first).
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void theApiSpecDeclaresBothFlagsAndRequiresThem() {
      final OpenAPI openAPI = new OpenAPI();
      openAPI.setPaths(new Paths());
      openAPI.setComponents(new Components());
      final OpenApiContributor contributor = new PluginApiSpec();
      contributor.contribute(openAPI);
      final Schema<?> status = openAPI.getComponents().getSchemas().get("ClusterStatus");

      for (final String member : List.of("criticalHalt", "raftLogFailure", "crashLoopEscalated")) {
        assertThat(status.getProperties()).as(member).containsKey(member);
        assertThat(status.getRequired()).as(member + " is written on every answer").contains(member);
      }

      // The members, asserted against what the handler actually writes rather than against a second hand-written
      // list: PluginApiSpec lives in arcadedb-server and cannot see GetClusterHandler, so this module is the only
      // place where the document and the code that produces it are both on the classpath.
      final Schema<?> halt = (Schema<?>) status.getProperties().get("criticalHalt");
      assertThat(halt.getNullable()).as("null is how a healthy node reports it").isTrue();
      assertThat(halt.getProperties().keySet()).containsExactlyInAnyOrderElementsOf(
          ((JSONObject) GetClusterHandler.buildCriticalHalt(
              new ArcadeStateMachine.CriticalHalt(1L, "boom", 2L), true)).keySet());
      assertThat(halt.getRequired())
          .as("built in one expression, so a halt that is reported is reported whole")
          .containsExactlyInAnyOrderElementsOf(halt.getProperties().keySet());

      final Schema<?> logFailure = (Schema<?>) status.getProperties().get("raftLogFailure");
      assertThat(logFailure.getNullable()).isTrue();
      assertThat(logFailure.getProperties().keySet()).containsExactlyInAnyOrderElementsOf(
          ((JSONObject) GetClusterHandler.buildRaftLogFailure(
              new ArcadeStateMachine.RaftLogFailure(1L, "boom", 2L), true)).keySet());
      assertThat(logFailure.getRequired())
          .containsExactlyInAnyOrderElementsOf(logFailure.getProperties().keySet());
    }

    /** A healthy node writes an explicit null for both, so a client can tell "healthy" from "not reported". */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void aHealthyNodeWritesAnExplicitNullForBoth() {
      assertThat(GetClusterHandler.buildCriticalHalt(null, true)).isEqualTo(JSONObject.NULL);
      assertThat(GetClusterHandler.buildRaftLogFailure(null, true)).isEqualTo(JSONObject.NULL);
    }

    /**
     * A non-root caller learns the CONDITION but not the raw text behind it (review on PR #7953).
     * <p>
     * Neither string is composed by this node: the log failure carries Ratis's own cause, which routinely names the
     * Raft storage path, and the halt carries an arbitrary {@code Throwable.toString()} from the apply, which can
     * name the database that was being applied. {@code GET /api/v1/cluster} is not root-gated outside its
     * {@code ?presence=true} branch, so publishing either unreduced would hand a tenant a filesystem path, or
     * another tenant's database name - exactly what the {@code visible()} machinery on this endpoint exists to
     * prevent.
     * <p>
     * What is NOT reduced is the index, the timestamp and the fact that the field is non-null. That is the half the
     * #7136 invariant owes every caller and the only half a tenant can act on; a field that went absent instead
     * would read as "this build does not report it".
     */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void aNonRootCallerSeesTheConditionButNotTheRawExceptionText() {
      final JSONObject halt = (JSONObject) GetClusterHandler.buildCriticalHalt(
          new ArcadeStateMachine.CriticalHalt(4711L, "NPE applying an entry for database 'other-tenant'", 7L), false);
      assertThat(halt.getString("reason")).isEqualTo(GetClusterHandler.REDACTED_REASON);
      assertThat(halt.getString("reason")).doesNotContain("other-tenant");
      assertThat(halt.getLong("index")).as("the index is not sensitive and is what a client correlates on")
          .isEqualTo(4711L);
      assertThat(halt.getLong("timestamp")).isEqualTo(7L);

      final JSONObject failure = (JSONObject) GetClusterHandler.buildRaftLogFailure(
          new ArcadeStateMachine.RaftLogFailure(99L, "java.io.IOException: No space left: /srv/data/.raft", 8L), false);
      assertThat(failure.getString("cause")).isEqualTo(GetClusterHandler.REDACTED_REASON);
      assertThat(failure.getString("cause")).doesNotContain("/srv/data");
      assertThat(failure.getLong("index")).isEqualTo(99L);
    }

    /** And the alerts built from the same pair are reduced identically - they carry the text in their details. */
    // Regression for #7872: HaltAndLogFailureArePublished
    @Test
    void theAlertsAreReducedForANonRootCallerToo() {
      final JSONArray alerts = new JSONArray();
      ClusterAlerts.addCriticalHaltAlert(
          new ArcadeStateMachine.CriticalHalt(4711L, "NPE applying an entry for database 'other-tenant'", 7L), false,
          alerts);
      ClusterAlerts.addRaftLogFailureAlert(
          new ArcadeStateMachine.RaftLogFailure(99L, "java.io.IOException: No space left: /srv/data/.raft", 8L), false,
          alerts);

      final JSONObject halt = alertWithId(alerts, "halted-after-critical-error");
      assertThat(halt).as("the alert still fires: the condition is not the secret").isNotNull();
      assertThat(halt.getJSONObject("details").getString("reason")).isEqualTo(GetClusterHandler.REDACTED_REASON);
      assertThat(halt.toString()).as("and the text must not survive anywhere else in the payload, message included")
          .doesNotContain("other-tenant");

      final JSONObject failure = alertWithId(alerts, "raft-log-writer-failed");
      assertThat(failure.getJSONObject("details").getString("cause")).isEqualTo(GetClusterHandler.REDACTED_REASON);
      assertThat(failure.toString()).doesNotContain("/srv/data");
    }
  }

  /**
   * Regression test for issue #7041 (follow-up to #4842).
   * <p>
   * When membership changes while the leader reads its follower indices, {@link RaftHAServer#getFollowerStates()}
   * degrades to entries WITHOUT a {@code matchIndex} rather than attributing an index to the wrong peer. The
   * exporter unboxed that key with a raw {@code (Long)} cast at two sites, both inside catch-alls, so the
   * {@code NullPointerException} never surfaced: the CLUSTER CONFIGURATION table was silently suppressed and the
   * lag-monitor tick aborted before the remaining followers were classified - exactly while membership churned.
   * <p>
   * Drives the exporter against a mocked {@link RaftHAServer} that answers with one degraded and one complete
   * follower entry, the shape {@link ReplicationStatistics#degradedFollowerStates} produces.

   */
  @Nested
  class DegradedFollowerState {

    private static final RaftPeerId LEADER   = RaftPeerId.valueOf("leader");
    private static final RaftPeerId DEGRADED = RaftPeerId.valueOf("degraded");
    private static final RaftPeerId HEALTHY  = RaftPeerId.valueOf("healthy");

    /** Exporter with the log-emission seam captured; data collection runs exactly as in production. */
    private static final class CapturingExporter extends RaftClusterStatusExporter {
      private final List<String> emitted = new ArrayList<>();

      CapturingExporter(final RaftHAServer haServer, final ClusterMonitor clusterMonitor) {
        super(haServer, clusterMonitor);
      }

      @Override
      void emit(final String output) {
        emitted.add(output);
      }
    }

    private FakeRaftHAServer haServer;
    private ClusterMonitor   clusterMonitor;

    @BeforeEach
    void setUp() {
      haServer = FakeRaftHAServer.detached();
      clusterMonitor = new ClusterMonitor(10L);

      haServer.leader(true);
      haServer.leaderId(LEADER);
      haServer.currentTerm(7L);
      haServer.commitIndex(100L);
      haServer.configuredServers(3);
      haServer.replicationLatencies(Map.of());
      haServer.noStateMachine();
      haServer.livePeers(List.of(peer(LEADER), peer(DEGRADED), peer(HEALTHY)));

      // The degraded entry comes first so the old raw cast aborted the loop BEFORE the healthy peer was read.
      haServer.followerStates(List.of(degradedState(DEGRADED, 12L), completeState(HEALTHY, 100L, 3L)));
    }

    // Regression for #7041: DegradedFollowerState
    @Test
    void degradedFollowerKeepsItsRowWithUnknownLagInsteadOfSuppressingTheTable() {
      final CapturingExporter exporter = new CapturingExporter(haServer, clusterMonitor);

      exporter.printClusterConfiguration();

      assertThat(exporter.emitted).as("the table must be emitted even though one entry has no match index").hasSize(1);
      final String table = exporter.emitted.get(0);
      assertThat(table).contains("CLUSTER CONFIGURATION (term=7, commitIndex=100)");
      assertThat(table).contains("degraded");
      assertThat(table).contains("healthy");

      final String degradedRow = rowOf(table, "degraded");
      assertThat(degradedRow).as("lag is reported as unknown, not as a number Ratis never gave").contains("| " + RaftClusterStatusExporter.LAG_UNKNOWN + " ");
      assertThat(degradedRow).as("last contact is still known for a degraded entry").contains("12 ms");

      final String healthyRow = rowOf(table, "healthy");
      assertThat(healthyRow).contains("| 0 ").contains("3 ms");
    }

    // Regression for #7041: DegradedFollowerState
    @Test
    void lagTickSkipsTheDegradedFollowerAndStillClassifiesTheOthers() {
      final CapturingExporter exporter = new CapturingExporter(haServer, clusterMonitor);

      exporter.checkReplicaLag();

      assertThat(clusterMonitor.getReplicaStatus(HEALTHY.toString()))
          .as("the tick must reach the followers listed after the degraded one")
          .isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
      assertThat(clusterMonitor.getReplicaStatus(DEGRADED.toString()))
          .as("a peer whose index is unknown keeps its previous classification rather than one derived from a placeholder")
          .isEqualTo(ClusterMonitor.ReplicaStatus.UNKNOWN);
      assertThat(exporter.emitted).as("the tick re-emits the table it used to lose").hasSize(1);
    }

    // Regression for #7041: DegradedFollowerState
    @Test
    void followerStateIndexReadsAbsentKeysAsUnknown() {
      final Map<String, Object> degraded = degradedState(DEGRADED, 5L);
      assertThat(ReplicationStatistics.hasFollowerStateIndex(degraded, "matchIndex")).isFalse();
      assertThat(ReplicationStatistics.followerStateIndex(degraded, "matchIndex")).isEqualTo(-1L);
      assertThat(ReplicationStatistics.followerStateIndex(degraded, "lastRpcElapsedMs")).isEqualTo(5L);

      final Map<String, Object> complete = completeState(HEALTHY, 42L, 1L);
      assertThat(ReplicationStatistics.hasFollowerStateIndex(complete, "matchIndex")).isTrue();
      assertThat(ReplicationStatistics.followerStateIndex(complete, "matchIndex")).isEqualTo(42L);
    }

    private static RaftPeer peer(final RaftPeerId id) {
      return RaftPeer.newBuilder().setId(id).setAddress(id + ":2434").build();
    }

    /** Same shape as {@link ReplicationStatistics#degradedFollowerStates}: peer id and last-RPC elapsed only. */
    private static Map<String, Object> degradedState(final RaftPeerId id, final long lastRpcElapsedMs) {
      final Map<String, Object> state = new LinkedHashMap<>();
      state.put("peerId", id.toString());
      state.put("lastRpcElapsedMs", lastRpcElapsedMs);
      return state;
    }

    private static Map<String, Object> completeState(final RaftPeerId id, final long matchIndex, final long lastRpcElapsedMs) {
      final Map<String, Object> state = degradedState(id, lastRpcElapsedMs);
      state.put("matchIndex", matchIndex);
      state.put("nextIndex", matchIndex + 1);
      return state;
    }

    private static String rowOf(final String table, final String peerId) {
      for (final String line : table.split("\n"))
        if (line.startsWith("| " + peerId + " "))
          return line;
      throw new AssertionError("No row for '" + peerId + "' in:\n" + table);
    }
  }

  /**
   * Issue #8457: the lag tick must actually forward the follower's {@code nextIndex} and the leader's own log start
   * from {@link RaftHAServer} to {@link ClusterMonitor}. {@link ClusterMonitorInstallSnapshotLoopTest} drives the
   * monitor directly; this drives {@link RaftClusterStatusExporter#checkReplicaLag()} against a mocked server.
   */
  @Nested
  class NotifyLoopExporterWiring {

    private static final RaftPeerId LEADER = RaftPeerId.valueOf("leader");
    private static final RaftPeerId STUCK  = RaftPeerId.valueOf("stuck");

    private static final class QuietExporter extends RaftClusterStatusExporter {
      QuietExporter(final RaftHAServer haServer, final ClusterMonitor clusterMonitor) {
        super(haServer, clusterMonitor);
      }

      @Override
      void emit(final String output) {
      }
    }

    private final AtomicLong       now = new AtomicLong(0);
    private       FakeRaftHAServer haServer;
    private       ClusterMonitor   monitor;

    @BeforeEach
    void setUp() {
      haServer = FakeRaftHAServer.detached();
      monitor = new ClusterMonitor(1000L, 60_000L, id -> {
      });
      monitor.setClock(now::get);

      haServer.leader(true);
      haServer.leaderId(LEADER);
      haServer.currentTerm(3L);
      haServer.commitIndex(105L);
      haServer.configuredServers(2);
      haServer.replicationLatencies(Map.of());
      haServer.noStateMachine();
      haServer.livePeers(List.of(peer(LEADER), peer(STUCK)));
    }

    // Regression for #8457: NotifyLoopExporterWiring
    @Test
    void lagTickForwardsNextIndexAndLogStartSoTheLoopIsDetected() {
      haServer.raftLogStartIndex(100L);
      haServer.followerStates(List.of(state(true)));

      tickAt(0);
      tickAt(60_000);

      assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.STALLED);
    }

    // Regression for #8457: NotifyLoopExporterWiring
    @Test
    void unknownLeaderLogStartLeavesTheReplicaHealthy() {
      haServer.raftLogStartIndex(-1L);
      haServer.followerStates(List.of(state(true)));

      tickAt(0);
      tickAt(60_000);

      assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
    }

    // Regression for #8457: NotifyLoopExporterWiring
    @Test
    void entryWithoutNextIndexLeavesTheReplicaHealthy() {
      haServer.raftLogStartIndex(100L);
      haServer.followerStates(List.of(state(false)));

      tickAt(0);
      tickAt(60_000);

      assertThat(monitor.getReplicaStatus(STUCK.toString())).isEqualTo(ClusterMonitor.ReplicaStatus.HEALTHY);
    }

    private void tickAt(final long timeMs) {
      now.set(timeMs);
      new QuietExporter(haServer, monitor).checkReplicaLag();
    }

    /** matchIndex 99, lag 6 (under the 1000 threshold), nextIndex 100 at the leader's log start. */
    private static Map<String, Object> state(final boolean withNextIndex) {
      final Map<String, Object> s = new LinkedHashMap<>();
      s.put("peerId", STUCK.toString());
      s.put("matchIndex", 99L);
      if (withNextIndex)
        s.put("nextIndex", 100L);
      s.put("lastRpcElapsedMs", 1L);
      return s;
    }

    private static RaftPeer peer(final RaftPeerId id) {
      return RaftPeer.newBuilder().setId(id).setAddress(id + ":2434").build();
    }
  }

  /**
   * Issue #9399, which consolidates #9013 and #8953: the state of a follower's replication path after an in-place Ratis
   * restart, as machine-readable fields of {@code GET /api/v1/cluster}.
   * <ul>
   *   <li>#9013: the {@code follower-stuck-at-stale-term} alert could not tell a node the health monitor holds back from
   *   reformatting (its replication path is unproven since an in-place restart, so it needs a restart by hand) from one
   *   that self-heals. The alert now carries {@code details.replicationPathUnproven} and a recommendation keyed on it,
   *   and the document carries {@code localReplicationPathUnproven}.</li>
   *   <li>#8953: a follower the leader cannot reach after an in-place restart reported {@code raftState=RUNNING} and
   *   nothing else. The health monitor now tracks it ({@link RaftHAServer#trackLeaderReachSinceRestart()}) and the
   *   document carries {@code localLeaderUnreachableSinceRestart}, with its own alert.</li>
   * </ul>
   */
  @Nested
  class ReplicationPathState {

    private static final String SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
    private static final long   TERM        = 3L;
    private static final LeaderReachSinceRestartTracker.Unreachable KNOWN =
        new LeaderReachSinceRestartTracker.Unreachable(25_000L, true);

    // ---- #8953: the rule, on the predicate and the tracker alone ----------------------------------------------------

    // Regression for #9399: ReplicationPathState
    @Test
    void theLeaderIsNotReachingOnlyWhileThePathIsUnprovenAndThereIsEvidence() {
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(false, false, false, 5_000L, 100L))
          .as("a proven path, or no in-place restart at all, is never reported").isFalse();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, true, false, 5_000L, 100L))
          .as("a leader has nobody to be reached by").isFalse();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, false, -1L, 100L))
          .as("no leader has made itself known since the restart").isTrue();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 5_000L, 100L))
          .as("the leader reports entries this node does not hold, and none arrived").isTrue();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 100L, 100L))
          .as("an idle leader with nothing new to send gives no evidence either way").isFalse();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, -1L, 100L))
          .as("no leader commit index learned yet").isFalse();
      assertThat(LeaderReachSinceRestartTracker.leaderNotReaching(true, false, true, 5_000L, -1L))
          .as("an unreadable local position").isFalse();
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void theTrackerReportsOnlyOnceTheConditionOutlastsTheGrace() {
      final LeaderReachSinceRestartTracker tracker = new LeaderReachSinceRestartTracker();
      tracker.observe(1_000L, true, true, 10_000L);
      assertThat(ms(tracker)).isEqualTo(-1L);
      tracker.observe(10_999L, true, true, 10_000L);
      assertThat(ms(tracker)).as("not before the grace").isEqualTo(-1L);
      tracker.observe(11_000L, true, true, 10_000L);
      assertThat(ms(tracker)).isEqualTo(10_000L);

      tracker.observe(12_000L, false, true, 10_000L);
      assertThat(ms(tracker)).as("one tick without the condition clears it").isEqualTo(-1L);
      tracker.observe(13_000L, true, true, 10_000L);
      assertThat(ms(tracker)).as("and the next spell gets its own grace").isEqualTo(-1L);
    }

    // ---- #8953: end to end through RaftHAServer's health hook --------------------------------------------------------

    // Regression for #9399: ReplicationPathState
    @Test
    void aFollowerTheLeaderDoesNotReachAfterAnInPlaceRestartIsReportedAfterTheGrace() throws Exception {
      final Fixture f = new Fixture();
      f.restartInPlace();

      f.tick(0L); // the first probe runs after the hook: no leader commit index known yet
      f.tick(3_000L); // the leader reports 5000, past the 100 this node holds: the spell starts
      f.tick(3_000L + f.grace - 1);
      assertThat(unreachableMs(f.raft)).as("not before the grace").isEqualTo(-1L);

      f.tick(3_000L + f.grace);
      assertThat(unreachableMs(f.raft)).isEqualTo(f.grace);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void aFollowerWithNoLeaderKnownSinceAnInPlaceRestartIsReported() throws Exception {
      final Fixture f = new Fixture();
      when(f.info.getLeaderId()).thenReturn(null);
      f.restartInPlace();

      f.tick(0L);
      f.tick(f.grace);
      assertThat(unreachableMs(f.raft)).isEqualTo(f.grace);
      assertThat(f.raft.getLeaderUnreachableSinceRestart().leaderKnown())
          .as("reported as the weaker arm, which a cluster without a quorum shows too").isFalse();
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void aLeaderMakingItselfKnownWithNothingNewToSendClearsTheLeaderlessReport() throws Exception {
      // The quorum-loss shape: leaderless after the restart, then a leader appears whose commit index this node holds.
      final Fixture f = new Fixture();
      f.leaderCommit.set(100L);
      when(f.info.getLeaderId()).thenReturn(null);
      f.restartInPlace();
      f.tick(0L);
      f.tick(f.grace);
      assertThat(unreachableMs(f.raft)).as("precondition: reported").isEqualTo(f.grace);

      when(f.info.getLeaderId()).thenReturn(RaftPeerId.valueOf("leader"));
      f.tick(f.grace + 3_000L); // the leader is known; its commit index is learned after this hook
      f.tick(f.grace + 6_000L);

      assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).as("no entry yet, same term").isTrue();
      assertThat(unreachableMs(f.raft)).as("but a known leader with nothing new to send is no evidence").isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void anIdleClusterDoesNotReadAsAnUnreachableLeader() throws Exception {
      // The case that rules out reporting "unproven" on its own: a healthy leader with nothing to send.
      final Fixture f = new Fixture();
      f.leaderCommit.set(100L);
      f.restartInPlace();

      for (long now = 0L; now <= 3 * f.grace; now += 3_000L)
        f.tick(now);

      assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).as("the path is still unproven").isTrue();
      assertThat(unreachableMs(f.raft)).as("but nothing says the leader is not reaching it")
          .isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void anEntryArrivingClearsTheReport() throws Exception {
      final Fixture f = reported();
      when(f.log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(TERM, 101L));
      f.tick(2 * f.grace);
      assertThat(f.raft.isReplicationPathUnprovenSinceRestart()).isFalse();
      assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void aNodeNeverRestartedInPlaceIsNeverReported() throws Exception {
      final Fixture f = new Fixture();
      for (long now = 0L; now <= 3 * f.grace; now += 3_000L)
        f.tick(now);
      assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void aResyncInFlightIsLeftToItsOwnAlert() throws Exception {
      final Fixture f = reported();
      f.stateMachine.resyncInProgress(true);
      f.tick(2 * f.grace);
      assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void aClosedDivisionIsLeftToRaftState() throws Exception {
      final Fixture f = reported();
      when(f.info.getLifeCycleState()).thenReturn(LifeCycle.State.CLOSED);
      f.tick(2 * f.grace);
      assertThat(unreachableMs(f.raft)).isEqualTo(-1L);
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void theHealthTickTracksTheLeaderReachEvenOnATickThatReturnsEarly() {
      final AtomicInteger tracked = new AtomicInteger();
      final HealthMonitor.HealthTarget target = new HealthMonitor.HealthTarget() {
        @Override
        public LifeCycle.State getRaftLifeCycleState() {
          return LifeCycle.State.CLOSED;
        }

        @Override
        public boolean isShutdownRequested() {
          return false;
        }

        @Override
        public void restartRatisIfNeeded() {
        }

        @Override
        public void trackLeaderReachSinceRestart() {
          tracked.incrementAndGet();
        }
      };
      final HealthMonitor monitor = new HealthMonitor(target, 0);

      monitor.tick();
      monitor.tick();

      assertThat(tracked.get()).isEqualTo(2);
    }

    // ---- the alerts --------------------------------------------------------------------------------------------------

    // Regression for #9399: ReplicationPathState
    @Test
    void theStaleTermAlertSaysWhetherTheReformatIsHeld() {
      final JSONObject held = alertById(scan(true, null, true, null), "follower-stuck-at-stale-term");
      assertThat(held).isNotNull();
      assertThat(held.getJSONObject("details").getBoolean("stuckAtStaleTerm")).isTrue();
      assertThat(held.getJSONObject("details").getBoolean("replicationPathUnproven")).isTrue();
      assertThat(held.getString("recommendation")).startsWith("Restart this node by hand");

      final JSONObject selfHealing = alertById(scan(true, null, false, null), "follower-stuck-at-stale-term");
      assertThat(selfHealing).isNotNull();
      assertThat(selfHealing.getJSONObject("details").getBoolean("replicationPathUnproven")).isFalse();
      assertThat(selfHealing.getString("recommendation")).contains("this self-heals");
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void anUnreachableLeaderRaisesACriticalAlertWithItsDuration() {
      final JSONArray alerts = scan(false, null, true, KNOWN);
      final JSONObject alert = alertById(alerts, "follower-leader-unreachable-since-restart");
      assertThat(alert).isNotNull();
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
      assertThat(alert.getString("message")).contains("25s");
      assertThat(alert.getJSONObject("details").getBoolean("leaderUnreachableSinceRestart")).isTrue();
      assertThat(alert.getJSONObject("details").getLong("unreachableForMs")).isEqualTo(25_000L);
      assertThat(alert.getJSONObject("details").getBoolean("leaderKnown")).isTrue();
    }

    /**
     * No leader known is also what every node of a cluster without a quorum sees, so that arm is a warning that says so,
     * not a critical alert on every node of every quorum loss (review on PR #9451).
     */
    // Regression for #9399: ReplicationPathState
    @Test
    void noLeaderKnownIsOnlyAWarningThatPointsAtTheQuorumFirst() {
      final JSONObject alert = alertById(scan(false, null, true, new LeaderReachSinceRestartTracker.Unreachable(25_000L, false)),
          "follower-leader-unreachable-since-restart");
      assertThat(alert).isNotNull();
      assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
      assertThat(alert.getString("message")).contains("no leader has made itself known");
      assertThat(alert.getString("recommendation")).startsWith("First check whether any node of the cluster has a leader");
      assertThat(alert.getJSONObject("details").getBoolean("leaderKnown")).isFalse();
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void oneAlertPerConditionMostSpecificCauseFirst() {
      final FollowerStallTracker.Stall stall = new FollowerStallTracker.Stall(5_000L, 100L, 4_900L, 12_000L);

      final JSONArray unreachable = scan(false, stall, true, KNOWN);
      assertThat(alertById(unreachable, "follower-leader-unreachable-since-restart")).isNotNull();
      assertThat(alertById(unreachable, "follower-stalled-behind-leader"))
          .as("the stall is the unreachable leader's symptom").isNull();

      final JSONArray stuck = scan(true, stall, true, KNOWN);
      assertThat(alertById(stuck, "follower-stuck-at-stale-term")).isNotNull();
      assertThat(alertById(stuck, "follower-leader-unreachable-since-restart")).isNull();
      assertThat(alertById(stuck, "follower-stalled-behind-leader")).isNull();

      final JSONArray stalledOnly = scan(false, stall, false, null);
      assertThat(alertById(stalledOnly, "follower-stalled-behind-leader")).isNotNull();
    }

    // Regression for #9399: ReplicationPathState
    @Test
    void noUnreachableLeaderNoAlert() {
      assertThat(alertById(scan(false, null, true, null), "follower-leader-unreachable-since-restart")).isNull();
    }

    // ---- helpers -----------------------------------------------------------------------------------------------------

    private static JSONArray scan(final boolean stuck, final FollowerStallTracker.Stall stall, final boolean unproven,
        final LeaderReachSinceRestartTracker.Unreachable unreachable) {
      final FakeArcadeDBServer server = FakeArcadeDBServer.create();
      return ClusterAlerts.scan(server, null, List.of(), Set.of(), null, null, null,
          new ClusterAlerts.NodeStatus(null, null, false, true), stuck, stall, unproven, unreachable, null);
    }

    private static long unreachableMs(final RaftHAServer raft) {
      final LeaderReachSinceRestartTracker.Unreachable unreachable = raft.getLeaderUnreachableSinceRestart();
      return unreachable != null ? unreachable.unreachableForMs() : -1L;
    }

    private static long ms(final LeaderReachSinceRestartTracker tracker) {
      final LeaderReachSinceRestartTracker.Unreachable unreachable = tracker.current();
      return unreachable != null ? unreachable.unreachableForMs() : -1L;
    }

    private static JSONObject alertById(final JSONArray alerts, final String id) {
      for (int i = 0; i < alerts.length(); i++)
        if (id.equals(alerts.getJSONObject(i).getString("id")))
          return alerts.getJSONObject(i);
      return null;
    }

    /** A fixture restarted in place whose leader has been reported as not reaching it. */
    private static Fixture reported() throws Exception {
      final Fixture f = new Fixture();
      f.restartInPlace();
      f.tick(0L);
      f.tick(3_000L);
      f.tick(3_000L + f.grace);
      assertThat(unreachableMs(f.raft)).as("precondition: reported").isGreaterThanOrEqualTo(0L);
      return f;
    }

    private static RaftPeer peer(final String id, final String address) {
      return RaftPeer.newBuilder().setId(id).setAddress(address).build();
    }

    /**
     * A {@link RaftHAServer} that believes it is a running follower of a three-peer group holding entries up to 100 at
     * term {@value #TERM}, whose leader answers the commit-index probe with {@link #leaderCommit} (5000 by default).
     */
    private static final class Fixture {
      final RaftHAServer       raft;
      final DivisionInfo       info;
      final RaftLog            log;
      final FakeArcadeStateMachine stateMachine;
      final AtomicLong         now          = new AtomicLong();
      final AtomicLong         leaderCommit = new AtomicLong(5_000L);
      final long               grace;

      Fixture() throws Exception {
        final ContextConfiguration config = new ContextConfiguration();
        config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
        config.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);
        grace = 2L * RaftPropertiesBuilder.electionTimeoutMaxFor(
            config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MIN),
            config.getValueAsInteger(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX));

        final FakeArcadeDBServer mockServer = FakeArcadeDBServer.create("ArcadeDB_0", new ContextConfiguration());
        raft = new RaftHAServer(mockServer, config);

        final RaftPeer self = peer(raft.getLocalPeerId().toString(), "localhost:2434");
        final RaftPeer leader = peer("leader", "localhost:2435");
        final RaftPeer third = peer("third", "localhost:2436");

        info = mock(DivisionInfo.class);
        when(info.getLeaderId()).thenReturn(leader.getId());
        when(info.isLeader()).thenReturn(false);
        when(info.getLastAppliedIndex()).thenReturn(100L);
        when(info.getCurrentTerm()).thenReturn(TERM);
        when(info.getLifeCycleState()).thenReturn(LifeCycle.State.RUNNING);

        final RaftConfiguration conf = mock(RaftConfiguration.class);
        when(conf.getCurrentPeers()).thenReturn(List.of(self, leader, third));

        log = mock(RaftLog.class);
        when(log.getLastCommittedIndex()).thenReturn(100L);
        when(log.getLastEntryTermIndex()).thenReturn(TermIndex.valueOf(TERM, 100L));

        final RaftServer.Division division = mock(RaftServer.Division.class);
        when(division.getInfo()).thenReturn(info);
        when(division.getRaftConf()).thenReturn(conf);
        when(division.getRaftLog()).thenReturn(log);

        final RaftServer ratis = mock(RaftServer.class);
        when(ratis.getDivision(any())).thenReturn(division);

        final Field field = RaftHAServer.class.getDeclaredField("raftServer");
        field.setAccessible(true);
        field.set(raft, ratis);

        stateMachine = new FakeArcadeStateMachine();
        final Field smField = RaftHAServer.class.getDeclaredField("stateMachine");
        smField.setAccessible(true);
        smField.set(raft, stateMachine);

        raft.setLeaderCommitProber(target -> leaderCommit.get());
        raft.setFollowerStallClock(now::get);
      }

      /** What {@code restartRatis(false)} records: the division's position right after the new server started. */
      @SuppressWarnings("unchecked")
      void restartInPlace() throws Exception {
        final Field field = RaftHAServer.class.getDeclaredField("inPlaceRestartBaseline");
        field.setAccessible(true);
        ((AtomicReference<RaftHAServer.InPlaceRestartBaseline>) field.get(raft)).set(
            new RaftHAServer.InPlaceRestartBaseline(TermIndex.valueOf(TERM, 100L), TERM, info.getLeaderId() != null));
      }

      /** The hooks a health tick runs, in its order: track (on the previous probe), then probe. */
      void tick(final long atMs) {
        now.set(atMs);
        raft.trackFollowerStall();
        raft.trackLeaderReachSinceRestart();
        raft.refreshLeaderCommitIndex();
      }
    }
  }
}
