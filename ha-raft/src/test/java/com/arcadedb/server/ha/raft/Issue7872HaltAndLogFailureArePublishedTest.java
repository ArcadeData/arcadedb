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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7872HaltAndLogFailureArePublishedTest {

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
  @Test
  void aHaltedStateMachineRaisesAnAlertNamingWhatTrippedIt() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addCriticalHaltAlert(
        new ArcadeStateMachine.CriticalHalt(4711L, "unknown Raft log entry type", 1_700_000_000_000L), alerts);

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
  @Test
  void anApplyingStateMachineRaisesNoHaltAlert() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addCriticalHaltAlert(null, alerts);
    assertThat(alerts).isEmpty();
  }

  /**
   * The log-writer failure, published for the first time here although the #7118 readiness gate has been reading
   * it since. Unlike the halt it IS recoverable in place, so the recommendation is about the storage volume
   * rather than about the process - the two conditions are not interchangeable and their alerts must not read as
   * though they were.
   */
  @Test
  void aFailedLogWriterRaisesAnAlertAboutTheVolume() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addRaftLogFailureAlert(
        new ArcadeStateMachine.RaftLogFailure(99L, "java.io.IOException: No space left on device", 1_700_000_000_001L),
        alerts);

    final JSONObject failure = alertWithId(alerts, "raft-log-writer-failed");
    assertThat(failure).isNotNull();
    assertThat(failure.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(failure.getJSONObject("details").getLong("index")).isEqualTo(99L);
    assertThat(failure.getJSONObject("details").getString("cause")).contains("No space left on device");
    assertThat(failure.getString("recommendation")).contains("Free space on the Raft storage volume");
  }

  @Test
  void aHealthyLogWriterRaisesNoAlert() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addRaftLogFailureAlert(null, alerts);
    assertThat(alerts).isEmpty();
  }

  /**
   * The liveness counterpart (issue #7622), equally invisible here until now. It is what fails
   * {@code /api/v1/health}, and a deployment without a liveness probe had nothing but a SEVERE line to read.
   */
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
  @Test
  void theThreeNodeLevelConditionsAreNotSuppressedForAScopedCaller() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of());

    // A caller authorized on no database at all: the strictest filter the endpoint can build.
    final JSONArray alerts = ClusterAlerts.scan(server, null, List.of(), Set.of(), null, null, null, true);

    assertThat(alertWithId(alerts, "crash-loop-escalated"))
        .as("whether this node's HA layer has given up is not a per-tenant fact")
        .isNotNull();
  }

  /**
   * And the contract declares all three, so a client generated from it can read them. The invariant lives in the
   * published document, not only in the handler, and #7872 is the second time a member reached the response
   * without reaching the document (#7741 was the first).
   */
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
            new ArcadeStateMachine.CriticalHalt(1L, "boom", 2L))).keySet());
    assertThat(halt.getRequired())
        .as("built in one expression, so a halt that is reported is reported whole")
        .containsExactlyInAnyOrderElementsOf(halt.getProperties().keySet());

    final Schema<?> logFailure = (Schema<?>) status.getProperties().get("raftLogFailure");
    assertThat(logFailure.getNullable()).isTrue();
    assertThat(logFailure.getProperties().keySet()).containsExactlyInAnyOrderElementsOf(
        ((JSONObject) GetClusterHandler.buildRaftLogFailure(
            new ArcadeStateMachine.RaftLogFailure(1L, "boom", 2L))).keySet());
    assertThat(logFailure.getRequired())
        .containsExactlyInAnyOrderElementsOf(logFailure.getProperties().keySet());
  }

  /** A healthy node writes an explicit null for both, so a client can tell "healthy" from "not reported". */
  @Test
  void aHealthyNodeWritesAnExplicitNullForBoth() {
    assertThat(GetClusterHandler.buildCriticalHalt(null)).isEqualTo(JSONObject.NULL);
    assertThat(GetClusterHandler.buildRaftLogFailure(null)).isEqualTo(JSONObject.NULL);
  }
}
