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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.PluginApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #7577 and #7578 for the cluster status document, checked from the module where both halves are visible.
 * <p>
 * {@code PluginApiSpec} lives in {@code arcadedb-server} and declares the HA routes on this module's behalf,
 * because {@code RaftHAPlugin} holds {@code arcadedb-server} at provided scope and cannot declare its own - the
 * same constraint {@code RaftHAPluginRegisteredRoutesMatchApiSpecTest} exists for. That means the spec cannot
 * reference {@code ClusterAlerts.SEVERITY_*} or {@code GetClusterHandler}'s members, so the values are written
 * out there and pinned here: this is the only place where the document and the code that produces it are both
 * on the classpath.
 * <p>
 * {@code alerts} was {@code SpecBuilders.object("One cluster alert")} - a bare object - so the entire
 * operator-facing payload of the endpoint was an empty model to a generated client (#7577), and
 * {@code localResync} plus this node's three own Raft indices were not declared at all (#7578's sweep).
 */
class Issue7577ClusterStatusSchemaMatchesTheHandlerTest {

  private static Schema<?> clusterStatus() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    final OpenApiContributor contributor = new PluginApiSpec();
    contributor.contribute(openAPI);
    return openAPI.getComponents().getSchemas().get("ClusterStatus");
  }

  private static Schema<?> property(final Schema<?> owner, final String name) {
    return (Schema<?>) owner.getProperties().get(name);
  }

  /**
   * The severity vocabulary is a copy, so it is checked against the original. A value added to
   * {@code ClusterAlerts} and not to the spec would otherwise reach operators through an enum that refuses it.
   */
  @Test
  void theAlertSeverityEnumIsTheSetClusterAlertsEmits() {
    final List<Object> declared = new ArrayList<>(
        property(property(clusterStatus(), "alerts").getItems(), "severity").getEnum());

    assertThat(declared).containsExactlyInAnyOrder(ClusterAlerts.SEVERITY_INFO, ClusterAlerts.SEVERITY_WARNING,
        ClusterAlerts.SEVERITY_CRITICAL);
  }

  /**
   * The {@code divergenceCauses} vocabulary is a copy too, for the same reason - {@code PluginApiSpec} lives in
   * {@code arcadedb-server} and cannot see {@link DivergenceCause} - so it is checked against the original here.
   * Without this, adding, renaming or removing a cause drifts the two lists apart silently, which is the failure
   * mode #7577 and #7741 were about: #7741 added this very member to the response and not to the document.
   */
  @Test
  void theDivergenceCauseEnumIsTheSetTheStateMachineRecords() {
    final Schema<?> localResync = property(clusterStatus(), "localResync");
    final List<Object> declared = new ArrayList<>(
        ((Schema<?>) localResync.getProperties().get("divergenceCauses")).getAdditionalProperties() instanceof Schema<?> values ?
            values.getEnum() :
            List.of());

    assertThat(declared)
        .as("the document must offer exactly the causes ArcadeStateMachine can record")
        .containsExactlyInAnyOrderElementsOf(
            Arrays.stream(DivergenceCause.values()).map(Enum::name).map(Object.class::cast).toList());
  }

  /** An alert is built as one chained expression, so every member it declares is on every alert. */
  @Test
  void anAlertDeclaresTheSixMembersEveryAlertCarries() {
    final Schema<?> alert = property(clusterStatus(), "alerts").getItems();

    assertThat(alert.getProperties().keySet())
        .containsExactlyInAnyOrder("id", "severity", "title", "message", "recommendation", "details");
    assertThat(alert.getRequired())
        .as("ClusterAlerts builds each alert whole, so an alert that is present is present whole")
        .containsExactlyInAnyOrderElementsOf(alert.getProperties().keySet());
  }

  /**
   * The divergence-cause vocabulary is a copy too (issue #7741), for the same reason: {@code DivergenceCause}
   * lives here and the spec lives in {@code arcadedb-server}. A cause added to the enum and not to the spec
   * would reach operators through an enum that refuses the value they are being shown.
   */
  @Test
  void theDivergenceCauseEnumIsTheSetTheHandlerEmits() {
    final List<Object> declared = new ArrayList<>(
        property(property(clusterStatus(), "localResync"), "divergenceCauses").getAdditionalProperties() instanceof Schema<?> values
            ? values.getEnum() : List.of());

    assertThat(declared).containsExactlyInAnyOrder(
        Stream.of(DivergenceCause.values()).map(DivergenceCause::name).toArray());
  }

  /**
   * {@code localResync} is the member a client watching a rolling restart reads instead of polling
   * {@code /api/v1/ready}, and it was in the response from issue #7136 and in the document from nowhere. Its
   * shape is asserted against {@code GetClusterHandler.buildLocalResync}, which is package-private for exactly
   * this kind of check.
   */
  @Test
  void localResyncDeclaresWhatBuildLocalResyncWrites() {
    final JSONObject emitted = GetClusterHandler.buildLocalResync(
        new ArcadeStateMachine.LocalResyncState(false, false, -1L, Map.of(), Map.of()), Set.of());

    final Schema<?> declared = property(clusterStatus(), "localResync");

    assertThat(declared).as("the document has to declare it at all").isNotNull();
    assertThat(declared.getProperties().keySet())
        .as("the document must name exactly the members the handler writes")
        .containsExactlyInAnyOrderElementsOf(emitted.keySet());
    assertThat(declared.getRequired())
        .as("they are written in one expression, so all of them are unconditional")
        .containsExactlyInAnyOrderElementsOf(emitted.keySet());
  }

  /**
   * This node's own Raft position. On a follower these are the only lag figures available - the per-peer ones
   * are the leader's view of its followers - so they are what an operator polls when that node is the suspect,
   * and none of them appeared in the contract.
   */
  @Test
  void theNodesOwnRaftPositionIsDeclaredAndRequired() {
    final Schema<?> status = clusterStatus();

    for (final String member : List.of("localAppliedIndex", "localCommitIndex", "localReplicationLag")) {
      assertThat(status.getProperties()).as(member).containsKey(member);
      assertThat(status.getRequired()).as(member + " is written on every answer").contains(member);
    }
  }

  /**
   * This node's own stuck-at-stale-term signal (issue #8289), written unconditionally next to the three Raft
   * position figures above - and, unlike them, {@code localReplicationLag} reads 0 while this is true, which is
   * exactly why it needed its own member rather than being inferred from the others.
   */
  @Test
  void theStuckAtStaleTermSignalIsDeclaredAndRequired() {
    final Schema<?> status = clusterStatus();

    assertThat(status.getProperties()).containsKey("localStuckAtStaleTerm");
    assertThat(status.getRequired()).contains("localStuckAtStaleTerm");
  }

  /**
   * And the conditional member stays conditional: {@code databasePresence} is written only by a leader answering
   * {@code ?presence=true}, so requiring it would make the contract lie in the other direction.
   */
  @Test
  void theOneConditionalMemberIsNotRequired() {
    final Schema<?> status = clusterStatus();

    assertThat(status.getProperties()).containsKey("databasePresence");
    assertThat(status.getRequired()).doesNotContain("databasePresence");
  }
}
