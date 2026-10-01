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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.SecurityConvergenceStatus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8555: the security-convergence readiness gate is a source of a 503 that
 * {@code GET /api/v1/cluster} did not publish. The {@code securityConvergence} member is written on every answer, and the
 * {@code security-documents-unconverged} alert is raised while the node is held ({@code warning}) and once it gave up
 * ({@code critical}: it is READY and enforcing copies of the documents the cluster never confirmed).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8555SecurityConvergencePublishedTest {

  private static final long WINDOW_OPENED_AT = 1_780_000_000_000L;

  @Test
  void theMemberIsWrittenWholeOnEveryAnswerEvenWhenNothingIsHeld() {
    final JSONObject idle = GetClusterHandler.buildSecurityConvergence(SecurityConvergenceStatus.NOT_CONVERGING);

    assertThat(idle.keySet()).containsExactlyInAnyOrder("held", "unconvergedDocuments", "armed", "sinceIndex",
        "windowOpenedAt", "gaveUp", "skippedBecauseLeading");
    assertThat(idle.getBoolean("held")).isFalse();
    assertThat(idle.getJSONArray("unconvergedDocuments")).isEmpty();
    assertThat(idle.getBoolean("gaveUp")).isFalse();
  }

  @Test
  void aHeldNodeIsRenderedWithEverythingTheOperatorNeeds() {
    final JSONObject member = GetClusterHandler.buildSecurityConvergence(
        new SecurityConvergenceStatus(true, List.of("users", "API tokens"), false, 5_000L, WINDOW_OPENED_AT, false, false,
            "held"));

    assertThat(member.getBoolean("held")).isTrue();
    assertThat(member.getJSONArray("unconvergedDocuments").toList()).containsExactly("users", "API tokens");
    assertThat(member.getBoolean("armed")).isFalse();
    assertThat(member.getLong("sinceIndex")).isEqualTo(5_000L);
    assertThat(member.getLong("windowOpenedAt")).isEqualTo(WINDOW_OPENED_AT);
    assertThat(member.getBoolean("gaveUp")).isFalse();
    assertThat(member.getBoolean("skippedBecauseLeading")).isFalse();
  }

  @Test
  void aHeldNodeRaisesAWarning() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addSecurityDocumentsUnconvergedAlert(
        new SecurityConvergenceStatus(true, List.of("groups"), true, 7L, WINDOW_OPENED_AT, false, false, "held"), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("security-documents-unconverged");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_WARNING);
    assertThat(alert.getString("message")).contains("groups").contains("503");
    assertThat(alert.getJSONObject("details").getBoolean("armed")).isTrue();
    assertThat(alert.getJSONObject("details").getJSONArray("unconvergedDocuments").toList()).containsExactly("groups");
  }

  /** READY and enforcing its own copies: the state that had no report anywhere but one SEVERE log line. */
  @Test
  void aNodeThatGaveUpRaisesACriticalAlert() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addSecurityDocumentsUnconvergedAlert(
        new SecurityConvergenceStatus(false, List.of("users"), false, 9L, WINDOW_OPENED_AT, true, false, null), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("message")).contains("READY").contains("users");
    assertThat(alert.getString("recommendation")).contains("/api/v1/cluster/peer");
    assertThat(alert.getJSONObject("details").getBoolean("gaveUp")).isTrue();
  }

  @Test
  void aConvergedNodeAndALeaderRaiseNothing() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addSecurityDocumentsUnconvergedAlert(SecurityConvergenceStatus.NOT_CONVERGING, alerts);
    ClusterAlerts.addSecurityDocumentsUnconvergedAlert(
        new SecurityConvergenceStatus(false, List.of("users"), false, 9L, 0L, false, true, null), alerts);
    ClusterAlerts.addSecurityDocumentsUnconvergedAlert(null, alerts);

    assertThat(alerts.length()).as("a leader is not held: nobody can confirm its documents").isZero();
  }

  @Test
  void theAlertScanCarriesTheSampleTheDocumentRendered() {
    final SecurityConvergenceStatus held = new SecurityConvergenceStatus(true, List.of("users"), true, 3L, WINDOW_OPENED_AT,
        false, false, "held");
    final ClusterAlerts.NodeStatus nodeStatus = new ClusterAlerts.NodeStatus(null, null, false, true, List.of(), List.of(),
        held);

    assertThat(nodeStatus.securityConvergence()).isSameAs(held);
    assertThat(new ClusterAlerts.NodeStatus(null, null, false, true).securityConvergence())
        .as("the older shapes report a gate that is not converging").isSameAs(SecurityConvergenceStatus.NOT_CONVERGING);
  }
}
