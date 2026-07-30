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
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PluginApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new PluginApiSpec().contribute(openAPI);
  }

  @Test
  void allTwelvePluginOperationsAreDeclared() {
    assertThat(openAPI.getPaths().keySet()).containsExactlyInAnyOrder(
        "/prometheus",
        "/api/v1/cluster",
        "/api/v1/cluster/peer",
        "/api/v1/cluster/peer/{peerId}",
        "/api/v1/cluster/leader",
        "/api/v1/cluster/stepdown",
        "/api/v1/cluster/leave",
        "/api/v1/cluster/verify/{database}",
        "/api/v1/cluster/resync/{database}",
        "/api/v1/cluster/bootstrap-state",
        "/api/v1/ha/snapshot/{database}",
        "/api/v1/ha/snapshot/{database}/checksums");

    final long operations = openAPI.getPaths().values().stream()
        .mapToLong(item -> item.readOperations().size()).sum();
    assertThat(operations).isEqualTo(12);
  }

  @Test
  void everyOperationNamesTheRequiredPlugin() {
    for (final Map.Entry<String, PathItem> entry : openAPI.getPaths().entrySet()) {
      for (final Operation op : entry.getValue().readOperations()) {
        assertThat(op.getDescription())
            .as("%s must tell a client which plugin has to be active", entry.getKey())
            .containsAnyOf("RaftHAPlugin", "PrometheusMetricsPlugin");
      }
    }
  }

  @Test
  void prometheusScrapeReturnsTextNotJson() {
    final Operation get = openAPI.getPaths().get("/prometheus").getGet();
    assertThat(get.getOperationId()).isEqualTo("scrapePrometheusMetrics");
    assertThat(get.getTags()).containsExactly("Metrics");
    assertThat(get.getResponses().get("200").getContent()).containsKey("text/plain");
  }

  @Test
  void prometheusScrapeIncludesForbiddenForUnsupportedAuthScheme() {
    // Correction: GetPrometheusMetricsHandler extends AbstractServerHttpHandler, whose shared auth
    // pipeline answers 403 for an Authorization header that is neither Bearer nor a well-formed Basic
    // pair - independent of whether this route requires authentication at all.
    final Operation get = openAPI.getPaths().get("/prometheus").getGet();
    assertThat(get.getResponses().keySet())
        .containsExactlyInAnyOrder("200", "401", "403", "500");
  }

  @Test
  void clusterStatusDeclaresTheUnstartedOutcome() {
    final Operation get = openAPI.getPaths().get("/api/v1/cluster").getGet();
    assertThat(get.getOperationId()).isEqualTo("getClusterStatus");
    assertThat(get.getResponses())
        .as("the endpoint is registered before Raft starts and answers 503 until it has")
        .containsKey("503");
  }

  @Test
  void clusterStatusAcceptsPresenceQueryParameter() {
    // Correction: GetClusterHandler.isPresenceRequested reads a 'presence' query parameter that gates
    // the 'databasePresence' field; the brief's path item carried no parameters at all.
    final Operation get = openAPI.getPaths().get("/api/v1/cluster").getGet();
    assertThat(get.getParameters())
        .extracting(Parameter::getName)
        .containsExactly("presence");
  }

  @Test
  void clusterStatusSchemaCarriesTheLeadershipAndPeerFields() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("ClusterStatus");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "implementation", "clusterName", "localPeerId", "raftState", "isLeader", "leaderReady",
        "leaderId", "leaderHttpAddress", "electionCount", "lastElectionTime", "uptime",
        "peers", "databases", "databasePresence", "alerts");

    // Pinned to the exact set (not .contains(...)): GetClusterHandler writes exactly these 12 fields
    // per peer, no more, no fewer.
    final Schema<?> peersProperty = schema.getProperties().get("peers");
    final Schema<?> peerItemSchema = peersProperty.getItems();
    assertThat(peerItemSchema.getProperties().keySet()).containsExactlyInAnyOrder(
        "id", "address", "role", "matchIndex", "nextIndex", "replicationLag", "lastContactMs",
        "replicaStatus", "laggingForMs", "lagging", "replicationRttMs", "replicationRttP99Ms");
  }

  @Test
  void clusterStatusPeerFieldsDocumentAbsenceForLeaderAndBeforeHealthSample() {
    // Correction (round 2): GetClusterHandler only writes matchIndex, nextIndex, replicationLag,
    // lastContactMs, replicaStatus, laggingForMs and lagging when '!peerIsLeader && health != null' -
    // i.e. never for the leader's own peer entry, and never before a health sample exists. The first
    // pass documented these seven with no absence note at all, while replicationRttMs/replicationRttP99Ms
    // two lines below (guarded by the same condition plus one more) correctly said so - an inconsistency
    // within the same schema. Every field gated by that condition must say so, so this cannot regress
    // silently behind a loose .contains(...) check again.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("ClusterStatus");
    final Schema<?> peersProperty = schema.getProperties().get("peers");
    final Schema<?> peerItemSchema = peersProperty.getItems();
    final Map<String, Schema> peerProperties = peerItemSchema.getProperties();

    for (final String conditionalField : List.of("matchIndex", "nextIndex", "replicationLag",
        "lastContactMs", "replicaStatus", "laggingForMs", "lagging", "replicationRttMs", "replicationRttP99Ms")) {
      final Schema<?> fieldSchema = peerProperties.get(conditionalField);
      assertThat(fieldSchema.getDescription())
          .as("'%s' is written only for a non-leader peer with a health sample; its description must say so",
              conditionalField)
          .containsIgnoringCase("absent");
    }

    // The three fields written unconditionally for every peer must NOT carry a misleading absence note.
    for (final String unconditionalField : List.of("id", "address", "role")) {
      final Schema<?> fieldSchema = peerProperties.get(unconditionalField);
      assertThat(fieldSchema.getDescription())
          .as("'%s' is written for every peer entry and must not claim to be sometimes absent", unconditionalField)
          .doesNotContainIgnoringCase("absent");
    }
  }

  @Test
  void clusterStatusDatabaseFieldsDocumentAbsenceConsistently() {
    // Correction (round 2): acquireTimestamp is set inside the same 'if (acquire != null)' block as
    // acquireStatus and acquireError, both of which already carried an absence note; acquireTimestamp
    // did not. bootstrapLastTxId/bootstrapFingerprint (a separate, independent condition) were already
    // correct before this fix and are re-checked here so a future edit cannot silently drop them.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("ClusterStatus");
    final Schema<?> databasesProperty = schema.getProperties().get("databases");
    final Schema<?> databaseItemSchema = databasesProperty.getItems();
    assertThat(databaseItemSchema.getProperties().keySet()).containsExactlyInAnyOrder(
        "name", "bootstrapLastTxId", "bootstrapFingerprint", "acquireStatus", "acquireTimestamp", "acquireError");

    final Map<String, Schema> databaseProperties = databaseItemSchema.getProperties();
    for (final String conditionalField : List.of("bootstrapLastTxId", "bootstrapFingerprint",
        "acquireStatus", "acquireTimestamp", "acquireError")) {
      final Schema<?> fieldSchema = databaseProperties.get(conditionalField);
      assertThat(fieldSchema.getDescription())
          .as("'%s' is populated only inside a guarding null-check and must document that", conditionalField)
          .containsIgnoringCase("absent");
    }

    assertThat(databaseProperties.get("name").getDescription())
        .as("'name' is written for every database entry and must not claim to be sometimes absent")
        .doesNotContainIgnoringCase("absent");
  }

  @Test
  void addPeerRequiresPeerIdAndAddress() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AddPeerRequest");
    assertThat(schema.getRequired()).containsExactlyInAnyOrder("peerId", "address");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("peerId", "address", "name");
  }

  @Test
  void peerRemovalAndLeaveDeclareConflict() {
    assertThat(openAPI.getPaths().get("/api/v1/cluster/peer/{peerId}").getDelete()
        .getResponses()).containsKey("409");
    assertThat(openAPI.getPaths().get("/api/v1/cluster/leave").getPost()
        .getResponses()).containsKey("409");
  }

  @Test
  void removePeerAcceptsForceQueryParameter() {
    // Correction: DeletePeerHandler.isForce reads a 'force' query parameter that bypasses the quorum
    // guard; the brief's path item carried only the 'peerId' path parameter.
    final Operation delete = openAPI.getPaths().get("/api/v1/cluster/peer/{peerId}").getDelete();
    assertThat(delete.getParameters())
        .extracting(Parameter::getName)
        .containsExactlyInAnyOrder("peerId", "force");
  }

  @Test
  void leaveAcceptsForceQueryParameter() {
    // Correction: PostLeaveHandler reads the same 'force' query parameter as peer removal; the brief's
    // path item carried no parameters at all.
    final Operation post = openAPI.getPaths().get("/api/v1/cluster/leave").getPost();
    assertThat(post.getParameters())
        .extracting(Parameter::getName)
        .containsExactly("force");
  }

  @Test
  void transferLeaderReportsTheResultingLeader() {
    final Operation post = openAPI.getPaths().get("/api/v1/cluster/leader").getPost();
    assertThat(post.getOperationId()).isEqualTo("transferClusterLeadership");
    assertThat(openAPI.getComponents().getSchemas().get("ClusterActionResponse")
        .getProperties().keySet()).contains("result", "leaderId");
  }

  @Test
  void transferLeaderRequestAcceptsOptionalTimeout() {
    // Correction: PostTransferLeaderHandler reads both 'peerId' and 'timeoutMs' from the body and
    // rejects any other field; the brief's schema declared only 'peerId', which would make a
    // legitimate 'timeoutMs' request look like it triggers the handler's "Unknown field" rejection.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TransferLeaderRequest");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("peerId", "timeoutMs");
  }

  @Test
  void resyncNeverReturnsNotFound() {
    // Correction: unlike PostVerifyDatabaseHandler, PostResyncDatabaseHandler never calls
    // existsDatabase(); every failure of resyncDatabaseFromLeader() is caught locally and reported as
    // 500, so 404 can never be returned. The brief's response set wrongly included 404.
    final Operation post = openAPI.getPaths().get("/api/v1/cluster/resync/{database}").getPost();
    assertThat(post.getResponses().keySet())
        .containsExactlyInAnyOrder("200", "400", "401", "403", "500", "503");
  }

  @Test
  void verifyReportsPerFileChecksums() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("VerifyDatabaseResponse");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("localChecksums", "files", "localServer", "result");

    final Schema<?> filesProperty = schema.getProperties().get("files");
    final Schema<?> fileItemSchema = filesProperty.getItems();
    assertThat(fileItemSchema.getProperties().keySet())
        .containsExactlyInAnyOrder("name", "checksum", "size", "type");
  }

  @Test
  void verifyLeaderBranchNestsAClusterWideComparison() {
    // Correction: PostVerifyDatabaseHandler returns a completely different shape on the leader
    // ({"result": {...}}) than on a follower ({localChecksums, files, localServer}). The brief's
    // schema documented only the follower branch and silently dropped the leader branch.
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("VerifyDatabaseResponse");
    final Schema<?> resultProperty = schema.getProperties().get("result");
    assertThat(resultProperty.getProperties().keySet()).containsExactlyInAnyOrder(
        "database", "files", "localServer", "localPeerId", "localChecksums", "peers", "overallStatus");

    final Schema<?> resultPeersProperty = resultProperty.getProperties().get("peers");
    final Schema<?> peerResultItemSchema = resultPeersProperty.getItems();
    assertThat(peerResultItemSchema.getProperties().keySet()).containsExactlyInAnyOrder(
        "peerId", "httpAddress", "status", "matchingFiles", "mismatchedFiles", "mismatches", "error");
  }

  @Test
  void bootstrapStateReportsPerDatabaseFingerprints() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BootstrapStateResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("databases", "peerId");

    final Schema<?> databasesProperty = schema.getProperties().get("databases");
    final Schema<?> databaseItemSchema = databasesProperty.getItems();
    assertThat(databaseItemSchema.getProperties().keySet())
        .containsExactlyInAnyOrder("name", "fingerprint", "lastTxId", "error");
  }

  @Test
  void snapshotOperationsAreBasicAuthOnlyAndStreamZip() {
    final Operation snapshot = openAPI.getPaths().get("/api/v1/ha/snapshot/{database}").getGet();
    assertThat(snapshot.getOperationId()).isEqualTo("downloadDatabaseSnapshot");
    assertThat(snapshot.getResponses().get("200").getContent()).containsKey("application/zip");
    assertThat(snapshot.getResponses())
        .as("the handler caps concurrent snapshots and refuses beyond the cap")
        .containsKey("503");
    assertThat(snapshot.getSecurity())
        .as("SnapshotHttpHandler parses Basic itself and never reaches the bearer branch")
        .hasSize(1);
    assertThat(snapshot.getSecurity().getFirst()).containsOnlyKeys("basicAuth");

    final Operation checksums = openAPI.getPaths()
        .get("/api/v1/ha/snapshot/{database}/checksums").getGet();
    assertThat(checksums.getOperationId()).isEqualTo("getDatabaseSnapshotChecksums");
    assertThat(checksums.getSecurity().getFirst()).containsOnlyKeys("basicAuth");
  }

  @Test
  void snapshotDownloadResponseCodesMatchTheHandler() {
    // Correction: SnapshotHttpHandler answers 403 when the authenticated user is not root; the brief
    // omitted it even though the handler checks it before anything else.
    final Operation get = openAPI.getPaths().get("/api/v1/ha/snapshot/{database}").getGet();
    assertThat(get.getResponses().keySet())
        .containsExactlyInAnyOrder("200", "400", "401", "403", "404", "503");
  }

  @Test
  void snapshotChecksumsResponseCodesMatchTheHandler() {
    // Correction: the checksums branch (SnapshotHttpHandler.handleChecksums) never validates the
    // database-name shape the way the download branch does, so 400 can never be returned; a checksum
    // computation failure is caught locally and reported as 500, and the same root-only 403 check
    // applies before either branch is reached. The brief's response set had 400 but neither 403 nor 500.
    final Operation checksums = openAPI.getPaths()
        .get("/api/v1/ha/snapshot/{database}/checksums").getGet();
    assertThat(checksums.getResponses().keySet())
        .containsExactlyInAnyOrder("200", "401", "403", "404", "500", "503");
  }

  @Test
  void noClusterOperationIsMarkedPublic() {
    for (final Map.Entry<String, PathItem> entry : openAPI.getPaths().entrySet()) {
      for (final Operation op : entry.getValue().readOperations()) {
        assertThat(op.getSecurity())
            .as("%s must not opt out of authentication", entry.getKey())
            .satisfiesAnyOf(
                security -> assertThat(security).isNull(),
                security -> assertThat(security).isNotEmpty());
      }
    }
  }
}
