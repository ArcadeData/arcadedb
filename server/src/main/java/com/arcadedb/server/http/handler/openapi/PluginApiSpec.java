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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the routes contributed by server plugins rather than by {@code HttpServer} itself: the
 * Prometheus scrape endpoint and the Raft high-availability cluster management surface.
 * <p>
 * These operations are declared here, in the server module, and declared unconditionally. Two
 * constraints force that shape.
 * <p>
 * First, the specification has to be deterministic. Client generation runs against a live server's
 * spec, and a default server runs neither the HA plugin nor the metrics plugin. A specification that
 * only listed the routes of the currently active plugins would therefore generate clients with no
 * cluster management and no scrape endpoint at all, which is the opposite of what a complete spec is
 * for. Every operation below instead names the plugin a deployment must run for the route to answer,
 * so a client always has the method and a reader always knows the precondition.
 * <p>
 * Second, the plugins cannot declare their own. The {@code ha-raft} and {@code metrics} modules hold
 * {@code arcadedb-server} at provided scope, so the swagger model classes are absent from their
 * compile classpath. Asking a plugin to return path items would mean adding a swagger dependency to
 * both modules.
 * <p>
 * The cost is that a plugin can add a route without touching this class. Closing that gap is the
 * anti-drift work tracked separately: the natural shape is a test inside each plugin module that
 * asserts every route the module's own {@code registerAPI} declares appears in the generated
 * specification, which needs no new dependency because the assertion can compare plain path strings.
 */
public class PluginApiSpec implements OpenApiContributor {

  private static final String RAFT_REQUIRED =
      "Requires RaftHAPlugin: the route is registered on every server, but answers only where high availability is configured.";
  private static final String METRICS_REQUIRED =
      "Requires PrometheusMetricsPlugin: absent unless the metrics plugin is enabled.";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/prometheus", createScrapePath());
    openAPI.getPaths().addPathItem("/api/v1/cluster", createClusterStatusPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/peer", createAddPeerPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/peer/{peerId}", createRemovePeerPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/leader", createTransferLeaderPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/stepdown", createStepDownPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/leave", createLeavePath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/verify/{database}", createVerifyPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/resync/{database}", createResyncPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/bootstrap-state", createBootstrapStatePath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}", createSnapshotPath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}/checksums", createChecksumsPath());

    openAPI.getComponents().addSchemas("ClusterStatus", createClusterStatusSchema());
    openAPI.getComponents().addSchemas("AddPeerRequest", createAddPeerRequestSchema());
    openAPI.getComponents().addSchemas("TransferLeaderRequest", createTransferLeaderRequestSchema());
    openAPI.getComponents().addSchemas("ClusterActionResponse", createClusterActionResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseResponse", createVerifyResponseSchema());
    openAPI.getComponents().addSchemas("BootstrapStateResponse", createBootstrapStateResponseSchema());
  }

  private PathItem createScrapePath() {
    final Operation get = SpecBuilders.operation("scrapePrometheusMetrics", "Metrics",
        "Scrape server metrics",
        """
            Exposes the server's metrics in the Prometheus text exposition format, for a Prometheus \
            scrape_config to poll.

            Authentication can be turned off for this route with \
            arcadedb.serverMetrics.prometheus.requireAuthentication=false, which is how most scrape \
            setups run it. """ + METRICS_REQUIRED);

    final ApiResponse success = new ApiResponse();
    success.setDescription("Metrics in the Prometheus text exposition format");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(SpecBuilders.string("Prometheus text exposition format"));
    success.setContent(new Content().addMediaType("text/plain", mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("401", SpecBuilders.errorResponse(
        "Unauthorized: returned only when the plugin requires authentication"));
    // Reachable independently of the authentication requirement: AbstractServerHttpHandler answers 403
    // whenever an Authorization header is present but carries neither a Bearer nor a well-formed Basic
    // pair, regardless of whether this route requires one.
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: unsupported or malformed Authorization header"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    get.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createClusterStatusPath() {
    final Operation get = SpecBuilders.operation("getClusterStatus", "Cluster",
        "Read cluster and replication status",
        """
            Reports this server's Raft role, the current leader, and per-peer replication health \
            including match and next index, lag, and round-trip latency. Answers 503 until Raft has \
            started, because the route is registered before the Raft server comes up.

            The cluster and peer fields are server-level and readable by any authenticated user. The \
            'databases' array and the database-scoped 'alerts' are restricted to the databases the \
            caller is authorized for, so a user granted one database does not learn the others. """
            + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.queryParam("presence",
        "When present with no value, 'true' or '1', includes the per-database x per-peer presence "
            + "matrix in the 'databasePresence' field. Restricted to the root user, because it fans a "
            + "bootstrap-state RPC out to every peer. Built only on the leader; a follower ignores it.",
        false, "boolean"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Cluster status", "ClusterStatus"),
        "401", "403", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createAddPeerPath() {
    final Operation post = SpecBuilders.operation("addClusterPeer", "Cluster",
        "Add a peer to the cluster",
        "Adds a peer to the Raft configuration. " + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody("Peer to add", "AddPeerRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer added", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createRemovePeerPath() {
    final Operation delete = SpecBuilders.operation("removeClusterPeer", "Cluster",
        "Remove a peer from the cluster",
        """
            Removes a peer from the Raft configuration. Answers 409 when the removal would break \
            quorum or the configuration is already changing, unless 'force' is set. """ + RAFT_REQUIRED);
    delete.addParametersItem(SpecBuilders.pathParam("peerId", "Peer identifier"));
    delete.addParametersItem(SpecBuilders.queryParam("force",
        "Bypasses the quorum guard and removes the peer even when it would break quorum.", false, "boolean"));
    delete.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer removed", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setDelete(delete);
    return pathItem;
  }

  private PathItem createTransferLeaderPath() {
    final Operation post = SpecBuilders.operation("transferClusterLeadership", "Cluster",
        "Transfer leadership",
        """
            Transfers Raft leadership, to the named peer when 'peerId' is given and to whichever peer \
            Raft selects otherwise. Unknown fields in the body are rejected. """ + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody(
        "Transfer target", "TransferLeaderRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Leadership transferred", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createStepDownPath() {
    final Operation post = SpecBuilders.operation("stepDownClusterLeader", "Cluster",
        "Step down from leadership",
        "Asks this server to give up leadership, triggering an election. " + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Step-down initiated", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLeavePath() {
    final Operation post = SpecBuilders.operation("leaveCluster", "Cluster",
        "Leave the cluster",
        """
            Removes this server from the Raft configuration. Answers 409 when leaving would break \
            quorum, unless 'force' is set. """ + RAFT_REQUIRED);
    post.addParametersItem(SpecBuilders.queryParam("force",
        "Bypasses the quorum guard and leaves even when it would break quorum.", false, "boolean"));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Leaving the cluster", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createVerifyPath() {
    final Operation post = SpecBuilders.operation("verifyClusterDatabase", "Cluster",
        "Checksum a database's files for comparison across peers",
        """
            Computes a per-file checksum of one database on this server. A follower returns only its \
            own checksums; the leader additionally fans the same call out to every peer and reports a \
            cluster-wide comparison in 'result'. """ + RAFT_REQUIRED);
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Per-file checksums", "VerifyDatabaseResponse"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createResyncPath() {
    final Operation post = SpecBuilders.operation("resyncClusterDatabase", "Cluster",
        "Re-fetch a database from the leader",
        """
            Discards this server's copy of one database and installs a fresh snapshot from the \
            leader. Refuses to run on the leader itself. Answers 503 when no leader is currently \
            reachable. """ + RAFT_REQUIRED);
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    // No 404: unlike the verify handler, this handler never checks existsDatabase() - an unknown or
    // invalid name simply fails inside resyncDatabaseFromLeader, which is caught locally and reported
    // as 500.
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Database resynced", "ClusterActionResponse"),
        "400", "401", "403", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createBootstrapStatePath() {
    final Operation post = SpecBuilders.operation("getClusterBootstrapState", "Cluster",
        "Report per-database bootstrap state",
        """
            Reports this peer's fingerprint and last transaction id for every database. Used by the \
            bootstrap leader at first cluster formation to decide which copy of each database wins. \
            A database this peer cannot read is reported with an 'error' and a last transaction id of \
            -1 rather than omitted.

            Restricted to the root user; peers satisfy this by forwarding as root with the cluster \
            token. """ + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Bootstrap state", "BootstrapStateResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createSnapshotPath() {
    final Operation get = SpecBuilders.operation("downloadDatabaseSnapshot", "Cluster",
        "Download a database snapshot",
        """
            Streams a consistent snapshot of one database as a ZIP archive, for a follower installing \
            a fresh copy. The stream ends with a completeness manifest, advertised by a response \
            header, so a consumer can tell a complete download from one truncated at an archive entry \
            boundary. Only the root user may download a snapshot. Answers 503 when the server's \
            concurrent-snapshot limit is already reached.

            This route accepts HTTP Basic only: it is served by a handler outside the standard chain \
            and never reads a bearer token. """ + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    SpecBuilders.basicAuthOnly(get);

    final ApiResponse success = new ApiResponse();
    success.setDescription("ZIP archive of the database, ending with a completeness manifest");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("string").format("binary"));
    success.setContent(new Content().addMediaType("application/zip", mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Missing or invalid database name"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden: only the root user may download a snapshot"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "Too many concurrent snapshots"));
    get.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createChecksumsPath() {
    final Operation get = SpecBuilders.operation("getDatabaseSnapshotChecksums", "Cluster",
        "Read the checksums of a snapshot's files",
        """
            Returns the per-file checksums of the database a snapshot download would produce, so a \
            follower can decide whether it needs the full transfer. Only the root user may read them.

            This route accepts HTTP Basic only, for the same reason as the snapshot download. """
            + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    SpecBuilders.basicAuthOnly(get);
    // No 400: unlike the snapshot download branch, the checksums branch never validates the database
    // name (missing/invalid characters); an unknown name simply resolves to 404. A checksum
    // computation failure is caught locally and reported as 500.
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Per-file checksums", null),
        "401", "403", "404", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createClusterStatusSchema() {
    final Schema<Object> peer = SpecBuilders.object("One peer's replication health");
    peer.addProperty("id", SpecBuilders.string("Peer identifier"));
    peer.addProperty("address", SpecBuilders.string("Peer address"));
    peer.addProperty("role", SpecBuilders.string("LEADER or FOLLOWER"));
    peer.addProperty("matchIndex", SpecBuilders.integer(
        "Highest log entry known replicated. Absent for the leader's own entry and until a health sample exists."));
    peer.addProperty("nextIndex", SpecBuilders.integer(
        "Next log entry to send. Absent for the leader's own entry and until a health sample exists."));
    peer.addProperty("replicationLag", SpecBuilders.integer(
        "Entries behind the leader. Absent for the leader's own entry and until a health sample exists."));
    peer.addProperty("lastContactMs", SpecBuilders.integer(
        "Milliseconds since last contact. Absent for the leader's own entry and until a health sample exists."));
    peer.addProperty("replicaStatus", SpecBuilders.string(
        "Replica health status. Absent for the leader's own entry and until a health sample exists."));
    peer.addProperty("laggingForMs", SpecBuilders.integer(
        "How long this peer has been lagging, in milliseconds. Absent for the leader's own entry and until "
            + "a health sample exists."));
    peer.addProperty("lagging", SpecBuilders.bool(
        "True when the lag exceeds the configured warning threshold. Absent for the leader's own entry and "
            + "until a health sample exists."));
    peer.addProperty("replicationRttMs", SpecBuilders.integer(
        "Mean replication round-trip time. Absent when no sample exists."));
    peer.addProperty("replicationRttP99Ms", SpecBuilders.integer(
        "99th percentile replication round-trip time. Absent when no sample exists."));

    final Schema<Object> database = SpecBuilders.object("One database's cluster state");
    database.addProperty("name", SpecBuilders.string("Database name"));
    database.addProperty("bootstrapLastTxId", SpecBuilders.integer(
        "Last transaction id recorded at bootstrap. Absent when no baseline exists."));
    database.addProperty("bootstrapFingerprint", SpecBuilders.string(
        "Fingerprint recorded at bootstrap. Absent when no baseline exists."));
    database.addProperty("acquireStatus", SpecBuilders.string(
        "State of the last acquisition attempt. Absent when none was made."));
    database.addProperty("acquireTimestamp", SpecBuilders.integer(
        "When the last acquisition attempt ran, as epoch milliseconds. Absent when none was made."));
    database.addProperty("acquireError", SpecBuilders.string(
        "Why the last acquisition failed. Absent on success."));

    final Schema<Object> schema = SpecBuilders.object("Cluster and replication status");
    schema.addProperty("implementation", SpecBuilders.string("Always 'raft'"));
    schema.addProperty("clusterName", SpecBuilders.string("Configured cluster name"));
    schema.addProperty("localPeerId", SpecBuilders.string("This server's peer identifier"));
    schema.addProperty("raftState", SpecBuilders.string("Raft lifecycle state"));
    schema.addProperty("isLeader", SpecBuilders.bool("True when this server is the leader"));
    schema.addProperty("leaderReady", SpecBuilders.bool(
        "True when the leader has finished the work that makes it safe to serve writes"));
    final Schema<String> leaderId = SpecBuilders.string("Current leader, null when unknown");
    leaderId.setNullable(true);
    schema.addProperty("leaderId", leaderId);
    final Schema<String> leaderAddress = SpecBuilders.string(
        "Leader HTTP address, null when unknown");
    leaderAddress.setNullable(true);
    schema.addProperty("leaderHttpAddress", leaderAddress);
    schema.addProperty("electionCount", SpecBuilders.integer("Elections observed since start"));
    schema.addProperty("lastElectionTime", SpecBuilders.integer(
        "Last election as epoch milliseconds"));
    schema.addProperty("uptime", SpecBuilders.integer("Milliseconds since the Raft server started"));
    schema.addProperty("peers", SpecBuilders.arrayOf(peer, "Known peers"));
    schema.addProperty("databases", SpecBuilders.arrayOf(database, "Replicated databases"));
    schema.addProperty("databasePresence", SpecBuilders.object(
        "Which peer holds which database. Present only when this server is the leader and the "
            + "request set '?presence=true'."));
    schema.addProperty("alerts", SpecBuilders.arrayOf(
        SpecBuilders.object("One cluster alert"), "Conditions worth an operator's attention"));
    return schema;
  }

  private Schema<?> createAddPeerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Peer to add");
    schema.addProperty("peerId", SpecBuilders.string("Peer identifier"));
    schema.addProperty("address", SpecBuilders.string("Peer address"));
    schema.addProperty("name", SpecBuilders.string("Optional display name"));
    schema.setRequired(List.of("peerId", "address"));
    return schema;
  }

  private Schema<?> createTransferLeaderRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Transfer target. Send an empty object to let Raft choose. Unknown fields are rejected.");
    schema.addProperty("peerId", SpecBuilders.string(
        "Peer to make leader. Raft chooses when omitted."));
    schema.addProperty("timeoutMs", SpecBuilders.integer(
        "How long to wait for the transfer to complete, in milliseconds. Defaults to 30000."));
    return schema;
  }

  private Schema<?> createClusterActionResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Outcome of a cluster management action");
    schema.addProperty("result", SpecBuilders.string("Human-readable outcome"));
    schema.addProperty("leaderId", SpecBuilders.string(
        "Leader after the action. Present on leadership transfer."));
    schema.addProperty("database", SpecBuilders.string(
        "Database the action applied to. Present on resync."));
    schema.addProperty("localServer", SpecBuilders.string(
        "Server that performed the action. Present on resync."));
    return schema;
  }

  private Schema<?> createVerifyResponseSchema() {
    final Schema<Object> file = SpecBuilders.object("One database file");
    file.addProperty("name", SpecBuilders.string("File name"));
    file.addProperty("checksum", SpecBuilders.integer("CRC of the file's contents"));
    file.addProperty("size", SpecBuilders.integer("File size in bytes"));
    file.addProperty("type", SpecBuilders.string("File category"));

    final Schema<Object> mismatch = SpecBuilders.object(
        "One file whose checksum differs between the leader and a peer");
    mismatch.addProperty("file", SpecBuilders.string("File name"));
    mismatch.addProperty("type", SpecBuilders.string("File category"));
    mismatch.addProperty("localChecksum", SpecBuilders.integer("Leader's CRC for the file"));
    mismatch.addProperty("remoteChecksum", SpecBuilders.string(
        "Peer's CRC for the file, or 'MISSING' when the peer does not have it"));

    final Schema<Object> peerResult = SpecBuilders.object(
        "One peer's comparison against the leader's checksums");
    peerResult.addProperty("peerId", SpecBuilders.string("Peer identifier"));
    peerResult.addProperty("httpAddress", SpecBuilders.string("Peer HTTP address"));
    peerResult.addProperty("status", SpecBuilders.string("CONSISTENT, INCONSISTENT, or ERROR"));
    peerResult.addProperty("matchingFiles", SpecBuilders.integer(
        "Files whose checksum matches. Absent when the peer could not be queried."));
    peerResult.addProperty("mismatchedFiles", SpecBuilders.integer(
        "Files whose checksum differs. Absent when the peer could not be queried."));
    peerResult.addProperty("mismatches", SpecBuilders.arrayOf(mismatch,
        "Present only when mismatchedFiles is greater than zero"));
    peerResult.addProperty("error", SpecBuilders.string(
        "Why the peer could not be queried or compared. Absent on a completed comparison."));

    final Schema<Object> result = SpecBuilders.object(
        "Leader-only cluster-wide comparison, fanned out to every peer");
    result.addProperty("database", SpecBuilders.string("Database name"));
    result.addProperty("files", SpecBuilders.arrayOf(file, "The leader's files with size and category"));
    result.addProperty("localServer", SpecBuilders.string("Leader server name"));
    result.addProperty("localPeerId", SpecBuilders.string("Leader's peer identifier"));
    result.addProperty("localChecksums", SpecBuilders.object("Leader's file name to checksum map"));
    result.addProperty("peers", SpecBuilders.arrayOf(peerResult, "Every other peer's comparison result"));
    result.addProperty("overallStatus", SpecBuilders.string("ALL_CONSISTENT or INCONSISTENCY_DETECTED"));

    final Schema<Object> schema = SpecBuilders.object(
        "Per-file checksums of one database. A follower response carries only its own 'localChecksums', "
            + "'files' and 'localServer'; the leader instead returns only 'result', nesting a "
            + "cluster-wide comparison against every other peer.");
    schema.addProperty("localChecksums", SpecBuilders.object(
        "File name to checksum map, for a quick cross-peer comparison. Present on a follower response."));
    schema.addProperty("files", SpecBuilders.arrayOf(file,
        "Files with size and category. Present on a follower response."));
    schema.addProperty("localServer", SpecBuilders.string(
        "Server the checksums were taken on. Present on a follower response."));
    schema.addProperty("result", result);
    return schema;
  }

  private Schema<?> createBootstrapStateResponseSchema() {
    final Schema<Object> database = SpecBuilders.object("One database's bootstrap state");
    database.addProperty("name", SpecBuilders.string("Database name"));
    database.addProperty("fingerprint", SpecBuilders.string(
        "Content fingerprint, empty when the database could not be read"));
    database.addProperty("lastTxId", SpecBuilders.integer(
        "Last transaction id, -1 when the database could not be read"));
    database.addProperty("error", SpecBuilders.string(
        "Why the database could not be read. Absent on success."));

    final Schema<Object> schema = SpecBuilders.object("Per-database bootstrap state of one peer");
    schema.addProperty("databases", SpecBuilders.arrayOf(database, "Databases on this peer"));
    schema.addProperty("peerId", SpecBuilders.string("Peer that reported the state"));
    return schema;
  }
}
