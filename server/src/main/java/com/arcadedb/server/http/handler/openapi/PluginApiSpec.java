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

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

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

  /**
   * The Raft high-availability cluster management and snapshot routes {@code RaftHAPlugin}
   * registers, mirrored here because that module cannot declare its own (see the class Javadoc
   * above). Verified against the plugin's actual {@code registerAPI} output by
   * {@code RaftHAPluginRegisteredRoutesMatchApiSpecTest} in the ha-raft module, and against
   * {@link #contribute} by {@code ApiSpecPathConstantsTest} (issue #4896).
   */
  public static final Set<String> HA_RAFT_PATHS = Set.of(
      "/api/v1/cluster", "/api/v1/cluster/peer", "/api/v1/cluster/peer/{peerId}",
      "/api/v1/cluster/leader", "/api/v1/cluster/stepdown", "/api/v1/cluster/leave",
      "/api/v1/cluster/verify/{database}", "/api/v1/cluster/resync/{database}",
      "/api/v1/cluster/bootstrap-state", "/api/v1/cluster/capabilities",
      "/api/v1/cluster/security-seed",
      "/api/v1/ha/snapshot/{database}", "/api/v1/ha/snapshot/{database}/checksums");

  /**
   * The Prometheus scrape route {@code PrometheusMetricsPlugin} registers, mirrored here for the
   * same reason as {@link #HA_RAFT_PATHS}. Verified against the plugin's actual {@code registerAPI}
   * output by {@code PrometheusMetricsPluginRegisteredRoutesMatchApiSpecTest} in the metrics module.
   */
  public static final Set<String> METRICS_PATHS = Set.of("/prometheus");

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
    openAPI.getPaths().addPathItem("/api/v1/cluster/capabilities", createCapabilitiesPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/security-seed", createSecuritySeedPath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}", createSnapshotPath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}/checksums", createChecksumsPath());

    openAPI.getComponents().addSchemas("ClusterStatus", createClusterStatusSchema());
    openAPI.getComponents().addSchemas("AddPeerRequest", createAddPeerRequestSchema());
    openAPI.getComponents().addSchemas("TransferLeaderRequest", createTransferLeaderRequestSchema());
    openAPI.getComponents().addSchemas("ClusterActionResponse", createClusterActionResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseResponse", createVerifyResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseLocalResponse", createVerifyLocalResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseClusterResponse", createVerifyClusterResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseClusterResult", createVerifyClusterResultSchema());
    openAPI.getComponents().addSchemas("BootstrapStateResponse", createBootstrapStateResponseSchema());
    openAPI.getComponents().addSchemas("PeerCapabilitiesResponse", createPeerCapabilitiesResponseSchema());
    openAPI.getComponents().addSchemas("SecuritySeedRequest", createSecuritySeedRequestSchema());
    openAPI.getComponents().addSchemas("SecuritySeedResponse", createSecuritySeedResponseSchema());
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
        """
            Adds a peer to the Raft configuration, then seeds it with the three security documents \
            (server-users.jsonl, server-groups.json, server-api-tokens.json) that a Raft snapshot install does \
            not carry.

            A 503 means the membership change succeeded and at least one of those seeds did not commit within \
            arcadedb.ha.securitySeedRetryTimeout: the peer IS a cluster member and serves requests against its \
            own copy of the documents that failed, which are named in 'failedSeeds'. Re-POST the same peer to \
            reissue the seed - the membership change is idempotent.

            The optional 'priority' carries the peer's Raft leader-election priority, which before it could only \
            be declared in arcadedb.ha.serverList at startup: a peer added at runtime always got the default and \
            a witness admitted this way could be elected leader.

            Note the direction: this grows the cluster the SERVER SERVING THIS REQUEST belongs to, with the peer \
            named in the body. It never makes that server join another cluster, so it has to be issued against a \
            member of the target cluster. An address that resolves to the serving node's own peer id is answered \
            400 rather than accepted: it used to report the peer as added while doing nothing, because a peer \
            already in the committed configuration is an idempotent no-op. """ + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody("Peer to add", "AddPeerRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer added and seeded", "ClusterActionResponse"),
        "400", "401", "403", "500", "503"));

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
            Raft selects otherwise. Unknown fields in the body are rejected. Only the leader can transfer \
            leadership: a server that is not the leader answers 409 naming the leader to reissue against, \
            rather than routing the request there and forcing an election nobody asked for. """ + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody(
        "Transfer target", "TransferLeaderRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Leadership transferred", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createStepDownPath() {
    final Operation post = SpecBuilders.operation("stepDownClusterLeader", "Cluster",
        "Step down from leadership",
        """
            Asks this server to give up leadership, triggering an election. Answers 409 when this server is \
            not the leader - it has nothing to step down from, and the request must be reissued against the \
            leader the response names rather than acted on remotely. """ + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Step-down initiated", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

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

  private PathItem createCapabilitiesPath() {
    final Operation post = SpecBuilders.operation("getClusterPeerCapabilities", "Cluster",
        "Report the wire-format capabilities of this peer",
        """
            Reports the optional replication wire-format sections this node can DECODE, as short stable \
            tokens. The leader polls it on every peer of its Raft configuration and writes an optional \
            section only when every peer has advertised it, so a rolling upgrade needs no ordering by \
            hand (issue #7219).

            A node running a release without this route answers 404, and the caller reads that as 'this \
            peer can decode nothing optional' - which is why the route is safe to add and why no version \
            comparison takes part in the decision.

            Restricted to the root user; peers satisfy this by forwarding as root with the cluster \
            token. """ + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer capabilities", "PeerCapabilitiesResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createSecuritySeedPath() {
    final Operation post = SpecBuilders.operation("seedClusterSecurityDocuments", "Cluster",
        "Have the leader replicate the cluster security documents",
        """
            Asks the Raft LEADER to submit server-users.jsonl, server-groups.json and \
            server-api-tokens.json to the cluster, and answers with the ones that did not commit.

            Two callers need it, and both are cluster-internal. A node that has just admitted a peer \
            reads the outcome here instead of running a seed of its own, so an admission is seeded once \
            rather than from two nodes under two different monitors (issue #7834). A node that came back \
            while it was still a Raft member - a rolling restart, a drain and reschedule, a pod whose \
            ordinal is in the static server list - sends the fingerprints of the documents it holds, and \
            is re-seeded only if they differ from the leader's (issue #7833); the three documents live \
            outside the database directory, so no snapshot install carries them.

            Answered 409 by a node that is not the leader, naming the one it believes leads. Answered \
            503 with the failedSeeds array when the seed ran but a document did not commit, which is the \
            same contract POST /api/v1/cluster/peer answers with.

            Restricted to the root user; peers satisfy this by forwarding as root with the cluster \
            token. """ + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody("What to seed, and what the caller already holds",
        "SecuritySeedRequest", false));

    final ApiResponses responses = SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("The seed outcome", "SecuritySeedResponse"),
        "400", "401", "403", "500");
    responses.addApiResponse("409", SpecBuilders.errorResponse(
        "This node is not the Raft leader; the answer names the one it believes leads"));
    // The partial-seed 503 carries the SAME body as the 200 - upToDate/seeded/failedSeeds - and failedSeeds is
    // the part a client acts on, so declaring a generic error here hid the one field that matters
    // (CodeRabbit on PR #7854). The other 503 this route can answer, "the seed could not be run at all", puts
    // its reason in `error`, which the schema carries as an optional property rather than a second shape.
    responses.addApiResponse("503", SpecBuilders.jsonResponse(
        "The seed ran but one or more documents did not commit (see failedSeeds), or it could not be run at all "
            + "(see error)", "SecuritySeedResponse"));
    post.setResponses(responses);

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
            Returns the per-file checksums of the database as a snapshot download would produce it, read \
            through the same point-in-time window. Only the root user may read them.

            This is an operator diagnostic: it answers "do these two nodes hold the same bytes?" without \
            transferring a database. Resync itself does not consult it - a follower that falls behind the \
            compacted Raft log always downloads the full snapshot ZIP - because a whole-file comparison is the \
            wrong granularity for an ArcadeDB database, which is usually dominated by one bucket file that any \
            single changed byte re-ships in full. Incremental resync is tracked as a page-level diff in #6115.

            This route accepts HTTP Basic only, for the same reason as the snapshot download. """
            + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    SpecBuilders.basicAuthOnly(get);
    // 400 since #6125: the database name is validated (non-empty, no separators, no '..', printable ASCII)
    // BEFORE this branch is taken, exactly as on the snapshot download route, so a malformed name is refused
    // here rather than resolving to a 404 further down. A checksum computation failure is caught locally and
    // reported as 500.
    // The body is a FLAT map of file name to checksum - SnapshotHttpHandler.putChecksums writes the map's
    // entries at top level, with no envelope - which is not the shape the /cluster/verify route answers with and
    // was declared as an un-named object here, so a generated client got an empty model (issue #7577).
    final ApiResponse checksums = new ApiResponse();
    checksums.setDescription("Per-file checksums, keyed by file name. No envelope: the map IS the body");
    final Schema<Object> checksumMap = SpecBuilders.mapOf(SpecBuilders.integer("CRC of the file's contents"),
        "File name to checksum");
    // The one key that is not a file name (issue #7956). A file that disappears between the directory listing and
    // the read - a TimeSeries sealed store dropped by retention, a component file dropped by index compaction -
    // used to take the whole answer down as a 500; it is now left out and NAMED here, so a comparison can say "this
    // answer does not cover these" instead of reading a silently short map as agreement. Absent when there are
    // none. It cannot collide with a file name because the map is keyed by File.getName(), which never contains a
    // path separator. Spelled as a literal rather than shared from SnapshotHttpHandler.UNREADABLE_FILES_KEY because
    // this module does not depend on ha-raft, exactly as the route strings above are.
    checksumMap.addProperty("/unreadableFiles", SpecBuilders.arrayOf(SpecBuilders.string("File name"),
        "Files listed in the database directory that were gone by the time this answer tried to read them, so it "
            + "does not cover them. Absent when the answer is complete."));
    checksums.setContent(new Content().addMediaType(SpecBuilders.JSON, new MediaType().schema(checksumMap)));
    get.setResponses(SpecBuilders.standardResponses("200", checksums,
        "400", "401", "403", "404", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createClusterStatusSchema() {
    final Schema<Object> peer = SpecBuilders.object("One peer's replication health");
    peer.addProperty("id", SpecBuilders.string("Peer identifier"));
    peer.addProperty("address", SpecBuilders.string("Peer address"));
    peer.addProperty("httpAddress", SpecBuilders.string(
        "Peer HTTP endpoint as resolved by this node. Absent when it cannot be resolved."));
    peer.addProperty("httpAddressAmbiguous", SpecBuilders.bool(
        "True when the HTTP endpoint above does not identify this peer alone: two or more peers resolve to it, "
            + "which is what happens when 'http' ports are not declared in arcadedb.ha.serverList and the nodes "
            + "differ by port rather than by host. Peer-to-peer operations (snapshot resync, cluster verify) "
            + "refuse to dial such a peer. Absent when the address is unambiguous."));
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
    peer.addProperty("capabilities", SpecBuilders.arrayOf(SpecBuilders.string("Capability token"),
        "Optional wire-format sections this peer can decode, as last observed by the leader (issue #7219). "
            + "Absent on a follower, which does not poll, and on the leader for a peer it has not reached: an "
            + "absent array means 'not known', which the leader treats exactly like 'cannot decode'."));
    peer.addProperty("version", SpecBuilders.string(
        "Server version this peer reported alongside its capabilities. Absent when the leader has no fresh "
            + "answer from it."));
    peer.addProperty("capabilitiesUnknownReason", SpecBuilders.string("""
        Why 'capabilities' is absent for this peer, when the leader knows why. An absent capabilities array \
        otherwise reads the same whether the peer runs a build that predates the capability route or was never \
        asked because its address identifies no single peer, and the two have nothing alike as remedies \
        (issue #7256). Written by the leader only."""));
    // Only these three are written for every peer; every other member above is conditional on a health sample,
    // on a resolvable endpoint, or on this node being the leader (issue #7578).
    peer.setRequired(List.of("id", "address", "role"));

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
    // The name is the row; everything else says so in its own description.
    database.setRequired(List.of("name"));

    final Schema<Object> schema = SpecBuilders.object("Cluster and replication status");
    schema.addProperty("implementation", SpecBuilders.string("Always 'raft'"));
    schema.addProperty("clusterName", SpecBuilders.string("Configured cluster name"));
    schema.addProperty("localPeerId", SpecBuilders.string("This server's peer identifier"));
    schema.addProperty("capabilities", SpecBuilders.arrayOf(SpecBuilders.string("Capability token"),
        "Optional wire-format sections THIS node can decode, sorted (issue #7219)"));
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
    // This node's OWN Raft position, written on every answer and documented nowhere until the #7578 sweep. The
    // per-peer figures above are the LEADER's view of its followers, so on a follower - the node an operator
    // polls when that node is the suspect - these three were the only lag figures available and none of them
    // appeared in the contract (issue #7136).
    schema.addProperty("localAppliedIndex", SpecBuilders.integer(
        "Last Raft index this node has applied. -1 when the division cannot be read, e.g. during an in-place "
            + "restart"));
    schema.addProperty("localCommitIndex", SpecBuilders.integer(
        "Last Raft index this node knows to be committed. -1 under the same condition"));
    schema.addProperty("localReplicationLag", SpecBuilders.integer(
        "Entries this node has yet to apply: 'localCommitIndex' minus 'localAppliedIndex'. -1 rather than a "
            + "fabricated difference whenever either side is unknown"));
    // Issue #8289: this node stuck at a stale term after a snapshot install applies everything it could
    // locally commit, so 'localReplicationLag' above reads 0 and this node looks caught up, yet it keeps
    // rejecting the leader's current-term entries and does not count toward the Raft quorum. Debounced
    // (seen on two consecutive health-monitor ticks) so a normal leader change is not reported as one.
    schema.addProperty("localStuckAtStaleTerm", SpecBuilders.bool(
        "True when this node recognizes a leader at a newer term but keeps rejecting its current-term entries "
            + "although it has applied everything it could locally commit. It does not count toward quorum while "
            + "this is true, even though 'localReplicationLag' reads 0. See the 'follower-stuck-at-stale-term' "
            + "alert for the operator-facing explanation"));
    schema.addProperty("peers", SpecBuilders.arrayOf(peer, "Known peers"));
    schema.addProperty("databases", SpecBuilders.arrayOf(database, "Replicated databases"));
    schema.addProperty("databasePresence", SpecBuilders.mapOf(
        SpecBuilders.arrayOf(SpecBuilders.string("Peer identifier"), "Peers that hold this database"),
        "Which peer holds which database, keyed by database name. Present only when this server is the leader "
            + "and the request set '?presence=true'."));
    schema.addProperty("alerts", SpecBuilders.arrayOf(alertSchema(),
        "Conditions worth an operator's attention. Empty when the cluster is healthy: an absent array is not a "
            + "state this endpoint produces"));
    // Emitted by GetClusterHandler on every answer and documented nowhere, which is the same defect as an
    // undeclared 'required' entry pointing the other way: a client generated from this contract could not read
    // the one field that says whether THIS node is serving traffic (issue #7577 sweep).
    schema.addProperty("localResync", localResyncSchema());
    // The two remaining readiness inputs, plus the liveness one. #7136 wrote the invariant into localResync's
    // description and delivered it for the resync inputs; these three were still reported nowhere, so a node
    // whose state machine had halted - or whose log writer had failed - read green here while '/api/v1/ready'
    // was pinned at 503 (issue #7872). Nullable objects rather than booleans, because what an operator does
    // next is decided by the index and the reason, not by the fact that something is wrong.
    schema.addProperty("criticalHalt", criticalHaltSchema());
    schema.addProperty("raftLogFailure", raftLogFailureSchema());
    schema.addProperty("crashLoopEscalated", SpecBuilders.bool("""
        True once the health monitor has given up restarting this node's HA layer (issue #7622). The liveness \
        counterpart of the two above: this is what makes '/api/v1/health' answer unhealthy."""));
    // 'databasePresence' is written only by a leader answering '?presence=true'; everything else above is on
    // every answer, with 'leaderId', 'leaderHttpAddress', 'criticalHalt' and 'raftLogFailure' carrying an
    // explicit null rather than going absent (issues #7578, #7872).
    schema.setRequired(List.of("implementation", "clusterName", "localPeerId", "capabilities", "raftState",
        "isLeader", "leaderReady", "leaderId", "leaderHttpAddress", "electionCount", "lastElectionTime",
        "uptime", "localAppliedIndex", "localCommitIndex", "localReplicationLag", "localStuckAtStaleTerm", "peers",
        "databases", "localResync", "criticalHalt", "raftLogFailure", "crashLoopEscalated", "alerts"));
    return schema;
  }

  /**
   * The critical error that halted this node's replication state machine, or null (issue #7872).
   * <p>
   * Terminal and not recoverable in place: the halt trips an asynchronous server stop, and the recovery is that
   * restart. It is published because that stop can fail, leaving a process up and answering HTTP with a state
   * machine that applies nothing.
   */
  private Schema<?> criticalHaltSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Why this node's replication state machine halted, or null while it is applying entries. Present on every \
        answer. A non-null value means this node's databases are frozen at 'index' and will not advance again in \
        this process: restart it.""");
    schema.addProperty("index", SpecBuilders.integer(
        "The Raft index being applied when the halt tripped, or -1 when the entry carried none"));
    schema.addProperty("reason", SpecBuilders.string(
        "One line naming what could not be applied. 'unknown Raft log entry type' means a newer peer is writing a "
            + "format this build cannot read, and the answer is to upgrade this node; anything else is a bug. Raw "
            + "exception text, so it is shown to the root user only: another caller reads a placeholder here while "
            + "still getting 'index' and 'timestamp'"));
    schema.addProperty("timestamp", SpecBuilders.integer("When it tripped, as epoch milliseconds"));
    schema.setNullable(true);
    // Recorded in one step, so a halt that is reported is reported whole.
    schema.setRequired(List.of("index", "reason", "timestamp"));
    return schema;
  }

  /**
   * The persistent Raft log-write failure that has wedged this node, or null (issue #7872, publishing the #7037
   * signal that issue #7118 already gates readiness on).
   */
  private Schema<?> raftLogFailureSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        The persistent Raft log-write failure wedging this node, or null while the log writer is healthy. Present \
        on every answer. A non-null value means Ratis is rejecting every append, so the node can neither catch up \
        nor become caught up; the usual cause is a full Raft storage volume, and it clears by itself once the \
        health monitor restarts the writer in place.""");
    schema.addProperty("index", SpecBuilders.integer(
        "The Raft index of the entry whose write failed, or -1 when the failure was on a log segment"));
    schema.addProperty("cause", SpecBuilders.string(
        "The failure Ratis reported, as its own text, which routinely names the Raft storage path - so it is shown "
            + "to the root user only: another caller reads a placeholder here while still getting 'index' and "
            + "'timestamp'"));
    schema.addProperty("timestamp", SpecBuilders.integer("When it was first reported, as epoch milliseconds"));
    schema.setNullable(true);
    // Recorded in one step, so a failure that is reported is reported whole.
    schema.setRequired(List.of("index", "cause", "timestamp"));
    return schema;
  }

  /**
   * One cluster alert. Was {@code SpecBuilders.object("One cluster alert")} - a bare object, so the whole
   * operator-facing payload of this endpoint was unreachable through typed access (issue #7577).
   * <p>
   * The severity vocabulary is written out here rather than read from {@code ClusterAlerts.SEVERITY_*}, for the
   * same reason {@link #HA_RAFT_PATHS} is written out: the {@code ha-raft} module depends on {@code server} and
   * not the other way round. {@code RaftHAPluginAlertSchemaMatchesClusterAlertsTest}, over in that module where
   * both are visible, is what keeps the two the same set.
   */
  private Schema<?> alertSchema() {
    final Schema<String> severity = SpecBuilders.string("""
        How urgent the condition is. 'critical' means this node or the cluster is not serving correctly right \
        now, 'warning' that it will not keep serving correctly, 'info' that a declared configuration and the \
        live one differ without consequence yet.""");
    severity.setEnum(List.of("info", "warning", "critical"));

    final Schema<Object> alert = SpecBuilders.object("One cluster alert");
    alert.addProperty("id", SpecBuilders.string("""
        Stable identifier of the condition, e.g. 'lagging-followers' or 'local-resync-in-progress'. Key a \
        monitoring rule on this rather than on 'title', which is prose and may be reworded."""));
    alert.addProperty("severity", severity);
    alert.addProperty("title", SpecBuilders.string("One line naming the condition, for a dashboard row"));
    alert.addProperty("message", SpecBuilders.string("What is wrong, in full sentences"));
    alert.addProperty("recommendation", SpecBuilders.string("What an operator should do about it"));
    alert.addProperty("details", SpecBuilders.freeFormObject("""
        The condition's own data - the peers involved, the databases behind, the lag figures. An open map \
        because each 'id' carries its own keys; read it against the 'id', not blind."""));
    // Every alert is built as one chained expression, so an alert that exists exists whole.
    alert.setRequired(List.of("id", "severity", "title", "message", "recommendation", "details"));
    return alert;
  }

  /**
   * This node's own resync / WAL-gap quarantine state (issue #7136). The invariant it exists to expose is that
   * anything making {@code /api/v1/ready} answer 503 is visible in this document, so a client watching a rolling
   * restart reads the document rather than polling the probe.
   * <p>
   * This member carries the resync inputs of that invariant, which is all #7136 delivered. The two terminal ones
   * are {@code criticalHalt} and {@code raftLogFailure} (issue #7872): a node in either state has
   * {@code inProgress: false} here and is still pinned at 503, so a rule keyed on this member alone reads it as
   * healthy and waits forever.
   */
  private Schema<?> localResyncSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        This node's resync state. Present on every answer. The database names it carries are reduced to the \
        ones the caller is authorized on, so a caller scoped to one database cannot learn another tenant's \
        database name from a status poll.""");
    schema.addProperty("inProgress", SpecBuilders.bool("""
        True while a resync is holding this node out of the ready set. NOT the whole answer '/api/v1/ready' \
        gives: a node halted by a critical error or wedged by a log-write failure has this false and answers 503 \
        anyway, so read it together with 'criticalHalt' and 'raftLogFailure' (issue #7872)."""));
    schema.addProperty("snapshotDownloadQueued", SpecBuilders.bool("A snapshot install is waiting to start"));
    schema.addProperty("snapshotDownloadInProgress", SpecBuilders.bool("A snapshot is being installed now"));
    schema.addProperty("divergedDatabases", SpecBuilders.arrayOf(SpecBuilders.string("Database name"),
        "Databases quarantined because this node's WAL diverged from the leader's"));
    final Schema<String> cause = SpecBuilders.string("""
        Why this database was quarantined. 'WAL_VERSION_GAP' means an intermediate transaction never reached \
        this node; 'UNDECODABLE_LOG_ENTRY' a corrupt local log segment or an entry written by a newer node, \
        which is not a replication fault; 'APPLY_ERROR' an unexpected error while applying a committed entry; \
        'SNAPSHOT_INSTALL_INCOMPLETE' an install that did not reach the snapshot's index.""");
    cause.setEnum(List.of("WAL_VERSION_GAP", "UNDECODABLE_LOG_ENTRY", "APPLY_ERROR", "SNAPSHOT_INSTALL_INCOMPLETE"));
    // Declared since issue #7741 added it to the response. The vocabulary is written out here rather than read
    // from DivergenceCause for the same reason the alert severities are - this module cannot see ha-raft - and
    // Issue7577ClusterStatusSchemaMatchesTheHandlerTest over there is what keeps the two the same set.
    schema.addProperty("divergenceCauses", SpecBuilders.mapOf(cause,
        "Why each quarantined database was quarantined, keyed by database name. Same keys as 'divergedDatabases'"));
    schema.addProperty("snapshotAppliedFloor", SpecBuilders.integer(
        "Raft index the last installed snapshot brought this node to"));
    schema.addProperty("databaseAppliedFloors", SpecBuilders.mapOf(
        SpecBuilders.integer("Raft index this database has been brought to"),
        "Per-database applied floor, keyed by database name"));
    // Built as one chained expression by GetClusterHandler.buildLocalResync, so it is present whole.
    schema.setRequired(List.of("inProgress", "snapshotDownloadQueued", "snapshotDownloadInProgress",
        "divergedDatabases", "divergenceCauses", "snapshotAppliedFloor", "databaseAppliedFloors"));
    return schema;
  }

  private Schema<?> createAddPeerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Peer to add");
    schema.addProperty("peerId", SpecBuilders.string("Peer identifier"));
    schema.addProperty("address", SpecBuilders.string("Peer address"));
    schema.addProperty("name", SpecBuilders.string("Optional display name"));
    final Schema<Number> priority = SpecBuilders.integer(
        "Raft leader-election priority, a non-negative integer. Defaults to 0, which is Ratis's own default and "
            + "leaves the peer as electable as every other peer on a cluster where nobody names a priority. Once ANY "
            + "peer carries a positive priority the priority-0 ones become witnesses that are never elected and are "
            + "skipped as step-down targets, so 0 is how a witness is declared and a higher value how a preferred "
            + "leader is. A fractional value or one that does not fit in a 32-bit integer is refused rather than "
            + "rounded, because the value it would round to declares a witness. The same field the 'priority' of an "
            + "arcadedb.ha.serverList entry sets.");
    // The three facets exist so a generated client refuses what the handler refuses, instead of sending it and
    // reading a 400: PostAddPeerHandler.readPriority takes an explicit null as "not stated" and rejects a
    // negative value by name. A schema declaring only 'integer' would have done the opposite on both counts -
    // rejected a null it accepts, and passed a negative one it does not (issue #7523).
    priority.setNullable(true);
    priority.setDefault(0);
    priority.setMinimum(BigDecimal.ZERO);
    schema.addProperty("priority", priority);
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
    // 'result' is the one member every one of these routes writes; the other three say in their own
    // descriptions which action produces them (issue #7578).
    schema.setRequired(List.of("result"));
    return schema;
  }

  /**
   * The two shapes {@code PostVerifyDatabaseHandler} answers with, as a {@code oneOf} rather than as one object
   * whose every member is conditional.
   * <p>
   * A follower - or a leader answering a request another peer already forwarded - reports only its own
   * checksums; a leader answering a first-hand request fans out and returns only {@code result}. Declared as
   * one object, the two shapes share no member at all, so the schema could say nothing about what it always
   * sends and a client had to probe for keys. Split, each branch says exactly what it carries (issues #7577,
   * #7578).
   */
  private Schema<?> createVerifyResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Per-file checksums of one database, in one of two shapes. A follower - and a leader answering a request \
        a peer already forwarded - answers the local shape, carrying only its own checksums. A leader answering \
        a first-hand request fans out to every peer and answers the cluster shape, carrying only 'result'. The \
        two share no member, so read 'result' to tell them apart.""");
    schema.setType(null);
    schema.setOneOf(List.of(SpecBuilders.ref("VerifyDatabaseLocalResponse"),
        SpecBuilders.ref("VerifyDatabaseClusterResponse")));
    return schema;
  }

  private Schema<?> createVerifyLocalResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        One node's own checksums, for the leader to compare against. Answered by a follower, and by a leader \
        whose request carried the already-forwarded marker.""");
    schema.addProperty("localChecksums", SpecBuilders.mapOf(SpecBuilders.integer("CRC of the file's contents"),
        "File name to checksum map, for a quick cross-peer comparison"));
    schema.addProperty("files", SpecBuilders.arrayOf(verifiedFileSchema(), "Files with size and category"));
    schema.addProperty("localServer", SpecBuilders.string("Server the checksums were taken on"));
    schema.addProperty("sealedStoresIncluded", SpecBuilders.bool("""
        True when this build checksums the TimeSeries sealed stores as well (issue #7338). A peer on an older \
        build omits the flag, and the leader then leaves its sealed files out of the comparison rather than \
        reporting every one of them MISSING - a rolling upgrade must not make the divergence detector cry \
        divergence over a file the other side was never asked to checksum. Stays true even when \
        'sealedStoresComplete' is false: "my build checksums them" and "this answer covers them" are different \
        statements."""));
    schema.addProperty("sealedStoresComplete", SpecBuilders.bool("""
        Present and false when this answer did NOT cover every sealed store - an unreadable file, or a \
        compaction pause the node could not take. Absent when coverage was complete."""));
    schema.setRequired(List.of("localChecksums", "files", "localServer", "sealedStoresIncluded"));
    return schema;
  }

  private Schema<?> createVerifyClusterResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "A leader's cluster-wide comparison, fanned out to every peer");
    schema.addProperty("result", SpecBuilders.ref("VerifyDatabaseClusterResult"));
    schema.setRequired(List.of("result"));
    return schema;
  }

  /** One file of a verified database. */
  private Schema<?> verifiedFileSchema() {
    final Schema<Object> file = SpecBuilders.object("One database file");
    file.addProperty("name", SpecBuilders.string("File name"));
    file.addProperty("checksum", SpecBuilders.integer("CRC of the file's contents"));
    file.addProperty("size", SpecBuilders.integer("File size in bytes"));
    file.addProperty("type", SpecBuilders.string("File category"));
    file.setRequired(List.of("name", "checksum", "size", "type"));
    return file;
  }

  private Schema<?> createVerifyClusterResultSchema() {
    final Schema<Object> mismatch = SpecBuilders.object(
        "One file whose checksum differs between the leader and a peer");
    mismatch.addProperty("file", SpecBuilders.string("File name"));
    mismatch.addProperty("type", SpecBuilders.string("File category"));
    mismatch.addProperty("localChecksum", SpecBuilders.integer("Leader's CRC for the file"));
    mismatch.addProperty("remoteChecksum", SpecBuilders.string(
        "Peer's CRC for the file, or 'MISSING' when the peer does not have it"));
    mismatch.setRequired(List.of("file", "type", "localChecksum", "remoteChecksum"));

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
    // The three identifying members are on every row; the counters and the mismatch list say in their own
    // descriptions when they are not (issue #7578).
    peerResult.setRequired(List.of("peerId", "httpAddress", "status"));

    final Schema<Object> result = SpecBuilders.object(
        "Leader-only cluster-wide comparison, fanned out to every peer");
    result.addProperty("database", SpecBuilders.string("Database name"));
    result.addProperty("files", SpecBuilders.arrayOf(verifiedFileSchema(),
        "The leader's files with size and category"));
    result.addProperty("localServer", SpecBuilders.string("Leader server name"));
    result.addProperty("localPeerId", SpecBuilders.string("Leader's peer identifier"));
    result.addProperty("localChecksums", SpecBuilders.mapOf(SpecBuilders.integer("CRC of the file's contents"),
        "Leader's file name to checksum map"));
    result.addProperty("peers", SpecBuilders.arrayOf(peerResult, "Every other peer's comparison result"));
    result.addProperty("overallStatus", SpecBuilders.string(
        "ALL_CONSISTENT when every peer was compared and agreed, INCONSISTENCY_DETECTED when a compared peer "
            + "differs, VERIFICATION_INCOMPLETE when nothing diverged but at least one peer could not be verified"));
    result.addProperty("incompleteSealedStores", SpecBuilders.bool("""
        Present and true when the LEADER's own answer was short of a sealed store, so no peer comparison can be \
        complete: the leader compares its own keys. Absent when its coverage was complete."""));
    result.setRequired(List.of("database", "files", "localServer", "localPeerId", "localChecksums", "peers",
        "overallStatus"));
    return result;
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
    // A database that could not be read still reports a name, an empty fingerprint and -1, so the three are
    // written whatever happened (issue #7578).
    database.setRequired(List.of("name", "fingerprint", "lastTxId"));

    final Schema<Object> schema = SpecBuilders.object("Per-database bootstrap state of one peer");
    schema.addProperty("databases", SpecBuilders.arrayOf(database,
        "Databases on this peer. Empty when it holds none"));
    schema.addProperty("peerId", SpecBuilders.string("Peer that reported the state"));
    schema.setRequired(List.of("databases", "peerId"));
    return schema;
  }

  private Schema<?> createSecuritySeedRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("What the caller wants seeded, and what it already holds");
    schema.addProperty("reason", SpecBuilders.string(
        "Why the seed was asked for, for the leader's log line. Optional."));
    schema.addProperty("catchUp", SpecBuilders.bool(
        "True when the caller is a node repairing ITSELF after coming back, false (or absent) for a node "
            + "reporting on an admission it performed. What it changes is REUSE: a catch-up is never answered "
            + "by a seed that completed for somebody else, while an admission may be, since its request "
            + "follows the membership change that already seeded for it. A catch-up can still complete "
            + "without anything being submitted - that is what the fingerprint comparison is for, and it "
            + "answers upToDate before any seeder is asked."));
    schema.addProperty("fingerprints", createSecuritySeedFingerprintsSchema());
    return schema;
  }

  private Schema<?> createSecuritySeedFingerprintsSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "The caller's own document digests. When all three match the leader's, nothing is submitted and the "
            + "answer is upToDate. Omit them to have every document seeded, which is what an admission does - "
            + "the admitting node does not hold the joining peer's copies.");
    schema.addProperty("users", SpecBuilders.string("Digest of server-users.jsonl as the caller holds it"));
    schema.addProperty("groups", SpecBuilders.string("Digest of server-groups.json as the caller holds it"));
    schema.addProperty("apiTokens", SpecBuilders.string("Digest of server-api-tokens.json as the caller holds it"));
    return schema;
  }

  private Schema<?> createSecuritySeedResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("What the leader did about the request");
    schema.addProperty("upToDate", SpecBuilders.bool(
        "True when the caller's fingerprints already matched the leader's and nothing was submitted"));
    schema.addProperty("seeded", SpecBuilders.bool("True when the documents were submitted to the cluster"));
    schema.addProperty("failedSeeds", SpecBuilders.arrayOf(SpecBuilders.string("Document name"),
        "The documents that did not commit, empty when all of them did"));
    schema.addProperty("error", SpecBuilders.string(
        "Why the seed could not be run or its outcome could not be read. Present only on the 503 that carries "
            + "no failedSeeds, since in that case which documents failed is exactly what is not known."));
    // failedSeeds is not required: the "could not be run at all" 503 names the reason in `error` instead, and
    // claiming an empty list there would tell a client that nothing failed.
    schema.setRequired(List.of("seeded"));
    return schema;
  }

  private Schema<?> createPeerCapabilitiesResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("The wire-format sections one peer can decode");
    schema.addProperty("peerId", SpecBuilders.string(
        "Peer that answered. A caller must check this against the peer it meant to ask: on a cluster that "
            + "declares no explicit 'http' ports several peers can resolve to one address."));
    schema.addProperty("version", SpecBuilders.string("Server version of the answering peer, for operators; "
        + "nothing decides on it"));
    schema.addProperty("capabilities", SpecBuilders.arrayOf(SpecBuilders.string("Capability token"),
        "Capability tokens this peer can decode, sorted. Empty when it can decode none, never absent"));
    schema.setRequired(List.of("peerId", "version", "capabilities"));
    return schema;
  }
}
