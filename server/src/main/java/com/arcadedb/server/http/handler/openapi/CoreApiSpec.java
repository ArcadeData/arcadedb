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

import com.arcadedb.server.http.HttpSessionManager;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.Discriminator;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Documents the endpoints every deployment exposes: server information and administration, the
 * probes, database enumeration, the query and command data plane, and the explicit transaction
 * lifecycle.
 */
public class CoreApiSpec implements OpenApiContributor {
  /** Media type of the streaming query encoding (issue #7306). */
  private static final String NDJSON = "application/x-ndjson";
  private static final String SESSION_HEADER = HttpSessionManager.ARCADEDB_SESSION_ID;
  private static final String COMMIT_INDEX_HEADER = "X-ArcadeDB-Commit-Index";
  /**
   * The statuses of the query and command operations that are decided BEFORE the read-your-writes bookmark
   * exists, so they can never carry it. See {@link #addCommitIndexBookmarkHeader}.
   */
  private static final Set<String> BOOKMARKLESS_STATUSES = Set.of("401", "404");

  private static final String SESSION_REQUEST_DESCRIPTION = """
      Session id returned by 'beginTransaction'. Present it on every call that must run inside that \
      transaction, and on the commit or rollback that ends it. Omit it to run outside a transaction.""";

  private static final String BEGIN_SESSION_REQUEST_DESCRIPTION = """
      Normally omitted: 'beginTransaction' opens a new transaction and returns its own session id. \
      Supplying a session id here that still resolves to an open transaction does not start a nested \
      transaction; it makes the call fail with 409 instead.""";

  // Used only by commit and rollback: on those two, unlike query/command, omitting the header is
  // NOT a safe "run outside a transaction". requiresTransaction() is false and isTransactionActive()
  // is false on the fresh context, so the handler still answers 204 while committing or rolling back
  // nothing. A generated client that loses the session id would otherwise report success while the
  // caller's writes are gone.
  private static final String TRANSACTION_END_SESSION_REQUEST_DESCRIPTION = """
      Session id returned by 'beginTransaction', identifying the transaction to end. Omitting it, or \
      presenting an id that no longer resolves to an open transaction, is an idempotent no-op: the \
      call still answers 204, but commits or rolls back nothing.""";

  // Shared by the POST query and command operations, which both reach the stale-session branch of
  // DatabaseAbstractHandler.setTransactionInThreadLocal. Held in one place so the two cannot desync:
  // the quoted text is the message AbstractServerHttpHandler actually sends, so a client matching on
  // it needs both operations to describe it identically. GET query does NOT use this - its handler
  // overrides requiresTransaction() to false, so a stale id there degrades and answers 200.
  private static final String STALE_SESSION_404_DESCRIPTION =
      "Database not found, or the session id header names a transaction that no longer resolves "
          + "(\"Remote transaction session not found or expired\")";

  // Shared by all three operations that advertise 'application/x-ndjson' under their 200. PostQueryHandler
  // extends PostCommandHandler and overrides only executeCommand(), and GetQueryHandler - a sibling under
  // AbstractQueryHandler, not a subclass - calls the same relocated requireStreamableStatement() before its own
  // query runs, so the three reach one gate. Held in one place so they cannot describe it differently
  // (issue #7569 documented it for the two POST operations, issue #7571 brought GET under the same gate and the
  // same words).
  //
  // BACKUP DATABASE is named explicitly because it is the one statement a reader would otherwise expect to
  // stream: it is idempotent, it mutates no record, and it reads as a query everywhere else in the SQL
  // reference - it is refused here because its declared operation types report the archive it writes to the
  // server filesystem.
  private static final String NDJSON_READ_ONLY_DESCRIPTION = """
      When 'Accept' requests the ndjson encoding, only a statement provably read-only may stream: one that \
      writes - INSERT, UPDATE, DELETE, DDL, BACKUP DATABASE, or one this analysis cannot classify - is refused \
      with 400 before it runs, because a streamed response puts its status code on the wire ahead of the rows \
      and so cannot report a statement that fails half-way through. Request the buffered 'application/json' \
      encoding for it instead.""";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server", createServerPath());
    openAPI.getPaths().addPathItem("/api/v1/ready", createReadyPath());
    openAPI.getPaths().addPathItem("/api/v1/health", createHealthPath());
    openAPI.getPaths().addPathItem("/api/v1/databases", createDatabasesPath());
    openAPI.getPaths().addPathItem("/api/v1/exists/{database}", createExistsPath());
    openAPI.getPaths().addPathItem("/api/v1/query/{database}/{language}/{command}", createGetQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/query/{database}", createPostQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/command/{database}", createCommandPath());
    openAPI.getPaths().addPathItem("/api/v1/begin/{database}", createBeginPath());
    openAPI.getPaths().addPathItem("/api/v1/commit/{database}", createCommitPath());
    openAPI.getPaths().addPathItem("/api/v1/rollback/{database}", createRollbackPath());
    openAPI.getPaths().addPathItem("/api/v1/batch/{database}", createBatchPath());
    openAPI.getPaths().addPathItem("/api/v1/progress/{database}", createProgressPath());

    openAPI.getComponents().addSchemas("QueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("QueryResponse", createQueryResponseSchema());
    openAPI.getComponents().addSchemas("NdJsonQueryEvent", createNdJsonQueryEventSchema());
    openAPI.getComponents().addSchemas("CommandRequest", createCommandRequestSchema());
    openAPI.getComponents().addSchemas("ErrorResponse", createErrorResponseSchema());
    openAPI.getComponents().addSchemas("ServerInfo", createServerInfoSchema());
    openAPI.getComponents().addSchemas("DatabaseList", createDatabaseListSchema());
    openAPI.getComponents().addSchemas("DatabaseExists", createDatabaseExistsSchema());
    openAPI.getComponents().addSchemas("BatchResponse", createBatchResponseSchema());
    openAPI.getComponents().addSchemas("BatchError", createBatchErrorSchema());
    openAPI.getComponents().addSchemas("ProgressResponse", createProgressResponseSchema());
    openAPI.getComponents().addSchemas("NdJsonBatchEvent", createNdJsonBatchEventSchema());
    openAPI.getComponents().addSchemas("BatchLine", createBatchLineSchema());
    openAPI.getComponents().addSchemas("BatchVertexLine", createBatchVertexLineSchema());
    openAPI.getComponents().addSchemas("BatchEdgeLine", createBatchEdgeLineSchema());
  }

  private PathItem createServerPath() {
    final PathItem pathItem = new PathItem();

    // GET /api/v1/server
    final Operation getOp = new Operation();
    getOp.setSummary("Get server information");
    getOp.setDescription("Retrieves server status, version, and configuration information");
    getOp.setOperationId("getServerInfo");
    getOp.addTagsItem("Server");
    getOp.setResponses(createServerGetResponses());
    pathItem.setGet(getOp);

    // POST /api/v1/server
    final Operation postOp = new Operation();
    postOp.setSummary("Execute server command");
    postOp.setDescription("""
        Executes administrative commands on the server (root user only). \
        Available commands: create database, drop database, open database, close database, \
        restore database <name> <url>, import database <name> <url>, \
        create user, drop user, shutdown, set server setting, get server events, align database, \
        connect cluster <address>, disconnect cluster. \
        Both restore and import support SSE progress streaming via Accept: text/event-stream header. \
        connect cluster <address> adds the server at <address> to this server's cluster - the operator \
        alias of POST /api/v1/cluster/peer - where <address> is one entry of arcadedb.ha.serverList \
        ([name@]host[:raftPort[:httpPort]] or the host:{raft:..,http:..} object form). It answers 400 \
        for a blank or malformed address and 500 when this server is not running an HA implementation \
        that supports runtime membership""");
    postOp.setOperationId("executeServerCommand");
    postOp.addTagsItem("Server");
    postOp.setRequestBody(SpecBuilders.jsonBody("Command request with command and optional parameters", "CommandRequest", true));
    postOp.setResponses(createCommandResponses());
    // Only this operation forwards to the HA leader, so the 504 is added here rather than in the shared
    // createCommandResponses() that POST /api/v1/command/{database} also uses (issue #7507).
    postOp.getResponses().addApiResponse("504", SpecBuilders.errorResponse(SpecBuilders.LEADER_FORWARD_TIMEOUT_DESCRIPTION));
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createReadyPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("Check server readiness");
    getOp.setDescription("Health check endpoint to verify if the server is ready to accept requests");
    getOp.setOperationId("checkReady");
    getOp.addTagsItem("Health");
    getOp.setResponses(createReadyResponses());
    SpecBuilders.publicOperation(getOp);
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createHealthPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("Check server liveness");
    getOp.setDescription(
        "Liveness probe: returns 204 when the server process and HTTP layer are up. Performs no database I/O and requires no authentication.");
    getOp.setOperationId("checkHealth");
    getOp.addTagsItem("Health");
    getOp.setResponses(createHealthResponses());
    SpecBuilders.publicOperation(getOp);
    pathItem.setGet(getOp);

    return pathItem;
  }

  private ApiResponses createHealthResponses() {
    final ApiResponses responses = new ApiResponses();

    // Liveness only ever responds with 204 when reachable; it never returns 503 (unlike readiness).
    final ApiResponse liveResponse = new ApiResponse();
    liveResponse.setDescription("Server process and HTTP layer are up");
    responses.addApiResponse("204", liveResponse);

    return responses;
  }

  private PathItem createDatabasesPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List databases");
    getOp.setDescription("Retrieves a list of all available databases");
    getOp.setOperationId("listDatabases");
    getOp.addTagsItem("Database");
    getOp.setResponses(createDatabasesResponses());
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createExistsPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("Check database existence");
    getOp.setDescription("Checks if a database exists");
    getOp.setOperationId("checkDatabaseExists");
    getOp.addTagsItem("Database");
    getOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    getOp.setResponses(createExistsResponses());
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createGetQueryPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("Execute query via GET");
    getOp.setDescription("Executes a query using GET method with parameters in URL. " + NDJSON_READ_ONLY_DESCRIPTION);
    getOp.setOperationId("executeQueryGet");
    getOp.addTagsItem("Query");
    getOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    getOp.addParametersItem(SpecBuilders.pathParam("language", "Query language (sql, cypher, gremlin, graphql, mongo)",
        List.of("sql", "cypher", "gremlin", "graphql", "mongo")));
    getOp.addParametersItem(SpecBuilders.pathParam("command", "Query or command to execute"));
    getOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
    getOp.addParametersItem(ndJsonAcceptParam());
    getOp.setResponses(createGetQueryResponses());
    addNdJsonAlternative(getOp.getResponses());
    addCommitIndexBookmarkHeader(getOp.getResponses());
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createPostQueryPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Execute query via POST");
    postOp.setDescription("Executes a query using POST method with query in request body. " + NDJSON_READ_ONLY_DESCRIPTION);
    postOp.setOperationId("executeQueryPost");
    postOp.addTagsItem("Query");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
    postOp.addParametersItem(ndJsonAcceptParam());
    postOp.setRequestBody(SpecBuilders.jsonBody("Query request with command and optional parameters", "QueryRequest", true));
    postOp.setResponses(createQueryResponses());
    addNdJsonAlternative(postOp.getResponses());
    addCommitIndexBookmarkHeader(postOp.getResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createCommandPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Execute command");
    postOp.setDescription("Executes a database command. " + NDJSON_READ_ONLY_DESCRIPTION);
    postOp.setOperationId("executeCommand");
    postOp.addTagsItem("Command");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, SESSION_REQUEST_DESCRIPTION, false));
    postOp.addParametersItem(ndJsonAcceptParam());
    postOp.setRequestBody(SpecBuilders.jsonBody("Command request with command and optional parameters", "CommandRequest", true));
    postOp.setResponses(createCommandResponses());
    addNdJsonAlternative(postOp.getResponses());
    addCommitIndexBookmarkHeader(postOp.getResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createBeginPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Begin transaction");
    postOp.setDescription("Begins a new transaction");
    postOp.setOperationId("beginTransaction");
    postOp.addTagsItem("Transaction");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, BEGIN_SESSION_REQUEST_DESCRIPTION, false));
    postOp.setResponses(createBeginResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createCommitPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Commit transaction");
    postOp.setDescription("Commits the current transaction");
    postOp.setOperationId("commitTransaction");
    postOp.addTagsItem("Transaction");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, TRANSACTION_END_SESSION_REQUEST_DESCRIPTION, false));
    postOp.setResponses(createTransactionResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createRollbackPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Rollback transaction");
    postOp.setDescription("Rolls back the current transaction");
    postOp.setOperationId("rollbackTransaction");
    postOp.addTagsItem("Transaction");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.addParametersItem(SpecBuilders.headerParam(SESSION_HEADER, TRANSACTION_END_SESSION_REQUEST_DESCRIPTION, false));
    postOp.setResponses(createTransactionResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createBatchPath() {
    final Operation post = SpecBuilders.operation("executeBatch", "Batch",
        "Bulk-load vertices and edges",
        """
            Streams vertices and then edges into the database using the GraphBatch API. The body is \
            never buffered, so a load is bounded by the server's memory only through the batching \
            parameters below.

            A batch is NOT atomic: GraphBatch commits every 'commitEvery' records, so a failure \
            mid-stream leaves earlier chunks durably committed. A client-input failure answers 400 \
            with 'verticesCreated', 'edgesCreated', and a 'partialCommit' flag; those counts are the \
            records attempted before the failure and are an upper bound on what is durable. Because \
            temporary ids are not keys, retrying the whole payload duplicates already-committed \
            vertices.

            A body that ends before its announced length answers 408 with the same partial-commit \
            counts, never a 200 with a truncated count. Compare the returned 'bytesRead' against the \
            bytes sent to verify a chunked upload arrived whole.

            Send 'Accept: application/x-ndjson' to be acknowledged while you are still uploading. The \
            answer is then a newline-delimited stream: a 'progress' line at every vertex commit and \
            every 'commitEvery' edges, then exactly one 'summary' or 'error' line carrying the same \
            object this endpoint would otherwise have returned. That is the HTTP counterpart of the \
            per-chunk acknowledgement of the gRPC InsertBidirectional RPC. A progress line counts \
            records attempted, the same upper bound the partial-commit counters carry. Anything else \
            in Accept, including an absent header, returns the buffered object unchanged.

            A load that fails before it has acknowledged anything still answers with its real status \
            code and the buffered error body, because the status line has not been sent yet: the 400 \
            and 408 below apply to a streaming request too. Only a failure raised after the first \
            progress line is reported in band under a 200.""");

    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.addParametersItem(batchNdJsonAcceptParam());
    post.addParametersItem(SpecBuilders.queryParam("batchSize",
        "Records buffered per GraphBatch flush. Default 100000.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("lightEdges",
        "Create edges without their own record. Default false.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("wal",
        "Write through the WAL. Default false.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("parallelFlush",
        "Flush buckets in parallel. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("preAllocateEdgeChunks",
        "Pre-allocate edge chunks. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("edgeListInitialSize",
        "Initial edge-list size in bytes. Default 2048.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("bidirectional",
        "Create edges in both directions. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("commitEvery",
        """
            Records written per transaction during the edge flush. On a replicated database that \
            transaction becomes one Raft entry, so lower this when the server warns that an entry \
            approaches the maximum entry size. Default 50000.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedEdgeCount",
        "Hint for the number of edges in the payload. Default 0.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("commitRetries",
        """
            Retries of a vertex-creation commit that fails with a transient retryable error, such as \
            a Raft leader re-election, so a cluster hiccup does not abort a streaming load. \
            Default 10.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("commitRetryDelayMs",
        "Initial back-off before the first commit retry. Default 1000.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("vertexBatchSize",
        """
            Vertices accumulated before they are created and committed in one transaction. On a \
            replicated database that transaction becomes one Raft entry. Default 10000.""",
        false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedVertexCount",
        """
            Hint used to pre-size the vertex references, saving the copies of their growth. Only a \
            hint: the payload may carry more. Default 0.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedRecords",
        """
            How many records, vertices plus edges, the payload carries. When given, a load that ends \
            with a different count is reported as incomplete instead of successful. This is the only \
            way to catch a chunked upload that stopped early, since a chunked body announces no \
            length.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("ordinalBase",
        """
            With refMode=ordinal, the position of the first vertex of this payload. A client that \
            splits one load across several requests keeps a single counter across all of them. \
            Positions below the base belong to an earlier request and must be referenced by RID. \
            Default 0.""", false, "integer"));

    final Parameter idMapping = SpecBuilders.queryParam("idMapping",
        """
            Whether the response echoes the temporary-id to RID mapping. 'auto' echoes it only below \
            10000 entries, because a larger mapping would hold a second full copy of the map as JSON \
            in one string.""", false);
    idMapping.getSchema().setEnum(List.of("auto", "true", "false"));
    post.addParametersItem(idMapping);

    final Parameter refMode = SpecBuilders.queryParam("refMode",
        """
            How edges name the vertices they connect. 'id' resolves @from and @to against the @id each \
            vertex declared, costing the id plus a hash slot per vertex. 'ordinal' resolves them \
            against the 0-based position of the vertex in the payload, storing no id at all.""",
        false);
    refMode.getSchema().setEnum(List.of("id", "ordinal"));
    post.addParametersItem(refMode);

    post.setRequestBody(createBatchRequestBody());

    // 400 and 408 carry the partial-commit counts rather than the generic error body: a batch is
    // not atomic, so a client that cannot read how much was committed cannot reconcile before
    // retrying. Every other failure keeps the base handler's standard error shape.
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Load completed", "BatchResponse"));
    // The streaming encoding answers 200 for a FAILED load too: by the time the verdict is reached the status
    // line is already sent, so the failure is the terminal 'error' line and its 'status' field instead.
    final MediaType ndjsonBatch = new MediaType();
    ndjsonBatch.setSchema(SpecBuilders.ref("NdJsonBatchEvent"));
    responses.get("200").getContent().addMediaType(NDJSON, ndjsonBatch);
    responses.addApiResponse("400", SpecBuilders.jsonResponse("""
        Client-input failure, with the counts attempted before it. Also the answer to a line that used a reserved \
        key: an '@'-prefixed key outside the five control keys, or a 'properties' key carrying an object.""",
        "BatchError"));
    responses.addApiResponse("408", SpecBuilders.jsonResponse(
        "The body ended before it was fully consumed, with the counts attempted before that", "BatchError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("409", SpecBuilders.errorResponse(
        "Concurrent modification: a page the load touched changed underneath it"));
    responses.addApiResponse("413", SpecBuilders.errorResponse("Request body too large"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "Service unavailable: on a replicated database, no leader was reachable"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createProgressPath() {
    final Operation get = SpecBuilders.operation("getOperationProgress", "Server",
        "List in-progress maintenance operations",
        """
            Returns the long-running maintenance operations currently running on this server for one \
            database, with their step-by-step progress. Reads a lock-free snapshot: no database \
            access and no transaction, so polling at any frequency is safe and cannot interfere with \
            the operation being watched.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Progress snapshot", "ProgressResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private ApiResponses createServerGetResponses() {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Server information retrieved successfully", "ServerInfo"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createReadyResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse readyResponse = new ApiResponse();
    readyResponse.setDescription("Server is ready to accept requests");
    responses.addApiResponse("204", readyResponse);

    final ApiResponse notReadyResponse = new ApiResponse();
    notReadyResponse.setDescription("""
        Server is not ready: it has not finished starting, has not yet joined the Raft group, \
        or has not caught up on replication""");
    responses.addApiResponse("503", notReadyResponse);

    return responses;
  }

  private ApiResponses createDatabasesResponses() {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("List of databases retrieved successfully", "DatabaseList"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createExistsResponses() {
    final ApiResponses responses = new ApiResponses();

    responses.addApiResponse("200", SpecBuilders.jsonResponse("""
        Whether the database exists and is visible to the authenticated user""", "DatabaseExists"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Missing database parameter"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));

    return responses;
  }

  // GetQueryHandler.requiresTransaction() returns false, so a stale session id on GET query degrades
  // session-less (DatabaseAbstractHandler.setTransactionInThreadLocal) and answers 200, never 404. The
  // POST endpoint has no such override, so its 404 does cover the stale-session case.
  private ApiResponses createQueryResponses() {
    return createQueryResponses(true);
  }

  private ApiResponses createGetQueryResponses() {
    return createQueryResponses(false);
  }

  private ApiResponses createQueryResponses(final boolean sessionAware) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Query executed successfully", "QueryResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse(sessionAware
        ? STALE_SESSION_404_DESCRIPTION
        : "Database not found"));
    responses.addApiResponse("413", SpecBuilders.errorResponse(
        "The result exceeds 'arcadedb.server.httpQueryMaxResultRows': narrow or page the query"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createCommandResponses() {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Command executed successfully", "QueryResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse(STALE_SESSION_404_DESCRIPTION));
    responses.addApiResponse("413", SpecBuilders.errorResponse(
        "The result exceeds 'arcadedb.server.httpQueryMaxResultRows': narrow or page the command"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createTransactionResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Transaction operation completed successfully");
    responses.addApiResponse("204", successResponse);

    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));

    return responses;
  }

  /**
   * The transaction responses plus the session-id header that 'begin' alone returns, and the 409
   * PostBeginHandler returns when the request carries a session id that still resolves. Built from a
   * fresh {@link #createTransactionResponses()} every call, so neither addition lands on the shared
   * instance that commit and rollback also use.
   */
  private ApiResponses createBeginResponses() {
    final ApiResponses responses = createTransactionResponses();
    responses.get("204").addHeaderObject(SESSION_HEADER, SpecBuilders.stringHeader("""
        Session id identifying the transaction just opened. Present it on the '%s' request header \
        of every subsequent call that belongs to this transaction.""".formatted(SESSION_HEADER)));
    responses.addApiResponse("409", SpecBuilders.errorResponse(
        "A transaction is already open on this session"));
    return responses;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Query request object");
    schema.addProperty("command", SpecBuilders.string("Query or command to execute"));
    schema.addProperty("language", SpecBuilders.string("Query language").example("sql"));
    schema.addProperty("params", SpecBuilders.object(
        """
        Query parameters. Values may be JSON primitives, arrays, or typed-marker objects: \
        {"$bytes": "<base64>"} for byte[] (standard or URL-safe base64), \
        {"$int8": [v0, v1, ...]} for byte[] from integers in [-128, 127] (used to send \
        INT8-encoded vectors to LSM_VECTOR indexes without a float32 round-trip)."""));
    schema.addProperty("serializer", SpecBuilders.string("Response serializer").example("json"));
    schema.addProperty("limit", SpecBuilders.integer(
        """
        Maximum number of rows to serialize into the response. When omitted, a LIMIT stated by the query is \
        honored as written and only a query stating none is capped by the server default \
        ('arcadedb.server.httpQueryDefaultLimit'). Use -1 for no cap. The response always reports the cap \
        that was applied ('limit'), how many rows it carries ('returned') and whether rows were left \
        behind ('truncated'). No value here can widen a single response past the server's hard ceiling \
        ('arcadedb.server.httpQueryMaxResultRows'): a result that would exceed it is refused with 413 \
        instead of being truncated.""").example(100));
    schema.setRequired(List.of("command"));
    return schema;
  }

  private Schema<?> createCommandRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Command request object");
    schema.addProperty("command", SpecBuilders.string("Command to execute"));
    schema.addProperty("language", SpecBuilders.string("Command language").example("sql"));
    schema.addProperty("params", SpecBuilders.object(
        """
        Command parameters. Values may be JSON primitives, arrays, or typed-marker objects: \
        {"$bytes": "<base64>"} for byte[] (standard or URL-safe base64), \
        {"$int8": [v0, v1, ...]} for byte[] from integers in [-128, 127] (used to send \
        INT8-encoded vectors to LSM_VECTOR indexes without a float32 round-trip)."""));
    // PostQueryHandler extends PostCommandHandler and overrides only executeCommand(), not execute(), so
    // both /api/v1/query/{database} and /api/v1/command/{database} honor 'limit' identically (issue #6584).
    schema.addProperty("limit", SpecBuilders.integer(
        """
        Maximum number of rows to serialize into the response. When omitted, a LIMIT stated by the query is \
        honored as written and only a query stating none is capped by the server default \
        ('arcadedb.server.httpQueryDefaultLimit'). Use -1 for no cap. The response always reports the cap \
        that was applied ('limit'), how many rows it carries ('returned') and whether rows were left \
        behind ('truncated'). No value here can widen a single response past the server's hard ceiling \
        ('arcadedb.server.httpQueryMaxResultRows'): a result that would exceed it is refused with 413 \
        instead of being truncated.""").example(100));
    schema.setRequired(List.of("command", "language"));
    return schema;
  }

  private Schema<?> createQueryResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Query response object");
    schema.addProperty("result", SpecBuilders.arrayOf(SpecBuilders.object(null), "Query results"));
    schema.addProperty("limit", SpecBuilders.integer(
        """
        Effective row cap applied while serializing, -1 when uncapped. This is the serializer's cap, not the \
        query's own LIMIT: a query stating a LIMIT below the server default reports the default here, and \
        'returned' with 'truncated' describe what the response actually carries."""));
    // 'executionTime' and 'recordCount' used to be documented here but no handler has ever emitted them:
    // 'returned' is the real row count, and timings are reported under 'profile' when profileExecution is set.
    schema.addProperty("returned", SpecBuilders.integer(
        """
        Number of rows carried by this response. With the 'graph' serializer, whose cap counts graph elements \
        rather than rows, it is the number of serialized vertices plus edges, and it can exceed 'limit': a \
        single row can expand into several elements, and the expansion of the row that reaches the cap is not \
        cut in half."""));
    schema.addProperty("truncated", SpecBuilders.bool(
        "True when the cap stopped the serialization with rows still pending, so the response is incomplete"));
    return schema;
  }

  /**
   * One line of the {@code application/x-ndjson} streaming encoding (issue #7306). Every line is an object with
   * exactly one key naming the kind of event, which is what makes the stream self-delimiting: a consumer can
   * tell a row from the trailer without guessing, and a stream that ends with no {@code stats} line is one that
   * did not complete.
   */
  private Schema<?> createNdJsonQueryEventSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        One line of a newline-delimited streaming response. Exactly one of 'record', 'stats' or 'error' is \
        present.""");
    schema.addProperty("record", SpecBuilders.object("""
        One result row, identical to an element of the 'result' array of the buffered application/json \
        response."""));

    final Schema<Object> stats = SpecBuilders.object("""
        Trailer, always the last line of a complete stream. Carries the same three numbers the buffered \
        response reports at top level.""");
    stats.addProperty("limit", SpecBuilders.integer("Effective row cap applied while streaming, -1 when uncapped"));
    stats.addProperty("returned", SpecBuilders.integer("Number of rows that reached the client"));
    stats.addProperty("truncated", SpecBuilders.bool(
        "True when the cap stopped the stream with rows still pending, so the result is incomplete"));
    schema.addProperty("stats", stats);

    final Schema<Object> error = SpecBuilders.object("""
        A failure raised after the 200 had already been sent. The status code cannot be taken back at that \
        point, so the failure is reported in band and no 'stats' line follows.""");
    error.addProperty("message", SpecBuilders.string("Why the stream failed"));
    schema.addProperty("error", error);
    return schema;
  }

  /**
   * Adds the streaming encoding to a 200 that already documents the buffered one. Negotiated by {@code Accept}
   * rather than routed, so the buffered body every existing client parses is what a request that does not ask
   * for the stream still receives (issue #7306).
   */
  private static void addNdJsonAlternative(final ApiResponses responses) {
    final MediaType ndjson = new MediaType();
    ndjson.setSchema(SpecBuilders.ref("NdJsonQueryEvent"));
    responses.get("200").getContent().addMediaType(NDJSON, ndjson);
  }

  /**
   * Declares the read-your-writes bookmark on the responses that can actually carry it, which is more than the
   * 200 and less than all of them.
   * <p>
   * It is emitted on both encodings - on the streamed one before the first row, since a header cannot be set
   * once the body has started (issue #7351) - so it is a property of the response rather than of either media
   * type. And it is emitted whatever the outcome, because the value means the same thing on a refused request:
   * what this server had applied when it answered is a valid barrier for the client's next read either way.
   * <p>
   * The boundary is <b>where the bookmark starts existing</b>, not the status code.
   * {@link AbstractServerHttpHandler#emitCommitIndexBookmarkOnResponseCommit} is registered partway through
   * {@link DatabaseAbstractHandler#execute}, once the request has been authenticated and its database
   * resolved - deliberately, since registering it earlier would hand a Raft index to a caller who has not
   * authenticated. So a failure raised before that point carries no bookmark and never can:
   * <ul>
   * <li>{@code 401} is produced by {@code handleRequest} before the request is dispatched at all;</li>
   * <li>{@code 404} on these operations means "database not found" or a stale session id, both of which are
   *     resolved before the registration.</li>
   * </ul>
   * Those two are therefore left undeclared rather than promised and not delivered - the same mismatch, only
   * pointing the other way (#7425 review). The rest - {@code 200}, {@code 413}, {@code 500}, and the
   * {@code 400} raised by the bookmark-header parsing itself - are answered from inside the request, so the
   * header rides along.
   */
  private static void addCommitIndexBookmarkHeader(final ApiResponses responses) {
    for (final Map.Entry<String, ApiResponse> entry : responses.entrySet()) {
      if (BOOKMARKLESS_STATUSES.contains(entry.getKey()))
        continue;
      // A new Header per response rather than one shared instance: aliasing them would make a later per-status
      // tweak to one silently rewrite the others (#7425 review).
      entry.getValue().addHeaderObject(COMMIT_INDEX_HEADER, SpecBuilders.stringHeader("""
          On a replicated (HA) database, the last Raft index this server had applied when it answered. Feed it \
          back as 'X-ArcadeDB-Read-After' on the next request to get read-your-writes consistency from a \
          follower. Sent on an error response too, once the request reached the database: it bookmarks what the \
          server had applied when it refused, which is still a valid barrier for the next read. Absent on a \
          standalone database, on a replicated one that has applied nothing yet, and on a failure that happens \
          before the request reaches the database at all.\
          """));
    }
  }

  /**
   * The {@code Accept} header that selects the streaming encoding. Declared as an explicit parameter as well as
   * a response content type because a generated client otherwise has no way to ask for it.
   */
  private static Parameter ndJsonAcceptParam() {
    final Parameter accept = SpecBuilders.headerParam("Accept", """
        Send 'application/x-ndjson' to receive the result as a stream of newline-delimited JSON events, one row \
        per line, flushed as the engine produces them instead of buffered in full server-side. Anything else - \
        including an absent header - returns the buffered application/json body unchanged.""", false);
    accept.getSchema().setEnum(List.of(SpecBuilders.JSON, NDJSON));
    return accept;
  }

  private Schema<?> createErrorResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Error response object");
    schema.addProperty("error", SpecBuilders.string("Error message"));
    schema.addProperty("detail", SpecBuilders.string("Error details"));
    schema.addProperty("exception", SpecBuilders.string("Exception class name"));
    schema.addProperty("exceptionArgs", SpecBuilders.string("Exception arguments"));
    schema.addProperty("help", SpecBuilders.string("Help information"));
    return schema;
  }

  private Schema<?> createServerInfoSchema() {
    final Schema<Object> schema = SpecBuilders.object("Server information object");
    schema.addProperty("version", SpecBuilders.string("Server version"));
    schema.addProperty("status", SpecBuilders.string("Server status"));
    schema.addProperty("mode", SpecBuilders.string("Server mode"));
    schema.addProperty("uptime", SpecBuilders.integer("Server uptime in milliseconds"));
    return schema;
  }

  private Schema<?> createDatabaseListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Database list response");
    schema.addProperty("result", SpecBuilders.arrayOf(SpecBuilders.string(null), "List of database names"));
    return schema;
  }

  private Schema<?> createDatabaseExistsSchema() {
    final Schema<Object> schema = SpecBuilders.object("Database existence check result");
    schema.addProperty("result", SpecBuilders.bool("""
        True when the database exists and is among the authenticated user's authorized databases. \
        False both when the database does not exist and when it exists but the caller is not \
        authorized to see it, since the response does not distinguish the two cases."""));
    return schema;
  }

  /**
   * The payload of a bulk load (issue #7570). All three media types used to be declared as a bare {@code string}, so
   * none of the five control keys appeared anywhere in the contract and every client in every language had to
   * reverse-engineer the encoding - while the gRPC sibling {@code GraphBatchRecord} had been schematized since it
   * shipped.
   * <p>
   * The JSON line encodings name the line schema rather than a string, which is the same convention this spec
   * already uses for its NDJSON <em>responses</em> ({@code NdJsonQueryEvent}, {@code NdJsonBatchEvent}): OpenAPI 3.0
   * cannot say "newline-delimited instances of this schema" for a body, so the media-type schema is the schema of one
   * line and the description carries the line orientation.
   */
  private RequestBody createBatchRequestBody() {
    final RequestBody body = new RequestBody();
    body.setDescription("""
        Vertices first, then edges. JSONL sends one JSON record per line; CSV sends a header row \
        followed by data rows. Vertices may declare a temporary '@id' that edges reference through \
        '@from' and '@to', or be referenced by position when refMode=ordinal. Edges may also \
        reference existing RIDs in #bucket:position form.

        The JSON schema below describes ONE LINE: the body is a sequence of them separated by newlines, not a JSON \
        array, and a line that is an array is refused as such.

        Properties sit FLAT beside the control keys - {"@type":"vertex","@class":"Person","name":"Alice"} - and are \
        NOT nested under a 'properties' object. The '@' prefix is reserved: a key starting with '@' that is not one \
        of @type, @class, @id, @from or @to is refused with a 400 naming the line, and so is a 'properties' key \
        carrying an object, because both can only ever be a misread of this encoding.

        The control keys and the '@type' values are matched case-sensitively: '@Type' is not '@type' and is refused \
        as an unknown control key, and 'Vertex' is not 'vertex'. Only the CSV boolean literals 'true' and 'false' \
        are matched ignoring case.

        A temporary id is resolved only within the request that declared it, and only if the vertex appeared \
        earlier in the same payload: a vertex loaded by an EARLIER request has to be referenced by RID \
        (#bucket:position). Under refMode=ordinal, use 'ordinalBase' to keep one position counter across a load \
        split into several requests.""");
    body.setRequired(true);

    final Content content = new Content();
    for (final String jsonLineMediaType : List.of(NDJSON, "application/jsonl")) {
      final MediaType jsonl = new MediaType();
      jsonl.setSchema(SpecBuilders.ref("BatchLine"));
      jsonl.setExample("""
          {"@type":"vertex","@class":"Person","@id":"p1","name":"Alice"}
          {"@type":"vertex","@class":"Person","@id":"p2","name":"Bob"}
          {"@type":"edge","@class":"Knows","@from":"p1","@to":"p2","since":2020}""");
      content.addMediaType(jsonLineMediaType, jsonl);
    }

    final MediaType csv = new MediaType();
    csv.setSchema(new Schema<>().type("string").description("""
        A header row naming the columns, then one data row per record. The control keys are column names: @type and \
        @class are required, @id names a vertex's temporary id, and @from and @to name an edge's endpoints. Every \
        other column is a property, and the '@' prefix is reserved there too - an unrecognised '@' column is refused \
        with a 400. A '---' row separates the vertex section from the edge section, and a new header row follows it. \
        Values are typed by inspection: 'true'/'false' become booleans, numeric text becomes a number, an empty \
        field sets no property at all. Quoting follows RFC 4180, single-line fields only."""));
    csv.setExample("""
        @type,@class,@id,name
        vertex,Person,p1,Alice
        vertex,Person,p2,Bob
        ---
        @type,@class,@from,@to,since
        edge,Knows,p1,p2,2020""");
    content.addMediaType("text/csv", csv);

    body.setContent(content);
    return body;
  }

  /**
   * One line of the JSON batch encoding: a vertex or an edge, told apart by {@code @type}. The discriminator maps the
   * short spellings as well, because the parsers accept {@code v} and {@code e} and a generated client that only knew
   * the long ones would reject its own valid payloads.
   */
  private Schema<?> createBatchLineSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        One line of the JSON batch encoding. Vertices must appear before the edges that reference them.""");
    schema.setType(null);
    schema.setOneOf(List.of(SpecBuilders.ref("BatchVertexLine"), SpecBuilders.ref("BatchEdgeLine")));

    final Discriminator discriminator = new Discriminator();
    discriminator.setPropertyName("@type");
    discriminator.mapping("vertex", "#/components/schemas/BatchVertexLine");
    discriminator.mapping("v", "#/components/schemas/BatchVertexLine");
    discriminator.mapping("edge", "#/components/schemas/BatchEdgeLine");
    discriminator.mapping("e", "#/components/schemas/BatchEdgeLine");
    schema.setDiscriminator(discriminator);
    return schema;
  }

  private Schema<?> createBatchVertexLineSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        A vertex line. Its properties are the keys of this same object, flat beside the control keys below - they are \
        NOT nested under a 'properties' key, and sending one carrying an object is refused with a 400.""");
    schema.addProperty("@type", SpecBuilders.string("Discriminator. 'v' is accepted as a synonym of 'vertex'")
        ._enum(List.of("vertex", "v")));
    schema.addProperty("@class", SpecBuilders.string("""
        Vertex type to create the record in. The type must already exist: a bulk load creates records, never \
        types."""));
    schema.addProperty("@id", SpecBuilders.string("""
        Temporary id, resolved only against the edges of THIS request. Optional - a vertex needs one only if an edge \
        in the same payload references it, and one that declares none is counted in 'verticesWithoutId'. Ignored \
        under refMode=ordinal, where an edge names a vertex by its 0-based position instead."""));
    schema.setRequired(List.of("@type", "@class"));
    addFlatPropertyPolicy(schema);
    return schema;
  }

  private Schema<?> createBatchEdgeLineSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        An edge line. Its properties are the keys of this same object, flat beside the control keys below - they are \
        NOT nested under a 'properties' key, and sending one carrying an object is refused with a 400.""");
    schema.addProperty("@type", SpecBuilders.string("Discriminator. 'e' is accepted as a synonym of 'edge'")
        ._enum(List.of("edge", "e")));
    schema.addProperty("@class", SpecBuilders.string("""
        Edge type to create the record in. The type must already exist: a bulk load creates records, never \
        types."""));
    schema.addProperty("@from", SpecBuilders.string("""
        Source vertex. Under refMode=id (the default) this is the '@id' a vertex declared EARLIER IN THIS PAYLOAD, \
        or an existing RID in #bucket:position form; each request resolves only the ids of its own payload. Under \
        refMode=ordinal it is the vertex's 0-based position, offset by 'ordinalBase'."""));
    schema.addProperty("@to", SpecBuilders.string("Destination vertex, named the same way as '@from'"));
    schema.setRequired(List.of("@type", "@class", "@from", "@to"));
    addFlatPropertyPolicy(schema);
    return schema;
  }

  /**
   * Declares that any other key of the line is a property. This is what makes the encoding schemaless, and it is also
   * what made the reserved-key refusals necessary: without a rule, a control key the loader does not understand is
   * indistinguishable from a property whose name happens to start with '@' (issue #7570).
   */
  private void addFlatPropertyPolicy(final Schema<Object> schema) {
    schema.setAdditionalProperties(SpecBuilders.object("""
        One property of the record, keyed by its name. Any JSON scalar, array or nested object; a nested object is \
        stored as an embedded document. The name may not start with '@' - that prefix is reserved for the control \
        keys - and a property named 'properties' may not carry an object, because that is the nested-form misreading \
        rather than data.""").type(null));
  }

  private Schema<?> createBatchResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Result of a bulk load");
    schema.addProperty("verticesCreated", SpecBuilders.integer("Vertices created"));
    schema.addProperty("edgesCreated", SpecBuilders.integer("Edges created"));
    schema.addProperty("elapsedMs", SpecBuilders.integer("Elapsed time in milliseconds"));
    addLoadAccounting(schema);
    schema.addProperty("idMapping", SpecBuilders.object(
        "Temporary id to RID mapping, present only when temporary ids were used and the mapping was small enough to echo"));
    schema.addProperty("idMappingOmitted", SpecBuilders.bool(
        "True when the mapping was too large to return"));
    schema.addProperty("idMappingSize", SpecBuilders.integer(
        "Number of entries in the omitted mapping"));
    return schema;
  }

  /**
   * The {@code Accept} header that selects the streaming batch encoding (issue #7311). Declared as an explicit
   * parameter as well as a response content type because a generated client otherwise has no way to ask for it.
   */
  private static Parameter batchNdJsonAcceptParam() {
    final Parameter accept = SpecBuilders.headerParam("Accept", """
        Send 'application/x-ndjson' to receive per-chunk acknowledgements while the request body is still being \
        uploaded, instead of one object after the whole load. Anything else - including an absent header - \
        returns the buffered application/json body unchanged.""", false);
    accept.getSchema().setEnum(List.of(SpecBuilders.JSON, NDJSON));
    return accept;
  }

  /**
   * One line of the streaming batch encoding (issue #7311). Same self-delimiting discipline as the streaming
   * query: exactly one key per line naming the event, and a stream that ends with neither {@code summary} nor
   * {@code error} is one that did not arrive whole.
   */
  private Schema<?> createNdJsonBatchEventSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        One line of a streamed bulk load. Exactly one of 'progress', 'summary' or 'error' is present.""");

    final Schema<Object> progress = SpecBuilders.object("""
        A chunk acknowledgement, written while the request body is still being read. Emitted at every vertex \
        commit and every 'commitEvery' edges. The counters are records ATTEMPTED, the same upper bound on what \
        is durable that the partial-commit counters carry: vertices are committed at each flush, while edges are \
        buffered and written when the load ends.""");
    progress.addProperty("phase", SpecBuilders.string("'vertices' or 'edges'"));
    progress.addProperty("verticesCreated", SpecBuilders.integer("Vertices attempted so far"));
    progress.addProperty("edgesCreated", SpecBuilders.integer("Edges attempted so far"));
    progress.addProperty("idMapping", SpecBuilders.object("""
        Temporary id to RID mapping of the vertices this chunk resolved, and only of those: the mapping is \
        handed back one committed chunk at a time so neither end ever holds the whole load's worth of it \
        (issue #7353). Concatenate the 'idMapping' of every line, in order, to obtain what the buffered \
        encoding returns in one object, and check the total against 'idMappingSize' on the terminal line. \
        Absent on an edge-phase acknowledgement, on a chunk whose vertices declared no @id under \
        refMode=tempId, and when the request sent idMapping=false."""));
    addLoadAccounting(progress);
    schema.addProperty("progress", progress);

    final Schema<Object> summary = SpecBuilders.object("""
        Terminal line of a successful load: the same object the buffered application/json response carries, \
        plus 'commitIndex' on a replicated database - the read-your-writes bookmark, which cannot be a response \
        header here because the response has already started when its value becomes known.""");
    summary.addProperty("commitIndex", SpecBuilders.integer(
        "Last applied Raft index, the value the X-ArcadeDB-Commit-Index header carries on the buffered encoding"));
    summary.addProperty("idMappingStreamed", SpecBuilders.bool("""
        Always true on this encoding when the load resolved any temporary id: the mapping travelled in the \
        'idMapping' of the progress lines rather than in this object, so 'idMapping' here is only whatever the \
        last chunk resolved after the final acknowledgement - usually nothing. 'idMappingOmitted' is never sent \
        on this encoding: the size cap it reports exists because the buffered encoding has to build the whole \
        mapping before it can send anything, which streaming removes (issue #7353)."""));
    summary.addProperty("idMappingSize", SpecBuilders.integer("""
        Total number of temporary ids the load resolved. Check the number of mapping entries received across \
        all the lines against it: a mapping that arrives in pieces can lose one to a truncated response \
        without any single piece looking wrong."""));
    schema.addProperty("summary", summary);

    final Schema<Object> error = SpecBuilders.object("""
        Terminal line of a failed load: the same object the buffered encoding carries, plus the 'status' it \
        would have been sent under. The status line cannot be taken back once the stream has started, so the \
        status travels in band.""");
    error.addProperty("status", SpecBuilders.integer(
        "HTTP status the buffered encoding would have used: 400, 408 or 500"));
    error.addProperty("statusMapped", SpecBuilders.bool("""
        Present and false when 'status' is the unclassified 500 fallback rather than the status the buffered \
        encoding would have chosen - the case of an engine failure raised after the stream had already \
        started. Key on 'exception' there, not on 'status'. Absent whenever 'status' is exact."""));
    // Carried on a FAILED load too, and not by accident: a batch is not atomic, so a load that failed
    // mid-stream still committed the chunks before the failure, and a READ_YOUR_WRITES client has to be able
    // to read them back. That is the same rule the buffered encoding follows by emitting the header on its
    // 400/408 answers (issue #5862), and a client generated from a document that declared the bookmark only
    // on 'summary' would not know to look for it where it matters most.
    error.addProperty("commitIndex", SpecBuilders.integer(
        "Last applied Raft index, present on a replicated database. On a failed load it bookmarks the chunks "
            + "that were committed before the failure"));
    schema.addProperty("error", error);
    return schema;
  }

  private Schema<?> createBatchErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Failed bulk load. Carries how much of the payload was attempted, because a batch is not \
        atomic and the caller has to reconcile before retrying.""");
    schema.addProperty("error", SpecBuilders.string("""
        Why the load failed. Carries the offending location, such as a line number or a temporary id, \
        because a batch failure echoes client input rather than engine internals."""));
    schema.addProperty("exception", SpecBuilders.string(
        "Exception class name, for distinguishing failure classes programmatically"));
    schema.addProperty("requestId", SpecBuilders.string("""
        Correlation id echoing X-Request-Id, for cross-referencing the failure against the server \
        log. Absent when the request carried no correlation id."""));
    schema.addProperty("verticesCreated", SpecBuilders.integer("""
        Vertices attempted before the failure. An upper bound on what is durable: records handled \
        since the last commit boundary were rolled back."""));
    schema.addProperty("edgesCreated", SpecBuilders.integer("""
        Edges attempted before the failure, with the same upper-bound caveat as \
        'verticesCreated'."""));
    schema.addProperty("partialCommit", SpecBuilders.bool("""
        True when earlier chunks are durably committed. Retrying the whole payload then duplicates \
        the already-committed vertices, because temporary ids are not keys."""));
    addLoadAccounting(schema);
    return schema;
  }

  /**
   * What the load did with the payload, on the successful answer and the failing ones alike: a client reconciling a
   * partial load needs them exactly where it needs the counts.
   */
  private void addLoadAccounting(final Schema<Object> schema) {
    schema.addProperty("bytesRead", SpecBuilders.integer("""
        Bytes of the upload the server consumed, so a client can verify its whole file arrived - and, on a \
        truncated load, how far the server got. Never more than the client sent."""));
    schema.addProperty("linesRead", SpecBuilders.integer(
        "Lines the parser read, so 'linesRead' minus 'linesSkipped' can be checked against the records created"));
    schema.addProperty("linesSkipped", SpecBuilders.integer(
        "Lines that carried no record: blank lines, plus CSV headers and '---' separators"));
    schema.addProperty("verticesWithoutId", SpecBuilders.integer("""
        Vertices created without an '@id' under refMode=id. They are loaded and durable, but no edge can \
        reference them. Absent when zero."""));
  }

  private Schema<?> createProgressResponseSchema() {
    final Schema<Object> operation = SpecBuilders.object("One in-progress operation");
    operation.addProperty("id", SpecBuilders.integer("Operation identifier"));
    operation.addProperty("database", SpecBuilders.string("Database the operation runs on"));
    operation.addProperty("operation", SpecBuilders.string("Operation name, for example CHECK DATABASE"));
    operation.addProperty("stepName", SpecBuilders.string("Current step name"));
    operation.addProperty("stepIndex", SpecBuilders.integer("Current step, 0-based"));
    operation.addProperty("totalSteps", SpecBuilders.integer("Total number of steps"));
    operation.addProperty("done", SpecBuilders.integer("Units completed in the current step"));
    operation.addProperty("total", SpecBuilders.integer("Units in the current step, -1 when unknown"));
    operation.addProperty("percentage", SpecBuilders.integer(
        "Completion percentage of the current step, -1 when the total is unknown"));
    operation.addProperty("startedOn", SpecBuilders.integer("Start time as epoch milliseconds"));
    operation.addProperty("elapsedMs", SpecBuilders.integer("Elapsed time in milliseconds"));

    final Schema<Object> schema = SpecBuilders.object("In-progress maintenance operations");
    schema.addProperty("result", SpecBuilders.arrayOf(operation, "In-progress operations"));
    return schema;
  }
}
