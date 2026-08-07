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
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the endpoints every deployment exposes: server information and administration, the
 * probes, database enumeration, the query and command data plane, and the explicit transaction
 * lifecycle.
 */
public class CoreApiSpec implements OpenApiContributor {

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
    openAPI.getComponents().addSchemas("CommandRequest", createCommandRequestSchema());
    openAPI.getComponents().addSchemas("ErrorResponse", createErrorResponseSchema());
    openAPI.getComponents().addSchemas("ServerInfo", createServerInfoSchema());
    openAPI.getComponents().addSchemas("DatabaseList", createDatabaseListSchema());
    openAPI.getComponents().addSchemas("DatabaseExists", createDatabaseExistsSchema());
    openAPI.getComponents().addSchemas("BatchResponse", createBatchResponseSchema());
    openAPI.getComponents().addSchemas("BatchError", createBatchErrorSchema());
    openAPI.getComponents().addSchemas("ProgressResponse", createProgressResponseSchema());
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
        create user, drop user, shutdown, set server setting, get server events, align database. \
        Both restore and import support SSE progress streaming via Accept: text/event-stream header""");
    postOp.setOperationId("executeServerCommand");
    postOp.addTagsItem("Server");
    postOp.setRequestBody(SpecBuilders.jsonBody("Command request with command and optional parameters", "CommandRequest", true));
    postOp.setResponses(createCommandResponses());
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
    getOp.setDescription("Executes a query using GET method with parameters in URL");
    getOp.setOperationId("executeQueryGet");
    getOp.addTagsItem("Query");
    getOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    getOp.addParametersItem(SpecBuilders.pathParam("language", "Query language (sql, cypher, gremlin, graphql, mongo)",
        List.of("sql", "cypher", "gremlin", "graphql", "mongo")));
    getOp.addParametersItem(SpecBuilders.pathParam("command", "Query or command to execute"));
    getOp.setResponses(createQueryResponses());
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createPostQueryPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Execute query via POST");
    postOp.setDescription("Executes a query using POST method with query in request body");
    postOp.setOperationId("executeQueryPost");
    postOp.addTagsItem("Query");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.setRequestBody(SpecBuilders.jsonBody("Query request with command and optional parameters", "QueryRequest", true));
    postOp.setResponses(createQueryResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createCommandPath() {
    final PathItem pathItem = new PathItem();

    final Operation postOp = new Operation();
    postOp.setSummary("Execute command");
    postOp.setDescription("Executes a database command");
    postOp.setOperationId("executeCommand");
    postOp.addTagsItem("Command");
    postOp.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    postOp.setRequestBody(SpecBuilders.jsonBody("Command request with command and optional parameters", "CommandRequest", true));
    postOp.setResponses(createCommandResponses());
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
    postOp.setResponses(createTransactionResponses());
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
            bytes sent to verify a chunked upload arrived whole.""");

    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
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

    final RequestBody body = new RequestBody();
    body.setDescription("""
        Vertices first, then edges. JSONL sends one JSON record per line; CSV sends a header row \
        followed by data rows. Vertices may declare a temporary '@id' that edges reference through \
        '@from' and '@to', or be referenced by position when refMode=ordinal. Edges may also \
        reference existing RIDs in #bucket:position form.""");
    body.setRequired(true);
    final Content content = new Content();
    final MediaType jsonl = new MediaType();
    jsonl.setSchema(new Schema<>().type("string").description("One JSON object per line"));
    content.addMediaType("application/x-ndjson", jsonl);
    content.addMediaType("application/jsonl", jsonl);
    final MediaType csv = new MediaType();
    csv.setSchema(new Schema<>().type("string").description("Header row followed by data rows"));
    content.addMediaType("text/csv", csv);
    body.setContent(content);
    post.setRequestBody(body);

    // 400 and 408 carry the partial-commit counts rather than the generic error body: a batch is
    // not atomic, so a client that cannot read how much was committed cannot reconcile before
    // retrying. Every other failure keeps the base handler's standard error shape.
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Load completed", "BatchResponse"));
    responses.addApiResponse("400", SpecBuilders.jsonResponse(
        "Client-input failure, with the counts attempted before it", "BatchError"));
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

  private ApiResponses createQueryResponses() {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Query executed successfully", "QueryResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createCommandResponses() {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Command executed successfully", "QueryResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private ApiResponses createTransactionResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Transaction operation completed successfully");
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));

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
        behind ('truncated').""").example(100));
    schema.setRequired(List.of("command"));
    return schema;
  }

  private Schema<?> createCommandRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Command request object");
    schema.addProperty("command", SpecBuilders.string("Command to execute"));
    schema.addProperty("params", SpecBuilders.object(
        """
        Command parameters. Values may be JSON primitives, arrays, or typed-marker objects: \
        {"$bytes": "<base64>"} for byte[] (standard or URL-safe base64), \
        {"$int8": [v0, v1, ...]} for byte[] from integers in [-128, 127] (used to send \
        INT8-encoded vectors to LSM_VECTOR indexes without a float32 round-trip)."""));
    schema.setRequired(List.of("command"));
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

  private Schema<?> createBatchResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Result of a bulk load");
    schema.addProperty("verticesCreated", SpecBuilders.integer("Vertices created"));
    schema.addProperty("edgesCreated", SpecBuilders.integer("Edges created"));
    schema.addProperty("elapsedMs", SpecBuilders.integer("Elapsed time in milliseconds"));
    schema.addProperty("bytesRead", SpecBuilders.integer(
        "Bytes of the upload the server consumed, so a client can verify its whole file arrived"));
    schema.addProperty("idMapping", SpecBuilders.object(
        "Temporary id to RID mapping, present only when temporary ids were used and the mapping was small enough to echo"));
    schema.addProperty("idMappingOmitted", SpecBuilders.bool(
        "True when the mapping was too large to return"));
    schema.addProperty("idMappingSize", SpecBuilders.integer(
        "Number of entries in the omitted mapping"));
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
    return schema;
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
