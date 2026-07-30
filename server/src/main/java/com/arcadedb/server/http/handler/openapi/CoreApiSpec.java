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
import io.swagger.v3.oas.models.media.Schema;
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

    openAPI.getComponents().addSchemas("QueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("QueryResponse", createQueryResponseSchema());
    openAPI.getComponents().addSchemas("CommandRequest", createCommandRequestSchema());
    openAPI.getComponents().addSchemas("ErrorResponse", createErrorResponseSchema());
    openAPI.getComponents().addSchemas("ServerInfo", createServerInfoSchema());
    openAPI.getComponents().addSchemas("DatabaseList", createDatabaseListSchema());
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

    // Liveness only ever responds with a 2xx when reachable; it never returns 503 (unlike readiness).
    final ApiResponse liveResponse = new ApiResponse();
    liveResponse.setDescription("Server process and HTTP layer are up");
    responses.addApiResponse("200", liveResponse);
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
    readyResponse.setDescription("Server is ready");
    responses.addApiResponse("200", readyResponse);
    responses.addApiResponse("204", readyResponse);

    final ApiResponse notReadyResponse = new ApiResponse();
    notReadyResponse.setDescription("Server not ready");
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

    final ApiResponse existsResponse = new ApiResponse();
    existsResponse.setDescription("Database exists");
    responses.addApiResponse("200", existsResponse);

    final ApiResponse notExistsResponse = new ApiResponse();
    notExistsResponse.setDescription("Database does not exist");
    responses.addApiResponse("404", notExistsResponse);

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
    schema.addProperty("limit", SpecBuilders.integer("Maximum number of results").example(100));
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
    schema.addProperty("executionTime", SpecBuilders.integer("Execution time in milliseconds"));
    schema.addProperty("recordCount", SpecBuilders.integer("Number of records returned"));
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
}
