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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.http.HttpServer;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Generates OpenAPI 3.0+ specification for ArcadeDB HTTP API endpoints.
 * This generator analyzes the registered HTTP handlers and creates a comprehensive
 * OpenAPI specification including all endpoints, request/response schemas, and security.
 */
public class OpenApiSpecGenerator {

  private final HttpServer httpServer;

  public OpenApiSpecGenerator(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  /**
   * Generates the complete OpenAPI specification for ArcadeDB HTTP API.
   *
   * @return OpenAPI specification object
   */
  public OpenAPI generateSpec() {
    final OpenAPI openAPI = new OpenAPI();

    // Set OpenAPI version
    openAPI.setOpenapi("3.0.3");

    // Set API info
    openAPI.setInfo(createApiInfo());

    // Set servers
    openAPI.setServers(createServers());

    // Set security
    openAPI.setComponents(createComponents());

    // Set paths
    openAPI.setPaths(createPaths());

    return openAPI;
  }

  private Info createApiInfo() {
    final Info info = new Info();
    info.setTitle("ArcadeDB HTTP API");
    info.setDescription("Multi-Model DBMS HTTP API for Graph, Document, Key/Value, Search Engine, Time Series, and Vector Embedding operations");
    info.setVersion("1.0.0");

    final Contact contact = new Contact();
    contact.setName("Arcade Data Ltd");
    contact.setEmail("info@arcadedata.com");
    contact.setUrl("https://arcadedb.com");
    info.setContact(contact);

    final License license = new License();
    license.setName("Apache 2.0");
    license.setUrl("https://www.apache.org/licenses/LICENSE-2.0");
    info.setLicense(license);

    return info;
  }

  private List<Server> createServers() {
    final Server server = new Server();
    server.setUrl("http://localhost:2480");
    server.setDescription("ArcadeDB Server");
    return Arrays.asList(server);
  }

  private Components createComponents() {
    final Components components = new Components();

    // Add security schemes
    final SecurityScheme basicAuth = new SecurityScheme();
    basicAuth.setType(SecurityScheme.Type.HTTP);
    basicAuth.setScheme("basic");
    basicAuth.setDescription("Basic HTTP authentication with username and password");

    components.addSecuritySchemes("basicAuth", basicAuth);

    final SecurityScheme bearerAuth = new SecurityScheme();
    bearerAuth.setType(SecurityScheme.Type.HTTP);
    bearerAuth.setScheme("bearer");
    bearerAuth.setDescription("API token authentication (Bearer token starting with 'at-')");
    components.addSecuritySchemes("bearerAuth", bearerAuth);

    // Add common schemas
    components.addSchemas("QueryRequest", createQueryRequestSchema());
    components.addSchemas("QueryResponse", createQueryResponseSchema());
    components.addSchemas("CommandRequest", createCommandRequestSchema());
    components.addSchemas("ErrorResponse", createErrorResponseSchema());
    components.addSchemas("ServerInfo", createServerInfoSchema());
    components.addSchemas("DatabaseList", createDatabaseListSchema());

    return components;
  }

  private Paths createPaths() {
    final Paths paths = new Paths();

    // Server endpoints
    paths.addPathItem("/api/v1/server", createServerPath());
    paths.addPathItem("/api/v1/ready", createReadyPath());
    paths.addPathItem("/api/v1/databases", createDatabasesPath());

    // Database endpoints
    paths.addPathItem("/api/v1/exists/{database}", createExistsPath());

    // Query endpoints
    paths.addPathItem("/api/v1/query/{database}/{language}/{command}", createGetQueryPath());
    paths.addPathItem("/api/v1/query/{database}", createPostQueryPath());

    // Command endpoints
    paths.addPathItem("/api/v1/command/{database}", createCommandPath());

    // Transaction endpoints
    paths.addPathItem("/api/v1/begin/{database}", createBeginPath());
    paths.addPathItem("/api/v1/commit/{database}", createCommitPath());
    paths.addPathItem("/api/v1/rollback/{database}", createRollbackPath());

    // User management endpoints
    paths.addPathItem("/api/v1/server/users", createUsersPath());

    // Group management endpoints
    paths.addPathItem("/api/v1/server/groups", createGroupsPath());

    // API token management endpoints
    paths.addPathItem("/api/v1/server/api-tokens", createApiTokensPath());

    return paths;
  }

  private PathItem createServerPath() {
    final PathItem pathItem = new PathItem();

    // GET /api/v1/server
    final Operation getOp = new Operation();
    getOp.setSummary("Get server information");
    getOp.setDescription("Retrieves server status, version, and configuration information");
    getOp.setOperationId("getServerInfo");
    getOp.addTagsItem("Server");
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.setResponses(createServerGetResponses());
    pathItem.setGet(getOp);

    // POST /api/v1/server
    final Operation postOp = new Operation();
    postOp.setSummary("Execute server command");
    postOp.setDescription("Executes administrative commands on the server (root user only)");
    postOp.setOperationId("executeServerCommand");
    postOp.addTagsItem("Server");
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.setRequestBody(createCommandRequestBody());
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
    pathItem.setGet(getOp);

    return pathItem;
  }

  private PathItem createDatabasesPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List databases");
    getOp.setDescription("Retrieves a list of all available databases");
    getOp.setOperationId("listDatabases");
    getOp.addTagsItem("Database");
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
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
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.addParametersItem(createDatabaseParameter());
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
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.addParametersItem(createDatabaseParameter());
    getOp.addParametersItem(createLanguageParameter());
    getOp.addParametersItem(createCommandParameter());
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
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.addParametersItem(createDatabaseParameter());
    postOp.setRequestBody(createQueryRequestBody());
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
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.addParametersItem(createDatabaseParameter());
    postOp.setRequestBody(createCommandRequestBody());
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
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.addParametersItem(createDatabaseParameter());
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
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.addParametersItem(createDatabaseParameter());
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
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.addParametersItem(createDatabaseParameter());
    postOp.setResponses(createTransactionResponses());
    pathItem.setPost(postOp);

    return pathItem;
  }

  private PathItem createUsersPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List users");
    getOp.setDescription("Lists all server users with their database/group assignments (root only)");
    getOp.setOperationId("listUsers");
    getOp.addTagsItem("Security");
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.setResponses(createAdminResponses("List of users retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create user");
    postOp.setDescription("Creates a new server user (root only). Requires name (string) and password (min 8 chars).");
    postOp.setOperationId("createUser");
    postOp.addTagsItem("Security");
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.setRequestBody(createJsonRequestBody("User creation request with name, password, and optional databases"));
    postOp.setResponses(createAdminResponses("User created", "201"));
    pathItem.setPost(postOp);

    final Operation putOp = new Operation();
    putOp.setSummary("Update user");
    putOp.setDescription("Updates an existing user's password and/or database assignments (root only)");
    putOp.setOperationId("updateUser");
    putOp.addTagsItem("Security");
    putOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    putOp.addParametersItem(createQueryParameter("name", "User name"));
    putOp.setRequestBody(createJsonRequestBody("User update request with optional password and databases"));
    putOp.setResponses(createAdminResponses("User updated"));
    pathItem.setPut(putOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete user");
    deleteOp.setDescription("Deletes a server user (root only)");
    deleteOp.setOperationId("deleteUser");
    deleteOp.addTagsItem("Security");
    deleteOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    deleteOp.addParametersItem(createQueryParameter("name", "User name to delete"));
    deleteOp.setResponses(createAdminResponses("User deleted"));
    pathItem.setDelete(deleteOp);

    return pathItem;
  }

  private PathItem createGroupsPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List groups");
    getOp.setDescription("Lists all security groups and their configurations (root only)");
    getOp.setOperationId("listGroups");
    getOp.addTagsItem("Security");
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.setResponses(createAdminResponses("List of groups retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create or update group");
    postOp.setDescription("Creates or updates a security group (root only)");
    postOp.setOperationId("createOrUpdateGroup");
    postOp.addTagsItem("Security");
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.setRequestBody(createJsonRequestBody("Group configuration with database, name, and access permissions"));
    postOp.setResponses(createAdminResponses("Group created or updated"));
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete group");
    deleteOp.setDescription("Deletes a security group (root only)");
    deleteOp.setOperationId("deleteGroup");
    deleteOp.addTagsItem("Security");
    deleteOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    deleteOp.addParametersItem(createQueryParameter("database", "Database name"));
    deleteOp.addParametersItem(createQueryParameter("name", "Group name to delete"));
    deleteOp.setResponses(createAdminResponses("Group deleted"));
    pathItem.setDelete(deleteOp);

    return pathItem;
  }

  private PathItem createApiTokensPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List API tokens");
    getOp.setDescription("Lists all API tokens with metadata (root only). Token values are never returned.");
    getOp.setOperationId("listApiTokens");
    getOp.addTagsItem("Security");
    getOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    getOp.setResponses(createAdminResponses("List of API tokens retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create API token");
    postOp.setDescription("Creates a new API token (root only). The plaintext token is returned only once in the response.");
    postOp.setOperationId("createApiToken");
    postOp.addTagsItem("Security");
    postOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    postOp.setRequestBody(createJsonRequestBody("Token creation with name, database, expiresAt, and permissions"));
    postOp.setResponses(createAdminResponses("API token created", "201"));
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete API token");
    deleteOp.setDescription("Deletes an API token by its hash (root only). Plaintext tokens are rejected.");
    deleteOp.setOperationId("deleteApiToken");
    deleteOp.addTagsItem("Security");
    deleteOp.setSecurity(Arrays.asList(createSecurityRequirement()));
    deleteOp.addParametersItem(createQueryParameter("token", "Token hash (SHA-256 hex)"));
    deleteOp.setResponses(createAdminResponses("API token deleted"));
    pathItem.setDelete(deleteOp);

    return pathItem;
  }

  private SecurityRequirement createSecurityRequirement() {
    final SecurityRequirement security = new SecurityRequirement();
    security.addList("basicAuth");
    return security;
  }

  private Parameter createDatabaseParameter() {
    final Parameter param = new Parameter();
    param.setName("database");
    param.setIn("path");
    param.setRequired(true);
    param.setDescription("Database name");
    param.setSchema(new Schema<>().type("string"));
    return param;
  }

  private Parameter createLanguageParameter() {
    final Parameter param = new Parameter();
    param.setName("language");
    param.setIn("path");
    param.setRequired(true);
    param.setDescription("Query language (sql, cypher, gremlin, graphql, mongo)");
    final Schema<String> enumSchema = new Schema<>();
    enumSchema.setType("string");
    enumSchema.setEnum(Arrays.asList("sql", "cypher", "gremlin", "graphql", "mongo"));
    param.setSchema(enumSchema);
    return param;
  }

  private Parameter createCommandParameter() {
    final Parameter param = new Parameter();
    param.setName("command");
    param.setIn("path");
    param.setRequired(true);
    param.setDescription("Query or command to execute");
    param.setSchema(new Schema<>().type("string"));
    return param;
  }

  private Parameter createQueryParameter(final String name, final String description) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("query");
    param.setRequired(true);
    param.setDescription(description);
    param.setSchema(new Schema<>().type("string"));
    return param;
  }

  private RequestBody createJsonRequestBody(final String description) {
    final RequestBody requestBody = new RequestBody();
    requestBody.setDescription(description);
    requestBody.setRequired(true);
    final Content content = new Content();
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("object"));
    content.addMediaType("application/json", mediaType);
    requestBody.setContent(content);
    return requestBody;
  }

  private ApiResponses createAdminResponses(final String successDescription) {
    return createAdminResponses(successDescription, "200");
  }

  private ApiResponses createAdminResponses(final String successDescription, final String successCode) {
    final ApiResponses responses = new ApiResponses();
    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription(successDescription);
    final Content content = new Content();
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("object"));
    content.addMediaType("application/json", mediaType);
    successResponse.setContent(content);
    responses.addApiResponse(successCode, successResponse);
    responses.addApiResponse("400", createErrorResponse("Bad request"));
    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("403", createErrorResponse("Forbidden - root user required"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));
    return responses;
  }

  private RequestBody createQueryRequestBody() {
    final RequestBody requestBody = new RequestBody();
    requestBody.setDescription("Query request with command and optional parameters");
    requestBody.setRequired(true);

    final Content content = new Content();
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().$ref("#/components/schemas/QueryRequest"));
    content.addMediaType("application/json", mediaType);
    requestBody.setContent(content);

    return requestBody;
  }

  private RequestBody createCommandRequestBody() {
    final RequestBody requestBody = new RequestBody();
    requestBody.setDescription("Command request with command and optional parameters");
    requestBody.setRequired(true);

    final Content content = new Content();
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().$ref("#/components/schemas/CommandRequest"));
    content.addMediaType("application/json", mediaType);
    requestBody.setContent(content);

    return requestBody;
  }

  private ApiResponses createServerGetResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Server information retrieved successfully");
    final Content successContent = new Content();
    final MediaType successMediaType = new MediaType();
    successMediaType.setSchema(new Schema<>().$ref("#/components/schemas/ServerInfo"));
    successContent.addMediaType("application/json", successMediaType);
    successResponse.setContent(successContent);
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

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

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("List of databases retrieved successfully");
    final Content successContent = new Content();
    final MediaType successMediaType = new MediaType();
    successMediaType.setSchema(new Schema<>().$ref("#/components/schemas/DatabaseList"));
    successContent.addMediaType("application/json", successMediaType);
    successResponse.setContent(successContent);
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

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

    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

    return responses;
  }

  private ApiResponses createQueryResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Query executed successfully");
    final Content successContent = new Content();
    final MediaType successMediaType = new MediaType();
    successMediaType.setSchema(new Schema<>().$ref("#/components/schemas/QueryResponse"));
    successContent.addMediaType("application/json", successMediaType);
    successResponse.setContent(successContent);
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("400", createErrorResponse("Bad request"));
    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("404", createErrorResponse("Database not found"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

    return responses;
  }

  private ApiResponses createCommandResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Command executed successfully");
    final Content successContent = new Content();
    final MediaType successMediaType = new MediaType();
    successMediaType.setSchema(new Schema<>().$ref("#/components/schemas/QueryResponse"));
    successContent.addMediaType("application/json", successMediaType);
    successResponse.setContent(successContent);
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("400", createErrorResponse("Bad request"));
    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("404", createErrorResponse("Database not found"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

    return responses;
  }

  private ApiResponses createTransactionResponses() {
    final ApiResponses responses = new ApiResponses();

    final ApiResponse successResponse = new ApiResponse();
    successResponse.setDescription("Transaction operation completed successfully");
    responses.addApiResponse("200", successResponse);

    responses.addApiResponse("400", createErrorResponse("Bad request"));
    responses.addApiResponse("401", createErrorResponse("Unauthorized"));
    responses.addApiResponse("404", createErrorResponse("Database not found"));
    responses.addApiResponse("500", createErrorResponse("Internal server error"));

    return responses;
  }

  private ApiResponse createErrorResponse(final String description) {
    final ApiResponse response = new ApiResponse();
    response.setDescription(description);
    final Content content = new Content();
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().$ref("#/components/schemas/ErrorResponse"));
    content.addMediaType("application/json", mediaType);
    response.setContent(content);
    return response;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Query request object");
    schema.addProperty("command", new Schema<>().type("string").description("Query or command to execute"));
    schema.addProperty("language", new Schema<>().type("string").description("Query language").example("sql"));
    schema.addProperty("params", new Schema<>().type("object").description("Query parameters"));
    schema.addProperty("serializer", new Schema<>().type("string").description("Response serializer").example("json"));
    schema.addProperty("limit", new Schema<>().type("integer").description("Maximum number of results").example(100));
    schema.setRequired(Arrays.asList("command"));
    return schema;
  }

  private Schema<?> createCommandRequestSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Command request object");
    schema.addProperty("command", new Schema<>().type("string").description("Command to execute"));
    schema.addProperty("params", new Schema<>().type("object").description("Command parameters"));
    schema.setRequired(Arrays.asList("command"));
    return schema;
  }

  private Schema<?> createQueryResponseSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Query response object");
    schema.addProperty("result", new Schema<>().type("array").items(new Schema<>().type("object")).description("Query results"));
    schema.addProperty("executionTime", new Schema<>().type("integer").description("Execution time in milliseconds"));
    schema.addProperty("recordCount", new Schema<>().type("integer").description("Number of records returned"));
    return schema;
  }

  private Schema<?> createErrorResponseSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Error response object");
    schema.addProperty("error", new Schema<>().type("string").description("Error message"));
    schema.addProperty("detail", new Schema<>().type("string").description("Error details"));
    schema.addProperty("exception", new Schema<>().type("string").description("Exception class name"));
    schema.addProperty("exceptionArgs", new Schema<>().type("string").description("Exception arguments"));
    schema.addProperty("help", new Schema<>().type("string").description("Help information"));
    return schema;
  }

  private Schema<?> createServerInfoSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Server information object");
    schema.addProperty("version", new Schema<>().type("string").description("Server version"));
    schema.addProperty("status", new Schema<>().type("string").description("Server status"));
    schema.addProperty("mode", new Schema<>().type("string").description("Server mode"));
    schema.addProperty("uptime", new Schema<>().type("integer").description("Server uptime in milliseconds"));
    return schema;
  }

  private Schema<?> createDatabaseListSchema() {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription("Database list response");
    schema.addProperty("result", new Schema<>().type("array").items(new Schema<>().type("string")).description("List of database names"));
    return schema;
  }
}
