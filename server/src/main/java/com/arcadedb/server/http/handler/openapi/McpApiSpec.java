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
import io.swagger.v3.oas.models.responses.ApiResponses;

/**
 * Documents the Model Context Protocol endpoint and its configuration. The protocol endpoint carries
 * JSON-RPC 2.0 envelopes whose method set belongs to the MCP specification rather than to this API,
 * so its request and response bodies stay opaque. Every non-200 status the protocol endpoint answers
 * with, other than the framework-level 401 and 500, is itself a JSON-RPC error envelope rather than
 * this API's standard error shape, because those statuses are raised by the JSON-RPC dispatcher, not
 * by the shared HTTP error path. The configuration resource is fully modelled, because a client edits
 * it field by field, and is restricted to the root user.
 */
public class McpApiSpec implements OpenApiContributor {

  private static final String MCP_PLUGIN_REQUIRED =
      "Requires MCPPlugin: present in every standard distribution, absent from a custom build that excludes the MCP module.";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/mcp", createMcpPath());
    openAPI.getPaths().addPathItem("/api/v1/mcp/config", createConfigPath());

    openAPI.getComponents().addSchemas("McpConfig", createConfigSchema());
    openAPI.getComponents().addSchemas("McpDatabaseOverride", createDatabaseOverrideSchema());
  }

  private PathItem createMcpPath() {
    final Operation post = SpecBuilders.operation("invokeMcp", "MCP",
        "Exchange a JSON-RPC message with the MCP server",
        """
            Accepts one JSON-RPC 2.0 request, notification, or response, or a batch of them as a \
            top-level array, and answers with the corresponding response. The method set and the \
            parameter and result shapes for each method are defined by the Model Context Protocol \
            specification, not by this API, so request and response bodies are not enumerated here.

            The route is always registered; when the MCP server is disabled the request is refused at \
            request time with 503, which is what makes runtime toggling possible without a restart. A \
            request carrying only notifications and/or responses receives 202 with no body, because \
            JSON-RPC forbids replying to those. Every other outcome, including a JSON-RPC-level error \
            such as an unknown method or a malformed request body, is reported inside a 200 response: \
            JSON-RPC layers its own error reporting over the HTTP transport, so a non-200 status is \
            reserved for transport-level failures such as missing credentials, a disallowed browser \
            Origin, an unauthorized user, an unsupported HTTP method, or the server being disabled.\s"""
            + MCP_PLUGIN_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody(
        "JSON-RPC 2.0 request, notification, or response, or a batch of them as a top-level array",
        null, true));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(
        "JSON-RPC 2.0 response, carrying either a 'result' or an 'error' member per the JSON-RPC 2.0 "
            + "envelope. A batch request answers with an array of these.", null));
    responses.addApiResponse("202", SpecBuilders.emptyResponse(
        "Accepted, no body. The request carried only notifications and/or JSON-RPC responses, which "
            + "this server never answers."));
    responses.addApiResponse("401", SpecBuilders.errorResponse(
        "Unauthorized: no credentials were supplied. Raised by the HTTP layer before the request "
            + "reaches the MCP dispatcher, so the body is this API's standard error shape."));
    responses.addApiResponse("403", SpecBuilders.jsonResponse(
        "Forbidden, reported as a JSON-RPC error envelope: either the request's Origin header failed "
            + "the anti-DNS-rebinding check, or the authenticated user is not on the MCP server's "
            + "allowedUsers list.", null));
    responses.addApiResponse("405", SpecBuilders.jsonResponse(
        "Method not allowed, reported as a JSON-RPC error envelope. This endpoint accepts POST only.",
        null));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("503", SpecBuilders.jsonResponse(
        "The MCP server is currently disabled, reported as a JSON-RPC error envelope.", null));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createConfigPath() {
    final PathItem pathItem = new PathItem();

    final Operation get = SpecBuilders.operation("getMcpConfig", "MCP",
        "Read the MCP server configuration",
        "Returns the MCP server's enablement, permission flags, tool profile, and access lists. "
            + "Restricted to the root user. " + MCP_PLUGIN_REQUIRED);
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Current configuration", "McpConfig"),
        "401", "403", "405", "500"));
    pathItem.setGet(get);

    final Operation post = SpecBuilders.operation("updateMcpConfig", "MCP",
        "Update the MCP server configuration",
        """
            Applies a partial configuration update: send only the fields to change. The update is \
            all-or-nothing: every field is parsed and validated before the first one is assigned, so a \
            payload rejected on any field leaves the configuration exactly as it was. Restricted to \
            the root user.

            Answers with the full configuration as it stands after the update.\s"""
            + MCP_PLUGIN_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody(
        "Partial configuration. Omitted fields keep their current value.", "McpConfig", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Configuration after the update", "McpConfig"),
        "400", "401", "403", "405", "500"));
    pathItem.setPost(post);

    return pathItem;
  }

  private Schema<?> createConfigSchema() {
    final Schema<Object> databases = SpecBuilders.object(
        "Per-database permission overrides, keyed by database name. Present only when at least one "
            + "override is configured.");
    databases.setAdditionalProperties(SpecBuilders.ref("McpDatabaseOverride"));

    final Schema<Object> principalProfiles = SpecBuilders.object(
        "Tool profile assigned per principal (user or API token) name. Present only when at least "
            + "one is configured.");
    principalProfiles.setAdditionalProperties(SpecBuilders.string(
        "Tool profile name: 'all', 'rag', or 'admin'"));

    final Schema<Object> schema = SpecBuilders.object("MCP server configuration");
    schema.addProperty("enabled", SpecBuilders.bool("Whether the MCP server answers requests"));
    schema.addProperty("allowReads", SpecBuilders.bool("Permit read operations"));
    schema.addProperty("allowInsert", SpecBuilders.bool("Permit inserts"));
    schema.addProperty("allowUpdate", SpecBuilders.bool("Permit updates"));
    schema.addProperty("allowDelete", SpecBuilders.bool("Permit deletes"));
    schema.addProperty("allowSchemaChange", SpecBuilders.bool("Permit schema changes"));
    schema.addProperty("allowAdmin", SpecBuilders.bool("Permit administrative operations"));
    schema.addProperty("profile", SpecBuilders.string("Default tool profile: 'all', 'rag', or 'admin'"));
    schema.addProperty("allowedUsers", SpecBuilders.arrayOf(
        SpecBuilders.string("User name"),
        "Users permitted to reach the MCP server. The value '*' permits any authenticated user."));
    schema.addProperty("allowedOrigins", SpecBuilders.arrayOf(
        SpecBuilders.string("Origin"),
        "Extra browser origins permitted for the HTTP transport, beyond loopback addresses which are "
            + "always allowed. The value '*' permits any origin, disabling the anti-DNS-rebinding "
            + "check."));
    schema.addProperty("principalProfiles", principalProfiles);
    schema.addProperty("databases", databases);
    return schema;
  }

  private Schema<?> createDatabaseOverrideSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Per-database permission override. Every field is optional; an omitted field inherits the "
            + "server-wide value. A permission set to true here still requires the corresponding "
            + "global permission to be true, so an override can only narrow access, never widen it. "
            + "'allowedUsers' is intersected with the global 'allowedUsers', not a replacement for it.");
    schema.addProperty("allowReads", SpecBuilders.bool("Restrict read access for this database"));
    schema.addProperty("allowInsert", SpecBuilders.bool("Restrict insert access for this database"));
    schema.addProperty("allowUpdate", SpecBuilders.bool("Restrict update access for this database"));
    schema.addProperty("allowDelete", SpecBuilders.bool("Restrict delete access for this database"));
    schema.addProperty("allowSchemaChange", SpecBuilders.bool(
        "Restrict schema-change access for this database"));
    schema.addProperty("allowAdmin", SpecBuilders.bool(
        "Restrict administrative access for this database"));
    schema.addProperty("allowedUsers", SpecBuilders.arrayOf(
        SpecBuilders.string("User name"),
        "Users permitted on this database, intersected with the global 'allowedUsers'"));
    return schema;
  }
}
