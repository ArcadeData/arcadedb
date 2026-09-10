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

import java.util.List;

/**
 * Documents the session endpoints. Login mints a bearer session token from credentials already
 * validated by the handler chain, so it declares no request body of its own.
 */
public class AuthApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/login", createLoginPath());
    openAPI.getPaths().addPathItem("/api/v1/logout", createLogoutPath());
    openAPI.getPaths().addPathItem("/api/v1/sessions", createSessionsPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/auth-session", createClusterAuthSessionPath());

    openAPI.getComponents().addSchemas("LoginResponse", createLoginResponseSchema());
    openAPI.getComponents().addSchemas("SessionList", createSessionListSchema());
    openAPI.getComponents().addSchemas("ClusterAuthSessionRequest", createClusterAuthSessionRequestSchema());
    openAPI.getComponents().addSchemas("ClusterAuthSessionResponse", createClusterAuthSessionResponseSchema());
  }

  private PathItem createLoginPath() {
    final Operation post = SpecBuilders.operation("login", "Auth",
        "Create an authentication session",
        """
            Exchanges the credentials on the Authorization header for a session token prefixed 'AU-'. \
            The token is then presented as a bearer token on subsequent requests. This operation \
            takes no request body: the credentials travel on the header, and geolocation metadata is \
            read from the CF-IPCountry, CF-IPCity, CF-Connecting-IP, X-Forwarded-For, and User-Agent \
            headers when a proxy supplies them (each is stored truncated to 256 characters). \
            Answers 503 when the server already holds 'arcadedb.server.httpAuthSessionMax' concurrent \
            sessions and none could be reclaimed; a principal that reaches \
            'arcadedb.server.httpAuthSessionMaxPerUser' instead has its own oldest session evicted and \
            still receives a token.""");
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Session created", "LoginResponse"),
        "401", "403", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLogoutPath() {
    final Operation post = SpecBuilders.operation("logout", "Auth",
        "Invalidate the current authentication session",
        "Invalidates the session token presented on the Authorization header. Answers 204 with no body.");
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("Session invalidated"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createClusterAuthSessionPath() {
    final Operation post = SpecBuilders.operation("resolveClusterAuthSession", "Auth",
        "Confirm or revoke an authentication session on the node that issued it",
        """
            Cluster-internal. A session token is held by the node that answered /api/v1/login and names that \
            node ('AU-<server name>-<uuid>'). A peer that receives the token asks the issuer through this route \
            whether the session is still valid ('validate', which also counts as activity on the issuer), and a \
            logout tells every peer to drop its copy ('revoke'). Peers authenticate with the cluster token; a \
            request that carries user credentials instead is refused with 403 (issue #7424).""");
    post.setRequestBody(SpecBuilders.jsonBody("The token and what to do with it", "ClusterAuthSessionRequest", true));
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("The session, as the issuer holds it",
        "ClusterAuthSessionResponse"));
    responses.addApiResponse("204", SpecBuilders.emptyResponse("Copy dropped"));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Missing token or unknown action"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Not a cluster peer"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("The issuer does not hold this session"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createSessionsPath() {
    final Operation get = SpecBuilders.operation("listSessions", "Auth",
        "List active authentication sessions",
        "Lists the authentication sessions currently held by this server, with their client metadata.");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Active sessions", "SessionList"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createLoginResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Newly created session");
    schema.addProperty("token", SpecBuilders.string(
        "Session token prefixed 'AU-', presented as a bearer token"));
    schema.addProperty("user", SpecBuilders.string("Authenticated user name"));
    return schema;
  }

  private Schema<?> createSessionListSchema() {
    final Schema<Object> session = SpecBuilders.object("One active session");
    session.addProperty("token", SpecBuilders.string("Session token"));
    session.addProperty("user", SpecBuilders.string("User the session belongs to"));
    session.addProperty("createdAt", SpecBuilders.integer("Creation time as epoch milliseconds"));
    session.addProperty("lastUpdate", SpecBuilders.integer("Last use as epoch milliseconds"));
    session.addProperty("elapsedMs", SpecBuilders.integer("Milliseconds since last use"));
    session.addProperty("sourceIp", SpecBuilders.string("Client address"));
    session.addProperty("userAgent", SpecBuilders.string("Client user agent"));
    session.addProperty("country", SpecBuilders.string("Country reported by the proxy, when available"));
    session.addProperty("city", SpecBuilders.string("City reported by the proxy, when available"));
    session.addProperty("issuer", SpecBuilders.string(
        "Name of the cluster node that issued the session, when this node holds a copy of it; absent for a session this node issued"));

    final Schema<Object> schema = SpecBuilders.object("Active authentication sessions");
    schema.addProperty("result", SpecBuilders.arrayOf(session, "Active sessions"));
    schema.addProperty("count", SpecBuilders.integer("Number of active sessions"));
    return schema;
  }

  private Schema<?> createClusterAuthSessionRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("A session token and the action to apply to it");
    schema.addProperty("token", SpecBuilders.string("The session token, 'AU-<server name>-<uuid>'"));
    schema.addProperty("action", SpecBuilders.string("'validate' (default) or 'revoke'"));
    schema.setRequired(List.of("token"));
    return schema;
  }

  private Schema<?> createClusterAuthSessionResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("The session as held by the node that issued it");
    schema.addProperty("user", SpecBuilders.string("The principal the session belongs to"));
    schema.addProperty("createdAt", SpecBuilders.integer("Creation time, epoch milliseconds"));
    return schema;
  }
}
