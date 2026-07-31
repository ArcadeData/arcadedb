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
 * Documents the session endpoints. Login mints a bearer session token from credentials already
 * validated by the handler chain, so it declares no request body of its own.
 */
public class AuthApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/login", createLoginPath());
    openAPI.getPaths().addPathItem("/api/v1/logout", createLogoutPath());
    openAPI.getPaths().addPathItem("/api/v1/sessions", createSessionsPath());

    openAPI.getComponents().addSchemas("LoginResponse", createLoginResponseSchema());
    openAPI.getComponents().addSchemas("SessionList", createSessionListSchema());
  }

  private PathItem createLoginPath() {
    final Operation post = SpecBuilders.operation("login", "Auth",
        "Create an authentication session",
        """
            Exchanges the credentials on the Authorization header for a session token prefixed 'AU-'. \
            The token is then presented as a bearer token on subsequent requests. This operation \
            takes no request body: the credentials travel on the header, and geolocation metadata is \
            read from the CF-IPCountry, CF-IPCity, CF-Connecting-IP, X-Forwarded-For, and User-Agent \
            headers when a proxy supplies them.""");
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Session created", "LoginResponse"),
        "401", "403", "500"));

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

    final Schema<Object> schema = SpecBuilders.object("Active authentication sessions");
    schema.addProperty("result", SpecBuilders.arrayOf(session, "Active sessions"));
    schema.addProperty("count", SpecBuilders.integer("Number of active sessions"));
    return schema;
  }
}
