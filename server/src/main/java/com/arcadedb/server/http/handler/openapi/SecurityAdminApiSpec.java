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
import io.swagger.v3.oas.models.responses.ApiResponses;

/**
 * Documents the root-only administration of users, groups, and API tokens.
 */
public class SecurityAdminApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server/users", createUsersPath());
    openAPI.getPaths().addPathItem("/api/v1/server/groups", createGroupsPath());
    openAPI.getPaths().addPathItem("/api/v1/server/api-tokens", createApiTokensPath());
  }

  private PathItem createUsersPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List users");
    getOp.setDescription("Lists all server users with their database/group assignments (root only)");
    getOp.setOperationId("listUsers");
    getOp.addTagsItem("Security");
    getOp.setResponses(createAdminResponses("List of users retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create user");
    postOp.setDescription("Creates a new server user (root only). Requires name (string) and password (min 8 chars).");
    postOp.setOperationId("createUser");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody("User creation request with name, password, and optional databases", null, true));
    postOp.setResponses(createAdminResponses("User created", "201"));
    pathItem.setPost(postOp);

    final Operation putOp = new Operation();
    putOp.setSummary("Update user");
    putOp.setDescription("Updates an existing user's password and/or database assignments (root only)");
    putOp.setOperationId("updateUser");
    putOp.addTagsItem("Security");
    putOp.addParametersItem(SpecBuilders.queryParam("name", "User name", true));
    putOp.setRequestBody(SpecBuilders.jsonBody("User update request with optional password and databases", null, true));
    putOp.setResponses(createAdminResponses("User updated"));
    pathItem.setPut(putOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete user");
    deleteOp.setDescription("Deletes a server user (root only)");
    deleteOp.setOperationId("deleteUser");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("name", "User name to delete", true));
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
    getOp.setResponses(createAdminResponses("List of groups retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create or update group");
    postOp.setDescription("Creates or updates a security group (root only)");
    postOp.setOperationId("createOrUpdateGroup");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody("Group configuration with database, name, and access permissions", null, true));
    postOp.setResponses(createAdminResponses("Group created or updated"));
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete group");
    deleteOp.setDescription("Deletes a security group (root only)");
    deleteOp.setOperationId("deleteGroup");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("database", "Database name", true));
    deleteOp.addParametersItem(SpecBuilders.queryParam("name", "Group name to delete", true));
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
    getOp.setResponses(createAdminResponses("List of API tokens retrieved successfully"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create API token");
    postOp.setDescription("Creates a new API token (root only). The plaintext token is returned only once in the response.");
    postOp.setOperationId("createApiToken");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody("Token creation with name, database, expiresAt, and permissions", null, true));
    postOp.setResponses(createAdminResponses("API token created", "201"));
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete API token");
    deleteOp.setDescription("Deletes an API token by its hash (root only). Plaintext tokens are rejected.");
    deleteOp.setOperationId("deleteApiToken");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("token", "Token hash (SHA-256 hex)", true));
    deleteOp.setResponses(createAdminResponses("API token deleted"));
    pathItem.setDelete(deleteOp);

    return pathItem;
  }

  private ApiResponses createAdminResponses(final String successDescription) {
    return createAdminResponses(successDescription, "200");
  }

  /**
   * Kept as a hand-written response set, not routed through {@link SpecBuilders#standardResponses},
   * because the 403 description here ("Forbidden - root user required") is specific to this domain
   * and must not be reworded to the generic "Forbidden" that standardResponses would produce.
   */
  private ApiResponses createAdminResponses(final String successDescription, final String successCode) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse(successCode, SpecBuilders.jsonResponse(successDescription, null));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden - root user required"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }
}
