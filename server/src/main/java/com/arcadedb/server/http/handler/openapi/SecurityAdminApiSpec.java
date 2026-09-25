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

import java.util.ArrayList;
import java.util.List;

/**
 * Documents the root-only administration of users, groups, and API tokens.
 * <p>
 * Every body on these three routes used to be an un-named {@code object} - no request schema, no response
 * schema, nothing this contributor registered under {@code components} at all - so a generated client got an
 * untyped map for the one API where a wrong field name silently grants or fails to revoke access. Issues #7577
 * and #7578 are the sweep that ended that: the bodies below are read off the handlers and the control plane
 * ({@code ServerControlPlane.listApiTokens}, {@code ServerSecurity.groupsToJSON},
 * {@code ServerControlPlane.saveGroup}), and every response says which of its fields is always there.
 */
public class SecurityAdminApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server/users", createUsersPath());
    openAPI.getPaths().addPathItem("/api/v1/server/groups", createGroupsPath());
    openAPI.getPaths().addPathItem("/api/v1/server/api-tokens", createApiTokensPath());

    openAPI.getComponents().addSchemas("UserList", createUserListSchema());
    openAPI.getComponents().addSchemas("CreateUserRequest", createCreateUserRequestSchema());
    openAPI.getComponents().addSchemas("UpdateUserRequest", createUpdateUserRequestSchema());
    openAPI.getComponents().addSchemas("GroupList", createGroupListSchema());
    openAPI.getComponents().addSchemas("GroupDefinition", createGroupDefinitionSchema());
    openAPI.getComponents().addSchemas("SaveGroupRequest", createSaveGroupRequestSchema());
    openAPI.getComponents().addSchemas("ApiTokenList", createApiTokenListSchema());
    openAPI.getComponents().addSchemas("CreateApiTokenRequest", createCreateApiTokenRequestSchema());
    openAPI.getComponents().addSchemas("CreateApiTokenResponse", createCreateApiTokenResponseSchema());
    openAPI.getComponents().addSchemas("SecurityAdminResult", createAdminResultSchema());
  }

  /**
   * The map a user's database assignments take: database name to the groups the user holds on it. {@code "*"} is
   * a legal key and means every database.
   */
  private Schema<?> userDatabasesSchema() {
    return SpecBuilders.mapOf(
        SpecBuilders.arrayOf(SpecBuilders.string("Group name"), "Groups the user holds on this database"),
        "Database assignments, keyed by database name. '*' means every database");
  }

  private Schema<?> createUserListSchema() {
    final Schema<Object> user = SpecBuilders.object(
        "One server user. The password hash is never returned");
    user.addProperty("name", SpecBuilders.string("User name"));
    user.addProperty("databases", userDatabasesSchema());
    // GetUsersHandler writes both on every row, defaulting 'databases' to an empty object rather than omitting
    // it, which is what makes it required here.
    user.setRequired(List.of("name", "databases"));

    final Schema<Object> schema = SpecBuilders.object("Every server user");
    schema.addProperty("result", SpecBuilders.arrayOf(user, "Users, one entry each"));
    schema.setRequired(List.of("result"));
    return schema;
  }

  private Schema<?> createCreateUserRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("A user to create");
    schema.addProperty("name", SpecBuilders.string("""
        User name. Must not be blank and must not start with 'apitoken:', which is the prefix reserved for the \
        synthetic principals API tokens authenticate as."""));
    final Schema<String> password = SpecBuilders.string(
        "Plaintext password, hashed by the server before it is stored");
    password.setMinLength(8);
    password.setMaxLength(256);
    schema.addProperty("password", password);
    schema.addProperty("databases", userDatabasesSchema());
    schema.setRequired(List.of("name", "password"));
    return schema;
  }

  private Schema<?> createUpdateUserRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Changes to apply to an existing user, named by the 'name' QUERY parameter rather than by the body. \
        Both members are optional and an omitted one is left alone; a body carrying neither is accepted and \
        changes nothing.""");
    final Schema<String> password = SpecBuilders.string(
        "New plaintext password. Omit to leave the current one in place");
    password.setMinLength(8);
    password.setMaxLength(256);
    schema.addProperty("password", password);
    schema.addProperty("databases", userDatabasesSchema());
    return schema;
  }

  /** One group definition, in the normalized form {@code ServerControlPlane.saveGroup} stores. */
  private Schema<?> createGroupDefinitionSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        One group's permissions on one database. Stored normalized: a member the request omitted is written \
        with the default below rather than left absent, so a group read back always carries all four.""");
    schema.addProperty("resultSetLimit", SpecBuilders.integer(
        "Maximum rows a member of this group may read in one result. -1 for no limit").example(-1));
    schema.addProperty("readTimeout", SpecBuilders.integer(
        "Maximum milliseconds a member's read may take. -1 for no limit").example(-1));
    schema.addProperty("access", SpecBuilders.arrayOf(SpecBuilders.string("Access right"),
        "Server-level rights the group grants. Empty when it grants none"));
    schema.addProperty("types", SpecBuilders.freeFormObject("""
        Per-type permissions, keyed by type name. '*' is a legal key and covers every type. An open map \
        because the keys are the schema's own type names."""));
    schema.setRequired(List.of("resultSetLimit", "readTimeout", "access", "types"));
    return schema;
  }

  private Schema<?> createGroupListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Every security group, grouped by database");
    schema.addProperty("result", groupDocumentSchema());
    schema.setRequired(List.of("result"));
    return schema;
  }

  /**
   * The {@code result} of the group listing: the group document as {@code ServerSecurity.groupsToJSON} writes it,
   * which is both of its members every time.
   * <p>
   * Three levels of map, and the middle one is easy to miss: a database entry is an OBJECT carrying a
   * {@code groups} member, not the group map itself. {@code groupsDocumentWith} creates it as
   * {@code {"groups": {}}}, so a client that read the entry as the group map would find one key named
   * {@code groups} and no groups.
   */
  private Schema<Object> groupDocumentSchema() {
    final Schema<Object> databaseEntry = SpecBuilders.object("One database's groups");
    databaseEntry.addProperty("groups", SpecBuilders.mapOf(SpecBuilders.ref("GroupDefinition"),
        "The database's groups, keyed by group name"));
    databaseEntry.setRequired(List.of("groups"));

    final Schema<Object> schema = SpecBuilders.object("The group document as stored");
    schema.addProperty("databases", SpecBuilders.mapOf(databaseEntry,
        "Group definitions per database, keyed by database name. '*' is a legal key and covers every database"));
    schema.addProperty("version", SpecBuilders.integer("Schema version of the group document"));
    schema.setRequired(List.of("databases", "version"));
    return schema;
  }

  private Schema<?> createSaveGroupRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        A group to create or replace. Replaces any group of the same name on the same database outright - the \
        members are not merged into the existing definition - and refreshes the cached permissions of every \
        open database it applies to.""");
    schema.addProperty("database", SpecBuilders.string(
        "Database the group applies to. '*' means every database"));
    schema.addProperty("name", SpecBuilders.string("Group name"));
    schema.addProperty("resultSetLimit", SpecBuilders.integer(
        "Maximum rows a member may read in one result. Defaults to -1, no limit"));
    schema.addProperty("readTimeout", SpecBuilders.integer(
        "Maximum milliseconds a member's read may take. Defaults to -1, no limit"));
    schema.addProperty("access", SpecBuilders.arrayOf(SpecBuilders.string("Access right"),
        "Server-level rights the group grants. Defaults to none"));
    schema.addProperty("types", SpecBuilders.freeFormObject(
        "Per-type permissions, keyed by type name. Defaults to none"));
    schema.setRequired(List.of("database", "name"));
    return schema;
  }

  /**
   * One issued token, in the projection {@code ServerControlPlane.listApiTokens} returns - which never includes
   * token material. {@code tokenHash} is the handle a revocation names.
   */
  private Schema<?> apiTokenSchema() {
    final Schema<Object> schema = SpecBuilders.object("One issued API token, as metadata only");
    schema.addProperty("name", SpecBuilders.string("Token name, unique among issued tokens"));
    schema.addProperty("database", SpecBuilders.string(
        "Database the token is scoped to. '*' means every database"));
    schema.addProperty("expiresAt", SpecBuilders.integer(
        "Expiry as epoch milliseconds. 0 means the token does not expire"));
    schema.addProperty("createdAt", SpecBuilders.integer("Issue time as epoch milliseconds"));
    schema.addProperty("permissions", SpecBuilders.freeFormObject(
        "Permissions the token carries, in the same shape a group's 'types' map takes"));
    schema.addProperty("tokenHash", SpecBuilders.string(
        "SHA-256 hex of the token. The handle DELETE names; the only one the server keeps"));
    schema.addProperty("tokenSuffix", SpecBuilders.string(
        "Last characters of the plaintext token, so an operator can tell two entries apart"));
    // Declared but NOT required, because this schema is shared with the mint response and only the LISTING
    // carries it: it is derived at read time from expiresAt, and a freshly minted token has nothing to derive.
    // Since issue #7601 an expired token stays in the document until the next token change retires it, so the
    // listing says so outright rather than leaving the reader to compare expiresAt with the current time.
    schema.addProperty("expired", SpecBuilders.bool(
        "Whether this token's expiry has passed. Carried by the listing only. An expired token authenticates "
            + "nobody, but it stays listed until the next token change retires it"));
    // listApiTokens builds each entry as one block of puts, so an entry is present whole.
    schema.setRequired(List.of("name", "database", "expiresAt", "createdAt", "permissions", "tokenHash",
        "tokenSuffix"));
    return schema;
  }

  private Schema<?> createApiTokenListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Every issued API token");
    schema.addProperty("result", SpecBuilders.arrayOf(apiTokenSchema(), "Tokens, metadata only"));
    schema.addProperty("count", SpecBuilders.integer("Number of tokens returned"));
    schema.setRequired(List.of("result", "count"));
    return schema;
  }

  private Schema<?> createCreateApiTokenRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("A token to issue");
    schema.addProperty("name", SpecBuilders.string("Token name. Must not already be in use"));
    schema.addProperty("database", SpecBuilders.string(
        "Database to scope the token to. Defaults to '*', every database, which an empty value also means"));
    schema.addProperty("expiresAt", SpecBuilders.integer(
        "Expiry as epoch milliseconds. Defaults to 0, which does not expire"));
    schema.addProperty("permissions", SpecBuilders.freeFormObject(
        "Permissions to grant, in the same shape a group's 'types' map takes. Defaults to none"));
    schema.setRequired(List.of("name"));
    return schema;
  }

  @SuppressWarnings("unchecked")
  private Schema<?> createCreateApiTokenResponseSchema() {
    // Built from the listing entry rather than declared again, so the two cannot come to describe different
    // tokens. The one field the listing can never carry is added on top.
    final Schema<Object> minted = (Schema<Object>) apiTokenSchema();
    minted.setDescription("""
        The issued token: every field the listing carries, plus the plaintext 'token'. Read it now - the \
        server stores only the hash, so this is the one and only time that value exists outside the caller's \
        hands.""");
    minted.addProperty("token", SpecBuilders.string("""
        The plaintext token, presented as a bearer credential. Returned exactly once, here; it cannot be read \
        back from the listing and cannot be recovered if lost."""));
    minted.setRequired(new ArrayList<>(minted.getRequired()));
    minted.getRequired().add("token");

    final Schema<Object> schema = SpecBuilders.object("Result of issuing an API token");
    schema.addProperty("result", minted);
    schema.setRequired(List.of("result"));
    return schema;
  }

  private Schema<?> createAdminResultSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Outcome of an administrative change, as one human-readable sentence. There is nothing else to report: \
        the change either applied or the call failed.""");
    schema.addProperty("result", SpecBuilders.string(
        "What was done, e.g. \"User 'alice' created\""));
    schema.setRequired(List.of("result"));
    return schema;
  }

  private PathItem createUsersPath() {
    final PathItem pathItem = new PathItem();

    final Operation getOp = new Operation();
    getOp.setSummary("List users");
    getOp.setDescription("Lists all server users with their database/group assignments (root only)");
    getOp.setOperationId("listUsers");
    getOp.addTagsItem("Security");
    getOp.setResponses(createAdminResponses("List of users retrieved successfully", "200", "UserList"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create user");
    postOp.setDescription("Creates a new server user (root only). Requires name (string) and password "
        + "(min 8 chars). On an HA cluster the change is replicated to every node as a Raft entry.");
    postOp.setOperationId("createUser");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody(
        "User creation request with name, password, and optional databases", "CreateUserRequest", true));
    postOp.setResponses(forwardedToLeaderResponses("User created", "201", "SecurityAdminResult"));
    pathItem.setPost(postOp);

    final Operation putOp = new Operation();
    putOp.setSummary("Update user");
    putOp.setDescription("Updates an existing user's password and/or database assignments (root only). "
        + "On an HA cluster the change is replicated to every node as a Raft entry.");
    putOp.setOperationId("updateUser");
    putOp.addTagsItem("Security");
    putOp.addParametersItem(SpecBuilders.queryParam("name", "User name", true));
    putOp.setRequestBody(SpecBuilders.jsonBody(
        "User update request with optional password and databases", "UpdateUserRequest", true));
    putOp.setResponses(forwardedToLeaderResponses("User updated", "200", "SecurityAdminResult"));
    pathItem.setPut(putOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete user");
    deleteOp.setDescription("Deletes a server user (root only). On an HA cluster the removal is "
        + "replicated to every node as a Raft entry.");
    deleteOp.setOperationId("deleteUser");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("name", "User name to delete", true));
    deleteOp.setResponses(forwardedToLeaderResponses("User deleted", "200", "SecurityAdminResult"));
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
    getOp.setResponses(createAdminResponses("List of groups retrieved successfully", "200", "GroupList"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create or update group");
    postOp.setDescription("Creates or updates a security group (root only). On an HA cluster a follower forwards "
        + "the request to the leader, which replicates the group document to every node as a Raft entry.");
    postOp.setOperationId("createOrUpdateGroup");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody(
        "Group configuration with database, name, and access permissions", "SaveGroupRequest", true));
    postOp.setResponses(forwardedToLeaderResponses("Group created or updated", "200", "SecurityAdminResult"));
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete group");
    deleteOp.setDescription("Deletes a security group (root only). On an HA cluster a follower forwards the "
        + "request to the leader.");
    deleteOp.setOperationId("deleteGroup");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("database", "Database name", true));
    deleteOp.addParametersItem(SpecBuilders.queryParam("name", "Group name to delete", true));
    deleteOp.setResponses(forwardedToLeaderResponses("Group deleted", "200", "SecurityAdminResult"));
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
    getOp.setResponses(createAdminResponses("List of API tokens retrieved successfully", "200", "ApiTokenList"));
    pathItem.setGet(getOp);

    final Operation postOp = new Operation();
    postOp.setSummary("Create API token");
    postOp.setDescription("Creates a new API token (root only). The plaintext token is returned only once in the "
        + "response. On an HA cluster a follower checks the transport of the client connection, then forwards the "
        + "request to the leader.");
    postOp.setOperationId("createApiToken");
    postOp.addTagsItem("Security");
    postOp.setRequestBody(SpecBuilders.jsonBody(
        "Token creation with name, database, expiresAt, and permissions", "CreateApiTokenRequest", true));
    final ApiResponses postResponses = forwardedToLeaderResponses("API token created", "201", "CreateApiTokenResponse");
    // Only the mint declares it: the list and the delete return no token material, so neither applies the
    // transport check that produces this status (issues #7372, #7804).
    postResponses.addApiResponse("412", SpecBuilders.errorResponse(
        "Precondition failed - the transport is not confidential. The token is returned in plaintext exactly "
            + "once, so it is not written back over a cleartext connection to a non-loopback client when "
            + "arcadedb.server.apiTokenRequireSecureTransport is enabled. Reconnect over HTTPS, or have a reverse "
            + "proxy listed in arcadedb.server.apiTokenTrustedProxies terminate TLS in front of the server. On an HA "
            + "cluster the leader applies the same check to the hop a follower forwarded the mint over"));
    postOp.setResponses(postResponses);
    pathItem.setPost(postOp);

    final Operation deleteOp = new Operation();
    deleteOp.setSummary("Delete API token");
    deleteOp.setDescription("Deletes an API token by its hash (root only). Plaintext tokens are rejected, on the "
        + "node that received them. On an HA cluster a follower forwards the request to the leader.");
    deleteOp.setOperationId("deleteApiToken");
    deleteOp.addTagsItem("Security");
    deleteOp.addParametersItem(SpecBuilders.queryParam("token", "Token hash (SHA-256 hex)", true));
    deleteOp.setResponses(forwardedToLeaderResponses("API token deleted", "200", "SecurityAdminResult"));
    pathItem.setDelete(deleteOp);

    return pathItem;
  }

  /**
   * The responses of a write that an HA follower forwards to the leader rather than executing locally. The
   * forward is bounded, so it can answer 504 where the other admin routes cannot (issue #7507).
   */
  private ApiResponses forwardedToLeaderResponses(final String successDescription, final String successCode,
      final String successSchema) {
    final ApiResponses responses = createAdminResponses(successDescription, successCode, successSchema);
    responses.addApiResponse("504", SpecBuilders.errorResponse(SpecBuilders.LEADER_FORWARD_TIMEOUT_DESCRIPTION));
    return responses;
  }

  /**
   * Kept as a hand-written response set, not routed through {@link SpecBuilders#standardResponses},
   * because the 403 description here ("Forbidden - root user required") is specific to this domain
   * and must not be reworded to the generic "Forbidden" that standardResponses would produce.
   */
  private ApiResponses createAdminResponses(final String successDescription, final String successCode,
      final String successSchema) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse(successCode, SpecBuilders.jsonResponse(successDescription, successSchema));
    responses.addApiResponse("400", SpecBuilders.errorResponse("Bad request"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden - root user required"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }
}
