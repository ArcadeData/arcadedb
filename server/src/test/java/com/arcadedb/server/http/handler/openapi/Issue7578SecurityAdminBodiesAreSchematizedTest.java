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

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #7577 and #7578 for the security administration routes, which had nothing at all: no request schema, no
 * response schema, no registered component. Every body was {@code SpecBuilders.jsonBody(..., null, ...)}, which
 * rendered as a bare {@code type: object}, so a generated client got an untyped map for the one API where a wrong
 * field name silently grants or fails to revoke access.
 * <p>
 * The schemas below are read off the handlers and the control plane, so this file is where the reading is
 * recorded: the sweeps say "something is declared", and these assertions say the declared thing is the right
 * thing.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7578">issue #7578</a>
 */
class Issue7578SecurityAdminBodiesAreSchematizedTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  private String bodyRef(final Operation operation) {
    return operation.getRequestBody().getContent().get(SpecBuilders.JSON).getSchema().get$ref();
  }

  private String responseRef(final Operation operation, final String status) {
    return operation.getResponses().get(status).getContent().get(SpecBuilders.JSON).getSchema().get$ref();
  }

  /** The defect: every body named no component, so none of them carried any information. */
  @Test
  void everyBodyAndEverySuccessResponseNamesAComponent() {
    final var users = openAPI.getPaths().get("/api/v1/server/users");
    final var groups = openAPI.getPaths().get("/api/v1/server/groups");
    final var tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(responseRef(users.getGet(), "200")).endsWith("/UserList");
    assertThat(bodyRef(users.getPost())).endsWith("/CreateUserRequest");
    assertThat(responseRef(users.getPost(), "201")).endsWith("/SecurityAdminResult");
    assertThat(bodyRef(users.getPut())).endsWith("/UpdateUserRequest");
    assertThat(responseRef(users.getDelete(), "200")).endsWith("/SecurityAdminResult");

    assertThat(responseRef(groups.getGet(), "200")).endsWith("/GroupList");
    assertThat(bodyRef(groups.getPost())).endsWith("/SaveGroupRequest");
    assertThat(responseRef(groups.getDelete(), "200")).endsWith("/SecurityAdminResult");

    assertThat(responseRef(tokens.getGet(), "200")).endsWith("/ApiTokenList");
    assertThat(bodyRef(tokens.getPost())).endsWith("/CreateApiTokenRequest");
    assertThat(responseRef(tokens.getPost(), "201")).endsWith("/CreateApiTokenResponse");
    assertThat(responseRef(tokens.getDelete(), "200")).endsWith("/SecurityAdminResult");
  }

  /**
   * A user's database assignments are a map of database name to the groups held on it, which is exactly the kind
   * of thing that used to be a bare object. Declared as a typed map so a generator emits
   * {@code Map&lt;String, List&lt;String&gt;&gt;}.
   */
  @Test
  void aUsersDatabaseAssignmentsAreATypedMapRatherThanAnUntypedOne() {
    final Schema<?> databases = (Schema<?>) schema("CreateUserRequest").getProperties().get("databases");

    assertThat(databases.getAdditionalProperties()).isInstanceOf(Schema.class);
    final Schema<?> groups = (Schema<?>) databases.getAdditionalProperties();
    assertThat(groups.getType()).isEqualTo("array");
    assertThat(groups.getItems().getType()).isEqualTo("string");
  }

  /** The password bound the handler enforces, so a client rejects a short one before sending it. */
  @Test
  void theCreateUserRequestCarriesThePasswordBoundsTheHandlerEnforces() {
    final Schema<?> password = (Schema<?>) schema("CreateUserRequest").getProperties().get("password");

    assertThat(password.getMinLength()).isEqualTo(8);
    assertThat(password.getMaxLength()).isEqualTo(256);
    assertThat(schema("CreateUserRequest").getRequired()).containsExactlyInAnyOrder("name", "password");
  }

  /**
   * The update is the counterpart: {@code PutUserHandler} reads each member only when present, so nothing is
   * required and a body carrying neither still answers 200. The bounds still apply to a password that IS sent.
   */
  @Test
  void theUpdateUserRequestRequiresNothingButStillBoundsThePassword() {
    assertThat(schema("UpdateUserRequest").getRequired()).isNullOrEmpty();
    assertThat(((Schema<?>) schema("UpdateUserRequest").getProperties().get("password")).getMinLength())
        .isEqualTo(8);
  }

  /**
   * A group is stored normalized - {@code ServerControlPlane.saveGroup} writes a default for every member the
   * request omitted - which is what lets the stored form require all four while the request requires only the
   * two that identify it.
   */
  @Test
  void aStoredGroupCarriesAllFourMembersWhileTheRequestNeedsOnlyItsIdentity() {
    assertThat(schema("GroupDefinition").getRequired())
        .containsExactlyInAnyOrder("resultSetLimit", "readTimeout", "access", "types");
    assertThat(schema("SaveGroupRequest").getRequired())
        .as("the other four are defaulted by saveGroup, so sending them is optional")
        .containsExactlyInAnyOrder("database", "name");
  }

  /**
   * The group document is three levels of map, and the middle one is the easy one to get wrong: a database entry
   * carries a {@code groups} member rather than being the group map itself. A client that read the entry as the
   * group map would find one key named {@code groups} and no groups.
   */
  @Test
  void theGroupDocumentNestsGroupsUnderTheirDatabaseEntry() {
    final Schema<?> document = (Schema<?>) schema("GroupList").getProperties().get("result");
    assertThat(document.getRequired()).containsExactlyInAnyOrder("databases", "version");

    final Schema<?> databaseEntry = (Schema<?>) document.getProperties().get("databases")
        .getAdditionalProperties();
    assertThat(databaseEntry.getProperties().keySet()).containsExactly("groups");

    final Schema<?> groups = (Schema<?>) databaseEntry.getProperties().get("groups");
    assertThat(((Schema<?>) groups.getAdditionalProperties()).get$ref()).endsWith("/GroupDefinition");
  }

  /**
   * The token listing never includes token material, and the mint response is the one and only place the
   * plaintext exists. Built from the listing entry rather than declared again, so the two cannot describe
   * different tokens.
   */
  @Test
  void onlyTheMintResponseCarriesThePlaintextToken() {
    final Schema<?> listed = schema("ApiTokenList").getProperties().get("result").getItems();
    assertThat(listed.getProperties()).doesNotContainKey("token");
    assertThat(listed.getRequired())
        .as("ServerControlPlane.listApiTokens builds each entry as one block of puts")
        .containsExactlyInAnyOrder("name", "database", "expiresAt", "createdAt", "permissions", "tokenHash",
            "tokenSuffix");
    assertThat(listed.getProperties())
        .as("'expired' is declared but not required: the schema is shared with the mint response, and only the "
            + "listing derives it (issue #7601)")
        .containsKey("expired");

    final Schema<?> minted = (Schema<?>) schema("CreateApiTokenResponse").getProperties().get("result");
    assertThat(minted.getProperties().keySet())
        .as("the mint carries everything the listing carries, plus the plaintext")
        .containsAll(listed.getProperties().keySet())
        .contains("token");
    assertThat(minted.getRequired()).contains("token");
    assertThat(((Schema<?>) minted.getProperties().get("token")).getDescription())
        .as("and it has to say the value cannot be read back, which is the whole operational point")
        .contains("once");
  }
}
