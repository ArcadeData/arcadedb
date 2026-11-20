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

import com.arcadedb.server.http.handler.OpenApiSpecGenerator;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SecurityAdminApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void usersPathKeepsAllFourMethodsAndOperationIds() {
    final PathItem users = openAPI.getPaths().get("/api/v1/server/users");
    assertThat(users.getGet().getOperationId()).isEqualTo("listUsers");
    assertThat(users.getPost().getOperationId()).isEqualTo("createUser");
    assertThat(users.getPut().getOperationId()).isEqualTo("updateUser");
    assertThat(users.getDelete().getOperationId()).isEqualTo("deleteUser");
  }

  @Test
  void groupsPathKeepsThreeMethods() {
    final PathItem groups = openAPI.getPaths().get("/api/v1/server/groups");
    assertThat(groups.getGet().getOperationId()).isEqualTo("listGroups");
    assertThat(groups.getPost().getOperationId()).isEqualTo("createOrUpdateGroup");
    assertThat(groups.getDelete().getOperationId()).isEqualTo("deleteGroup");
    assertThat(groups.getPut()).isNull();
  }

  @Test
  void apiTokensPathKeepsThreeMethods() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");
    assertThat(tokens.getGet().getOperationId()).isEqualTo("listApiTokens");
    assertThat(tokens.getPost().getOperationId()).isEqualTo("createApiToken");
    assertThat(tokens.getDelete().getOperationId()).isEqualTo("deleteApiToken");
  }

  @Test
  void createUserAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void createApiTokenAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void deleteOperationsDeclareTheirQueryParameters() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("name");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactlyInAnyOrder("database", "name");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("token");
  }

  @Test
  void everyOperationIsTaggedSecurity() {
    for (final String path : List.of("/api/v1/server/users", "/api/v1/server/groups",
        "/api/v1/server/api-tokens")) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item.readOperations()).allSatisfy(op ->
          assertThat(op.getTags()).as("%s", path).containsExactly("Security"));
    }
  }

  /*
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
  private Schema<?> schema(final String name) {
    return openAPI.getComponents().getSchemas().get(name);
  }

  private String bodyRef(final Operation operation) {
    return operation.getRequestBody().getContent().get(SpecBuilders.JSON).getSchema().get$ref();
  }

  private String responseRef(final Operation operation, final String status) {
    return operation.getResponses().get(status).getContent().get(SpecBuilders.JSON).getSchema().get$ref();
  }

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  // Regression: #7578 the security admin bodies and responses name real component schemas
  /** The password bound the handler enforces, so a client rejects a short one before sending it. */
  @Test
  void theCreateUserRequestCarriesThePasswordBoundsTheHandlerEnforces() {
    final Schema<?> password = (Schema<?>) schema("CreateUserRequest").getProperties().get("password");

    assertThat(password.getMinLength()).isEqualTo(8);
    assertThat(password.getMaxLength()).isEqualTo(256);
    assertThat(schema("CreateUserRequest").getRequired()).containsExactlyInAnyOrder("name", "password");
  }

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  // Regression: #7578 the security admin bodies and responses name real component schemas
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

  /*
   * Issue #7804: the transport refusal #7372 added to {@code POST /api/v1/server/api-tokens} answers 412, and
   * the generated OpenAPI document did not mention it. Studio is not the only client of this route - a
   * generated SDK builds its error handling from this document, and a status absent from the spec is a status
   * the SDK turns into an unhandled fault.
   * <p>
   * Only the mint is affected. The list and the delete on the same path apply no transport check, because
   * neither returns token material, and asserting that here keeps a later "add 412 everywhere on this path"
   * from widening a contract nothing enforces.
   */
  // Regression: #7804 the API-token mint declares the 412 transport refusal
  @Test
  void theMintDocumentsThe412ItCanAnswer() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().get("412"))
        .as("PostApiTokenHandler.checkTransport answers 412 and a client has to be told it can")
        .isNotNull();
    assertThat(tokens.getPost().getResponses().get("412").getDescription())
        .containsIgnoringCase("transport");
  }

  // Regression: #7804 the API-token mint declares the 412 transport refusal
  /**
   * The 412 means "reconnect over TLS", which is only worth saying to a caller that is being handed a secret.
   */
  @Test
  void theRoutesThatReturnNoTokenMaterialDoNotClaimIt() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getGet().getResponses().get("412")).isNull();
    assertThat(tokens.getDelete().getResponses().get("412")).isNull();
  }

  // Regression: #7804 the API-token mint declares the 412 transport refusal
  /**
   * The mint keeps every response it already declared: the 412 is an addition, not a replacement.
   */
  @Test
  void theExistingMintResponsesSurvive() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().keySet())
        .contains("201", "400", "401", "403", "500");
    assertThat(tokens.getPost().getOperationId()).isEqualTo("createApiToken");
  }

  /*
   * Issue #8109: the group and API-token write routes now forward to the HA leader like the user routes, so they can
   * answer the bounded forward's 504 (issue #7507), and the spec has to say so. The reads do not forward.
   */
  // Regression: #8109 the leader-forwarded security writes declare the 504
  @Test
  void theGroupWritesDeclareTheLeaderForwardTimeout() {
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getPost().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getDelete().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getGet().getResponses()).doesNotContainKey("504");
  }

  // Regression: #8109 the leader-forwarded security writes declare the 504
  @Test
  void theApiTokenWritesDeclareTheLeaderForwardTimeout() {
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getPost().getResponses())
        .containsKey("504").containsKey("412").containsKey("201");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getDelete().getResponses()).containsKey("504");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getGet().getResponses()).doesNotContainKey("504");
  }

  /*
   * Issue #10075: {@code POST /api/v1/server/api-tokens} answers 409 when the token name is taken, and now names the
   * exception in the body so a client can type the conflict. The generated OpenAPI document did not mention the 409 at all,
   * and a generated SDK builds its error handling from that document.
   * <p>
   * Only the mint can find a name taken: the list and the delete on the same path create nothing.
   */
  // Regression: #10075 the API-token mint declares the 409 for a taken name
  @Test
  void theMintDocumentsTheConflictItAnswersForATakenName() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getPost().getResponses().get("409")).as("PostApiTokenHandler answers 409 for a taken name").isNotNull();
    assertThat(tokens.getPost().getResponses().get("409").getDescription()).containsIgnoringCase("already issued")
        .contains("RemoteAlreadyExistsException");
  }

  // Regression: #10075 the API-token mint declares the 409 for a taken name
  @Test
  void theRoutesThatCreateNothingDoNotClaimIt() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");

    assertThat(tokens.getGet().getResponses().get("409")).isNull();
    assertThat(tokens.getDelete().getResponses().get("409")).isNull();
  }

  // Regression: #10110 the security admin routes that answer 404 declare it on the generated document
  /**
   * Issue #10110: the security admin routes that answer 404 for a name or hash nothing has did not declare it, so a
   * client generated from the document treated the status as undocumented. {@code DeleteApiTokenHandler},
   * {@code DeleteUserHandler}, {@code PutUserHandler} and {@code DeleteGroupHandler} all answer it. The second half of
   * the issue, the duplicate-name case of {@code createApiToken}'s 409, was declared by #10075; it is pinned here on the
   * generated document, where the in-flight replay description is appended to it.
   */
  @Nested
  class NotFoundOnTheGeneratedDocument {
    private static final String TOKENS = "/api/v1/server/api-tokens";
    private static final String USERS  = "/api/v1/server/users";
    private static final String GROUPS = "/api/v1/server/groups";

    private final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

    @Test
    void deleteApiTokenDeclaresItsNotFound() {
      final ApiResponse notFound = path(TOKENS).getDelete().getResponses().get("404");
      assertThat(notFound).isNotNull();
      assertThat(notFound.getDescription()).contains("No API token has this hash");
    }

    @Test
    void deleteUserDeclaresItsNotFound() {
      final ApiResponse notFound = path(USERS).getDelete().getResponses().get("404");
      assertThat(notFound).isNotNull();
      assertThat(notFound.getDescription()).contains("No user has this name");
    }

    @Test
    void updateUserDeclaresItsNotFound() {
      final ApiResponse notFound = path(USERS).getPut().getResponses().get("404");
      assertThat(notFound).isNotNull();
      assertThat(notFound.getDescription()).contains("No user has this name");
    }

    @Test
    void deleteGroupDeclaresItsNotFound() {
      final ApiResponse notFound = path(GROUPS).getDelete().getResponses().get("404");
      assertThat(notFound).isNotNull();
      assertThat(notFound.getDescription()).contains("No group has this name");
    }

    @Test
    void routesThatNeverAnswerNotFoundDoNotDeclareIt() {
      // The handlers behind these create or list: nothing they look up can be missing, so a 404 here would be a promise
      // a client codes against and never receives
      assertThat(path(TOKENS).getGet().getResponses()).doesNotContainKey("404");
      assertThat(path(TOKENS).getPost().getResponses()).doesNotContainKey("404");
      assertThat(path(USERS).getGet().getResponses()).doesNotContainKey("404");
      assertThat(path(USERS).getPost().getResponses()).doesNotContainKey("404");
      assertThat(path(GROUPS).getGet().getResponses()).doesNotContainKey("404");
      assertThat(path(GROUPS).getPost().getResponses()).doesNotContainKey("404");
    }

    @Test
    void createApiTokenConflictNamesBothTheDuplicateNameAndTheInFlightReplay() {
      final String description = path(TOKENS).getPost().getResponses().get("409").getDescription();
      assertThat(description).contains("a token with this name was already issued");
      assertThat(description).contains("still executing");
    }

    private PathItem path(final String path) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item).as(path).isNotNull();
      return item;
    }
  }
}
