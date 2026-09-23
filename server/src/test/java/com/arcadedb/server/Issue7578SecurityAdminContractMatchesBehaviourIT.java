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
package com.arcadedb.server;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.openapi.SecurityAdminApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.media.Schema;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7578 for the security administration routes, driven end to end.
 * <p>
 * {@code Issue7578SecurityAdminBodiesAreSchematizedTest} asserts what the document says; this asserts the server
 * agrees. The distinction matters most here of all: these three routes had no schemas at all before this PR, so
 * every {@code required} list in {@code SecurityAdminApiSpec} was written by reading handlers rather than by
 * observing them, and this is the one API where a field name a client gets wrong silently grants or fails to
 * revoke access (code review on PR #7749 named it as the remaining gap).
 * <p>
 * Every expectation is read out of the document itself, so the assertions cannot drift from it: a name added to
 * a {@code required} list and not to the response fails here, and so does a response member the schema never
 * declared.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7578">issue #7578</a>
 */
class Issue7578SecurityAdminContractMatchesBehaviourIT extends BaseGraphServerTest {
  private static final String USER_NAME  = "issue7578user";
  private static final String GROUP_NAME = "issue7578group";
  private static final String TOKEN_NAME = "issue7578token";

  private final OpenAPI openAPI = contribute();

  @Override
  protected int getServerCount() {
    return 1;
  }

  private static OpenAPI contribute() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
    return openAPI;
  }

  private Schema<?> schema(final String name) {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get(name);
    assertThat(schema).as("%s must be registered", name).isNotNull();
    return schema;
  }

  /** Every name {@code component} requires is a key {@code body} carries. */
  private void assertCarriesEveryRequiredName(final String component, final JSONObject body) {
    final List<String> required = schema(component).getRequired();
    assertThat(required).as("%s must declare what it always sends", component).isNotEmpty();
    for (final String name : required)
      assertThat(body.has(name))
          .as("%s promises '%s'; the response was %s", component, name, body)
          .isTrue();
  }

  /** ... and nothing the response carries is a member the document forgot to declare. */
  private void assertDeclaresEveryMemberSent(final String component, final JSONObject body) {
    assertThat(schema(component).getProperties().keySet())
        .as("%s must declare every member the handler sends, response was %s", component, body)
        .containsAll(body.keySet());
  }

  @Test
  void theUserListMatchesItsSchemaBothWays() throws Exception {
    request("POST", "/server/users", 201, new JSONObject()
        .put("name", USER_NAME)
        .put("password", "aStrongPassword7578")
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new Object[] { "admin" }))));

    final JSONObject list = request("GET", "/server/users", 200, null);
    assertCarriesEveryRequiredName("UserList", list);
    assertDeclaresEveryMemberSent("UserList", list);

    final JSONObject created = rowNamed(list.getJSONArray("result"), USER_NAME);
    final Schema<?> user = schema("UserList").getProperties().get("result").getItems();
    for (final String name : user.getRequired())
      assertThat(created.has(name)).as("a user row promises '%s'; the row was %s", name, created).isTrue();
    assertThat(user.getProperties().keySet())
        .as("and declares every member GetUsersHandler writes")
        .containsAll(created.keySet());
    assertThat(created.has("password"))
        .as("the schema says the hash is never returned, so it had better not be")
        .isFalse();

    // The write responses are the other half of the pair.
    final JSONObject updated = request("PUT", "/server/users?name=" + USER_NAME, 200,
        new JSONObject().put("password", "anEvenStrongerPassword7578"));
    assertCarriesEveryRequiredName("SecurityAdminResult", updated);
    assertDeclaresEveryMemberSent("SecurityAdminResult", updated);

    final JSONObject deleted = request("DELETE", "/server/users?name=" + USER_NAME, 200, null);
    assertCarriesEveryRequiredName("SecurityAdminResult", deleted);
  }

  /**
   * The group document is the schema with the most nesting, and the one whose middle level is easiest to get
   * wrong: a database entry carries a {@code groups} member rather than being the group map itself.
   */
  @Test
  void theGroupListMatchesItsSchemaIncludingTheNesting() throws Exception {
    final JSONObject saved = request("POST", "/server/groups", 200, new JSONObject()
        .put("database", getDatabaseName())
        .put("name", GROUP_NAME)
        .put("resultSetLimit", 100));
    assertCarriesEveryRequiredName("SecurityAdminResult", saved);

    final JSONObject list = request("GET", "/server/groups", 200, null);
    assertCarriesEveryRequiredName("GroupList", list);
    assertDeclaresEveryMemberSent("GroupList", list);

    final JSONObject document = list.getJSONObject("result");
    final Schema<?> documentSchema = (Schema<?>) schema("GroupList").getProperties().get("result");
    for (final String name : documentSchema.getRequired())
      assertThat(document.has(name)).as("the group document promises '%s', it was %s", name, document).isTrue();

    // The nesting the document claims: databases -> <db> -> groups -> <group> -> GroupDefinition.
    final JSONObject entry = document.getJSONObject("databases").getJSONObject(getDatabaseName());
    assertThat(entry.keySet())
        .as("a database entry carries a 'groups' member; it is NOT the group map itself")
        .containsExactly("groups");

    final JSONObject group = entry.getJSONObject("groups").getJSONObject(GROUP_NAME);
    for (final String name : schema("GroupDefinition").getRequired())
      assertThat(group.has(name))
          .as("saveGroup normalizes every member, so GroupDefinition promises '%s'; the group was %s", name, group)
          .isTrue();
    assertThat(schema("GroupDefinition").getProperties().keySet()).containsAll(group.keySet());

    request("DELETE", "/server/groups?database=" + getDatabaseName() + "&name=" + GROUP_NAME, 200, null);
  }

  /**
   * The mint answers with the plaintext token exactly once and the listing never does - which is the whole
   * operational point of the split between {@code CreateApiTokenResponse} and {@code ApiTokenList}, and the
   * thing the document would be worst to get wrong.
   */
  @Test
  void theApiTokenSchemasMatchTheListingAndTheMint() throws Exception {
    final JSONObject minted = request("POST", "/server/api-tokens", 201, new JSONObject()
        .put("name", TOKEN_NAME)
        .put("database", getDatabaseName()));

    assertCarriesEveryRequiredName("CreateApiTokenResponse", minted);
    final JSONObject mintedToken = minted.getJSONObject("result");
    final Schema<?> mintedSchema = (Schema<?>) schema("CreateApiTokenResponse").getProperties().get("result");
    for (final String name : mintedSchema.getRequired())
      assertThat(mintedToken.has(name))
          .as("the mint promises '%s'; it answered %s", name, mintedToken)
          .isTrue();
    assertThat(mintedSchema.getProperties().keySet()).containsAll(mintedToken.keySet());
    assertThat(mintedToken.getString("token")).as("the plaintext exists here and nowhere else").isNotBlank();

    final JSONObject list = request("GET", "/server/api-tokens", 200, null);
    assertCarriesEveryRequiredName("ApiTokenList", list);
    assertDeclaresEveryMemberSent("ApiTokenList", list);

    final JSONObject listed = rowNamed(list.getJSONArray("result"), TOKEN_NAME);
    final Schema<?> listedSchema = schema("ApiTokenList").getProperties().get("result").getItems();
    for (final String name : listedSchema.getRequired())
      assertThat(listed.has(name)).as("a token row promises '%s'; the row was %s", name, listed).isTrue();
    assertThat(listedSchema.getProperties().keySet()).containsAll(listed.keySet());
    assertThat(listed.has("token"))
        .as("the listing never carries token material, and the schema says so by not declaring it")
        .isFalse();

    request("DELETE", "/server/api-tokens?token=" + listed.getString("tokenHash"), 200, null);
  }

  private static JSONObject rowNamed(final JSONArray rows, final String name) {
    for (int i = 0; i < rows.length(); i++)
      if (name.equals(rows.getJSONObject(i).getString("name", null)))
        return rows.getJSONObject(i);
    throw new AssertionError("no row named '" + name + "' in " + rows);
  }

  private JSONObject request(final String method, final String path, final int expectedStatus,
      final JSONObject body) throws Exception {
    // Never a hardcoded 2480: the server binds the first free port of the configured range.
    final String url = "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1" + path;

    final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    if (body != null) {
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      final byte[] data = body.toString().getBytes(StandardCharsets.UTF_8);
      conn.setRequestProperty("Content-Length", Integer.toString(data.length));
      try (final DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
        out.write(data);
      }
    }
    conn.connect();

    try {
      final int status = conn.getResponseCode();
      final InputStream in = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(status).as("%s %s answered: %s", method, path, response).isEqualTo(expectedStatus);
      return new JSONObject(response);
    } finally {
      conn.disconnect();
    }
  }
}
