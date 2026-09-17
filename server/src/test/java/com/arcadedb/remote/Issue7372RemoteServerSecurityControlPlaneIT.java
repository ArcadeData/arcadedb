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
package com.arcadedb.remote;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7372: every {@code /api/v1/server/&#123;users,groups,api-tokens&#125;} route driven through the method
 * {@link RemoteServer} now exposes for it. Before this, a Java caller on HTTP could create and drop a user
 * and nothing else, while the gRPC client covered all nine routes.
 * <p>
 * One test per route, driven through the client rather than through a hand-built {@code HttpURLConnection},
 * because what is being asserted is that the client method reaches the route and reads its answer - a test
 * that built the request itself would pass against a client that does not exist.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7372RemoteServerSecurityControlPlaneIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  private RemoteServer server() {
    return new RemoteServer("127.0.0.1", getServer(0).getHttpServer().getPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  /**
   * {@code GET /server/users}. The projection the route returns carries the grants and never the password
   * hash, and the assertion says so rather than only counting rows.
   */
  @Test
  void listUsersReturnsTheUsersWithoutTheirPasswords() {
    final RemoteServer server = server();
    try {
      final List<JSONObject> users = server.listUsers();

      assertThat(users).isNotEmpty();
      assertThat(users).allSatisfy(user -> {
        assertThat(user.has("name")).isTrue();
        assertThat(user.has("databases")).isTrue();
        assertThat(user.has("password")).isFalse();
      });
      assertThat(users.stream().map(u -> u.getString("name"))).contains("root");
    } finally {
      server.close();
    }
  }

  /**
   * {@code PUT /server/users}. Both halves of the partial update, and the property that makes it partial:
   * changing the password must not clear the grants, and replacing the grants must not touch the password.
   */
  @Test
  void updateUserAppliesEachHalfWithoutClearingTheOther() {
    final RemoteServer server = server();
    final String userName = "issue7372-update";
    try {
      server.createUser(userName, "initialPassword1", Map.of(getDatabaseName(), "admin"));

      // Grants only: the password must survive, which is asserted by opening a database with it below.
      server.updateUserGrants(userName, Map.of(getDatabaseName(), List.of("admin"), "*", List.of("admin")));
      assertThat(grantsOf(server, userName).keySet()).contains(getDatabaseName(), "*");

      final RemoteDatabase withOldPassword = new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(),
          getDatabaseName(), userName, "initialPassword1");
      try {
        assertThat(withOldPassword.query("sql", "select 1 as one").hasNext()).isTrue();
      } finally {
        withOldPassword.close();
      }

      // Password only: the grants must survive.
      server.updateUserPassword(userName, "rotatedPassword1");
      assertThat(grantsOf(server, userName).keySet()).contains(getDatabaseName(), "*");

      final RemoteDatabase withNewPassword = new RemoteDatabase("127.0.0.1", getServer(0).getHttpServer().getPort(),
          getDatabaseName(), userName, "rotatedPassword1");
      try {
        assertThat(withNewPassword.query("sql", "select 1 as one").hasNext()).isTrue();
      } finally {
        withNewPassword.close();
      }

      // A non-null but empty map DOES clear them - that is the caller saying so, not staying silent.
      server.updateUserGrants(userName, Map.of());
      assertThat(grantsOf(server, userName)).isEmpty();
    } finally {
      dropQuietly(server, userName);
      server.close();
    }
  }

  /**
   * A name no user has is the server's 404, and the client surfaces it rather than swallowing it.
   */
  @Test
  void updateUserOnAnUnknownNameFails() {
    final RemoteServer server = server();
    try {
      assertThatThrownBy(() -> server.updateUserPassword("issue7372-nobody", "somePassword1"))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("issue7372-nobody");
    } finally {
      server.close();
    }
  }

  /**
   * {@code GET /server/groups}: the whole group document, with the {@code admin} group every deployment
   * starts with.
   */
  @Test
  void listGroupsReturnsTheWholeDocument() {
    final RemoteServer server = server();
    try {
      final JSONObject groups = server.listGroups();

      assertThat(groups.has("databases")).isTrue();
      assertThat(groups.getJSONObject("databases").has("*")).isTrue();
      assertThat(groups.getJSONObject("databases").getJSONObject("*").getJSONObject("groups").has("admin")).isTrue();
    } finally {
      server.close();
    }
  }

  /**
   * {@code POST /server/groups} then {@code DELETE /server/groups}, asserted through
   * {@link RemoteServer#listGroups} so what is checked is the server's state and not the status code.
   */
  @Test
  void saveGroupAndDeleteGroupRoundTrip() {
    final RemoteServer server = server();
    final String groupName = "issue7372-reader";
    try {
      final JSONObject group = new JSONObject()
          .put("resultSetLimit", 100)
          .put("readTimeout", 5_000)
          .put("access", new JSONArray())
          .put("types", new JSONObject().put("*",
              new JSONObject().put("access", new JSONArray().put("readRecord"))));

      server.saveGroup("*", groupName, group);

      final JSONObject saved = server.listGroups().getJSONObject("databases").getJSONObject("*")
          .getJSONObject("groups").getJSONObject(groupName);
      assertThat(saved.getLong("resultSetLimit", -1L)).isEqualTo(100L);
      assertThat(saved.getJSONObject("types").getJSONObject("*").getJSONArray("access").toList())
          .containsExactly("readRecord");

      server.deleteGroup("*", groupName);

      assertThat(server.listGroups().getJSONObject("databases").getJSONObject("*")
          .getJSONObject("groups").has(groupName)).isFalse();
    } finally {
      server.close();
    }
  }

  /**
   * The refusal the route is careful to make: the {@code admin} group of the default database is what every
   * deployment authorizes against, and dropping it would lock the operator out.
   */
  @Test
  void deleteGroupRefusesTheDefaultAdminGroup() {
    final RemoteServer server = server();
    try {
      assertThatThrownBy(() -> server.deleteGroup("*", "admin"))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("admin");

      assertThat(server.listGroups().getJSONObject("databases").getJSONObject("*")
          .getJSONObject("groups").has("admin")).isTrue();
    } finally {
      server.close();
    }
  }

  /**
   * {@code POST}, {@code GET} and {@code DELETE /server/api-tokens} in one round trip, because the three are
   * only usable together: the mint is the only time the token exists, the listing is where its hash comes
   * from, and the hash is the handle the revocation takes.
   */
  @Test
  void createListAndDeleteApiTokenRoundTrip() {
    final RemoteServer server = server();
    final String tokenName = "issue7372-token";
    try {
      final JSONObject minted = server.createApiToken(tokenName, getDatabaseName(), 0,
          new JSONObject().put("types", new JSONObject().put("*",
              new JSONObject().put("access", new JSONArray().put("readRecord")))));

      assertThat(minted.getString("token")).isNotBlank();
      assertThat(minted.getString("tokenHash")).isNotBlank();

      final List<JSONObject> listed = server.listApiTokens();
      final JSONObject entry = listed.stream().filter(t -> tokenName.equals(t.getString("name"))).findFirst()
          .orElseThrow();

      // The listing carries the handle and never the material: the server keeps only the hash.
      assertThat(entry.getString("tokenHash")).isEqualTo(minted.getString("tokenHash"));
      assertThat(entry.has("token")).isFalse();
      assertThat(entry.getString("database")).isEqualTo(getDatabaseName());

      server.deleteApiToken(entry.getString("tokenHash"));

      assertThat(server.listApiTokens().stream().map(t -> t.getString("name"))).doesNotContain(tokenName);
    } finally {
      server.close();
    }
  }

  /**
   * The server refuses a plaintext token where a hash belongs - accepting one would put live token material
   * into whatever logged the request, which is the exposure the revocation is ending - and the client
   * surfaces that refusal instead of reporting a successful revocation.
   */
  @Test
  void deleteApiTokenRefusesThePlaintextToken() {
    final RemoteServer server = server();
    final String tokenName = "issue7372-plaintext";
    try {
      final JSONObject minted = server.createApiToken(tokenName, getDatabaseName(), 0, null);

      assertThatThrownBy(() -> server.deleteApiToken(minted.getString("token")))
          .isInstanceOf(RuntimeException.class);

      server.deleteApiToken(minted.getString("tokenHash"));
    } finally {
      server.close();
    }
  }

  /**
   * With {@code arcadedb.server.apiTokenRequireSecureTransport} on, a mint from a loopback client still
   * succeeds: the bytes never reach a network, which is one of the two conditions the gate accepts. The
   * other - HTTPS - and the refusal itself are asserted in
   * {@code Issue7372ApiTokenTransportGateTest}, which can produce a remote cleartext peer that a test
   * talking to 127.0.0.1 cannot.
   */
  @Test
  void theTransportGateStillAllowsALoopbackMint() {
    final RemoteServer server = server();
    final String tokenName = "issue7372-gated";
    final Object previous = GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getValue();
    GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.setValue(true);
    try {
      final JSONObject minted = server.createApiToken(tokenName, getDatabaseName(), 0, null);
      assertThat(minted.getString("token")).isNotBlank();

      server.deleteApiToken(minted.getString("tokenHash"));
    } finally {
      GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.setValue(previous);
      server.close();
    }
  }

  private JSONObject grantsOf(final RemoteServer server, final String userName) {
    return server.listUsers().stream().filter(u -> userName.equals(u.getString("name"))).findFirst().orElseThrow()
        .getJSONObject("databases");
  }

  private void dropQuietly(final RemoteServer server, final String userName) {
    try {
      server.dropUser(userName);
    } catch (final Exception e) {
      // The test owns this user and nothing else reads it; a failure to clean up must not mask the assertion
      // that already ran.
    }
  }
}
