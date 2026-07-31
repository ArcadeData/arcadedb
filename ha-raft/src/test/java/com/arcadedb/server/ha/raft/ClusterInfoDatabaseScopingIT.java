/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster-info endpoints authenticate but must also authorize what they disclose: a user granted
 * access to one database must never learn the name, transaction state, bootstrap fingerprint or schema
 * of another tenant's database through them.
 * <p>
 * Three surfaces are covered, because the database registry is enumerated in three different places:
 * <ul>
 *   <li>{@code GET /api/v1/cluster} - the {@code databases} array and the {@code alerts} payload, which
 *       carries per-database type names;</li>
 *   <li>{@code GET /api/v1/server?mode=cluster} - the {@code ha.databases} array;</li>
 *   <li>{@code POST /api/v1/cluster/bootstrap-state} - per-database SHA-256 directory fingerprints.</li>
 * </ul>
 * The counter-tests matter as much as the leak tests: the topology fields of {@code ?mode=cluster} feed
 * the remote driver's leader/replica routing for every user, so scoping must not turn into a blanket
 * rejection there.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClusterInfoDatabaseScopingIT extends BaseRaftHATest {

  /** Deliberately distinctive so a substring assertion over the whole response body cannot match by accident. */
  private static final String OTHER_TENANT_DATABASE = "zzsecretdb79wq";
  private static final String OTHER_TENANT_TYPE     = "ZzSecretType79wq";

  private static final String TENANT_USER     = "tenant79wq";
  private static final String TENANT_PASSWORD = "tenantpassword79wq";

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The {@code databases} array and the type names embedded in {@code alerts} must be scoped to what the
   * caller may access, while root keeps the full operator view.
   */
  @Test
  void clusterEndpointScopesDatabaseDisclosureToAuthorizedDatabases() throws Exception {
    final int node = findLeaderIndex();
    assertThat(node).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    withOtherTenantDatabase(node, () -> {
      createTenantUser(node);

      final Response tenant = call(node, "GET", "/api/v1/cluster", null, TENANT_USER, TENANT_PASSWORD);
      assertThat(tenant.status).as("a scoped tenant may still read cluster status").isEqualTo(200);
      assertThat(tenant.body)
          .as("the other tenant's database name must not appear anywhere in the payload")
          .doesNotContain(OTHER_TENANT_DATABASE);
      assertThat(tenant.body)
          .as("the other tenant's schema must not leak through the single-bucket-types alert")
          .doesNotContain(OTHER_TENANT_TYPE);
      assertThat(databaseNames(tenant.json().getJSONArray("databases")))
          .as("the tenant sees exactly the database it is authorized for")
          .containsExactly(getDatabaseName());

      // Counter-test: without it the assertions above would also pass on a handler that returns nothing.
      final Response root = call(node, "GET", "/api/v1/cluster", null, "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(root.status).isEqualTo(200);
      final List<String> rootView = databaseNames(root.json().getJSONArray("databases"));
      assertThat(rootView).as("root keeps the unfiltered operator view")
          .contains(getDatabaseName(), OTHER_TENANT_DATABASE);
      assertThat(rootView).as("reserved internal databases are not operator-visible state")
          .noneMatch(name -> name.startsWith(ArcadeDBServer.RESERVED_DATABASE_PREFIX));
    });
  }

  /**
   * {@code GET /api/v1/server?mode=cluster} enumerates the same registry under {@code ha.databases}.
   */
  @Test
  void serverClusterModeScopesDatabaseDisclosureToAuthorizedDatabases() throws Exception {
    final int node = findLeaderIndex();
    assertThat(node).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    withOtherTenantDatabase(node, () -> {
      createTenantUser(node);

      final Response tenant = call(node, "GET", "/api/v1/server?mode=cluster", null, TENANT_USER, TENANT_PASSWORD);
      assertThat(tenant.status).isEqualTo(200);
      assertThat(tenant.body).doesNotContain(OTHER_TENANT_DATABASE);
      assertThat(databaseNames(tenant.json().getJSONObject("ha").getJSONArray("databases")))
          .containsExactly(getDatabaseName());

      final Response root = call(node, "GET", "/api/v1/server?mode=cluster", null, "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(root.status).isEqualTo(200);
      assertThat(databaseNames(root.json().getJSONObject("ha").getJSONArray("databases")))
          .contains(getDatabaseName(), OTHER_TENANT_DATABASE);
    });
  }

  /**
   * The remote driver calls {@code ?mode=cluster} on every connection, as whatever user the application
   * uses, and reads {@code ha.leaderAddress} / {@code ha.replicaAddresses} to route requests. Scoping the
   * database list must not regress into rejecting non-root callers outright, which would break routing for
   * every non-root application.
   */
  @Test
  void serverClusterModeKeepsTopologyReadableByNonRootForDriverRouting() throws Exception {
    final int node = findLeaderIndex();
    assertThat(node).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    createTenantUser(node);

    final Response tenant = call(node, "GET", "/api/v1/server?mode=cluster", null, TENANT_USER, TENANT_PASSWORD);
    assertThat(tenant.status).as("non-root must not be rejected: the driver rethrows 403 instead of falling back")
        .isEqualTo(200);

    final JSONObject ha = tenant.json().getJSONObject("ha");
    assertThat(ha.has("leaderAddress")).as("driver routing needs the leader address").isTrue();
    assertThat(ha.has("replicaAddresses")).as("driver routing needs the replica addresses").isTrue();
  }

  /**
   * {@code POST /api/v1/cluster/bootstrap-state} is a peer-to-peer RPC: peers reach it with the cluster
   * token forwarded as root. It has no browser or driver consumer, and it computes a SHA-256 over every
   * database directory, so it belongs with the mutating cluster endpoints behind the root check rather
   * than merely being filtered.
   */
  @Test
  void bootstrapStateRequiresRoot() throws Exception {
    final int node = findLeaderIndex();
    assertThat(node).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    createTenantUser(node);

    assertThat(call(node, "POST", "/api/v1/cluster/bootstrap-state", new JSONObject(), TENANT_USER, TENANT_PASSWORD).status)
        .as("a non-root tenant must not be able to fingerprint every database on the node").isEqualTo(403);

    final Response root = call(node, "POST", "/api/v1/cluster/bootstrap-state", new JSONObject(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(root.status).as("root (and therefore the peer RPC, which forwards as root) still passes").isEqualTo(200);
    assertThat(root.json().getJSONArray("databases").length()).isPositive();
  }

  /**
   * The presence matrix fans bootstrap-state RPCs out to every peer, and every action it informs
   * (resync, transfer leader) is already root-only, so the fan-out itself is gated on root.
   */
  @Test
  void presenceMatrixRequiresRoot() throws Exception {
    final int node = findLeaderIndex();
    assertThat(node).as("a leader must be elected").isGreaterThanOrEqualTo(0);

    createTenantUser(node);

    assertThat(call(node, "GET", "/api/v1/cluster?presence=true", null, TENANT_USER, TENANT_PASSWORD).status)
        .as("the peer fan-out is an operator diagnostic").isEqualTo(403);
    assertThat(call(node, "GET", "/api/v1/cluster", null, TENANT_USER, TENANT_PASSWORD).status)
        .as("the cheap poll stays available to the tenant").isEqualTo(200);
  }

  // ---------------------------------------------------------------------------------------------
  //  Fixture helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * Runs {@code body} with a second database holding one single-bucket type, so the
   * {@code single-bucket-types} alert has a type name to disclose. The database is created on every node
   * before the type is added: the schema DDL runs through the Raft-wrapped handle and replicates, and a
   * peer that does not hold the database would quarantine the entry and churn snapshot resyncs. The
   * database and the tenant user are always removed afterwards, whatever the outcome.
   */
  private void withOtherTenantDatabase(final int serverIndex, final ThrowingRunnable body) throws Exception {
    for (int i = 0; i < getServerCount(); i++)
      getServer(i).createDatabase(OTHER_TENANT_DATABASE, ComponentFile.MODE.READ_WRITE);
    try {
      final ServerDatabase other = getServer(serverIndex).getDatabase(OTHER_TENANT_DATABASE);
      other.getSchema().createDocumentType(OTHER_TENANT_TYPE, 1);
      body.run();
    } finally {
      dropTenantUser(serverIndex);
      dropOtherTenantDatabase();
    }
  }

  private void dropOtherTenantDatabase() {
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer server = getServer(i);
      if (server == null || !server.existsDatabase(OTHER_TENANT_DATABASE))
        continue;
      // ServerDatabase refuses drop()/close() because the handle is shared, and dropping through the
      // Raft-wrapped handle would replicate the drop; go through the embedded instance so each node
      // cleans up its own copy, then unregister it from that server's registry.
      server.getDatabase(OTHER_TENANT_DATABASE).getEmbedded().drop();
      server.removeDatabase(OTHER_TENANT_DATABASE);
    }
  }

  private void createTenantUser(final int serverIndex) {
    final ServerSecurity security = getServer(serverIndex).getSecurity();
    if (security.getUser(TENANT_USER) != null)
      return;
    security.createUser(new JSONObject()
        .put("name", TENANT_USER)
        .put("password", security.encodePassword(TENANT_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private void dropTenantUser(final int serverIndex) {
    final ServerSecurity security = getServer(serverIndex).getSecurity();
    if (security.getUser(TENANT_USER) != null)
      security.dropUser(TENANT_USER);
  }

  private static List<String> databaseNames(final JSONArray databases) {
    final List<String> names = new ArrayList<>(databases.length());
    for (int i = 0; i < databases.length(); i++)
      names.add(databases.getJSONObject(i).getString("name"));
    return names;
  }

  private Response call(final int serverIndex, final String method, final String path, final JSONObject body,
      final String user, final String password) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI("http://localhost:" + port + path).toURL().openConnection();
    conn.setRequestMethod(method);
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
    try {
      if (body != null) {
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));
      }
      final int status = conn.getResponseCode();
      final var stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String payload = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, payload);
    } finally {
      conn.disconnect();
    }
  }

  private record Response(int status, String body) {
    JSONObject json() {
      return new JSONObject(body);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
