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

import com.arcadedb.engine.ComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code GET /api/v1/server} in its default mode reports {@code metrics.sparseVectorIndexes} keyed by
 * database name, so it enumerates the whole registry exactly like the cluster routes do - but without
 * needing HA, which is why it is covered here rather than next to
 * {@code ClusterInfoDatabaseScopingIT} in the ha-raft module.
 * <p>
 * The root assertions are not decoration: they establish that the other tenant's database really does
 * produce a metrics row. Without them the tenant-side assertion would pass just as happily against a
 * server where no sparse index exists at all, which is the shape this map has by default.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerMetricsDatabaseScopingIT extends BaseGraphServerTest {

  private static final String OTHER_TENANT_DATABASE = "zzsparsemetricsdb";
  private static final String OTHER_TENANT_TYPE     = "ZzSparseRecord";

  private static final String TENANT_USER     = "tenantmetrics79wq";
  private static final String TENANT_PASSWORD = "tenantpassword79wq";

  @Test
  void sparseVectorIndexMetricsAreScopedToAuthorizedDatabases() throws Exception {
    createOtherTenantDatabaseWithSparseIndex();
    try {
      createTenantUser();

      final Response root = call("root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(root.status).isEqualTo(200);
      final JSONObject rootMetrics = root.json().getJSONObject("metrics").getJSONObject("sparseVectorIndexes");
      assertThat(rootMetrics.keySet())
          .as("precondition: the other tenant's sparse index must produce a metrics row for root to see")
          .contains(OTHER_TENANT_DATABASE);

      final Response tenant = call(TENANT_USER, TENANT_PASSWORD);
      assertThat(tenant.status).as("a scoped tenant may still read server metrics").isEqualTo(200);
      assertThat(tenant.body)
          .as("the other tenant's database name must not appear anywhere in the payload")
          .doesNotContain(OTHER_TENANT_DATABASE);
      assertThat(tenant.json().getJSONObject("metrics").getJSONObject("sparseVectorIndexes").keySet())
          .as("the tenant sees no row for a database it is not authorized for")
          .doesNotContain(OTHER_TENANT_DATABASE);
    } finally {
      dropTenantUser();
      dropOtherTenantDatabase();
    }
  }

  private void createOtherTenantDatabaseWithSparseIndex() {
    final ServerDatabase db = getServer(0).createDatabase(OTHER_TENANT_DATABASE, ComponentFile.MODE.READ_WRITE);
    db.command("sql", "CREATE DOCUMENT TYPE " + OTHER_TENANT_TYPE + " BUCKETS 1");
    db.command("sql", "CREATE PROPERTY " + OTHER_TENANT_TYPE + ".tokens ARRAY_OF_INTEGERS");
    db.command("sql", "CREATE PROPERTY " + OTHER_TENANT_TYPE + ".weights ARRAY_OF_FLOATS");
    db.command("sql", "CREATE INDEX ON " + OTHER_TENANT_TYPE + " (tokens, weights) LSM_SPARSE_VECTOR"
        + " METADATA { dimensions: 8, weightQuantization: 'FP32' }");
    db.newDocument(OTHER_TENANT_TYPE)
        .set("tokens", new int[] { 1, 5 })
        .set("weights", new float[] { 0.1f, 0.3f })
        .save();
  }

  private void dropOtherTenantDatabase() {
    final ArcadeDBServer server = getServer(0);
    if (server == null || !server.existsDatabase(OTHER_TENANT_DATABASE))
      return;
    // ServerDatabase refuses drop() because the handle is shared: go through the embedded instance,
    // then unregister the now-deleted database from the server registry.
    server.getDatabase(OTHER_TENANT_DATABASE).getEmbedded().drop();
    server.removeDatabase(OTHER_TENANT_DATABASE);
  }

  private void createTenantUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(TENANT_USER) != null)
      return;
    security.createUser(new JSONObject()
        .put("name", TENANT_USER)
        .put("password", security.encodePassword(TENANT_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { "admin" }))));
  }

  private void dropTenantUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(TENANT_USER) != null)
      security.dropUser(TENANT_USER);
  }

  private Response call(final String user, final String password) throws Exception {
    final int port = getServer(0).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + port + "/api/v1/server").toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
    try {
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
}
