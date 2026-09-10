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
package com.arcadedb.server.security;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.remote.timeseries.TimeSeriesWriteSummary;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The per-type ACL is the reason the gRPC time-series RPCs (issue #7305) had to be built on the shared
 * {@code TimeSeriesGateway} rather than on the engine directly.
 * <p>
 * A TimeSeries type owns no record bucket, so the bucket/file-id permission map a group's {@code types} ACL is
 * compiled into has no entry for it, and every check on it falls back to "allow". The type-name check the
 * gateway performs - {@code getEngine(READ_RECORD)} on a read, {@code checkAccess(CREATE_RECORD)} on a write -
 * is therefore the ONLY thing standing between a denied user and the samples. A new protocol that reached the
 * engine through the unchecked {@code getEngine()} accessor would be silently ungated, and the tests in
 * {@code Issue7305TimeSeriesGrpcIT} would all still pass, because they run as root.
 * <p>
 * This test grants a user every access on every type except one TimeSeries type, then drives all four RPCs.
 * The authorized type is the positive control: it keeps each refusal honest by proving it is per-type
 * authorization and not a blanket lockout of the new RPCs.
 * <p>
 * It lives in this package because the group configuration is set through
 * {@code ServerSecurity.getDatabaseGroupsConfiguration}, which is package-visible, exactly as
 * {@code TimeSeriesPerTypeAclIT} does for the HTTP endpoints.
 */
class Issue7305TimeSeriesGrpcAclIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String SCOPED_USER     = "ts-grpc-scoped-user";
  private static final String SCOPED_PWD      = "tsgrpcscoped1";
  private static final String RESTRICTED_TYPE = "SecretMetrics";
  private static final String AUTHORIZED_TYPE = "PublicMetrics";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase scoped;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (scoped != null) {
      scoped.close();
      scoped = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void everyTimeSeriesRpcAppliesThePerTypeAcl() throws Exception {
    // The types must exist BEFORE the scoped user's per-type map is built, so the map segments the restricted
    // one rather than being compiled against a schema that does not yet name it.
    command(0, "CREATE TIMESERIES TYPE " + RESTRICTED_TYPE
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    command(0, "CREATE TIMESERIES TYPE " + AUTHORIZED_TYPE
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    for (final String typeName : new String[] { RESTRICTED_TYPE, AUTHORIZED_TYPE })
      command(0, "INSERT INTO " + typeName + " SET ts = 1000, host = 'a', value = 42.0");

    createScopedUser();

    final RemoteGrpcDatabase database = scopedClient();

    // --- TimeSeriesQuery ---
    assertThatThrownBy(() -> database.timeSeriesQuery(new TimeSeriesQuery(RESTRICTED_TYPE)))
        .as("querying a denied TimeSeries type over gRPC must be refused")
        .isInstanceOf(SecurityException.class);
    assertThat(database.timeSeriesQuery(new TimeSeriesQuery(AUTHORIZED_TYPE)).count())
        .as("the same RPC on an authorized type must still work").isEqualTo(1);

    // --- TimeSeriesLatest ---
    assertThatThrownBy(() -> database.timeSeriesLatest(RESTRICTED_TYPE))
        .as("reading the latest sample of a denied TimeSeries type over gRPC must be refused")
        .isInstanceOf(SecurityException.class);
    assertThat(database.timeSeriesLatest(AUTHORIZED_TYPE).isPresent()).isTrue();

    // --- TimeSeriesWrite ---
    final TimeSeriesPoint restricted = new TimeSeriesPoint(RESTRICTED_TYPE, 9_999L, Map.of("host", "a"),
        Map.of("value", -1.0));
    assertThatThrownBy(() -> database.timeSeriesWrite(List.of(restricted)))
        .as("writing to a denied TimeSeries type over gRPC must be refused")
        .isInstanceOf(SecurityException.class);

    final TimeSeriesPoint authorized = new TimeSeriesPoint(AUTHORIZED_TYPE, 9_999L, Map.of("host", "a"),
        Map.of("value", -1.0));
    final TimeSeriesWriteSummary written = database.timeSeriesWrite(List.of(authorized));
    assertThat(written.isComplete()).as("the same RPC on an authorized type must still work").isTrue();

    // --- TimeSeriesWriteStream ---
    assertThatThrownBy(() -> database.timeSeriesWriteStream(List.of(restricted), 10))
        .as("the streaming write must apply the same per-type check as the unary one")
        .isInstanceOf(SecurityException.class);

    // The refusal must also have written nothing: the ACL runs during grouping, before the first append, which
    // is the only placement that keeps a rejected request from leaving samples behind.
    assertThat(rootQueryCount(RESTRICTED_TYPE))
        .as("a refused write must leave the denied type untouched").isEqualTo(1);
  }

  private long rootQueryCount(final String typeName) {
    return ((Number) getServer(0).getDatabase(getDatabaseName())
        .query("sql", "SELECT count(*) AS cnt FROM " + typeName).next().getProperty("cnt")).longValue();
  }

  private RemoteGrpcDatabase scopedClient() {
    if (scoped == null) {
      grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, SCOPED_USER, SCOPED_PWD, true, List.of());
      // The port the test server actually bound: the configured range starts at 2480 but a server already
      // listening there pushes this one up.
      scoped = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, getServer(0).getHttpServer().getPort(),
          getDatabaseName(), SCOPED_USER, SCOPED_PWD);
    }
    return scoped;
  }

  /**
   * Grants every access on every type except {@link #RESTRICTED_TYPE}, whose access list is empty.
   * <p>
   * The user is created through the server's own {@code POST /api/v1/server/users} endpoint rather than through
   * {@code ServerSecurity.createUser}, because the handler is what encodes the password into the form the
   * authentication path expects; calling the API directly with either the plaintext or a pre-encoded value
   * leaves a user that cannot log in. Same approach as {@code TimeSeriesPerTypeAclIT}.
   */
  private void createScopedUser() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();

    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("tsGrpcScoped",
        new JSONObject().put("access", new JSONArray().put("updateSecurity").put("updateSchema"))
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord").put("deleteRecord")))
                .put(RESTRICTED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    // The user is granted the group on EVERY database ("*") rather than only on this one, which the HTTP
    // equivalent of this test can afford to do. GrpcAuthInterceptor authenticates each call against the
    // database named in the 'arcadedb-database' metadata header, and RemoteGrpcServer sends only username and
    // password - so the interceptor falls back to the literal name "default" and a user scoped to one real
    // database cannot authenticate over gRPC at all. That is a pre-existing gRPC authentication gap, filed
    // separately; widening the DATABASE grant here keeps this test about the per-TYPE ACL, which is what the
    // new RPCs are responsible for and which is unaffected by it - the group's 'types' map still denies
    // RESTRICTED_TYPE on this database.
    final JSONObject payload = new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", SCOPED_PWD)
        .put("databases", new JSONObject().put("*", new JSONArray().put("tsGrpcScoped")));

    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/server/users").toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }
}
