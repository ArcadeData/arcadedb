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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7320: a principal whose grants name real databases could not use gRPC at all. The client sent
 * no {@code x-arcade-database} metadata, so the server authenticated every call against the literal
 * name {@code "default"} - a database this user is not granted - and refused the first RPC with
 * {@code UNAUTHENTICATED: Invalid credentials}. Only a principal granted {@code "*"} could connect.
 * <p>
 * The user here is granted the test database and nothing else, which is the shape the issue reports.
 * The test drives both stubs {@link RemoteGrpcDatabase} builds: the blocking one (query and command)
 * and the async one (bidirectional ingestion), because each takes its credentials from its own
 * {@code CallCredentials} instance.
 */
class Issue7320ScopedUserGrpcIT extends BaseGraphServerTest {

  private static final String SCOPED_USER = "scoped7320";
  private static final String SCOPED_PASS = "scoped7320password";
  private static final String TYPE        = "Scoped7320";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void createScopedUserAndConnect() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (!security.existsUser(SCOPED_USER)) {
      final JSONObject config = new JSONObject();
      config.put("name", SCOPED_USER);
      config.put("password", security.encodePassword(SCOPED_PASS));
      // Granted the test database ONLY: no "*" wildcard, which is what made gRPC work before the fix.
      config.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
      security.createUser(config);
    }

    grpcServer = new RemoteGrpcServer("localhost", 50051, SCOPED_USER, SCOPED_PASS, true, List.of());
    database = new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), SCOPED_USER,
        SCOPED_PASS);
  }

  @AfterEach
  void disconnectAndDropScopedUser() {
    if (database != null) {
      database.close();
      database = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void scopedUserCanQueryOverGrpc() {
    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    database.command("sql", "INSERT INTO `" + TYPE + "` SET id = 1");

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }

  @Test
  void scopedUserCanIngestOverTheAsyncStub() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (id) UNIQUE");

    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final Map<String, Object> row = new HashMap<>();
      row.put("id", "row-" + i);
      rows.add(row);
    }

    final InsertOptions options = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(TYPE)
        .addKeyColumns("id")
        .setConflictMode(ConflictMode.CONFLICT_ERROR)
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setServerBatchSize(10)
        .setCredentials(database.buildCredentials())
        .build();

    final InsertSummary summary = database.ingestBidi(options, rows, 5, 5, 60_000);

    assertThat(summary.getInserted()).isEqualTo(10);
  }
}
