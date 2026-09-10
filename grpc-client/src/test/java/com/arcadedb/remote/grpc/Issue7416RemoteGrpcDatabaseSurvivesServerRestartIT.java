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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7416: a {@link RemoteGrpcDatabase} built its two data-plane stubs once, in the constructor, on the
 * channel its {@link RemoteGrpcServer} had at the time. A {@code close()} / {@code start()} cycle on the server
 * replaces that channel, and every query, command, lookup and insert on the same database object then failed
 * with {@code UNAVAILABLE: Channel shutdown invoked} - while {@code getProgress()}, whose admin stub was built
 * per call, kept working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7416RemoteGrpcDatabaseSurvivesServerRestartIT extends BaseGraphServerTest {
  private static final int GRPC_PORT = 50051;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  void closeClient() {
    if (database != null)
      database.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
  }

  @Test
  void theSameDatabaseObjectKeepsWorkingAcrossACloseAndStartOfItsServer() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    assertThat(countV1()).isGreaterThan(0);
    final long generationBefore = grpcServer.channelGeneration();

    grpcServer.close();
    // While closed, the same object fails with the reason rather than with a dead channel's UNAVAILABLE.
    assertThatThrownBy(this::countV1).hasMessageContaining("closed");

    grpcServer.start();
    assertThat(grpcServer.channelGeneration()).isNotEqualTo(generationBefore);

    // The unary data plane, on the stub the constructor built.
    assertThat(countV1()).isGreaterThan(0);
    // A write and a read-back through the same object, so the command path is covered as well as the query one.
    database.command("sql", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = 7416, name = 'after-restart'");
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " WHERE id = 7416")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("after-restart");
    }
    // The admin stub the class already rebuilt per call, kept as the control.
    assertThat(database.getProgress()).isNotNull();
  }

  /** A second restart is followed the same way: the check is per call, not once. */
  @Test
  void everyRestartIsFollowedNotJustTheFirst() {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT, getServer(0).getHttpServer().getPort(),
        getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    for (int cycle = 0; cycle < 3; cycle++) {
      grpcServer.close();
      grpcServer.start();
      assertThat(countV1()).as("cycle " + cycle).isGreaterThan(0);
    }
  }

  private long countV1() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM " + VERTEX1_TYPE_NAME)) {
      return rs.next().<Long>getProperty("total");
    }
  }
}
