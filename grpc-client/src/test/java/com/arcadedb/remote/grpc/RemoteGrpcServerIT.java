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
import com.arcadedb.test.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for RemoteGrpcServer.
 * Tests server connection management and database operations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteGrpcServerIT extends BaseGraphServerTest {

  private RemoteGrpcServer server;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  @Override
  public void endTest() {
    if (server != null) {
      server.close();
      server = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void shouldConnectToServer() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    assertThat(server).isNotNull();
  }

  @Test
  void shouldListDatabases() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final List<String> databases = server.listDatabases();

    assertThat(databases).isNotNull();
    assertThat(databases).contains(getDatabaseName());
  }

  @Test
  void shouldCheckDatabaseExists() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    assertThat(server.existsDatabase(getDatabaseName())).isTrue();
    assertThat(server.existsDatabase("nonexistent_database")).isFalse();
  }

  @Test
  void shouldCreateAndDropDatabase() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final String testDb = "test_grpc_db";

    // Create database
    server.createDatabase(testDb);
    assertThat(server.existsDatabase(testDb)).isTrue();

    // Drop database
    server.dropDatabase(testDb);
    assertThat(server.existsDatabase(testDb)).isFalse();
  }

  @Test
  void shouldHandleInvalidCredentials() {
    assertThatThrownBy(() -> {
      server = new RemoteGrpcServer("localhost", 50051, "root", "wrongpassword", true, List.of());
      server.listDatabases();
    }).isInstanceOf(Exception.class);
  }

  @Test
  void shouldCloseServerGracefully() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    server.close();

    // Should be able to close multiple times
    server.close();
  }

  @Test
  void shouldCreateDatabaseIfNotExists() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final String testDb = "test_create_if_not_exists";

    // Drop if exists from previous test
    if (server.existsDatabase(testDb)) {
      server.dropDatabase(testDb);
    }

    // Create
    server.createDatabaseIfMissing(testDb);
    assertThat(server.existsDatabase(testDb)).isTrue();

    // Try to create again (should be no-op)
    server.createDatabaseIfMissing(testDb);
    assertThat(server.existsDatabase(testDb)).isTrue();

    // Cleanup
    server.dropDatabase(testDb);
  }

  @Test
  void shouldHandleDropNonExistentDatabase() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    // Dropping a non-existent database may or may not throw an exception depending on implementation
    try {
      server.dropDatabase("nonexistent_database_12345");
    } catch (final Exception e) {
      // Expected - database doesn't exist
      assertThat(e.getMessage()).containsIgnoringCase("not found");
    }
  }

  @Test
  void shouldHandleDatabaseNamesWithUnderscores() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final String testDb = "test_db_123";

    server.createDatabase(testDb);
    assertThat(server.existsDatabase(testDb)).isTrue();

    server.dropDatabase(testDb);
    assertThat(server.existsDatabase(testDb)).isFalse();
  }

  @Test
  void shouldHandleMultipleDatabaseOperations() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final String[] testDbs = { "test_db_1", "test_db_2", "test_db_3" };

    // Create multiple databases
    for (final String db : testDbs) {
      server.createDatabase(db);
      assertThat(server.existsDatabase(db)).isTrue();
    }

    // Verify all exist
    final List<String> databases = server.listDatabases();
    for (final String db : testDbs) {
      assertThat(databases).contains(db);
    }

    // Drop all
    for (final String db : testDbs) {
      server.dropDatabase(db);
      assertThat(server.existsDatabase(db)).isFalse();
    }
  }

  @Test
  void shouldReconnectAfterClose() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    // First operation
    final List<String> databases1 = server.listDatabases();
    assertThat(databases1).isNotEmpty();

    // Close
    server.close();

    // Create new connection
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    // Second operation
    final List<String> databases2 = server.listDatabases();
    assertThat(databases2).isNotEmpty();
  }

  @Test
  void shouldGetEndpoint() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    assertThat(server.endpoint()).isEqualTo("localhost:50051");
  }

  @Test
  void shouldHaveValidToString() {
    server = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());

    final String toString = server.toString();
    assertThat(toString).contains("localhost:50051");
    assertThat(toString).contains("root");
  }
}
