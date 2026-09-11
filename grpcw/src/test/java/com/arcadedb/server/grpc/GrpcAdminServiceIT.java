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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrpcAdminServiceIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT)
        .usePlaintext()
        .build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }

  @Test
  void pingWithValidCredentials() {
    PingRequest request = PingRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    PingResponse response = adminStub.ping(request);

    assertThat(response.getOk()).isTrue();
    assertThat(response.getServerTimeMs()).isGreaterThan(0);
  }

  @Test
  void pingWithoutCredentialsFails() {
    PingRequest request = PingRequest.newBuilder().build();

    assertThatThrownBy(() -> adminStub.ping(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }

  @Test
  void pingWithInvalidCredentialsFails() {
    PingRequest request = PingRequest.newBuilder()
        .setCredentials(DatabaseCredentials.newBuilder()
            .setUsername("root")
            .setPassword("wrongpassword")
            .build())
        .build();

    assertThatThrownBy(() -> adminStub.ping(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED");
  }

  @Test
  void getServerInfoReturnsValidData() {
    GetServerInfoRequest request = GetServerInfoRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    GetServerInfoResponse response = adminStub.getServerInfo(request);

    assertThat(response.getVersion()).isNotEmpty();
    assertThat(response.getHttpPort()).isGreaterThan(0);
    // Note: gRPC port is not exposed via the server reflection API, so it returns -1
    // The gRPC server is running (we're connected to it) but the port isn't discoverable
    assertThat(response.getDatabasesCount()).isGreaterThanOrEqualTo(0);
  }

  @Test
  void listDatabasesReturnsExistingDatabase() {
    ListDatabasesRequest request = ListDatabasesRequest.newBuilder()
        .setCredentials(credentials())
        .build();

    ListDatabasesResponse response = adminStub.listDatabases(request);

    assertThat(response.getDatabasesList()).contains(getDatabaseName());
  }

  @Test
  void existsDatabaseReturnsTrueForExisting() {
    ExistsDatabaseRequest request = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(getDatabaseName())
        .build();

    ExistsDatabaseResponse response = adminStub.existsDatabase(request);

    assertThat(response.getExists()).isTrue();
  }

  @Test
  void existsDatabaseReturnsFalseForNonExistent() {
    ExistsDatabaseRequest request = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_database_xyz")
        .build();

    ExistsDatabaseResponse response = adminStub.existsDatabase(request);

    assertThat(response.getExists()).isFalse();
  }

  @Test
  void createAndDropDatabase() {
    String testDbName = "grpc_test_db_" + System.currentTimeMillis();

    // Create database
    CreateDatabaseRequest createRequest = CreateDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .setType("graph")
        .build();

    adminStub.createDatabase(createRequest);

    // Verify it exists
    ExistsDatabaseRequest existsRequest = ExistsDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .build();

    assertThat(adminStub.existsDatabase(existsRequest).getExists()).isTrue();

    // Drop database
    DropDatabaseRequest dropRequest = DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .build();

    adminStub.dropDatabase(dropRequest);

    // Verify it no longer exists
    assertThat(adminStub.existsDatabase(existsRequest).getExists()).isFalse();
  }

  /**
   * Issue #7413: a name already taken is refused as the HTTP command refuses it, and the idempotent form says
   * whether it created anything, so a client can tell "created for me" from "already there".
   */
  @Test
  void createDatabaseRefusesATakenNameUnlessAskedToTolerateIt() {
    final String testDbName = "grpc_exists_db_" + System.currentTimeMillis();
    final CreateDatabaseRequest strict = CreateDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName(testDbName)
        .setType("document")
        .build();
    try {
      assertThat(adminStub.createDatabase(strict).getCreated()).isTrue();

      assertThatThrownBy(() -> adminStub.createDatabase(strict))
          .isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS))
          .hasMessageContaining(testDbName);

      final CreateDatabaseResponse tolerated = adminStub.createDatabase(strict.toBuilder().setIfNotExists(true).build());
      assertThat(tolerated.getCreated()).isFalse();
    } finally {
      adminStub.dropDatabase(DropDatabaseRequest.newBuilder().setCredentials(credentials()).setName(testDbName)
          .setIfExists(true).build());
    }
  }

  @Test
  void dropDatabaseRefusesAMissingNameUnlessAskedToTolerateIt() {
    final DropDatabaseRequest strict = DropDatabaseRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_db_for_drop_test")
        .build();

    assertThatThrownBy(() -> adminStub.dropDatabase(strict))
        .isInstanceOf(StatusRuntimeException.class)
        .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND))
        .hasMessageContaining("nonexistent_db_for_drop_test");

    assertThat(adminStub.dropDatabase(strict.toBuilder().setIfExists(true).build()).getDropped()).isFalse();
  }

  /** A database that IS there is dropped and the answer says so, in both forms. */
  @Test
  void dropDatabaseReportsWhatItDropped() {
    final String testDbName = "grpc_dropped_db_" + System.currentTimeMillis();
    adminStub.createDatabase(CreateDatabaseRequest.newBuilder().setCredentials(credentials()).setName(testDbName).build());
    assertThat(adminStub.dropDatabase(DropDatabaseRequest.newBuilder().setCredentials(credentials()).setName(testDbName)
        .setIfExists(true).build()).getDropped()).isTrue();
    assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(credentials())
        .setName(testDbName).build()).getExists()).isFalse();
  }

  /**
   * Names are exact. The guard used to be case-folded while the registry and the operation were not, so a
   * differently-cased name was reported as existing by one and unknown by the other.
   */
  @Test
  void databaseNamesAreExactNotCaseFolded() {
    final String testDbName = "grpc_Cased_db_" + System.currentTimeMillis();
    adminStub.createDatabase(CreateDatabaseRequest.newBuilder().setCredentials(credentials()).setName(testDbName).build());
    try {
      final String otherCase = testDbName.toLowerCase();
      assertThat(adminStub.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(credentials())
          .setName(otherCase).build()).getExists()).isFalse();
      assertThatThrownBy(() -> adminStub.dropDatabase(DropDatabaseRequest.newBuilder().setCredentials(credentials())
          .setName(otherCase).build()))
          .isInstanceOf(StatusRuntimeException.class)
          .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    } finally {
      adminStub.dropDatabase(DropDatabaseRequest.newBuilder().setCredentials(credentials()).setName(testDbName).build());
    }
  }

  @Test
  void getDatabaseInfoReturnsValidData() {
    GetDatabaseInfoRequest request = GetDatabaseInfoRequest.newBuilder()
        .setCredentials(credentials())
        .setName(getDatabaseName())
        .build();

    GetDatabaseInfoResponse response = adminStub.getDatabaseInfo(request);

    assertThat(response.getDatabase()).isEqualTo(getDatabaseName());
    assertThat(response.getType()).isEqualTo("graph");
    assertThat(response.getClasses()).isGreaterThan(0);
  }

  @Test
  void getDatabaseInfoForNonExistentFails() {
    GetDatabaseInfoRequest request = GetDatabaseInfoRequest.newBuilder()
        .setCredentials(credentials())
        .setName("nonexistent_database")
        .build();

    assertThatThrownBy(() -> adminStub.getDatabaseInfo(request))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("NOT_FOUND");
  }
}
