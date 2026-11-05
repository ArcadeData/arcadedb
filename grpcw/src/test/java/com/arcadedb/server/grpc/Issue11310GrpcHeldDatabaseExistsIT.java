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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11310 over gRPC: a database an operator closed exists. {@code ExistsDatabase} answered false for it,
 * {@code CreateDatabase(if_not_exists)} then failed ALREADY_EXISTS where the contract is to leave an existing database alone, and
 * {@code DropDatabase(if_exists)} answered dropped=false - "nothing to drop" - and left it on disk. {@code GetDatabaseInfo} said
 * NOT_FOUND where the data path says UNAVAILABLE with the reason.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11310GrpcHeldDatabaseExistsIT extends BaseGrpcServerTest {
  private static final String HELD = "held11310grpc";

  private ManagedChannel                                          channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub admin;
  private DatabaseCredentials                                     credentials;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void connectAndCloseADatabase() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    admin = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
    credentials = DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();

    final ArcadeDBServer server = getServer(0);
    server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    new ServerControlPlane(server).closeDatabase(HELD);
  }

  @AfterEach
  void disconnectAndDrop() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted() && server.isDatabaseOfServer(HELD))
      new ServerControlPlane(server).dropDatabase(HELD);
  }

  @Test
  void aClosedDatabaseExistsAndItsNameIsTaken() {
    assertThat(admin.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).build())
        .getExists()).isTrue();

    // THE IDEMPOTENT CREATE LEAVES AN EXISTING DATABASE AS IT IS: IT USED TO THROW ALREADY_EXISTS
    assertThat(admin.createDatabase(
        CreateDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).setIfNotExists(true).build()).getCreated())
        .isFalse();
    assertThatThrownBy(() -> admin.createDatabase(CreateDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.ALREADY_EXISTS));
  }

  @Test
  void anIdempotentDropDropsAClosedDatabase() {
    // IT USED TO ANSWER dropped=false AND LEAVE THE DATABASE ON DISK
    assertThat(admin.dropDatabase(
        DropDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).setIfExists(true).build()).getDropped()).isTrue();
    assertThat(new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD))
        .doesNotExist();
    assertThat(admin.existsDatabase(ExistsDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).build())
        .getExists()).isFalse();

    // AND NOW THERE IS NOTHING TO DROP
    assertThat(admin.dropDatabase(
        DropDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).setIfExists(true).build()).getDropped()).isFalse();
  }

  @Test
  void aStrictDropDropsAClosedDatabase() {
    assertThat(admin.dropDatabase(DropDatabaseRequest.newBuilder().setCredentials(credentials).setName(HELD).build()).getDropped())
        .isTrue();
    assertThat(getServer(0).isDatabaseOfServer(HELD)).isFalse();
  }

  @Test
  void theInfoOfAClosedDatabaseIsUnavailableNotMissing() {
    assertThatThrownBy(() -> admin.getDatabaseInfo(GetDatabaseInfoRequest.newBuilder().setCredentials(credentials).setName(HELD).build()))
        .isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
  }
}
