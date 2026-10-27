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
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.utility.FileUtils;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10794, through one long-lived channel: the server replaces the instance behind a database name - a {@code close
 * database} followed by {@code open database}, a {@code drop database} followed by {@code create database}, a {@code restore
 * backup} with {@code overwrite} - while a gRPC client keeps the same channel and stubs. Every call after the swap is served by
 * the new instance, unary and streaming alike. A call in the window between the two halves is refused with what the name is at
 * that moment: {@code UNAVAILABLE} for a database the server holds closed, {@code NOT_FOUND} for one that no longer exists (issue
 * #10795, it used to be {@code INTERNAL}), never a server fault.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10794GrpcReplacedDatabaseIT extends BaseGrpcServerTest {
  private static final String NAME = "grpcswap10794";

  private ManagedChannel                                    channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub   stub;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;
  private ServerControlPlane                                controlPlane;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openChannel() {
    controlPlane = new ServerControlPlane(getServer(0));
    dropIfPresent();
    controlPlane.createDatabase(NAME);

    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, NAME)));
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);

    command("create document type S");
    insert(5);
    assertThat(count()).isEqualTo(5);
  }

  @AfterEach
  void closeChannel() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      dropIfPresent();
    }
  }

  @Test
  void aDatabaseClosedAndOpenedAgainIsServedAgainOnTheSameChannel() {
    controlPlane.closeDatabase(NAME);

    assertRefused(Status.Code.UNAVAILABLE, DatabaseNotAvailableException.class);
    assertThat(getServer(0).existsDatabase(NAME)).as("the refused call opened nothing").isFalse();

    controlPlane.openDatabase(NAME);

    assertThat(count()).as("the reopened instance, not the closed one").isEqualTo(5);
    insert(2);
    assertThat(count()).isEqualTo(7);
    assertThat(streamedCount()).as("a streaming call on the same channel").isEqualTo(7);
  }

  @Test
  void aDatabaseDroppedAndCreatedAgainIsServedAgainOnTheSameChannel() {
    for (final int rows : new int[] { 7, 9 }) {
      controlPlane.dropDatabase(NAME);

      // Issue #10795: the name is no database now, which is the caller's to correct and not a server fault
      assertRefused(Status.Code.NOT_FOUND, DatabaseNotFoundException.class);

      controlPlane.createDatabase(NAME);
      command("create document type S");

      insert(rows);
      assertThat(count()).as("the database created again, holding only what was written to it").isEqualTo(rows);
      assertThat(streamedCount()).isEqualTo(rows);
    }
  }

  @Test
  void aDatabaseARestoreReplacedIsServedAgainOnTheSameChannel() {
    // THE FILE NAME, RESOLVED UNDER THE SERVER'S BACKUP DIRECTORY
    final String backupFile = new File(controlPlane.triggerBackup(NAME).getString("backupFile")).getName();
    insert(2);
    assertThat(count()).isEqualTo(7);

    try {
      controlPlane.restoreBackup(NAME, backupFile, NAME, true, ServerControlPlane.ProgressListener.NOOP);

      assertThat(count()).as("the restored copy, taken before the last two inserts").isEqualTo(5);
      insert(1);
      assertThat(count()).isEqualTo(6);
      assertThat(streamedCount()).isEqualTo(6);
    } finally {
      FileUtils.deleteRecursively(controlPlane.resolveBackupFile(NAME, backupFile).getParent().toFile());
    }
  }

  /** The admin plane answers a name that is no database as the data plane does (issue #10795): it was INTERNAL. */
  @Test
  void anAdminCallOnANameThatIsNoDatabaseIsNotFound() {
    final String missing = "grpcnothing10795";

    assertThatThrownBy(() -> adminStub.openDatabase(OpenDatabaseRequest.newBuilder().setCredentials(credentials()).setName(missing)
        .build())).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
      assertThat(e.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(DatabaseNotFoundException.class.getName());
    });
    assertThat(new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), missing))
        .as("the refused call created nothing").doesNotExist();
  }

  /** The next call in the window between the two halves of a swap: refused as what the name is now, never a server fault. */
  private void assertRefused(final Status.Code code, final Class<? extends Throwable> named) {
    assertThatThrownBy(this::count).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
      assertThat(e.getStatus().getCode()).isEqualTo(code);
      assertThat(e.getTrailers().get(GrpcErrorMapper.EXCEPTION_CLASS_KEY)).isEqualTo(named.getName());
    });
    assertThatThrownBy(() -> insert(1)).isInstanceOfSatisfying(StatusRuntimeException.class,
        e -> assertThat(e.getStatus().getCode()).isEqualTo(code));
  }

  private void command(final String command) {
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(NAME).setCommand(command).setCredentials(credentials()).build());
  }

  private void insert(final int rows) {
    for (int i = 0; i < rows; i++)
      command("insert into S set i = " + i);
  }

  private long count() {
    final ExecuteQueryResponse response = stub.executeQuery(
        ExecuteQueryRequest.newBuilder().setDatabase(NAME).setQuery("select from S").setCredentials(credentials()).build());
    long total = 0;
    for (final QueryResult result : response.getResultsList())
      total += result.getRecordsCount();
    return total;
  }

  private long streamedCount() {
    final Iterator<QueryResult> batches = stub.streamQuery(
        StreamQueryRequest.newBuilder().setDatabase(NAME).setQuery("select from S").setCredentials(credentials()).setBatchSize(2)
            .build());
    long total = 0;
    while (batches.hasNext())
      total += batches.next().getRecordsCount();
    return total;
  }

  private static DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private void dropIfPresent() {
    final ArcadeDBServer server = getServer(0);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabase(NAME).getEmbedded(), NAME);
    server.unmarkClosedByCommand(NAME);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }
}
