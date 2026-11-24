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
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.utility.FileUtils;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A database the server holds on disk but has not open, or has replaced, as seen over gRPC: the data service, the admin
 * RPCs and the HA-only verbs on a server without HA.
 * Each nested class is the regression of one issue and keeps that issue's own client fixture; they share the server,
 * started with the gRPC plugin and nothing else configured.
 */
public class GrpcHeldDatabaseIT extends BaseGrpcServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  // Regression: #9449
  /**
   * Issue #9449 over gRPC on a server without HA: the override is root-only, and with no HA there is no quarantine to lift,
   * which is a FAILED_PRECONDITION rather than a silent OK.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class AcceptDivergedWithoutHa {
    private static final String LIMITED_USER = "limited9449";
    private static final String LIMITED_PASS = "limited9449pass";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

    @BeforeEach
    void setupUserAndChannel() {
      final ServerSecurity security = getServer(0).getSecurity();
      if (!security.existsUser(LIMITED_USER)) {
        final JSONObject config = new JSONObject();
        config.put("name", LIMITED_USER);
        config.put("password", security.encodePassword(LIMITED_PASS));
        config.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
        security.createUser(config);
      }

      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void withoutHaTheOverrideFailsThePrecondition() {
      assertThatThrownBy(() -> accept("root", DEFAULT_PASSWORD_FOR_TESTS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("FAILED_PRECONDITION")
          .hasMessageContaining("High Availability");
    }

    @Test
    void anAuthenticatedNonRootCallerIsDenied() {
      assertThatThrownBy(() -> accept(LIMITED_USER, LIMITED_PASS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");
    }

    private AcceptDivergedDatabaseResponse accept(final String user, final String password) {
      return adminStub.acceptDivergedDatabase(AcceptDivergedDatabaseRequest.newBuilder()
          .setCredentials(DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build())
          .setDatabase(getDatabaseName()).build());
    }
  }

  // Regression: #9498
  /**
   * Issue #9498 over gRPC on a server without HA: the override is root-only, and with no HA there is no read floor to lift,
   * which is a FAILED_PRECONDITION rather than a silent OK.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class AcceptStaleSnapshotWithoutHa {
    private static final String LIMITED_USER = "limited9498";
    private static final String LIMITED_PASS = "limited9498pass";

    private ManagedChannel                                            channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

    @BeforeEach
    void setupUserAndChannel() {
      final ServerSecurity security = getServer(0).getSecurity();
      if (!security.existsUser(LIMITED_USER)) {
        final JSONObject config = new JSONObject();
        config.put("name", LIMITED_USER);
        config.put("password", security.encodePassword(LIMITED_PASS));
        config.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")));
        security.createUser(config);
      }

      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void teardown() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void withoutHaTheOverrideFailsThePrecondition() {
      assertThatThrownBy(() -> accept("root", DEFAULT_PASSWORD_FOR_TESTS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("FAILED_PRECONDITION")
          .hasMessageContaining("High Availability");
    }

    @Test
    void anAuthenticatedNonRootCallerIsDenied() {
      assertThatThrownBy(() -> accept(LIMITED_USER, LIMITED_PASS))
          .isInstanceOf(StatusRuntimeException.class)
          .hasMessageContaining("PERMISSION_DENIED");
    }

    private AcceptStaleSnapshotResponse accept(final String user, final String password) {
      return adminStub.acceptStaleSnapshot(AcceptStaleSnapshotRequest.newBuilder()
          .setCredentials(DatabaseCredentials.newBuilder().setUsername(user).setPassword(password).build()).build());
    }
  }

  // Regression: #10533
  /**
   * Issue #10533, through a real call: a gRPC client that names a database an {@code overwrite} restore holds, or that a
   * {@code close database} closed, is refused with {@code UNAVAILABLE} - the database exists, so it is neither {@code NOT_FOUND} nor
   * a server fault ({@code INTERNAL}) - and the same call opens it once nothing holds the name.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeldDatabase {
    private static final String HELD = "grpcheld10533";

    private ManagedChannel                                channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

    @BeforeEach
    void openChannel() {
      channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
      stub = ArcadeDbServiceGrpc.newBlockingStub(
          ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
    }

    @AfterEach
    void closeChannel() throws InterruptedException {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Test
    void aCallOnADatabaseARestoreHoldsIsUnavailable() {
      final ArcadeDBServer server = getServer(0);
      onDiskNotOpen(server);

      server.reserveDatabaseNameForRestore(HELD);
      try {
        assertThatThrownBy(() -> stub.executeQuery(query(HELD))).isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(server.existsDatabase(HELD)).as("the call opened nothing").isFalse();
      } finally {
        server.releaseDatabaseNameReservedForRestore(HELD);
      }

      try {
        assertThat(stub.executeQuery(query(HELD))).as("a released reservation holds nothing").isNotNull();
      } finally {
        dropIfPresent(server);
      }
    }

    @Test
    void aCallOnADatabaseACloseCommandClosedIsUnavailable() {
      final ArcadeDBServer server = getServer(0);
      onDiskNotOpen(server);

      server.markClosedByCommand(HELD);
      try {
        assertThatThrownBy(() -> stub.executeQuery(query(HELD))).isInstanceOfSatisfying(StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
      } finally {
        server.unmarkClosedByCommand(HELD);
        dropIfPresent(server);
      }
    }

    private static ExecuteQueryRequest query(final String database) {
      return ExecuteQueryRequest.newBuilder().setDatabase(database).setQuery("select 1 as one")
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build()).build();
    }

    /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
    private static void onDiskNotOpen(final ArcadeDBServer server) {
      final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
      created.getEmbedded().close();
      server.removeDatabase(HELD);
      assertThat(server.existsDatabase(HELD)).isFalse();
    }

    private static void dropIfPresent(final ArcadeDBServer server) {
      if (server.existsDatabase(HELD))
        server.dropLocalDatabase(server.getDatabaseForOperator(HELD).getEmbedded(), HELD);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
    }
  }

  // Regression: #10794
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
  @Nested
  class ReplacedDatabase {
    private static final String NAME = "grpcswap10794";

    private ManagedChannel                                    channel;
    private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub   stub;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;
    private ServerControlPlane                                controlPlane;

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
        server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
      server.unmarkClosedByCommand(NAME);
      FileUtils.deleteRecursively(
          new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
    }
  }

  // Regression: #11310
  /**
   * Issue #11310 over gRPC: a database an operator closed exists. {@code ExistsDatabase} answered false for it,
   * {@code CreateDatabase(if_not_exists)} then failed ALREADY_EXISTS where the contract is to leave an existing database alone, and
   * {@code DropDatabase(if_exists)} answered dropped=false - "nothing to drop" - and left it on disk. {@code GetDatabaseInfo} said
   * NOT_FOUND where the data path says UNAVAILABLE with the reason.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeldDatabaseExists {
    private static final String HELD = "held11310grpc";

    private ManagedChannel                                          channel;
    private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub admin;
    private DatabaseCredentials                                     credentials;

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
}
