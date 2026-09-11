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
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7308: the SSRF and local-file guard {@code restore database} and {@code import database}
 * apply over HTTP has to hold on the gRPC RPCs too, and it does because it lives inside the shared
 * {@code ServerControlPlane} rather than in either handler.
 * <p>
 * A separate fixture from {@link Issue7308GrpcRestoreImportIT} because
 * {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} is a server-wide setting: that
 * class needs it on to restore from an archive on disk, and this one needs it at its default, off.
 * <p>
 * The refusal is {@code PERMISSION_DENIED} and deliberately not {@code UNAUTHENTICATED}: the
 * caller's credentials are fine and re-sending them changes nothing, it is the URL that is refused.
 * The HTTP counterpart answers 403 for the same reason.
 */
public class Issue7308GrpcRestoreImportUrlGuardIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;

  private ManagedChannel                                            channel;
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    adminStub = ArcadeDbAdminServiceGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  @Test
  void restoreDatabaseRefusesAFileUrl() {
    assertRefused(() -> adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("grpc7308_file_url").setUrl("file:///etc/passwd").build()));

    assertThat(getServer(0).existsDatabase("grpc7308_file_url")).isFalse();
  }

  @Test
  void restoreDatabaseRefusesALinkLocalMetadataHost() {
    assertRefused(() -> adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("grpc7308_metadata").setUrl("http://169.254.169.254/latest/meta-data/").build()));
  }

  @Test
  void restoreDatabaseRefusesALoopbackHost() {
    assertRefused(() -> adminStub.restoreDatabase(RestoreDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("grpc7308_loopback").setUrl("http://127.0.0.1:9/archive.zip").build()));
  }

  /**
   * The import guard runs before the database is created, so a refused URL leaves nothing behind for
   * the operator to clean up - the ordering the HTTP command established and this transport inherits
   * by sharing the implementation.
   */
  @Test
  void importDatabaseRefusesAPrivateHostWithoutCreatingTheDatabase() {
    assertRefused(() -> adminStub.importDatabase(ImportDatabaseRequest.newBuilder().setCredentials(root())
        .setDatabase("grpc7308_import_ssrf").setUrl("http://192.168.0.1/data.csv").build()));

    assertThat(getServer(0).existsDatabase("grpc7308_import_ssrf")).isFalse();
  }

  @Test
  void restoreBackupNeedsNoUrlGuardBecauseTheArchiveIsResolvedServerSide() {
    // No auto-backup is configured in this fixture, so the archive cannot be resolved at all. The
    // point is which refusal arrives: a rejected argument, never the URL guard - restore backup
    // builds its own file:// URL from a server-side path and never sees a caller-supplied one.
    assertThatThrownBy(() -> drain(adminStub.restoreBackup(RestoreBackupRequest.newBuilder().setCredentials(root())
        .setDatabase(getDatabaseName()).setFileName("db-backup-1.zip").setTargetDatabase("grpc7308_no_backup").build())))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("INVALID_ARGUMENT");
  }

  private static void assertRefused(final ThrowingCall call) {
    assertThatThrownBy(() -> drain(call.run()))
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED")
        .hasMessageNotContaining("UNAUTHENTICATED");
  }

  private static void drain(final Iterator<?> stream) {
    while (stream.hasNext())
      stream.next();
  }

  @FunctionalInterface
  private interface ThrowingCall {
    Iterator<?> run();
  }
}
