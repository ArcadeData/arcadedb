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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import com.arcadedb.utility.FileUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

// Regression: #12417
/**
 * Issue #12417: {@code RestoreBackup} runs on the leader only (issue #7308), while {@code TriggerBackup},
 * {@code ListBackups} and {@code DeleteBackup} ran wherever the call landed. An archive triggered on a follower was
 * written into that follower's own backup directory, where no restore could reach it.
 * <p>
 * HTTP forwards the three commands to the leader. gRPC has no request proxy, so its gate is the refusal that names the
 * leader, the same {@code requireLeader} the restore uses. Every server of this cluster has its own backup directory,
 * as separate hosts do.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
@Tag("slow")
class GrpcBackupLeaderRoutingIT extends BaseRaftHATest {

  private static final String BACKUP_ROOT    = "./target/backups-issue12417-grpc";
  // The Raft and HTTP ports are BaseRaftHATest's own, see GrpcLeaderRoutingIT
  private static final int    BASE_HTTP_PORT = 2480;
  private final        int[]  grpcPorts      = allocateFixturePorts(2);

  private ManagedChannel channel;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected String getServerAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(raftPort(i))
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(grpcPorts[i]).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final int index = serverIndexOf(config);

    final File directory = new File(BACKUP_ROOT, "server" + index);
    FileUtils.deleteRecursively(directory);
    config.setValue(GlobalConfiguration.SERVER_BACKUP_DIRECTORY, directory.getPath());

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(grpcPorts[index]));
    config.setValue("arcadedb.grpc.host", "localhost");
    config.setValue("arcadedb.grpc.reflection.enabled", "false");
    config.setValue("arcadedb.grpc.health.enabled", "false");

    final String existingPlugins = config.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    final String pluginEntry = "GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin";
    if (existingPlugins == null || existingPlugins.isEmpty())
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, pluginEntry);
    else if (!existingPlugins.contains(pluginEntry))
      config.setValue(GlobalConfiguration.SERVER_PLUGINS, existingPlugins + "," + pluginEntry);
  }

  @AfterEach
  void closeChannelAndDeleteArchives() {
    closeChannel();
    FileUtils.deleteRecursively(new File(BACKUP_ROOT));
  }

  /**
   * One test method: every method of this class restarts the cluster, and the leader half is the control the
   * refusals need, since a gate that refused on every node would pass the follower half alone.
   */
  @Test
  void theBackupRpcsAreRefusedOnAFollowerNamingTheLeaderAndRunOnTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    waitForAllServers();

    final String database = getDatabaseName();
    final Path leaderDirectory = backupDirectoryOf(leader).resolve(database);
    final Path followerDirectory = backupDirectoryOf(follower).resolve(database);
    assertThat(leaderDirectory).as("the two servers must not share a backup directory, or the test proves nothing")
        .isNotEqualTo(followerDirectory);

    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(follower).triggerBackup(
        TriggerBackupRequest.newBuilder().setCredentials(root()).setDatabase(database).build())));
    assertThat(followerDirectory).as("the follower writes no archive the restore cannot reach").doesNotExist();

    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(follower).listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase(database).build())));

    // The leader runs the same calls, and the archive it writes is the one its own restore reads
    final String archive = new File(adminStubOn(leader).triggerBackup(
        TriggerBackupRequest.newBuilder().setCredentials(root()).setDatabase(database).build()).getBackupFile()).getName();
    assertThat(leaderDirectory.resolve(archive)).isRegularFile();

    final ListBackupsResponse listing = adminStubOn(leader).listBackups(
        ListBackupsRequest.newBuilder().setCredentials(root()).setDatabase(database).build());
    assertThat(listing.getBackupsList()).extracting(BackupInfo::getFileName).containsExactly(archive);

    assertRefusedAndNamesTheLeader(catchStatus(() -> adminStubOn(follower).deleteBackup(
        DeleteBackupRequest.newBuilder().setCredentials(root()).setDatabase(database).setFileName(archive).build())));
    assertThat(leaderDirectory.resolve(archive)).as("a refused delete removes nothing").isRegularFile();

    adminStubOn(leader).deleteBackup(
        DeleteBackupRequest.newBuilder().setCredentials(root()).setDatabase(database).setFileName(archive).build());
    assertThat(leaderDirectory.resolve(archive)).doesNotExist();
  }

  private Path backupDirectoryOf(final int serverIndex) {
    return new ServerControlPlane(getServer(serverIndex)).resolveBackupDirectory();
  }

  private static void assertRefusedAndNamesTheLeader(final StatusRuntimeException refusal) {
    assertThat(refusal.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(refusal.getTrailers()).isNotNull();
    assertThat(refusal.getTrailers().get(LeaderRedirectProtocol.LEADER_HTTP_ADDRESS))
        .as("the refusal must name where to go instead").isNotBlank();
  }

  private static StatusRuntimeException catchStatus(final Runnable call) {
    final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class, call::run);
    assertThat(e).as("the call must have been refused").isNotNull();
    return e;
  }

  /**
   * A blocking admin stub aimed at one node. Every call here is blocking and has returned by the time the next stub
   * is asked for, so the previous channel is closed rather than left behind.
   */
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
    closeChannel();
    channel = ManagedChannelBuilder.forTarget("localhost:" + grpcPorts[serverIndex]).usePlaintext().build();
    return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60, TimeUnit.SECONDS);
  }

  private void closeChannel() {
    if (channel != null) {
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      channel = null;
    }
  }

  private static DatabaseCredentials root() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }
}
