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
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #7308: {@code restore backup}, {@code restore database} and {@code import database} are in
 * the set the HTTP control plane forwards to the leader
 * ({@code PostServerCommandHandler.forwardToLeaderIfReplica}), so over HTTP they never execute on a
 * follower. gRPC has no request proxy, so the equivalent gate is a refusal that names the leader -
 * the same {@code requireLeader} the four #7304 mutations use, extended to these three.
 * <p>
 * Without it, a restore accepted on a follower would swap a database directory into place behind the
 * cluster's back, and an import would create a database on one node only.
 * <p>
 * The sibling of {@code Issue7304GrpcAdminLeaderRoutingIT} for the RPCs that stream. They need their
 * own assertions because a server-streaming call reports nothing until its response is read: the
 * refusal arrives when the stream is drained, not when the stub method returns.
 */
@Tag("slow")
class Issue7308GrpcRestoreImportLeaderRoutingIT extends BaseRaftHATest {

  // The Raft and HTTP ports are BaseRaftHATest's own: the base class binds those regardless of what
  // this server list says, so naming any others here would advertise addresses nobody listens on and
  // no leader would ever be elected. Only the gRPC port is this class's to choose, and it differs
  // from Issue7304GrpcAdminLeaderRoutingIT's so the two never contend for it.
  private static final int BASE_RAFT_PORT = 2434;
  private static final int BASE_HTTP_PORT = 2480;
  private static final int BASE_GRPC_PORT = 51161;

  private ManagedChannel channel;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(BASE_RAFT_PORT + i)
          .append(",http:").append(BASE_HTTP_PORT + i)
          .append(",grpc:").append(BASE_GRPC_PORT + i).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));

    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(BASE_GRPC_PORT + index));
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
  void closeChannelAfterEach() {
    closeChannel();
  }

  /**
   * All three RPCs in one test method: each method of this class restarts a three-node cluster, and
   * the assertions are the same gate seen through three entry points rather than three behaviours.
   * <p>
   * Every request names a database that does not exist and a URL that is never fetched. If the gate
   * holds, none of that is reached; the final loop is what proves it, by checking no node was
   * touched.
   */
  @Test
  void restoreAndImportRpcsAreRefusedOnAFollowerAndNameTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final int followerIndex = anyFollowerOf(leaderIndex);
    final String database = "issue7308_follower_db";

    assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).restoreBackup(
        RestoreBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
            .setFileName("anything-backup-20260101.zip").setTargetDatabase(database).build()))));

    assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).restoreDatabase(
        RestoreDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(database)
            .setUrl("https://example.invalid/archive.zip").build()))));

    assertRefusedAndNamesTheLeader(catchStatus(() -> drain(adminStubOn(followerIndex).importDatabase(
        ImportDatabaseRequest.newBuilder().setCredentials(root()).setDatabase(database)
            .setUrl("https://example.invalid/data.csv").build()))));

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServer(i).existsDatabase(database))
          .as("server %d must not hold a database the follower refused to create", i).isFalse();
  }

  /**
   * The control the refusals need to mean anything: a gate that refused on every node would satisfy
   * the test above and fail this one. The leader runs the same call and gets past the gate, failing
   * instead on the archive that is genuinely not there - INVALID_ARGUMENT, not FAILED_PRECONDITION.
   */
  @Test
  void theSameCallGetsPastTheGateOnTheLeader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final StatusRuntimeException failure = catchStatus(() -> drain(adminStubOn(leaderIndex).restoreBackup(
        RestoreBackupRequest.newBuilder().setCredentials(root()).setDatabase(getDatabaseName())
            .setFileName("anything-backup-20260101.zip").setTargetDatabase("issue7308_leader_db").build())));

    assertThat(failure.getStatus().getCode())
        .as("the leader must reach the operation and fail on its arguments, not on the leader gate")
        .isEqualTo(Status.Code.INVALID_ARGUMENT);
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

  private static void drain(final Iterator<?> stream) {
    while (stream.hasNext())
      stream.next();
  }

  /**
   * A blocking admin stub aimed at one node. Every call here is blocking and has returned by the time
   * the next stub is asked for, so the previous channel is closed rather than left behind.
   */
  private ArcadeDbAdminServiceGrpc.ArcadeDbAdminServiceBlockingStub adminStubOn(final int serverIndex) {
    closeChannel();
    channel = ManagedChannelBuilder.forTarget("localhost:" + (BASE_GRPC_PORT + serverIndex)).usePlaintext().build();
    return ArcadeDbAdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
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

  private int anyFollowerOf(final int leaderIndex) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        return i;
    throw new IllegalStateException("At least one follower must exist");
  }
}
