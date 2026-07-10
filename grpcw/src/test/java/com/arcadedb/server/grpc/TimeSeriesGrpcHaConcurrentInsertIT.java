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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.server.ha.raft.BaseRaftHATest;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: concurrent single-row time-series INSERTs over gRPC against a Raft HA cluster
 * must replicate to followers without diverging.
 * <p>
 * Each single-row {@code INSERT} into a time-series type must commit as part of its enclosing
 * command transaction so the mutable-bucket page changes ship to followers as a single, correctly
 * ordered {@code TX_ENTRY}.  Routing the append onto a separate executor thread (committing it as an
 * independent top-level transaction, out of band with the command transaction) reorders the
 * mutable-bucket page versions relative to the Raft log; a follower then observes a WAL page-version
 * gap, force-triggers a snapshot resync, and queries transiently fail (e.g. an empty schema type)
 * until it recovers.  This test guards against that regression.
 */
@Tag("slow")
class TimeSeriesGrpcHaConcurrentInsertIT extends BaseRaftHATest {

  private static final int    BASE_GRPC_PORT    = 51081;
  private static final int    NUM_THREADS       = 3;
  private static final int    POINTS_PER_THREAD = 2000;
  private static final String TYPE_NAME         = "sensor";

  private static final Metadata.Key<String> USER_HEADER     =
      Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER =
      Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER =
      Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel channel;

  private final class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue("arcadedb.grpc.enabled", "true");
    config.setValue("arcadedb.grpc.port", String.valueOf(BASE_GRPC_PORT + index));
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

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
  }

  @Test
  void concurrentSingleRowTsInsertsReplicateWithoutDivergence() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE_NAME
            + " TIMESTAMP ts TAGS (sensor_id STRING, region STRING) FIELDS (temperature DOUBLE, humidity DOUBLE)");

    waitForAllServers();

    channel = ManagedChannelBuilder
        .forAddress("localhost", BASE_GRPC_PORT + leaderIndex)
        .usePlaintext()
        .maxInboundMessageSize(64 * 1024 * 1024)
        .build();
    final Channel authenticated = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(authenticated);

    final DatabaseCredentials credentials = DatabaseCredentials.newBuilder()
        .setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();

    final ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();

    final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; t++) {
      final int threadIndex = t;
      executor.submit(() -> {
        final long base = 1_000_000_000L + threadIndex * 100_000_000L;
        for (int i = 0; i < POINTS_PER_THREAD; i++) {
          final long ts = base + i;
          final Map<String, GrpcValue> params = Map.of(
              "0", GrpcValue.newBuilder().setInt64Value(ts).build(),
              "1", GrpcValue.newBuilder().setStringValue("sensor-" + threadIndex).build(),
              "2", GrpcValue.newBuilder().setStringValue("region-" + (threadIndex % 3)).build(),
              "3", GrpcValue.newBuilder().setDoubleValue(15.0 + (i % 20)).build(),
              "4", GrpcValue.newBuilder().setDoubleValue(40.0 + (i % 30)).build());

          final ExecuteCommandRequest req = ExecuteCommandRequest.newBuilder()
              .setDatabase(getDatabaseName())
              .setCommand("INSERT INTO " + TYPE_NAME
                  + " SET ts = ?, sensor_id = ?, region = ?, temperature = ?, humidity = ?")
              .setLanguage("sql")
              .setReturnRows(true)
              .setCredentials(credentials)
              .putAllParameters(params)
              .build();
          try {
            final ExecuteCommandResponse resp = stub.executeCommand(req);
            if (!resp.getSuccess())
              errors.add(resp.getMessage());
          } catch (final Exception e) {
            errors.add(e.getMessage());
          }
        }
      });
    }
    executor.shutdown();
    assertThat(executor.awaitTermination(5, TimeUnit.MINUTES)).as("ingestion completes (no resync stall)").isTrue();

    // A WAL page-version gap from out-of-band append commits surfaces here either as ingest errors
    // (transient schema-unavailable during the follower's forced snapshot resync) or as a stalled
    // ingestion that misses the await deadline above.
    final List<String> sample = errors.stream().distinct().limit(10).toList();
    assertThat(errors).as("no ingest errors over gRPC (%d total, distinct sample: %s)", errors.size(), sample).isEmpty();

    // Data integrity: every node must converge to the full sample count. The Raft entries are
    // committed to a majority before each INSERT acks, but the follower applies them to its state
    // machine asynchronously, so poll for convergence rather than asserting immediately. A WAL
    // version gap (the regression this test guards) would prevent a follower from converging.
    final long expected = (long) NUM_THREADS * POINTS_PER_THREAD;
    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServerDatabase(i, getDatabaseName());
      final long deadline = System.currentTimeMillis() + 60_000;
      long count = -1;
      while (System.currentTimeMillis() < deadline) {
        count = ((Number) db.command("sql", "SELECT count(*) AS cnt FROM " + TYPE_NAME).next().getProperty("cnt")).longValue();
        if (count == expected)
          break;
        Thread.sleep(1_000);
      }
      assertThat(count).as("server %d converged to the full sample count", i).isEqualTo(expected);
    }
  }
}
