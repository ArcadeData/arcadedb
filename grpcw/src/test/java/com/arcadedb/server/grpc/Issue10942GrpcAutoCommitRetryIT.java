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
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10942: an auto-commit command sent over gRPC that hits a transient write conflict is retried on the server up to
 * {@link GlobalConfiguration#TX_RETRIES} times, exactly as the HTTP handler does, instead of failing back to the client as
 * {@code ABORTED}.
 */
public class Issue10942GrpcAutoCommitRetryIT extends BaseGrpcServerTest {
  private static final int WRITERS = 4;
  private static final int UPDATES = 150;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Same-record writers conflict on most commits; a generous budget makes "retried" and "never aborted" the same claim
    GlobalConfiguration.TX_RETRIES.setValue(200);
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

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
  void concurrentAutoCommitUpdatesOnTheSamePageAreNotAborted() throws Exception {
    command("CREATE DOCUMENT TYPE Counter10942");
    command("INSERT INTO Counter10942 SET id = 1, n = 0");

    final ExecutorService pool = Executors.newFixedThreadPool(WRITERS);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicInteger failures = new AtomicInteger();
    final AtomicReference<String> firstFailure = new AtomicReference<>();
    final List<Future<?>> futures = new ArrayList<>();
    for (int w = 0; w < WRITERS; w++) {
      futures.add(pool.submit(() -> {
        start.await();
        for (int i = 0; i < UPDATES; i++)
          try {
            command("UPDATE Counter10942 SET n = n + 1 WHERE id = 1");
          } catch (final StatusRuntimeException e) {
            failures.incrementAndGet();
            firstFailure.compareAndSet(null, e.getStatus().toString());
          }
        return null;
      }));
    }
    start.countDown();
    for (final Future<?> f : futures)
      f.get(120, TimeUnit.SECONDS);
    pool.shutdownNow();

    assertThat(failures.get()).as("auto-commit commands that failed on a conflict the server should have retried (first: " + firstFailure.get() + ")").isZero();
    final ExecuteCommandResponse rs = stub.executeCommand(request("SELECT n FROM Counter10942 WHERE id = 1").setReturnRows(true).build());
    assertThat(rs.getRecordsCount()).isEqualTo(1);
    assertThat(rs.getRecords(0).getPropertiesMap().get("n").getInt32Value()).isEqualTo(WRITERS * UPDATES);
  }

  private ExecuteCommandResponse command(final String sql) {
    return stub.executeCommand(request(sql).build());
  }

  private ExecuteCommandRequest.Builder request(final String sql) {
    return ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName()).setCommand(sql)
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build());
  }
}
