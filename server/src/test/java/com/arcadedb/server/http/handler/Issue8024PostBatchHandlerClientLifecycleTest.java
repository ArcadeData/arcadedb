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
package com.arcadedb.server.http.handler;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8024: {@link PostBatchHandler} builds its own follower-to-leader {@link HttpClient} - which owns a selector
 * thread, an executor and a connection pool - and nothing ever released it. One handler is built per
 * {@link HttpServer} start, so every start/stop cycle of an in-process server leaked one client for the life of
 * the JVM. This pins the release at the {@code HttpServer} lifecycle level: the client the live
 * {@code /api/v1/batch} route dials on is terminated once the server stops, and is so even when an earlier step
 * of {@link HttpServer#stopService()} throws (the #7985 guard shape).
 * <p>
 * <b>On the reflection.</b> The handler is registered inline on the route table, and its client is a private
 * field; neither is an API anyone else needs, so the test reads both reflectively rather than widening either.
 */
class Issue8024PostBatchHandlerClientLifecycleTest extends BaseGraphServerTest {

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void stoppingTheServerReleasesTheBatchHandlersHttpClient() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final HttpClient client = batchClientOf(server.getHttpServer());
    assertThat(client.isTerminated()).as("the batch forward client is live while the server is up").isFalse();

    server.stop();

    assertThat(client.isTerminated()).as("stopping the server releases the batch handler's HTTP client").isTrue();

    // A restart builds a fresh HttpServer and a fresh handler: the route must not be left dialing on the client
    // the previous stop released.
    server.start();
    final HttpClient restarted = batchClientOf(server.getHttpServer());
    assertThat(restarted).isNotSameAs(client);
    assertThat(restarted.isTerminated()).as("the restarted server's batch forward client is live").isFalse();
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void aThrowFromAnEarlierStepStillReleasesTheBatchHandlersHttpClient() throws Exception {
    final ArcadeDBServer server = getServer(0);
    final HttpServer httpServer = server.getHttpServer();
    final HttpClient client = batchClientOf(httpServer);
    assertThat(client.isTerminated()).isFalse();

    final Field bus = HttpServer.class.getDeclaredField("webSocketEventBus");
    bus.setAccessible(true);
    bus.set(httpServer, new Issue7985StopServiceReleasesForwarderOnFailureTest.FailingWebSocketEventBus(server));

    // stopInternal() swallows whatever stopService() throws, so this returns either way.
    server.stop();

    assertThat(client.isTerminated())
        .as("a failing step earlier in stopService() must not skip the release of the batch handler's HTTP client")
        .isTrue();

    // Restart only to hand the fixture a running server: its teardown checks and drops the databases.
    server.start();
  }

  private static HttpClient batchClientOf(final HttpServer httpServer) throws ReflectiveOperationException {
    final Field handlerField = HttpServer.class.getDeclaredField("postBatchHandler");
    handlerField.setAccessible(true);
    final PostBatchHandler handler = (PostBatchHandler) handlerField.get(httpServer);
    assertThat(handler).as("the HttpServer keeps the /batch handler it registered").isNotNull();

    final Field clientField = PostBatchHandler.class.getDeclaredField("httpClient");
    clientField.setAccessible(true);
    return (HttpClient) clientField.get(handler);
  }
}
