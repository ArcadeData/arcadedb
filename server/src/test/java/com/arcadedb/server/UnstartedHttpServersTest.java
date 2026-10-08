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
package com.arcadedb.server;

import com.arcadedb.server.http.HttpServer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Issue #9464: {@link UnstartedHttpServers} hands out real servers bound to the given server and stops them all. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UnstartedHttpServersTest {
  @RegisterExtension
  static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();

  @Test
  @Order(1)
  void aServerIsBoundToTheGivenServerAndTracked() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    final HttpServer first = HTTP_SERVERS.of(server);
    HTTP_SERVERS.of(server);

    assertThat(first.getServer()).isSameAs(server);
    assertThat(HTTP_SERVERS.pending()).isEqualTo(2);
  }

  @Test
  @Order(2)
  void theRegisteredExtensionStoppedTheServersOfThePreviousTest() {
    assertThat(HTTP_SERVERS.pending()).as("the static registration runs afterEach for every test").isZero();
  }

  @Test
  @Order(4)
  void aListeningServerReportsItsPortsWithoutBinding() {
    final ArcadeDBServer server = TestServerHelper.unstartedServer();
    assertThat(HTTP_SERVERS.of(server).getPort()).as("an unstarted server listens on nothing").isZero();

    final HttpServer plain = HTTP_SERVERS.listeningOn(server, 41234);
    assertThat(plain.getPort()).isEqualTo(41234);
    assertThat(plain.getHttpsPort()).isEqualTo(-1);
    assertThat(HTTP_SERVERS.listeningOn(server, 41234, 41235).getHttpsPort()).isEqualTo(41235);
  }

  @Test
  @Order(3)
  void everyServerIsStoppedEvenWhenOneWasAlreadyStopped() {
    final long before = liveCleanupTimers();
    final UnstartedHttpServers servers = new UnstartedHttpServers();
    final HttpServer first = servers.of(TestServerHelper.unstartedServer());
    servers.of(TestServerHelper.unstartedServer());
    assertThat(liveCleanupTimers()).as("each server starts its own auth-session cleanup timer").isEqualTo(before + 2);
    first.stopService();

    servers.afterEach(null);

    assertThat(servers.pending()).isZero();
    // Timer.cancel() lets the thread exit on its own: wait for it rather than for a fixed time
    await().atMost(Duration.ofSeconds(30)).until(() -> liveCleanupTimers() == before);
  }

  private static long liveCleanupTimers() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && t.getName().equals("HttpAuthSessionManager-Cleanup")).count();
  }
}
