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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7247: the four {@code lifecycleEvent(...)} call sites in {@link ArcadeDBServer} caught every exception a
 * plugin or a {@link ReplicationCallback} threw and re-threw a {@link ServerException} carrying nothing but the
 * server's own name. This is the message printed on a server that will not come up, thrown from the exact frame
 * that knew why - so the cause has to travel with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7247LifecycleFailureCauseTest extends StaticBaseServerTest {
  private static final String MARKER = "the reason this server did not come up";

  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    server = null;
    super.endTest();
  }

  @Test
  void serverStartingFailureCarriesItsCause() {
    server = newServer();
    server.registerTestEventListener(throwingOn(ReplicationCallback.TYPE.SERVER_STARTING));

    assertThatThrownBy(() -> server.start())
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Error on starting the server")
        .cause().hasMessageContaining(MARKER);
  }

  @Test
  void serverUpFailureCarriesItsCause() {
    server = newServer();
    server.registerTestEventListener(throwingOn(ReplicationCallback.TYPE.SERVER_UP));

    assertThatThrownBy(() -> server.start())
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Error on starting the server")
        .cause().hasMessageContaining(MARKER);

    // SERVER_UP stops the server before re-throwing, so nothing is left running for the next test.
    assertThat(server.isStarted()).isFalse();
  }

  @Test
  void serverShuttingDownFailureCarriesItsCause() {
    server = newServer();
    server.start();
    server.registerTestEventListener(throwingOn(ReplicationCallback.TYPE.SERVER_SHUTTING_DOWN));

    assertThatThrownBy(() -> server.stop())
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Error on stopping the server")
        .cause().hasMessageContaining(MARKER);
  }

  @Test
  void serverDownFailureCarriesItsCause() {
    server = newServer();
    server.start();
    server.registerTestEventListener(throwingOn(ReplicationCallback.TYPE.SERVER_DOWN));

    assertThatThrownBy(() -> server.stop())
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("Error on stopping the server")
        .cause().hasMessageContaining(MARKER);
  }

  /**
   * ONE-SHOT: a listener cannot be unregistered, and the shutdown events fire again from {@link #endTest()}, where
   * a second throw would leave the server half-stopped for the next test in the class.
   */
  private static ReplicationCallback throwingOn(final ReplicationCallback.TYPE failing) {
    final AtomicBoolean fired = new AtomicBoolean();
    return (type, object, server) -> {
      if (type == failing && fired.compareAndSet(false, true))
        throw new IllegalStateException(MARKER);
    };
  }

  private static ArcadeDBServer newServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    return new ArcadeDBServer(config);
  }
}
