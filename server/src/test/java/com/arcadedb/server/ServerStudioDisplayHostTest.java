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

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the Studio URL displayed in the log uses a user-friendly host (localhost)
 * instead of internal hostnames like Docker container IDs (issue #3562).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerStudioDisplayHostTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    super.endTest();
  }

  @Test
  void studioDisplayHostDefaultIsLocalhost() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();

    // Default HTTP host is 0.0.0.0, Studio display should show "localhost"
    final Method method = ArcadeDBServer.class.getDeclaredMethod("getStudioDisplayHost");
    method.setAccessible(true);
    final String studioHost = (String) method.invoke(server);

    assertThat(studioHost).isEqualTo("localhost");
    // The HA host address may differ (e.g. Docker container ID), but Studio URL should always be user-friendly
    assertThat(studioHost).doesNotMatch("[0-9a-f]{12}");

    // Also verify that when a custom HTTP host is configured, it is used instead
    server.stop();

    final ContextConfiguration config2 = new ContextConfiguration();
    config2.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config2.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config2.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config2.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    config2.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "127.0.0.1");

    server = new ArcadeDBServer(config2);
    server.start();

    final String studioHost2 = (String) method.invoke(server);
    assertThat(studioHost2).isEqualTo("127.0.0.1");
  }
}
