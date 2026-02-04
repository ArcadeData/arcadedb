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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteServerTest {

  private TestableRemoteServer server;

  /**
   * Testable subclass that overrides requestClusterConfiguration() to avoid HTTP calls.
   */
  static class TestableRemoteServer extends RemoteServer {
    TestableRemoteServer(final String serverAddress, final int port, final String userName, final String userPassword) {
      super(serverAddress, port, userName, userPassword, new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No-op to avoid HTTP calls during tests
    }
  }

  @BeforeEach
  void setUp() {
    server = new TestableRemoteServer("localhost", 2480, "root", "test");
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  @Test
  void toStringContainsServerInfo() {
    assertThat(server.toString()).isEqualTo("http://localhost:2480");
  }

  @Test
  void toStringWithHttpsPrefix() {
    final TestableRemoteServer httpsServer = new TestableRemoteServer("https://secure.host", 2480, "root", "test");
    try {
      assertThat(httpsServer.toString()).isEqualTo("https://secure.host:2480");
    } finally {
      httpsServer.close();
    }
  }
}
