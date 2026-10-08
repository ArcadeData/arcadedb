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
import com.arcadedb.utility.CodeUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Builds real, never-started {@link HttpServer}s for handler tests and stops every one of them after each test (issue
 * #9464). The constructor of an {@link HttpServer} already starts cleanup threads - two timers, one of them non-daemon,
 * and a scheduled executor - that only {@link HttpServer#stopService()} ends, so a test that builds one without this
 * leaks them.
 * <p>
 * Register it as a {@code static} field, which also serves the {@code static} helpers handler tests build their
 * handlers in: {@code @RegisterExtension static final UnstartedHttpServers HTTP_SERVERS = new UnstartedHttpServers();}
 * <p>
 * The static registration shares one list across a class's tests, so it is not safe under concurrent method execution
 * ({@code junit.jupiter.execution.parallel.mode.default=concurrent}): one test's {@code afterEach} would stop a server
 * another test is still using.
 */
public final class UnstartedHttpServers implements AfterEachCallback {
  private final List<HttpServer> servers = new CopyOnWriteArrayList<>();

  /** A real HTTP server for {@code server}, never started, stopped after the current test. */
  public HttpServer of(final ArcadeDBServer server) {
    final HttpServer httpServer = new HttpServer(server);
    servers.add(httpServer);
    return httpServer;
  }

  /** How many servers are waiting to be stopped: zero after every test. */
  int pending() {
    return servers.size();
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    // One server failing to stop must not leave the others running
    for (final HttpServer httpServer : servers)
      CodeUtils.executeIgnoringExceptions(httpServer::stopService, "Error on stopping a test HTTP server", true);
    servers.clear();
  }
}
