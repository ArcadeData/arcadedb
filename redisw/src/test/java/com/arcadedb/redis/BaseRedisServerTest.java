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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;

/**
 * Base for the tests that start the Redis plugin. The plugin is started on port {@code 0}, so the operating system
 * assigns a free port, and every test must connect to {@link #getServerRedisPort()} rather than to 6379 or to the
 * configured {@link GlobalConfiguration#REDIS_PORT}: anything already listening on the production default (a
 * developer's own Redis, a second concurrent build, another agent) used to fail the module at {@code @BeforeEach} or,
 * worse, answer the test's connections (issue #8209).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class BaseRedisServerTest extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // After resetAll(), which super.setTestConfiguration() runs: set earlier the value would be discarded before the
    // server reads it. A subclass that needs a fixed port sets its own after calling this method.
    GlobalConfiguration.REDIS_PORT.setValue(0);
  }

  /**
   * The Redis port the first server ACTUALLY bound.
   */
  protected int getServerRedisPort() {
    return getServerRedisPort(0);
  }

  /**
   * The Redis port server {@code serverIndex} ACTUALLY bound, the counterpart of
   * {@link BaseGraphServerTest#getServerHttpPort(int)}.
   */
  protected int getServerRedisPort(final int serverIndex) {
    return getServerRedisPort(getServer(serverIndex));
  }

  /**
   * @throws IllegalStateException when the server is not started or does not run the Redis plugin, because there is
   *                               no port to answer with and a guess would reintroduce the defect this method exists
   *                               to remove
   */
  static int getServerRedisPort(final ArcadeDBServer server) {
    if (server != null)
      for (final ServerPlugin plugin : server.getPlugins())
        if (plugin instanceof RedisProtocolPlugin p && p.getPort() > 0)
          return p.getPort();
    throw new IllegalStateException("The Redis plugin is not listening: it has not bound a port, so there is none to address");
  }
}
