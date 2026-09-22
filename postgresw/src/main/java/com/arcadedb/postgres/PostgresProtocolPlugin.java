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
package com.arcadedb.postgres;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.network.DefaultServerSocketFactory;

public class PostgresProtocolPlugin implements ServerPlugin {
  private          ArcadeDBServer          server;
  private volatile PostgresNetworkListener listener;
  private          String                  host;
  private          String                  portRange;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.host = configuration.getValueAsString(GlobalConfiguration.POSTGRES_HOST);
    // A single port, a range `<from>-<to>` or a comma-separated list: the listener binds the first free one (#8142).
    this.portRange = configuration.getValueAsString(GlobalConfiguration.POSTGRES_PORT).trim();
  }

  @Override
  public void startService() {
    listener = new PostgresNetworkListener(server, new DefaultServerSocketFactory(), host, portRange);
  }

  /**
   * The port the listener ACTUALLY bound, which is not necessarily the first one of the configured range: a port
   * already taken is skipped. Returns -1 when the service is not listening.
   */
  public int getPort() {
    final PostgresNetworkListener l = listener;
    return l != null ? l.getPort() : -1;
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();
  }
}
