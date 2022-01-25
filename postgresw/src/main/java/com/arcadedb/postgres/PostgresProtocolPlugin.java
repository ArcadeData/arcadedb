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
import com.arcadedb.server.ha.network.DefaultServerSocketFactory;
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.handlers.PathHandler;

public class PostgresProtocolPlugin implements ServerPlugin {
  private ArcadeDBServer          server;
  private ContextConfiguration    configuration;
  private PostgresNetworkListener listener;
  private String                  host;
  private int                     port;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
    this.host = configuration.getValueAsString(GlobalConfiguration.POSTGRES_HOST);
    this.port = configuration.getValueAsInteger(GlobalConfiguration.POSTGRES_PORT);
  }

  @Override
  public void startService() {
    listener = new PostgresNetworkListener(server, new DefaultServerSocketFactory(), host, "" + port);
  }

  @Override
  public void stopService() {
    if (listener != null)
      listener.close();
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
  }
}
