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
package com.arcadedb.mongo;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.DatabaseResolver;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDBProtocolPlugin implements ServerPlugin, DatabaseResolver {
  private volatile MongoServer                mongoDBServer;
  private MongoDBBackend                      mongoDBBackend;
  private ArcadeDBServer                      server;
  private String                              host;
  private int                                 port;
  private Map<String, MongoDBDatabaseWrapper> databases = new ConcurrentHashMap<>();

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.host = configuration.getValueAsString(GlobalConfiguration.MONGO_HOST);
    this.port = configuration.getValueAsInteger(GlobalConfiguration.MONGO_PORT);
  }

  @Override
  public void startService() {
    mongoDBBackend = new MongoDBBackend(server, this);
    mongoDBServer = new MongoServer(mongoDBBackend);
    mongoDBServer.bind(host, port);
  }

  @Override
  public void stopService() {
    mongoDBServer.shutdown();
  }

  /**
   * The port the MongoDB listener ACTUALLY bound, which is not necessarily the configured one: {@code 0} asks the
   * operating system for a free port (issue #8209). Returns -1 when the service is not listening.
   */
  public int getPort() {
    final MongoServer s = mongoDBServer;
    if (s == null)
      return -1;
    try {
      final InetSocketAddress address = s.getLocalAddress();
      return address != null ? address.getPort() : -1;
    } catch (final RuntimeException e) {
      // NOT BOUND (YET, OR ANY MORE)
      return -1;
    }
  }

  @Override
  public MongoDatabase resolve(final String s) {
    return databases.get(s);
  }
}
