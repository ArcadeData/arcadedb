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
import com.arcadedb.server.network.MultiAddressServerSocket;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.DatabaseResolver;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDBProtocolPlugin implements ServerPlugin, DatabaseResolver {
  private volatile MongoServer                mongoDBServer;
  /** The listeners on the other local addresses of the host, when it resolves to several (issue #9224). */
  private final List<MongoServer>             additionalServers = new ArrayList<>();
  private MongoDBBackend                      mongoDBBackend;
  private ArcadeDBServer                      server;
  private String                              host;
  private int                                 port;
  private Map<String, MongoDBDatabaseWrapper> databases = new ConcurrentHashMap<>();

  /**
   * Whether key values must stay out of what a client is told: the same policy every other surface applies (issue #8931).
   */
  boolean isProductionMode() {
    return server != null && server.isProductionMode();
  }

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.host = configuration.getValueAsString(GlobalConfiguration.MONGO_HOST);
    this.port = configuration.getValueAsInteger(GlobalConfiguration.MONGO_PORT);
  }

  @Override
  public void startService() {
    mongoDBBackend = new MongoDBBackend(server, this);
    // Every local address the host resolves to, not only the first one: a port held on [::1] must not look free for "localhost"
    // (issue #9224). The library binds one address per server, so each further address gets a server of its own on the same port.
    // They share the backend: MongoServer.shutdown() only clears its database map, which is harmless to repeat (the backend opens its databases lazily, so it stays usable for the next attempt). A name resolving to
    // several local addresses is bound by their literals (the configured name is not what the sockets show)
    final List<String> hosts = MultiAddressServerSocket.resolveListenHosts(host);
    // an ephemeral port picked on the first address can be taken on another one: the whole set is tried again, with fresh servers
    final int attempts = port == 0 && hosts.size() > 1 ? 10 : 1;
    for (int attempt = 1; ; attempt++)
      try {
        mongoDBServer = new MongoServer(mongoDBBackend);
        mongoDBServer.bind(hosts.getFirst(), port);
        final int boundPort = getPort();
        for (final String address : hosts.subList(1, hosts.size())) {
          final MongoServer additional = new MongoServer(mongoDBBackend);
          additionalServers.add(additional);
          additional.bind(address, boundPort);
        }
        return;
      } catch (final RuntimeException e) {
        stopService();
        if (attempt >= attempts)
          throw e;
      }
  }

  @Override
  public void stopService() {
    for (final MongoServer additional : additionalServers)
      additional.shutdown();
    additionalServers.clear();
    if (mongoDBServer != null)
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
