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

import java.util.*;
import java.util.concurrent.*;

public class MongoDBProtocolPlugin implements ServerPlugin, DatabaseResolver {
  private MongoServer                         mongoDBServer;
  private MongoDBBackend                      mongoDBBackend;
  private ArcadeDBServer                      server;
  private Map<String, MongoDBDatabaseWrapper> databases = new ConcurrentHashMap<>();

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
  }

  @Override
  public void startService() {
    mongoDBBackend = new MongoDBBackend(server, this);
    mongoDBServer = new MongoServer(mongoDBBackend);
    mongoDBServer.bind(GlobalConfiguration.MONGO_HOST.getValueAsString(), GlobalConfiguration.MONGO_PORT.getValueAsInteger());
  }

  @Override
  public void stopService() {
    mongoDBServer.shutdown();
  }

  @Override
  public MongoDatabase resolve(final String s) {
    return databases.get(s);
  }
}
