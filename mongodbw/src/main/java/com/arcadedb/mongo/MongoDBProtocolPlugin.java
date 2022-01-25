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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;
import io.undertow.server.handlers.PathHandler;

public class MongoDBProtocolPlugin implements ServerPlugin {
  private MongoServer          mongoDBServer;
  private ArcadeDBServer       server;
  private ContextConfiguration configuration;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    mongoDBServer = new MongoServer(new AbstractMongoBackend() {
      @Override
      protected MongoDatabase openOrCreateDatabase(final String databaseName) throws MongoServerException {
        return new MongoDBDatabaseWrapper(server.getDatabase(databaseName), this);
      }

      @Override
      public void close() {
      }
    });
    mongoDBServer.bind("localhost", 27017);
  }

  @Override
  public void stopService() {
    mongoDBServer.shutdown();
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
  }
}
