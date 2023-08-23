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

import com.arcadedb.server.ArcadeDBServer;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoBackend;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MongoDBBackend extends AbstractMongoBackend {
  private final ArcadeDBServer        server;
  private final MongoDBProtocolPlugin plugin;

  public MongoDBBackend(final ArcadeDBServer server, final MongoDBProtocolPlugin plugin) {
    this.server = server;
    this.plugin = plugin;
  }

  @Override
  protected MongoDatabase openOrCreateDatabase(final String databaseName) throws MongoServerException {
    return new MongoDBDatabaseWrapper(server.getDatabase(databaseName), plugin, this);
  }
}
