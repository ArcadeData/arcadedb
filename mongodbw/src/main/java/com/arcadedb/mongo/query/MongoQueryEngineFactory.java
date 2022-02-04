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
package com.arcadedb.mongo.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.mongo.MongoDBDatabaseWrapper;
import com.arcadedb.query.QueryEngine;

import java.util.logging.*;

public class MongoQueryEngineFactory implements QueryEngine.QueryEngineFactory {
  @Override
  public String getLanguage() {
    return "mongo";
  }

  @Override
  public QueryEngine getInstance(final DatabaseInternal database) {
    Object engine = database.getWrappers().get(MongoQueryEngine.ENGINE_NAME);
    if (engine != null)
      return (MongoQueryEngine) engine;

    try {

      engine = new MongoQueryEngine(MongoDBDatabaseWrapper.open(database));
      database.setWrapper(MongoQueryEngine.ENGINE_NAME, engine);
      return (MongoQueryEngine) engine;

    } catch (Throwable e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Mongo query engine", e);
      throw new QueryParsingException("Error on initializing Mongo query engine", e);
    }
  }
}
