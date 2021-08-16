/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.cypher.CypherQueryEngine;
import com.arcadedb.query.gremlin.GremlinQueryEngine;
import com.arcadedb.query.mongo.MongoQueryEngine;
import com.arcadedb.query.sql.SQLQueryEngine;

import java.util.HashMap;
import java.util.Map;

public class QueryEngineManager {
  private Map<String, QueryEngine.QueryEngineFactory> implementations = new HashMap<>();

  public QueryEngineManager() {
    register(new SQLQueryEngine.SQLQueryEngineFactory());
    register(new GremlinQueryEngine.GremlinQueryEngineFactory());
    register(new CypherQueryEngine.CypherQueryEngineFactory());
    register(new MongoQueryEngine.MongoQueryEngineFactory());
  }

  public void register(final QueryEngine.QueryEngineFactory impl) {
    if (impl.isAvailable())
      implementations.put(impl.getLanguage().toLowerCase(), impl);
  }

  public QueryEngine create(final String language, DatabaseInternal database) {
    final QueryEngine.QueryEngineFactory impl = implementations.get(language.toLowerCase());
    if (impl == null)
      throw new IllegalArgumentException("Query engine '" + language + "' was not found");
    return impl.create(database);
  }
}