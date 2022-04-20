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
package com.arcadedb.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.java.JavaQueryEngine;
import com.arcadedb.query.polyglot.PolyglotQueryEngine;
import com.arcadedb.query.sql.SQLQueryEngine;

import java.util.*;
import java.util.logging.*;

public class QueryEngineManager {
  private final Map<String, QueryEngine.QueryEngineFactory> implementations = new HashMap<>();

  public QueryEngineManager() {
    // REGISTER ALL THE SUPPORTED LANGUAGE FROM POLYGLOT ENGINE
    for (String language : PolyglotQueryEngine.PolyglotQueryEngineFactory.getSupportedLanguages())
      register(new PolyglotQueryEngine.PolyglotQueryEngineFactory(language));

    register(new JavaQueryEngine.JavaQueryEngineFactory());
    register(new SQLQueryEngine.SQLQueryEngineFactory());

    // REGISTER QUERY ENGINES IF AVAILABLE ON CLASSPATH AT RUN-TIME
    register("com.arcadedb.gremlin.query.GremlinQueryEngineFactory");
    register("com.arcadedb.gremlin.query.CypherQueryEngineFactory");
    register("com.arcadedb.mongo.query.MongoQueryEngineFactory");
    register("com.arcadedb.graphql.query.GraphQLQueryEngineFactory");
  }

  public void register(final String className) {
    try {
      register((QueryEngine.QueryEngineFactory) Class.forName(className).getConstructor().newInstance());
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to register engine '%s' (%s)", className, e.getMessage());
    }
  }

  public void register(final QueryEngine.QueryEngineFactory impl) {
    implementations.put(impl.getLanguage().toLowerCase(), impl);
  }

  public QueryEngine getInstance(final String language, DatabaseInternal database) {
    final QueryEngine.QueryEngineFactory impl = implementations.get(language.toLowerCase());
    if (impl == null)
      throw new IllegalArgumentException("Query engine '" + language + "' was not found");
    return impl.getInstance(database);
  }

  public List<String> getAvailableLanguages() {
    final List<String> available = new ArrayList<>();
    for (QueryEngine.QueryEngineFactory impl : implementations.values()) {
      available.add(impl.getLanguage());
    }
    return available;
  }
}
