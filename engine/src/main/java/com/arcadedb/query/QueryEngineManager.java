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
import com.arcadedb.query.sql.SQLScriptQueryEngine;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class QueryEngineManager {
  private static final QueryEngineManager                         INSTANCE        = new QueryEngineManager();
  private final        Map<String, QueryEngine.QueryEngineFactory> implementations = new HashMap<>();
  private final        ExecutorService                             executorService;

  private QueryEngineManager() {
    final int maxThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    executorService = new ThreadPoolExecutor(0, maxThreads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), r -> {
      final Thread t = new Thread(r, "ArcadeDB-QueryWorker");
      t.setDaemon(true);
      return t;
    });

    // REGISTER ALL THE SUPPORTED LANGUAGE FROM POLYGLOT ENGINE
    for (final String language : PolyglotQueryEngine.PolyglotQueryEngineFactory.getSupportedLanguages())
      register(new PolyglotQueryEngine.PolyglotQueryEngineFactory(language));

    register(new JavaQueryEngine.JavaQueryEngineFactory());
    register(new SQLQueryEngine.SQLQueryEngineFactory());
    register(new SQLScriptQueryEngine.SQLScriptQueryEngineFactory());

    // REGISTER QUERY ENGINES IF AVAILABLE ON CLASSPATH AT RUN-TIME
    register("com.arcadedb.query.opencypher.query.OpenCypherQueryEngineFactory");
    register("com.arcadedb.mongo.query.MongoQueryEngineFactory");
    register("com.arcadedb.graphql.query.GraphQLQueryEngineFactory");
    register("com.arcadedb.redis.query.RedisQueryEngineFactory");

    // REGISTER OPENCYPHER AS DEFAULT "cypher" ENGINE, SO CYPHER WORKS EVEN WITHOUT GREMLIN MODULE
    final QueryEngine.QueryEngineFactory openCypherFactory = implementations.get("opencypher");
    if (openCypherFactory != null)
      implementations.put("cypher", openCypherFactory);

    // REGISTER GREMLIN AND ITS CYPHER ENGINE (OVERRIDES "cypher" WITH GREMLIN-BASED IMPLEMENTATION IF AVAILABLE)
    register("com.arcadedb.gremlin.query.GremlinQueryEngineFactory");
    register("com.arcadedb.cypher.query.CypherQueryEngineFactory");
  }

  public static QueryEngineManager getInstance() {
    return INSTANCE;
  }

  public void register(final String className) {
    try {

      register((QueryEngine.QueryEngineFactory) Class.forName(className).getConstructor().newInstance());
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unable to register engine '%s' (%s)", className, e.getMessage());
    }
  }

  public void register(final QueryEngine.QueryEngineFactory impl) {
    implementations.put(impl.getLanguage().toLowerCase(Locale.ENGLISH), impl);
  }

  public QueryEngine getEngine(final String language, final DatabaseInternal database) {
    final QueryEngine.QueryEngineFactory impl = implementations.get(language.toLowerCase(Locale.ENGLISH));
    if (impl == null)
      throw new IllegalArgumentException("Query engine '" + language + "' was not found. Check your configuration");
    return impl.getInstance(database);
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public void close() {
    executorService.shutdownNow();
  }

  public List<String> getAvailableLanguages() {
    final List<String> available = new ArrayList<>();
    for (final QueryEngine.QueryEngineFactory impl : implementations.values()) {
      available.add(impl.getLanguage());
    }
    return available;
  }
}
