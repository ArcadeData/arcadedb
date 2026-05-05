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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.java.JavaQueryEngine;
import com.arcadedb.query.polyglot.PolyglotQueryEngine;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.SQLScriptQueryEngine;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

/**
 * JVM-wide registry of query-language engines plus the default executor any query-time
 * parallelism should fork to.
 * <p>
 * <b>"No JDK common ForkJoinPool" rule.</b> Engine and server code MUST NOT submit work to the
 * common pool (no {@code parallelStream()}, no {@code Arrays.parallelSort} on hot paths, no
 * {@code CompletableFuture.supplyAsync} / {@code runAsync} without an explicit executor). The
 * common pool is shared with user-supplied scripts (Gremlin, Polyglot, SQL functions) and has
 * no back-pressure - long-running ArcadeDB work there can starve user code, the JDK reference
 * handler, and parallel GC. Fork to this manager's {@link #getExecutorService()} (sized via
 * {@link GlobalConfiguration#QUERY_PARALLELISM_POOL_THREADS}) for general query parallelism, or
 * to a feature-specific dedicated pool for hot, fine-grained workloads (e.g. sparse-vector
 * scoring). Two pre-existing common-pool callers are tracked for migration:
 * <ul>
 *   <li>{@code GraphBatch} bulk-load uses {@code Arrays.parallelSort} on the vertex-key array.</li>
 *   <li>{@code ArcadeStateMachine.notifyInstallSnapshotFromLeader} forks the snapshot download via
 *       {@code CompletableFuture.supplyAsync} (no explicit executor).</li>
 * </ul>
 * Both run during operational events (bulk import, HA snapshot install) rather than the per-query
 * hot path; migrating them off the common pool is queued as follow-up work.
 */
public class QueryEngineManager {
  private static final QueryEngineManager                         INSTANCE        = new QueryEngineManager();
  private final        Map<String, QueryEngine.QueryEngineFactory> implementations = new HashMap<>();
  private final        ExecutorService                             executorService;

  private QueryEngineManager() {
    // Pool sizing: explicit knob first, then "as many threads as cores (min 2)". Configurable so
    // operators can cap or expand without rebuild; the previous hardcoded {@code max(2, cpuCount)}
    // is preserved as the default behaviour when the knob is left at its default of 0.
    final int configured = GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS.getValueAsInteger();
    final int maxThreads = configured > 0 ? configured : Math.max(2, Runtime.getRuntime().availableProcessors());
    final AtomicInteger workerSeq = new AtomicInteger();
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(maxThreads, maxThreads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), r -> {
      final Thread t = new Thread(r, "ArcadeDB-QueryWorker-" + workerSeq.incrementAndGet());
      t.setDaemon(true);
      return t;
    });
    pool.allowCoreThreadTimeOut(true);
    executorService = pool;

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
