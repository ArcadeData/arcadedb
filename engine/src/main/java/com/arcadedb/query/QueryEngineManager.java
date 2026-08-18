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
import com.arcadedb.utility.DedicatedThreadPool;

import java.util.*;
import java.util.logging.Level;

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
public class QueryEngineManager extends DedicatedThreadPool {
  private static final QueryEngineManager                         INSTANCE        = new QueryEngineManager();
  // #4961: register() is public and may be called after construction while getEngine()/
  // getAvailableLanguages() read the map without locks: registration publishes a new copy
  // (copy-on-write) instead of mutating in place. Registration is rare, reads are hot.
  private volatile     Map<String, QueryEngine.QueryEngineFactory> implementations = new LinkedHashMap<>();

  private QueryEngineManager() {
    // Pool sizing: explicit knob first, then "as many threads as cores (min 2)". Configurable so
    // operators can cap or expand without rebuild; the previous hardcoded {@code max(2, cpuCount)}
    // is preserved as the default behaviour when the knob is left at its default of 0.
    //
    // Bound the queue so a runaway producer can't OOM the JVM, and caller-runs gives graceful degradation: when the
    // queue saturates, the submitter - which was going to block waiting for the result anyway - runs the task
    // itself, so the query loses parallelism but never fails. The construction, the counted-and-throttled
    // saturation warning and the PoolStats all come from DedicatedThreadPool (issue #6324, item 4).
    //
    // Sized through the shared queueSizeOrDefault, so a configured 0 falls back to the documented 1024 the way it
    // does on the other pools, rather than to the max(1, ...) this class used to apply - which collapsed the queue
    // to a single slot on the very setting meant to size it.
    super("ArcadeDB-QueryWorker-", autoSizeThreads(GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS.getValueAsInteger()),
        queueSizeOrDefault(GlobalConfiguration.QUERY_PARALLELISM_QUEUE_SIZE.getValueAsInteger()),
        SaturationPolicy.CALLER_RUNS, DedicatedThreadPool::plainWorker, "Query parallelism pool",
        "the query loses its parallelism but never fails",
        GlobalConfiguration.QUERY_PARALLELISM_POOL_THREADS, GlobalConfiguration.QUERY_PARALLELISM_QUEUE_SIZE);

    // REGISTER ALL THE SUPPORTED LANGUAGE FROM POLYGLOT ENGINE.
    // Guarded by POLYGLOT_ENGINE_ENABLED: when disabled we skip the iteration completely, so
    // GraalPolyglotEngine.getSupportedLanguages() is never invoked and the shared Engine - which
    // pulls in Truffle and every GraalVM language jar on the classpath - is never created.
    if (GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.getValueAsBoolean()) {
      for (final String language : PolyglotQueryEngine.PolyglotQueryEngineFactory.getSupportedLanguages())
        register(new PolyglotQueryEngine.PolyglotQueryEngineFactory(language));
    }

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

  public synchronized void register(final QueryEngine.QueryEngineFactory impl) {
    final Map<String, QueryEngine.QueryEngineFactory> copy = new LinkedHashMap<>(implementations);
    copy.put(impl.getLanguage().toLowerCase(Locale.ENGLISH), impl);
    implementations = copy;
  }

  public QueryEngine getEngine(final String language, final DatabaseInternal database) {
    final QueryEngine.QueryEngineFactory impl = implementations.get(language.toLowerCase(Locale.ENGLISH));
    if (impl == null)
      throw new IllegalArgumentException("Query engine '" + language + "' was not found. Check your configuration");
    return impl.getInstance(database);
  }

  /**
   * Point-in-time snapshot of the query-parallelism pool's load, for metrics exporters and ad-hoc operational
   * debugging. The long-standing name for {@link #getPoolStats()} on this pool, kept because it is what the metrics
   * binder and the pool's own tests call it.
   */
  public PoolStats getExecutorStats() {
    return getPoolStats();
  }

  public List<String> getAvailableLanguages() {
    return new ArrayList<>(implementations.keySet());
  }
}
