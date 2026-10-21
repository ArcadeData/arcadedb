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
import java.util.function.Supplier;
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
  // #10245: where the GraalVM languages come from, and whether they have been asked for yet. Listing them builds the
  // shared polyglot Engine, which loads Truffle and every GraalVM language jar on the classpath, so it is deferred
  // until a caller asks for a language no built-in engine answers, or for the complete list.
  private final        Supplier<Iterable<String>>                  polyglotLanguages;
  private volatile     boolean                                     polyglotLanguagesResolved;
  // Serializes the one resolution, apart from the registry monitor: building the Engine takes long, and register() and the
  // other resolvers must not wait behind it on the monitor of the registry itself
  private final        Object                                      polyglotResolution = new Object();

  private QueryEngineManager() {
    this(PolyglotQueryEngine.PolyglotQueryEngineFactory::getSupportedLanguages);
  }

  /**
   * Visible for tests: {@code polyglotLanguages} replaces the enumeration of the GraalVM languages, so a test can see
   * when (and whether) the registry asks for them without building the JVM-wide shared Engine.
   */
  QueryEngineManager(final Supplier<Iterable<String>> polyglotLanguages) {
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
    this.polyglotLanguages = polyglotLanguages;

    // THE POLYGLOT LANGUAGES ARE NOT REGISTERED HERE BUT ON FIRST DEMAND, SEE resolvePolyglotLanguages() (#10245)
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
    final String key = language.toLowerCase(Locale.ENGLISH);
    QueryEngine.QueryEngineFactory impl = implementations.get(key);
    if (impl == null && resolvePolyglotLanguages())
      impl = implementations.get(key);
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
    resolvePolyglotLanguages();
    return new ArrayList<>(implementations.keySet());
  }

  /**
   * Whether {@code language} names an engine registered on this JVM, matched the way
   * {@link #getEngine(String, DatabaseInternal)} matches it (case-insensitively). Unlike
   * {@link #getAvailableLanguages()} this answers the question without copying the key set, so it is usable
   * from instrumentation that runs once per query: one volatile read of the copy-on-write map plus one hash
   * lookup, no allocation.
   * <p>
   * Exists so the {@code language} tag of {@code arcadedb.query.duration} can be bounded at its source. The
   * language travels straight from the caller - {@code db.query("<language>", ...)}, the {@code {lang}} path
   * segment of the HTTP API - so a metric that echoed it verbatim registered one permanent meter per invented
   * name; the engine registry is the component that knows which values are real (issue #7122).
   */
  public boolean isLanguageRegistered(final String language) {
    if (language == null)
      return false;
    final String key = language.toLowerCase(Locale.ENGLISH);
    return implementations.containsKey(key) || (resolvePolyglotLanguages() && implementations.containsKey(key));
  }

  /**
   * Registers the GraalVM polyglot languages the first time a caller needs them: a language no built-in engine
   * answers, or the complete list. They used to be registered by the constructor, so the first statement of ANY
   * language in a JVM - an embedded {@code SELECT} included - built the shared polyglot Engine and loaded Truffle
   * with every GraalVM language jar, about a third of that statement (#10245).
   * <p>
   * Guarded by {@link GlobalConfiguration#POLYGLOT_ENGINE_ENABLED}, read at resolution time: while it is off the Engine
   * is never created and nothing is marked resolved, so enabling it later still makes the languages appear. A
   * polyglot language never displaces an engine registered under the same name, as it never did when the
   * constructor registered the polyglot languages first and the built-in ones over them.
   *
   * @return true if the polyglot languages are registered, by this call or an earlier one, so a lookup that missed is
   * worth repeating: a caller that missed while another thread was registering them must not conclude the language
   * does not exist
   */
  private boolean resolvePolyglotLanguages() {
    if (polyglotLanguagesResolved)
      return true;
    if (!GlobalConfiguration.POLYGLOT_ENGINE_ENABLED.getValueAsBoolean())
      return false;

    synchronized (polyglotResolution) {
      if (!polyglotLanguagesResolved) {
        Iterable<String> languages;
        try {
          languages = polyglotLanguages.get();
        } catch (final RuntimeException e) {
          // resolved as none, once: a lookup of an unknown name must not retry the enumeration every time
          LogManager.instance().log(this, Level.WARNING, "Cannot list the polyglot languages: %s", e, e.getMessage());
          languages = List.of();
        }
        registerAbsent(languages);
        polyglotLanguagesResolved = true;
      }
      return true;
    }
  }

  private synchronized void registerAbsent(final Iterable<String> languages) {
    final Map<String, QueryEngine.QueryEngineFactory> copy = new LinkedHashMap<>(implementations);
    for (final String language : languages)
      copy.putIfAbsent(language.toLowerCase(Locale.ENGLISH), new PolyglotQueryEngine.PolyglotQueryEngineFactory(language));
    implementations = copy;
  }
}
