package com.arcadedb.function.polyglot;/*
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

import com.arcadedb.database.Database;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import com.arcadedb.query.polyglot.PolyglotEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PolyglotFunctionLibraryDefinition<T extends PolyglotFunctionDefinition> implements FunctionLibraryDefinition<T> {
  protected final Database                 database;
  protected final String                   libraryName;
  protected final String                   language;
  protected final List<String>             allowedPackages;
  protected final ConcurrentMap<String, T> functions = new ConcurrentHashMap<>();
  /**
   * Serializes the rebuilds of the engine among themselves. A rebuild never waits for a call.
   */
  private final    Object                  reloadLock = new Object();
  /**
   * The engine the calls run on. Every call locks the engine it runs on, so the calls stay serialized against each other:
   * the shared GraalVM {@code Context} does not support concurrent invocation without explicit multi-threaded access
   * configuration (issues #6759, #7006). A rebuild publishes a new engine and retires this one, which is closed by the
   * last call that holds it, never under a running call.
   */
  private volatile EngineHolder            current;
  /**
   * The engine the current thread is running a call on, or declaring the functions into during a rebuild, so a nested
   * call stays on it. {@code null} outside a call.
   */
  private final    ThreadLocal<EngineHolder> running  = new ThreadLocal<>();

  public interface Callback {
    Object execute(GraalPolyglotEngine polyglotEngine);
  }

  /** An engine and the lock its calls take. */
  private static final class EngineHolder {
    private final    GraalPolyglotEngine engine;
    private final    ReentrantLock       lock = new ReentrantLock();
    private volatile boolean             retired;
    // GUARDED BY lock
    private          boolean             closed;

    private EngineHolder(final GraalPolyglotEngine engine) {
      this.engine = engine;
    }

    /**
     * Closes the engine once it is retired and no call holds it. A call still running on it closes it when it returns,
     * since it checks {@link #retired} after it released the lock.
     */
    private void closeIfRetiredAndIdle() {
      if (!retired || lock.isHeldByCurrentThread() || !lock.tryLock())
        return;
      try {
        if (!closed) {
          closed = true;
          engine.close();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  protected PolyglotFunctionLibraryDefinition(final Database database, final String libraryName, final String language, final List<String> allowedPackages) {
    this.database = database;
    this.libraryName = libraryName;
    this.language = language;
    this.allowedPackages = allowedPackages;
    this.current = new EngineHolder(newEngine());
  }

  public PolyglotFunctionLibraryDefinition registerFunction(final T function) {
    synchronized (reloadLock) {
      if (functions.putIfAbsent(function.getName(), function) != null)
        throw new AlreadyExistsException(AlreadyExistsException.Kind.FUNCTION,
            "Function '" + function.getName() + "' already defined in library '" + libraryName + "'");

      try {
        reloadEngine();
      } catch (final RuntimeException e) {
        // A broken definition (e.g. invalid JS) must not poison the whole library (issue #5121). The rebuilt engine was
        // never published, so the current one still holds every remaining (already valid) function: dropping the
        // just-added function from the map is all it takes.
        functions.remove(function.getName());
        throw e;
      }
    }
    return this;
  }

  /**
   * Builds a new polyglot engine, declares every registered function into it and publishes it. Nothing here waits for a
   * call: the functions are declared before the engine is visible to any call, and the replaced engine is closed by the
   * last call that holds it. Any invalid definition makes this method throw before the new engine is published, leaving
   * the current one in place. Called under {@link #reloadLock}.
   */
  private void reloadEngine() {
    final EngineHolder next = new EngineHolder(newEngine());
    // THE DECLARATIONS GO THROUGH execute(): THEY RUN ON THE NEW ENGINE, ALSO WHEN THIS THREAD IS INSIDE A CALL
    final EngineHolder outer = running.get();
    running.set(next);
    try {
      for (final T f : functions.values())
        f.init(this);
    } catch (final Throwable e) {
      next.engine.close();
      throw e;
    } finally {
      running.set(outer);
    }

    final EngineHolder previous = current;
    current = next;
    previous.retired = true;
    previous.closeIfRetiredAndIdle();
  }

  private GraalPolyglotEngine newEngine() {
    return GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
        .setLanguage(language).setAllowedPackages(allowedPackages).build();
  }

  @Override
  public PolyglotFunctionLibraryDefinition unregisterFunction(final String functionName) {
    functions.remove(functionName);
    return this;
  }

  @Override
  public String getName() {
    return libraryName;
  }

  @Override
  public String getLanguage() {
    return language;
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("language", language);

    final JSONObject functionsJSON = new JSONObject();
    for (final T f : new TreeMap<>(functions).values()) {
      final JSONObject fJSON = new JSONObject();
      fJSON.put("code", f.getImplementation());
      fJSON.put("parameters", new JSONArray(f.getParameters()));
      functionsJSON.put(f.getName(), fJSON);
    }
    json.put("functions", functionsJSON);
    return json;
  }

  @Override
  public Iterable<T> getFunctions() {
    return Collections.unmodifiableCollection(functions.values());
  }

  @Override
  public boolean hasFunction(final String functionName) {
    return functions.containsKey(functionName);
  }

  @Override
  public T getFunction(final String functionName) {
    final T f = functions.get(functionName);
    if (f == null)
      throw new UnknownFunctionException(functionName, "Function '" + functionName + "' not defined");
    return f;
  }

  /**
   * Runs {@code callback} on the engine of the library. The calls on one engine are serialized. A
   * {@link #registerFunction} that runs meanwhile does not wait for them: it publishes a new engine, so a call running
   * on the replaced (retired) engine can run at the same time as the registration and as the new calls, which take the
   * new engine. A call nested in a running call stays on the engine of that call.
   */
  public Object execute(final Callback callback) {
    final EngineHolder active = running.get();
    if (active != null) {
      // A CALL NESTED IN A RUNNING CALL, OR A FUNCTION BEING DECLARED BY A REBUILD: THE ENGINE OF THAT CALL OR REBUILD,
      // EVEN IF A NEWER ONE WAS PUBLISHED SINCE
      active.lock.lock();
      try {
        return run(active.engine, callback);
      } finally {
        active.lock.unlock();
      }
    }

    while (true) {
      final EngineHolder holder = current;
      holder.lock.lock();
      try {
        // A RETIRED ENGINE ONLY FINISHES THE CALLS ALREADY RUNNING ON IT: A NEW CALL TAKES THE CURRENT ONE
        if (!holder.retired) {
          running.set(holder);
          try {
            return run(holder.engine, callback);
          } finally {
            running.set(null);
          }
        }
      } finally {
        holder.lock.unlock();
        if (holder.retired)
          holder.closeIfRetiredAndIdle();
      }
    }
  }

  private static Object run(final GraalPolyglotEngine engine, final Callback callback) {
    // A function looping in the guest language never returns to the statement's checks: a terminate interrupts it
    // (issue #9689)
    return engine.runTerminable(RunningQuery.current(), "the function", () -> callback.execute(engine));
  }
}
