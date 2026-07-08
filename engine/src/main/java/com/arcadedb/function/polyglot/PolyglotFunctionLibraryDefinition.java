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
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import com.arcadedb.query.polyglot.PolyglotEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class PolyglotFunctionLibraryDefinition<T extends PolyglotFunctionDefinition> implements FunctionLibraryDefinition<T> {
  protected final Database                 database;
  protected final String                   libraryName;
  protected final String                   language;
  protected final List<String>             allowedPackages;
  protected final ConcurrentMap<String, T> functions = new ConcurrentHashMap<>();
  protected       GraalPolyglotEngine      polyglotEngine;

  public interface Callback {
    Object execute(GraalPolyglotEngine polyglotEngine);
  }

  protected PolyglotFunctionLibraryDefinition(final Database database, final String libraryName, final String language, final List<String> allowedPackages) {
    this.database = database;
    this.libraryName = libraryName;
    this.language = language;
    this.allowedPackages = allowedPackages;
    this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
        .setLanguage(language).setAllowedPackages(allowedPackages).build();
  }

  public PolyglotFunctionLibraryDefinition registerFunction(final T function) {
    if (functions.putIfAbsent(function.getName(), function) != null)
      throw new IllegalArgumentException("Function '" + function.getName() + "' already defined in library '" + libraryName + "'");

    try {
      reloadEngine();
    } catch (final RuntimeException e) {
      // A broken definition (e.g. invalid JS) must not poison the whole library (issue #5121). Roll back the
      // just-added function and rebuild the engine with the remaining (already valid) functions so they keep working
      // and no map entry is ever left with a null library back-reference.
      functions.remove(function.getName());
      try {
        reloadEngine();
      } catch (final RuntimeException ignore) {
        // best effort: the remaining functions were already valid, so this rebuild is expected to succeed
      }
      throw e;
    }

    return this;
  }

  /**
   * Rebuilds the polyglot engine and (re)declares every registered function into it. Any invalid definition makes this
   * method throw, leaving the caller to decide how to recover.
   */
  private void reloadEngine() {
    // REGISTER ALL THE FUNCTIONS UNDER THE NEW ENGINE INSTANCE
    this.polyglotEngine.close();
    this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, PolyglotEngineManager.getInstance().getSharedEngine())
        .setLanguage(language).setAllowedPackages(allowedPackages).build();
    for (final T f : functions.values())
      f.init(this);
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
    for (final T f : functions.values()) {
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
      throw new IllegalArgumentException("Function '" + functionName + "' not defined");
    return f;
  }

  public Object execute(final Callback callback) {
    synchronized (polyglotEngine) {
      return callback.execute(polyglotEngine);
    }
  }
}
