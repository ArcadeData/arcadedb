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
 */

import com.arcadedb.database.Database;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.polyglot.GraalPolyglotEngine;
import org.graalvm.polyglot.Engine;

import java.util.*;
import java.util.concurrent.*;

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
    this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, Engine.create()).setLanguage(language).setAllowedPackages(allowedPackages).build();
  }

  public PolyglotFunctionLibraryDefinition registerFunction(T function) {
    if (functions.putIfAbsent(function.getName(), function) != null)
      throw new IllegalArgumentException("Function '" + function.getName() + "' already defined in library '" + libraryName + "'");

    // REGISTER ALL THE FUNCTIONS UNDER THE NEW ENGINE INSTANCE
    this.polyglotEngine.close();
    this.polyglotEngine = GraalPolyglotEngine.newBuilder(database, Engine.create()).setLanguage(language).setAllowedPackages(allowedPackages).build();
    for (T f : functions.values())
      f.init(this);

    return this;
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
  public Iterable<T> getFunctions() {
    return Collections.unmodifiableCollection(functions.values());
  }

  @Override
  public T getFunction(final String functionName) {
    return functions.get(functionName);
  }

  public Object execute(final Callback callback) {
    synchronized (polyglotEngine) {
      return callback.execute(polyglotEngine);
    }
  }
}
