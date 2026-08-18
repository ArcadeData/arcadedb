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
package com.arcadedb.gremlin.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;

import java.util.logging.Level;

public class GremlinQueryEngineFactory implements QueryEngine.QueryEngineFactory {
  @Override
  public String getLanguage() {
    return "gremlin";
  }

  @Override
  public QueryEngine getInstance(final DatabaseInternal database) {
    Object engine = database.getWrappers().get(GremlinQueryEngine.ENGINE_NAME);
    if (engine != null)
      return (GremlinQueryEngine) engine;

    try {

      engine = new GremlinQueryEngine(ArcadeGraph.open(database));
      database.setWrapper(GremlinQueryEngine.ENGINE_NAME, engine);
      return (GremlinQueryEngine) engine;

    } catch (final Throwable e) {
      final String message = "Error on initializing Gremlin query engine" + classpathHint(e);
      LogManager.instance().log(this, Level.SEVERE, message, e);
      throw new CommandParsingException(message, e);
    }
  }

  /**
   * Names the real problem when the engine could not start because TinkerPop is not on the classpath, rather than
   * leaving it as a {@code NoClassDefFoundError} buried under a generic message (issue #6359, item 3).
   * <p>
   * The way to reach this is not exotic: {@code arcadedb-gremlin}'s plain jar carries no TinkerPop of its own, and a
   * build that consumes it in place of the {@code shaded} uber-jar - which is what Maven substitutes for a
   * classified dependency whose module is in the same reactor but has not reached the {@code package} phase - gets
   * ArcadeDB's Gremlin classes with nothing behind them.
   */
  static String classpathHint(final Throwable e) {
    for (Throwable cause = e; cause != null; cause = cause.getCause())
      if (cause instanceof NoClassDefFoundError || cause instanceof ClassNotFoundException) {
        final String missing = cause.getMessage();
        if (missing != null && missing.contains("tinkerpop"))
          return ": the Apache TinkerPop libraries are not on the classpath (missing '" + missing.replace('/', '.')
              + "'). Use the 'shaded' arcadedb-gremlin artifact, or add the TinkerPop dependencies alongside the plain one";
      }
    return "";
  }
}
