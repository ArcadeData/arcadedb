/*
 * Copyright 2023 Arcade Data Ltd
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
package com.arcadedb.gremlin.query;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.gremlin.ArcadeGraph;

import java.util.logging.*;

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
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Gremlin query engine", e);
      throw new CommandParsingException("Error on initializing Gremlin query engine", e);
    }
  }
}
