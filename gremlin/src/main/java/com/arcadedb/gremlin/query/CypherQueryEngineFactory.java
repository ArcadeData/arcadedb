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
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;

import java.util.logging.*;

public class CypherQueryEngineFactory implements QueryEngine.QueryEngineFactory {
  @Override
  public String getLanguage() {
    return "cypher";
  }

  @Override
  public QueryEngine getInstance(final DatabaseInternal database) {
    Object engine = database.getWrappers().get(CypherQueryEngine.ENGINE_NAME);
    if (engine != null)
      return (CypherQueryEngine) engine;

    try {
      engine = new CypherQueryEngine(ArcadeGraph.open(database));
      database.setWrapper(CypherQueryEngine.ENGINE_NAME, engine);
      return (CypherQueryEngine) engine;

    } catch (Throwable e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on initializing Cypher query engine", e);
      throw new QueryParsingException("Error on initializing Cypher query engine", e);
    }
  }
}
