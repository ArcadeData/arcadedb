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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Base class for query implementation from Gremlin/Tinkerpop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public abstract class ArcadeQuery {
  protected final ArcadeGraph         graph;
  protected       String              query;
  protected       Map<String, Object> parameters;

  protected ArcadeQuery(final ArcadeGraph graph, final String query) {
    this.graph = graph;
    this.query = query;
  }

  public abstract ResultSet execute() throws ExecutionException, InterruptedException;

  public Map<String, Object> getParameters() {
    return Collections.unmodifiableMap(parameters);
  }

  public ArcadeQuery setParameters(final Map<String, Object> parameters) {
    if (this.parameters == null)
      this.parameters = new HashMap<>();
    this.parameters.putAll(parameters);
    return this;
  }

  public ArcadeQuery setParameters(final Object... parameters) {
    if (parameters.length % 2 != 0)
      throw new IllegalArgumentException("Command parameters must be as pairs `<key>, <value>`");

    if (this.parameters == null)
      this.parameters = new HashMap<>();

    for (int i = 0; i < parameters.length; i += 2)
      this.parameters.put((String) parameters[i], parameters[i + 1]);
    return this;
  }

  public ArcadeQuery setParameter(final String name, final Object value) {
    if (this.parameters == null)
      this.parameters = new HashMap<>();
    this.parameters.put(name, value);
    return this;
  }

  public String getQuery() {
    return query;
  }

  public ArcadeQuery setQuery(final String query) {
    this.query = query;
    return this;
  }

}
