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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

/**
 * Shared plumbing of the three {@code /api/v1/vector/{database}/*} routes.
 * <p>
 * The routes exist because vector retrieval had no structured wire surface at all before issue #7306: the only
 * way to reach it over HTTP was to hand-write the {@code vector.neighbors} SQL, and the only bounded, validated
 * surface was MCP. Each handler is therefore deliberately thin - it resolves nothing and validates nothing of
 * its own. Everything is delegated to {@code com.arcadedb.server.vector}, which the MCP tools and the gRPC
 * vector RPCs call as well, so a request that one surface accepts every surface accepts, and a request one
 * rejects every surface rejects with the same message.
 * <p>
 * {@link #requiresTransaction()} is false: these are reads, and an auto-commit wrapper around a read would only
 * add a transaction to roll back.
 */
public abstract class AbstractVectorSearchHandler extends DatabaseAbstractHandler {
  protected AbstractVectorSearchHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * Runs the search. Implementations delegate straight to the matching service in
   * {@code com.arcadedb.server.vector}; an {@link IllegalArgumentException} raised there is mapped to HTTP 400
   * by {@link AbstractServerHttpHandler}'s error mapping, which is what makes the argument bounds observable
   * to a client as a client error rather than a 500.
   */
  protected abstract JSONObject search(Database database, JSONObject payload);

  /**
   * Name of the Micrometer counter this route increments, so the three searches are separable in the metrics
   * the way {@code http.query} and {@code http.command} already are.
   */
  protected abstract String metricName();

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database database, final JSONObject payload) {
    if (payload == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Request body is required\"}");

    Metrics.counter(metricName()).increment();
    return new ExecutionResponse(200, search(database, payload).toString());
  }
}
