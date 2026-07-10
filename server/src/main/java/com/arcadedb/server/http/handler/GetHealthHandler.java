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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;

import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

/**
 * Kubernetes liveness probe. Reports whether the server process and HTTP layer are up.
 * Performs no database I/O and requires no authentication so an orchestrator can poll it
 * cheaply. Distinct from readiness: a node that is merely warming up is still live and must
 * not be killed by the orchestrator.
 */
public class GetHealthHandler extends AbstractServerHttpHandler {
  public GetHealthHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    Metrics.counter("http.health").increment();

    // Liveness only: reaching this handler proves the HTTP layer is up, so the process is live.
    // It deliberately does not consult server status, so a node still warming up is not killed.
    return new ExecutionResponse(204, "");
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
