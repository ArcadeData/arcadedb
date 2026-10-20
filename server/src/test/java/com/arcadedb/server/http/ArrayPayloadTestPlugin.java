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
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;

/**
 * Registers /api/v1/test/array, the only route in the server module that opts in to a top-level JSON array
 * body. It exists so the issue #5415 pipeline contract - array bodies parsed once and delivered through
 * getPayloadAsArray, object bodies unaffected, every other route answering 400 - is pinned against the
 * mechanism itself rather than against whichever production handler happens to accept arrays today.
 * <p>
 * Not auto-discovered: the route appears only for a test that names this plugin in SERVER_PLUGINS.
 */
public class ArrayPayloadTestPlugin implements ServerPlugin {

  public static class Handler extends AbstractServerHttpHandler {
    public Handler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected boolean mustExecuteOnWorkerThread() {
      return true;
    }

    @Override
    protected boolean acceptsArrayPayload() {
      return true;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      final JSONArray array = getPayloadAsArray(exchange);
      if (array != null)
        return new ExecutionResponse(200, new JSONObject().put("shape", "array").put("size", array.length()).toString());
      return new ExecutionResponse(200, new JSONObject().put("shape", "object")
          .put("id", payload == null ? -1 : payload.getInt("id", -1)).toString());
    }
  }

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    routes.addExactPath("/api/v1/test/array", new Handler(httpServer));
  }
}
