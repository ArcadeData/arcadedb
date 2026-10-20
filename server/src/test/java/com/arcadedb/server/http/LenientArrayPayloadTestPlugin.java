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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathHandler;

/**
 * Registers /api/v1/test/array-lenient: the route of {@link ArrayPayloadTestPlugin} for a protocol that answers an unparseable body
 * with an error of its own (the MCP endpoint answers JSON-RPC -32700). Its handler opts out of the shared 400 (issue #10395) through
 * {@code refusesMalformedPayload()} and finds the raw body, which it still has, in {@code RAW_PAYLOAD}.
 * <p>
 * A plugin of its own and not a second route of {@link ArrayPayloadTestPlugin}, whose routes are listed exactly by
 * {@code Issue10296PluginRoutesRegisterApiTest}. Not auto-discovered: the route appears only for a test that names this plugin in
 * SERVER_PLUGINS.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LenientArrayPayloadTestPlugin implements ServerPlugin {

  public static class Handler extends ArrayPayloadTestPlugin.Handler {
    public Handler(final HttpServer httpServer) {
      super(httpServer);
    }

    @Override
    protected boolean refusesMalformedPayload() {
      return false;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
        final JSONObject payload) {
      final JSONObject answer = new JSONObject(super.execute(exchange, user, payload).getResponse());
      return new ExecutionResponse(200, answer.put("raw", exchange.getAttachment(RAW_PAYLOAD)).toString());
    }
  }

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    routes.addExactPath("/api/v1/test/array-lenient", new Handler(httpServer));
  }
}
