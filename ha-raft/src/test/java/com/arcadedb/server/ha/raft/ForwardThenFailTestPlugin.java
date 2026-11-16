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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.PluginRoutes;
import com.arcadedb.server.http.handler.DatabaseAbstractHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * Registers {@code POST /api/v1/test/forward-then-fail?database=name}: a request that runs the SQL {@code command} of its body,
 * which a follower forwards to the leader, and then fails (issue #11740). It stands for any request that forwards a write and
 * leaves through an exception afterwards, which no production route does on demand.
 * <p>
 * Not auto-discovered: the route appears only for a test that names this plugin in SERVER_PLUGINS.
 */
public class ForwardThenFailTestPlugin implements ServerPlugin {
  public static final String PATH = "/api/v1/test/forward-then-fail";

  public static class Handler extends DatabaseAbstractHandler {
    public Handler(final HttpServer httpServer) {
      super(httpServer);
    }

    /** The body carries the command: it is read only on a worker thread. */
    @Override
    protected boolean mustExecuteOnWorkerThread() {
      return true;
    }

    /**
     * Outside a transaction, as {@code /query} runs: inside the one {@code requiresTransaction()} opens, a follower runs the
     * write locally in that transaction (issue #10040) instead of forwarding it, and the failure rolls it back.
     */
    @Override
    protected boolean requiresTransaction() {
      return false;
    }

    @Override
    protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
        final JSONObject payload) {
      database.command("sql", payload.getString("command")).close();
      throw new IllegalStateException("failed after the write was forwarded (issue #11740 test route)");
    }
  }

  @Override
  public void startService() {
    // NO-OP
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PluginRoutes routes) {
    routes.post(PATH, new Handler(httpServer));
  }
}
