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
import com.arcadedb.query.search.HybridSearch;

/**
 * Fused vector + full-text + graph-expansion retrieval.
 * <p>
 * Endpoint: {@code POST /api/v1/vector/{database}/hybrid}. The request body, the argument bounds and the
 * response shape are {@link HybridSearch}'s, shared with the MCP tool and the gRPC RPC of the same name.
 */
public class PostVectorHybridSearchHandler extends AbstractVectorSearchHandler {
  public PostVectorHybridSearchHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected JSONObject search(final Database database, final JSONObject payload) {
    return HybridSearch.search(database, payload);
  }

  @Override
  protected String metricName() {
    return "http.vector.hybrid";
  }
}
