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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HTTP handler for listing PromQL label names.
 * Endpoint: GET /api/v1/ts/{database}/prom/api/v1/labels
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} reads through that session's
 * transaction, under its lock and on its principal and refreshing its idle timer, and the base class subsumes
 * the {@code checkAuthorizationOnDatabase} call this handler used to make by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetPromQLLabelsHandler extends DatabaseAbstractHandler {

  public GetPromQLLabelsHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A read: an auto-commit wrapper would only add a commit with nothing to commit, so an unresolvable session
   * id degrades to a session-less read rather than being refused - see
   * {@link DatabaseAbstractHandler#rejectsUnresolvableSession()}.
   */
  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * A session-less read is answered on the IO thread, which is what this handler has always done. A request
   * that names a session is not: see {@link DatabaseAbstractHandler#carriesSessionId}.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread(final HttpServerExchange exchange) {
    return carriesSessionId(exchange);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final DatabaseInternal database = (DatabaseInternal) db;
    final Set<String> labelNames = new LinkedHashSet<>();
    labelNames.add("__name__");

    for (final DocumentType type : database.getSchema().getTypes()) {
      if (!(type instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
        continue;
      // Label discovery must not expose the tag names of a type the caller cannot read.
      if (!SecurityHelper.canAccessType(database, tsType, SecurityDatabaseUser.ACCESS.READ_RECORD))
        continue;
      for (final ColumnDefinition col : tsType.getTsColumns())
        if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
          labelNames.add(col.getName());
    }

    final List<String> sorted = new ArrayList<>(labelNames);
    Collections.sort(sorted);
    return new ExecutionResponse(200, PromQLResponseFormatter.formatLabelsResponse(sorted));
  }
}
