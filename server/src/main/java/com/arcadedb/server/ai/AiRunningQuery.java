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
package com.arcadedb.server.ai;

import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.function.Supplier;

/**
 * The work the AI Assistant does in a database for a user, run like the work of every other client request: listed by
 * "list queries" and stopped by a terminate for as long as it runs, and, when it runs a statement, started only once the query
 * admission gate has a slot for it. Two separate decisions, as for the MCP tools: a read of the schema is listed but never
 * waits for the gate, so it stays answerable on a server busy with queries.
 * <p>
 * Taken per call, never for a whole chat request: the request holds its worker thread for minutes while the model answers, and a
 * slot held that long would starve the gate.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class AiRunningQuery {
  /** How the listing names the work of the AI Assistant. */
  static final String PROTOCOL = "ai";

  private AiRunningQuery() {
  }

  /** The client of a request, as every HTTP route lists it: no connection id, the address of the peer. */
  static RunningQuery.Client clientOf(final HttpServerExchange exchange) {
    return RunningQuery.Client.of(null, exchange.getSourceAddress());
  }

  /**
   * Runs {@code work} on the calling thread inside its own registry entry, which the statements it runs find through
   * {@link RunningQuery#current()}.
   *
   * @param admitted  whether the work waits for a slot of the query admission gate (it runs a statement) or not (it reads the schema)
   * @param language  what the entry lists as the language of the work
   * @param text      what the entry lists as the work: the statement, or the name of the tool
   *
   * @throws QueryAdmissionException when {@code admitted} and the gate has no slot for the work in time
   */
  static <T> T run(final ArcadeDBServer server, final ServerSecurityUser user, final RunningQuery.Client client,
      final String databaseName, final boolean admitted, final String language, final String text, final Supplier<T> work) {
    // Admitted first, so work waiting for its slot is not listed as running
    final QueryAdmissionGate.Ticket admission = admitted ? QueryAdmissionGate.getInstance().admit() : null;
    RunningQuery entry = null;
    try {
      entry = server.getRunningQueries().register(databaseName, user.getName(), PROTOCOL, null, null, client);
      entry.setStatement(language, text);
      return work.get();
    } finally {
      try {
        if (admission != null)
          admission.close();
      } finally {
        // Last: the entry leaving the list is the proof the work is over
        if (entry != null)
          entry.close();
      }
    }
  }
}
