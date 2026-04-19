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
package com.arcadedb.server.mcp.tools;

import java.util.Set;
import java.util.TreeSet;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.security.ServerSecurityUser;

public class MCPToolUtils {

  private MCPToolUtils() {
  }

  /**
   * Resolves a database by name, throwing an {@link IllegalArgumentException} with the list of databases
   * accessible to the user when the requested database does not exist — so the LLM can self-correct
   * without a separate list_databases round-trip.
   */
  public static ServerDatabase resolveDatabase(final ArcadeDBServer server, final ServerSecurityUser user,
      final String databaseName) {
    if (!server.existsDatabase(databaseName)) {
      final Set<String> installed = new TreeSet<>(server.getDatabaseNames());
      final Set<String> allowed = user.getAuthorizedDatabases();
      if (!allowed.contains("*"))
        installed.retainAll(allowed);
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not exist. Available databases: " + installed
              + ". Use one of these names or call list_databases to refresh the list.");
    }
    return server.getDatabase(databaseName);
  }
}
