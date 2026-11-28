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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;

import java.util.Map;

/**
 * HTTP clients of the server under test, shared by the workloads.
 */
public final class RemoteClients {
  private static final int TIMEOUT_MS = 30 * 60 * 1000;

  private RemoteClients() {
  }

  /** Creates the database when missing. */
  public static void ensureDatabase(final ServerUnderTest server, final String database) {
    final RemoteServer remote = remoteServer(server);
    if (!remote.exists(database))
      remote.create(database);
  }

  /** Drops the database when it exists and creates it empty. */
  public static void recreateDatabase(final ServerUnderTest server, final String database) {
    final RemoteServer remote = remoteServer(server);
    if (remote.exists(database))
      remote.drop(database);
    remote.create(database);
  }

  public static boolean databaseExists(final ServerUnderTest server, final String database) {
    return remoteServer(server).exists(database);
  }

  private static RemoteServer remoteServer(final ServerUnderTest server) {
    final RemoteServer remote = new RemoteServer(server.wrapper().host(), server.wrapper().httpPort(), "root",
        ServerUnderTest.PASSWORD);
    remote.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    remote.setTimeout(TIMEOUT_MS);
    return remote;
  }

  public static RemoteDatabase open(final ServerUnderTest server, final String database) {
    final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), database, "root",
        ServerUnderTest.PASSWORD);
    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    db.setTimeout(TIMEOUT_MS);
    return db;
  }

  /** The {@code n} of a {@code SELECT count(*) AS n} query; an answer with no row is a server fault. */
  public static long count(final RemoteDatabase db, final String language, final String query) {
    return count(db, language, query, Map.of());
  }

  /** As {@link #count(RemoteDatabase, String, String)}, with parameters. */
  public static long count(final RemoteDatabase db, final String language, final String query, final Map<String, Object> params) {
    try (final ResultSet rs = db.query(language, query, params)) {
      if (!rs.hasNext())
        throw new IllegalStateException("the server answered no row to: " + query);
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
