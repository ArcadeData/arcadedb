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
package com.arcadedb.bench.store;

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The results server: database {@code perf} with the {@code Run} and {@code Measurement} types, and the two users that touch
 * it. Users get one group each on {@code perf}, defined at type level: {@code perf-writer-group} creates, reads and deletes
 * {@code Run} and {@code Measurement} records and holds no database-level right (no schema, security or settings change),
 * {@code perf-reader-group} only reads. Every other type is closed to both. {@link #init} can be run any number of times.
 */
public final class ResultsStore {
  public static final String DATABASE = "perf";
  public static final String WRITER   = "perf-writer";
  public static final String READER   = "perf-reader";

  static final String WRITER_GROUP = "perf-writer-group";
  static final String READER_GROUP = "perf-reader-group";

  private static final int TIMEOUT_MS = 60_000;

  private static final List<String> SCHEMA = List.of(
      "CREATE DOCUMENT TYPE Run IF NOT EXISTS",
      "CREATE PROPERTY Run.runId IF NOT EXISTS STRING (mandatory true, notnull true)",
      "CREATE PROPERTY Run.schemaVersion IF NOT EXISTS INTEGER",
      "CREATE PROPERTY Run.startedAt IF NOT EXISTS DATETIME",
      "CREATE PROPERTY Run.finishedAt IF NOT EXISTS DATETIME",
      "CREATE PROPERTY Run.`trigger` IF NOT EXISTS STRING",
      "CREATE PROPERTY Run.source IF NOT EXISTS STRING",
      "CREATE PROPERTY Run.arcadedb IF NOT EXISTS MAP",
      "CREATE PROPERTY Run.machine IF NOT EXISTS MAP",
      "CREATE PROPERTY Run.config IF NOT EXISTS MAP",
      "CREATE INDEX IF NOT EXISTS ON Run (runId) UNIQUE",

      "CREATE DOCUMENT TYPE Measurement IF NOT EXISTS",
      "CREATE PROPERTY Measurement.runId IF NOT EXISTS STRING (mandatory true, notnull true)",
      "CREATE PROPERTY Measurement.workload IF NOT EXISTS STRING (mandatory true, notnull true)",
      "CREATE PROPERTY Measurement.`case` IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.scale IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.metric IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.unit IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.better IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.median IF NOT EXISTS DOUBLE",
      "CREATE PROPERTY Measurement.`values` IF NOT EXISTS LIST",
      "CREATE PROPERTY Measurement.status IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.error IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.commit IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.variant IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.machineId IF NOT EXISTS STRING",
      "CREATE PROPERTY Measurement.startedAt IF NOT EXISTS DATETIME",
      "CREATE INDEX IF NOT EXISTS ON Measurement (workload, `case`, scale, metric, variant, machineId, startedAt) NOTUNIQUE",
      "CREATE INDEX IF NOT EXISTS ON Measurement (runId) NOTUNIQUE");

  private ResultsStore() {
  }

  /**
   * Creates the database, the schema, the two groups and the two users; an existing piece is left as it is, except the groups
   * (replaced) and the user passwords and grants (set to the given values).
   */
  public static void init(final String url, final String rootUser, final String rootPassword, final String writerPassword,
      final String readerPassword) {
    final URI uri = parse(url);
    final RemoteServer server = new RemoteServer(uri.getHost(), port(uri), rootUser, rootPassword);
    server.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    server.setTimeout(TIMEOUT_MS);

    if (!server.exists(DATABASE))
      server.create(DATABASE);

    try (final RemoteDatabase db = open(url, rootUser, rootPassword)) {
      for (final String statement : SCHEMA)
        db.command("sql", statement).close();
    }

    server.saveGroup(DATABASE, WRITER_GROUP, group(Set.of("createRecord", "readRecord", "deleteRecord"), Set.of("Run", "Measurement")));
    server.saveGroup(DATABASE, READER_GROUP, group(Set.of("readRecord"), Set.of("Run", "Measurement")));

    ensureUser(server, WRITER, writerPassword, WRITER_GROUP);
    ensureUser(server, READER, readerPassword, READER_GROUP);
  }

  /** Opens the {@code perf} database with the given credentials. */
  static RemoteDatabase open(final String url, final String user, final String password) {
    final URI uri = parse(url);
    final RemoteDatabase db = new RemoteDatabase(uri.getHost(), port(uri), DATABASE, user, password);
    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    db.setTimeout(TIMEOUT_MS);
    return db;
  }

  private static JSONObject group(final Set<String> access, final Set<String> types) {
    final JSONObject typesJson = new JSONObject();
    // anything not named here is closed
    typesJson.put("*", new JSONObject().put("access", new JSONArray()));
    for (final String type : types)
      typesJson.put(type, new JSONObject().put("access", new JSONArray(access)));
    return new JSONObject().put("access", new JSONArray()).put("resultSetLimit", -1).put("readTimeout", -1).put("types", typesJson);
  }

  private static void ensureUser(final RemoteServer server, final String name, final String password, final String group) {
    boolean exists = false;
    for (final JSONObject user : server.listUsers())
      if (name.equals(user.getString("name", null))) {
        exists = true;
        break;
      }
    if (exists)
      server.updateUser(name, password, Map.of(DATABASE, List.of(group)));
    else
      server.createUser(name, password, Map.of(DATABASE, group));
  }

  private static URI parse(final String url) {
    final URI uri = URI.create(url);
    if (uri.getHost() == null)
      throw new IllegalArgumentException("--url must look like http://host:2480, got '" + url + "'");
    return uri;
  }

  private static int port(final URI uri) {
    return uri.getPort() > 0 ? uri.getPort() : 2480;
  }
}
