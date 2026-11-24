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
package com.arcadedb.server.security;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Bucket;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.HttpSessionManager;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-type permissions of a principal follow the schema as it is now, whether or not the principal was refreshed
 * after the change: a principal built for one request (an API token) is never reached by the server-wide refresh, and a
 * schema change made inside the transaction of a request postpones that refresh to the commit. In both cases a type or a
 * bucket that appeared after the principal's permissions were computed is governed by the configured grants, not open.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PermissionsFollowSchemaChangesIT extends BaseGraphServerTest {
  private static final String ALL             = "createRecord,readRecord,updateRecord,deleteRecord";
  private static final String EDITOR_GROUP    = "schemaEditors";
  private static final String EDITOR_USER     = "schemaEditor";
  private static final String EDITOR_PASSWORD = "schemaEditorPassword1";
  private              String lastResponse;

  @Test
  void requestPrincipalSeesGrantsOfTypesCreatedAfterItWasBuilt() throws Exception {
    final String tokenName = "late-types-token";
    final String token = createToken(tokenName);
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());

      // THE PRINCIPAL OF A REQUEST IN FLIGHT: BUILT BEFORE THE TYPES BELOW EXIST
      final ServerSecurityUser principal = security.authenticateByApiToken(token);
      final ServerSecurityDatabaseUser dbUser = principal.getDatabaseUser(database);

      database.getSchema().createDocumentType("LateDocument");
      database.getSchema().createDocumentType("Granted");
      final Bucket extraBucket = database.getSchema().createBucket("late_extra");
      database.getSchema().getType("LateDocument").addBucket(extraBucket);
      assertThat(adminCommand("sql",
          "CREATE TIMESERIES TYPE LateSeries TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1"))
          .as(this::lastResponse).isEqualTo(200);

      final int lateBucketId = database.getSchema().getType("LateDocument").getBuckets(false).getFirst().getFileId();
      assertThat(dbUser.requestAccessOnFile(lateBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD)).isFalse();
      assertThat(dbUser.requestAccessOnFile(lateBucketId, SecurityDatabaseUser.ACCESS.CREATE_RECORD)).isFalse();
      assertThat(dbUser.requestAccessOnFile(extraBucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD)).isFalse();
      assertThat(dbUser.requestAccessOnType("LateDocument", SecurityDatabaseUser.ACCESS.READ_RECORD)).isFalse();
      assertThat(dbUser.requestAccessOnType("LateSeries", SecurityDatabaseUser.ACCESS.READ_RECORD)).isFalse();
      assertThat(dbUser.requestAccessOnType("LateSeries", SecurityDatabaseUser.ACCESS.CREATE_RECORD)).isFalse();

      // THE TYPE THE PRINCIPAL IS GRANTED KEEPS WORKING
      final int grantedBucketId = database.getSchema().getType("Granted").getBuckets(false).getFirst().getFileId();
      assertThat(dbUser.requestAccessOnFile(grantedBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD)).isTrue();
      assertThat(dbUser.requestAccessOnFile(grantedBucketId, SecurityDatabaseUser.ACCESS.CREATE_RECORD)).isTrue();
      assertThat(dbUser.requestAccessOnType("Granted", SecurityDatabaseUser.ACCESS.UPDATE_RECORD)).isTrue();
    } finally {
      deleteToken(tokenName);
    }
  }

  @Test
  void renamedSeriesTypeKeepsItsRestrictionWithinOneTransaction() throws Exception {
    assertThat(adminCommand("sql",
        "CREATE TIMESERIES TYPE Readings TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1"))
        .as(this::lastResponse).isEqualTo(200);
    assertThat(adminCommand("sql", "INSERT INTO Readings SET ts = 1000, id = 'a', value = 1.5"))
        .as(this::lastResponse).isEqualTo(200);

    withSchemaEditor(auth -> {
      final String sessionId = beginSession(auth);
      try {
        assertThat(sessionCommand(auth, sessionId, "ALTER TYPE Readings NAME Measures")).as(this::lastResponse).isEqualTo(200);
        assertThat(sessionCommand(auth, sessionId, "SELECT FROM Measures")).as(this::lastResponse).isEqualTo(403);
        assertThat(sessionCommand(auth, sessionId, "INSERT INTO Measures SET ts = 2000, id = 'b', value = 2.5"))
            .as(this::lastResponse).isEqualTo(403);
      } finally {
        endSession(auth, sessionId, "rollback");
      }
    });
  }

  @Test
  void bucketAttachedToRestrictedTypeIsRestrictedWithinOneTransaction() throws Exception {
    assertThat(adminCommand("sql", "CREATE DOCUMENT TYPE Restricted")).as(this::lastResponse).isEqualTo(200);
    assertThat(adminCommand("sql", "CREATE BUCKET loose_bucket")).as(this::lastResponse).isEqualTo(200);

    withSchemaEditor(auth -> {
      final String sessionId = beginSession(auth);
      try {
        assertThat(sessionCommand(auth, sessionId, "ALTER TYPE Restricted BUCKET +loose_bucket"))
            .as(this::lastResponse).isEqualTo(200);
        assertThat(sessionCommand(auth, sessionId, "INSERT INTO bucket:loose_bucket SET name = 'x'"))
            .as(this::lastResponse).isEqualTo(403);
      } finally {
        endSession(auth, sessionId, "rollback");
      }
      assertThat(countRecords("Restricted")).isZero();
    });
  }

  @Test
  void typesCreatedWithinOneTransactionFollowTheirGrants() throws Exception {
    withSchemaEditor(auth -> {
      final String sessionId = beginSession(auth);
      try {
        assertThat(sessionCommand(auth, sessionId,
            "CREATE TIMESERIES TYPE Hourly TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1"))
            .as(this::lastResponse).isEqualTo(200);
        assertThat(sessionCommand(auth, sessionId, "INSERT INTO Hourly SET ts = 1000, id = 'a', value = 1.5"))
            .as(this::lastResponse).isEqualTo(403);
      } finally {
        endSession(auth, sessionId, "rollback");
      }

      // THE TYPE THE USER IS GRANTED BY NAME CAN BE CREATED AND WRITTEN IN THE SAME TRANSACTION
      final String grantedSessionId = beginSession(auth);
      try {
        assertThat(sessionCommand(auth, grantedSessionId, "CREATE DOCUMENT TYPE Granted")).as(this::lastResponse)
            .isEqualTo(200);
        assertThat(sessionCommand(auth, grantedSessionId, "INSERT INTO Granted SET name = 'ok'")).as(this::lastResponse)
            .isEqualTo(200);
      } finally {
        endSession(auth, grantedSessionId, "commit");
      }
      assertThat(countRecords("Granted")).isEqualTo(1);
    });
  }

  @Test
  void seriesTypeCreatedInRequestFollowsDefaultGrant() throws Exception {
    final String tokenName = "series-create-token";
    final String token = createToken(tokenName);
    try {
      assertThat(tokenCommand(token, "sqlscript",
          "CREATE TIMESERIES TYPE Created TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1;"
              + " INSERT INTO Created SET ts = 1000, id = 'a', value = 1.5;")).as(this::lastResponse).isEqualTo(403);
      assertThat(countRecords("Created")).isZero();

      // A TYPE THE TOKEN IS GRANTED BY NAME CAN BE CREATED AND WRITTEN IN THE SAME REQUEST
      assertThat(tokenCommand(token, "sqlscript", "CREATE DOCUMENT TYPE Granted; INSERT INTO Granted SET name = 'ok';"))
          .as(this::lastResponse).isEqualTo(200);
      assertThat(countRecords("Granted")).isEqualTo(1);
    } finally {
      deleteToken(tokenName);
    }
  }

  private long countRecords(final String typeName) {
    return getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count(*) AS c FROM " + typeName).next()
        .<Number>getProperty("c").longValue();
  }

  /** A token granted every access to the type {@code Granted}, nothing on any other type, and schema changes. */
  private String createToken(final String name) throws Exception {
    deleteToken(name);

    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("Granted", new JSONObject().put("access", new JSONArray(ALL.split(","))))
            .put("*", new JSONObject().put("access", new JSONArray())))
        .put("database", new JSONArray().put("updateSchema"));

    final HttpURLConnection connection = open("/api/v1/server/api-tokens", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject()
        .put("name", name)
        .put("database", getDatabaseName())
        .put("expiresAt", 0)
        .put("permissions", permissions);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).as(this::lastResponse).isEqualTo(201);
      return new JSONObject(readResponse(connection)).getJSONObject("result").getString("token");
    } finally {
      connection.disconnect();
    }
  }

  private interface AuthCallback {
    void call(String auth) throws Exception;
  }

  /**
   * Runs {@code callback} as a user of a group with the same grants as {@link #createToken}: every access to
   * {@code Granted}, nothing on any other type, and schema changes.
   */
  private void withSchemaEditor(final AuthCallback callback) throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put(EDITOR_GROUP, new JSONObject()
        .put("access", new JSONArray().put("updateSchema"))
        .put("types", new JSONObject()
            .put("Granted", new JSONObject().put("access", new JSONArray(ALL.split(","))))
            .put("*", new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();
    security.createUser(new JSONObject().put("name", EDITOR_USER).put("password", security.encodePassword(EDITOR_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(EDITOR_GROUP))));
    try {
      callback.call("Basic " + Base64.getEncoder().encodeToString((EDITOR_USER + ":" + EDITOR_PASSWORD).getBytes()));
    } finally {
      security.dropUser(EDITOR_USER);
      security.getDatabaseGroupsConfiguration(getDatabaseName()).remove(EDITOR_GROUP);
      security.saveGroups();
    }
  }

  private String beginSession(final String auth) throws Exception {
    final HttpURLConnection connection = open("/api/v1/begin/" + getDatabaseName(), auth);
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(204);
      final String sessionId = connection.getHeaderField(HttpSessionManager.ARCADEDB_SESSION_ID);
      assertThat(sessionId).isNotBlank();
      return sessionId;
    } finally {
      connection.disconnect();
    }
  }

  private void endSession(final String auth, final String sessionId, final String operation) throws Exception {
    final HttpURLConnection connection = open("/api/v1/" + operation + "/" + getDatabaseName(), auth);
    connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int sessionCommand(final String auth, final String sessionId, final String command) throws Exception {
    return command(auth, sessionId, "sql", command);
  }

  private void deleteToken(final String name) {
    final ApiTokenConfiguration tokenConfig = getServer(0).getSecurity().getApiTokenConfiguration();
    tokenConfig.listTokens().stream()
        .filter(t -> name.equals(t.getString("name", "")))
        .forEach(t -> tokenConfig.deleteToken(t.getString("tokenHash")));
  }

  private int adminCommand(final String language, final String command) throws Exception {
    return command(basicAuth(), null, language, command);
  }

  private int tokenCommand(final String token, final String language, final String command) throws Exception {
    return command("Bearer " + token, null, language, command);
  }

  private int command(final String auth, final String sessionId, final String language, final String command) throws Exception {
    final HttpURLConnection connection = open("/api/v1/command/" + getDatabaseName(), auth);
    if (sessionId != null)
      connection.setRequestProperty(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(new JSONObject().put("language", language).put("command", command).toString().getBytes());
    connection.connect();
    try {
      final int code = connection.getResponseCode();
      lastResponse = code + " " + (code >= 400 ? readError(connection) : "");
      return code;
    } finally {
      connection.disconnect();
    }
  }

  /** The status and error text of the last command, for the description of a failed assertion. */
  private String lastResponse() {
    return lastResponse;
  }

  private HttpURLConnection open(final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(0, path)).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
