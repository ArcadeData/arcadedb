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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-type ACLs are keyed by type name (the group's {@code types} object) and per-bucket ones by the type that owns the
 * bucket. A schema change that moves records from under one of those keys to another must not change who can read them: a
 * user holding {@code updateSchema} but denied a type must not be able to rename the type, or detach its bucket, to reach
 * its records. A change that keeps every group's access identical stays allowed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SchemaChangePerTypeAclIT extends BaseGraphServerTest {
  private static final String SCOPED_USER   = "schema-scoped-user";
  private static final String SCOPED_PWD    = "schemascoped1";
  private static final String GROUP         = "schemaScoped";
  private static final String SECRET_TS     = "SecretMetrics";
  private static final String SECRET_DOC    = "SecretDoc";
  private static final String SECRET_BUCKET = "secretbucket";
  private static final String PUBLIC_DOC    = "PublicDoc";

  private String lastResponseBody = "";

  @AfterEach
  void dropScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
  }

  @Test
  void renamingADeniedTimeSeriesTypeIsRefused() throws Exception {
    command(0, "CREATE TIMESERIES TYPE " + SECRET_TS + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)");
    command(0, "INSERT INTO " + SECRET_TS + " SET ts = 1000, host = 'a', value = 42.0");
    createScopedUser();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    assertThat(sql(auth, "query", "SELECT FROM " + SECRET_TS)).isEqualTo(403);

    assertThat(sql(auth, "command", "ALTER TYPE " + SECRET_TS + " NAME OpenMetrics"))
        .as("a rename that would move the type out of its ACL entry must be refused: %s", lastResponseBody).isNotEqualTo(200);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType(SECRET_TS)).isTrue();
    assertThat(sql(auth, "query", "SELECT FROM " + SECRET_TS)).isEqualTo(403);
    assertThat(sql(auth, "command", "DELETE FROM " + SECRET_TS + " WHERE ts < 1500")).isEqualTo(403);
  }

  @Test
  void renamingADeniedDocumentTypeIsRefusedEvenAfterARefresh() throws Exception {
    createSecretDocument();
    createScopedUser();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    assertThat(sql(auth, "command", "ALTER TYPE " + SECRET_DOC + " NAME OpenDoc"))
        .as("a rename that would move the type out of its ACL entry must be refused: %s", lastResponseBody).isNotEqualTo(200);

    // ANY LATER SCHEMA CHANGE REBUILDS THE PER-BUCKET MAP FROM THE TYPE NAMES
    command(0, "CREATE DOCUMENT TYPE Unrelated");
    assertThat(sql(auth, "query", "SELECT FROM " + SECRET_DOC)).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM OpenDoc")).isNotEqualTo(200);
  }

  @Test
  void detachingTheBucketOfADeniedTypeIsRefused() throws Exception {
    createSecretDocument();
    command(0, "CREATE DOCUMENT TYPE " + PUBLIC_DOC);
    createScopedUser();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    assertThat(sql(auth, "command", "ALTER TYPE " + SECRET_DOC + " BUCKET -" + SECRET_BUCKET))
        .as("detaching a bucket that holds records of a denied type must be refused: %s", lastResponseBody).isNotEqualTo(200);
    command(0, "CREATE DOCUMENT TYPE Unrelated");
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + SECRET_BUCKET)).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM " + SECRET_DOC)).isEqualTo(403);

    // AN EMPTY BUCKET OPENS NO RECORD
    command(0, "DELETE FROM " + SECRET_DOC);
    assertThat(sql(auth, "command", "ALTER TYPE " + SECRET_DOC + " BUCKET -" + SECRET_BUCKET)).as(lastResponseBody)
        .isEqualTo(200);
  }

  @Test
  void droppingADeniedTypeIsNotRefused() throws Exception {
    createSecretDocument();
    createScopedUser();

    // THE DROP DETACHES THE BUCKETS TO DROP THEM, RECORDS AND ALL: IT OPENS NOTHING
    command(0, "DROP TYPE " + SECRET_DOC);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType(SECRET_DOC)).isFalse();
  }

  @Test
  void aRenameThatKeepsTheAccessIsAllowed() throws Exception {
    createSecretDocument();
    command(0, "CREATE DOCUMENT TYPE " + PUBLIC_DOC);
    createScopedUser();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    // RESOLVED BY THE "*" ENTRY UNDER BOTH NAMES: NOTHING CHANGES FOR ANY GROUP
    assertThat(sql(auth, "command", "ALTER TYPE " + PUBLIC_DOC + " NAME OtherPublicDoc")).as(lastResponseBody).isEqualTo(200);

    // THE ADMINISTRATOR'S WAY TO RENAME A RESTRICTED TYPE: GIVE THE NEW NAME THE SAME ACCESS FIRST
    final ServerSecurity security = getServer(0).getSecurity();
    final JSONObject group = security.getDatabaseGroupsConfiguration(getDatabaseName()).getJSONObject(GROUP);
    group.getJSONObject("types").put("RenamedSecretDoc", new JSONObject().put("access", new JSONArray()));
    security.saveGroups();
    command(0, "ALTER TYPE " + SECRET_DOC + " NAME RenamedSecretDoc");
    assertThat(sql(auth, "query", "SELECT FROM RenamedSecretDoc")).isEqualTo(403);
  }

  private void createSecretDocument() throws Exception {
    command(0, "CREATE BUCKET " + SECRET_BUCKET);
    command(0, "CREATE DOCUMENT TYPE " + SECRET_DOC + " BUCKET " + SECRET_BUCKET);
    command(0, "INSERT INTO " + SECRET_DOC + " SET secret = 'topsecret'");
  }

  private void createScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put(GROUP,
        new JSONObject().put("access", new JSONArray().put("updateSchema"))
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord").put("deleteRecord")))
                .put(SECRET_TS, new JSONObject().put("access", new JSONArray()))
                .put(SECRET_DOC, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    security.createUser(new JSONObject().put("name", SCOPED_USER).put("password", security.encodePassword(SCOPED_PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));
  }

  private int sql(final String auth, final String endpoint, final String sql) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/" + endpoint + "/" + getDatabaseName()).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream()
        .write(new JSONObject().put("language", "sql").put("command", sql).toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      final int status = connection.getResponseCode();
      final var stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      lastResponseBody = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return status;
    } finally {
      connection.disconnect();
    }
  }

  private static String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }
}
