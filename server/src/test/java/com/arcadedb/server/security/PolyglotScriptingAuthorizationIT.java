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
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-48qw-824m-86pr: a user with only read-level privileges on a database could execute arbitrary
 * scripts through the {@code language: "js"} option of {@code /api/v1/command/{database}}, escaping the database scope and
 * reading host files. Polyglot script execution must now require database-administrator privileges, so a non-admin user is
 * rejected with HTTP 403 while still being able to run ordinary read-only SQL, and an administrator is unaffected.
 */
class PolyglotScriptingAuthorizationIT extends BaseGraphServerTest {

  private static final String READER_USER = "scripting-reader";
  private static final String READER_PWD  = "readerpass1";

  // The published proof-of-concept: walk from the bound database object to the class loader and read a host file.
  private static final String PoC_SCRIPT =
      "var cl=database.getClass().getClassLoader();var F=cl.loadClass('java.io.File');F.getName();";

  @Test
  void readerCannotExecuteScript() throws Exception {
    testEachServer((serverIndex) -> {
      createReaderUser(serverIndex);
      try {
        final String readerAuth = basicAuth(READER_USER, READER_PWD);

        // Control: the reader account is live and authenticates fine on the ordinary read-only SQL path.
        assertThat(command(serverIndex, readerAuth, "sql", "SELECT FROM schema:types"))
            .as("reader must be able to run read-only SQL").isEqualTo(200);

        // The scripting privilege-escalation path must be denied.
        assertThat(command(serverIndex, readerAuth, "js", PoC_SCRIPT))
            .as("reader must not execute the proof-of-concept js script").isEqualTo(403);
        assertThat(command(serverIndex, readerAuth, "js", "1 + 1"))
            .as("reader must not execute any js, even a harmless expression").isEqualTo(403);
      } finally {
        deleteUser(serverIndex, READER_USER);
      }
    });
  }

  @Test
  void adminCanStillExecuteScript() throws Exception {
    testEachServer((serverIndex) -> {
      // Positive control: an administrator (root) must not be blocked by the new check.
      assertThat(command(serverIndex, basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS), "js", "1 + 1"))
          .as("admin must still execute js").isEqualTo(200);
    });
  }

  /**
   * Regression test for GHSA-vwjc-v7x7-cm6g: {@code DEFINE FUNCTION ... LANGUAGE js} is arbitrary host code
   * execution and must require security-admin privileges (UPDATE_SECURITY), not merely UPDATE_SCHEMA.
   * Otherwise a schema administrator could define a JavaScript function and invoke it, bypassing the polyglot
   * scripting gate. A declarative SQL function stays available to a schema admin.
   */
  @Test
  void schemaAdminCannotDefineJsFunctionButRootCan() throws Exception {
    testEachServer((serverIndex) -> {
      createSchemaAdminUser(serverIndex);
      try {
        final String schemaAdminAuth = basicAuth(SCHEMA_ADMIN_USER, SCHEMA_ADMIN_PWD);

        // The escalation path: defining a JS function must now require UPDATE_SECURITY -> 403 for a schema admin.
        assertThat(command(serverIndex, schemaAdminAuth, "sql",
            "DEFINE FUNCTION libjs.jsFn \"return 1\" LANGUAGE js"))
            .as("schema admin must NOT define a js function").isEqualTo(403);

        // Control: a schema admin CAN define a declarative SQL function (needs UPDATE_SCHEMA, which it has).
        assertThat(command(serverIndex, schemaAdminAuth, "sql",
            "DEFINE FUNCTION libsql.sqlFn \"SELECT 1 AS r\" LANGUAGE sql"))
            .as("schema admin may define a SQL function").isEqualTo(200);

        // Positive control: root (security admin) can still define a JS function.
        assertThat(command(serverIndex, basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS), "sql",
            "DEFINE FUNCTION libroot.jsFnRoot \"return 1\" LANGUAGE js"))
            .as("root may define a js function").isEqualTo(200);
      } finally {
        deleteUser(serverIndex, SCHEMA_ADMIN_USER);
      }
    });
  }

  /**
   * Regression test for GHSA-38pf-6hp2-pxww: a JAVASCRIPT trigger binds the real database object into a GraalVM
   * context (HostAccess.ALL minus reflection), so its script can call {@code database.getSecurity().createUser(...)}
   * and escalate a schema administrator to server-wide admin. Creating a JAVASCRIPT (or JAVA) trigger must therefore
   * require security-admin privileges (UPDATE_SECURITY), not merely UPDATE_SCHEMA. A declarative SQL trigger stays
   * available to a schema admin.
   */
  @Test
  void schemaAdminCannotCreateJsTriggerButRootCan() throws Exception {
    testEachServer((serverIndex) -> {
      createSchemaAdminUser(serverIndex);
      try {
        final String schemaAdminAuth = basicAuth(SCHEMA_ADMIN_USER, SCHEMA_ADMIN_PWD);

        // The escalation path: creating a JS trigger must now require UPDATE_SECURITY -> 403 for a schema admin.
        assertThat(command(serverIndex, schemaAdminAuth, "sql",
            "CREATE TRIGGER jsTrig BEFORE CREATE ON " + VERTEX1_TYPE_NAME
                + " EXECUTE JAVASCRIPT 'database.getSecurity().createUser(\"ev\",\"p\"); true'"))
            .as("schema admin must NOT create a JS trigger").isEqualTo(403);

        // A JAVA trigger is arbitrary host code too and must be denied to a schema admin.
        assertThat(command(serverIndex, schemaAdminAuth, "sql",
            "CREATE TRIGGER javaTrig BEFORE CREATE ON " + VERTEX1_TYPE_NAME + " EXECUTE JAVA 'com.example.MyTrigger'"))
            .as("schema admin must NOT create a JAVA trigger").isEqualTo(403);

        // Control: a schema admin CAN create a declarative SQL trigger (needs UPDATE_SCHEMA, which it has).
        assertThat(command(serverIndex, schemaAdminAuth, "sql",
            "CREATE TRIGGER sqlTrig BEFORE CREATE ON " + VERTEX1_TYPE_NAME + " EXECUTE SQL 'SELECT 1'"))
            .as("schema admin may create a SQL trigger").isEqualTo(200);

        // Positive control: root (security admin) can still create a JS trigger.
        assertThat(command(serverIndex, basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS), "sql",
            "CREATE TRIGGER jsTrigRoot BEFORE CREATE ON " + VERTEX1_TYPE_NAME + " EXECUTE JAVASCRIPT 'true'"))
            .as("root may create a JS trigger").isEqualTo(200);
      } finally {
        deleteUser(serverIndex, SCHEMA_ADMIN_USER);
      }
    });
  }

  private static final String SCHEMA_ADMIN_USER = "schema-admin";
  private static final String SCHEMA_ADMIN_PWD  = "schemaadmin1";

  private void createSchemaAdminUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants database-level UPDATE_SCHEMA (but NOT updateSecurity) plus record reads/writes.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("schemaAdmin",
        new JSONObject().put("access", new JSONArray().put("updateSchema")).put("types",
            new JSONObject().put("*",
                new JSONObject().put("access", new JSONArray().put("readRecord").put("createRecord").put("updateRecord")))));
    security.saveGroups();

    if (security.existsUser(SCHEMA_ADMIN_USER))
      security.dropUser(SCHEMA_ADMIN_USER);

    final JSONObject payload = new JSONObject()
        .put("name", SCHEMA_ADMIN_USER)
        .put("password", SCHEMA_ADMIN_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("schemaAdmin")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void createReaderUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // A group that grants record reads but no database-level (admin) permission.
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put("scriptReader",
        new JSONObject().put("access", new JSONArray()).put("types",
            new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();

    if (security.existsUser(READER_USER))
      security.dropUser(READER_USER);

    final JSONObject payload = new JSONObject()
        .put("name", READER_USER)
        .put("password", READER_PWD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("scriptReader")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }

  private void deleteUser(final int serverIndex, final String name) throws Exception {
    final HttpURLConnection connection = open(serverIndex,
        "/api/v1/server/users?name=" + URLEncoder.encode(name, StandardCharsets.UTF_8), basicAuth("root", DEFAULT_PASSWORD_FOR_TESTS));
    connection.setRequestMethod("DELETE");
    connection.connect();
    try {
      connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private int command(final int serverIndex, final String auth, final String language, final String script) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject().put("language", language).put("command", script);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes());
  }
}
