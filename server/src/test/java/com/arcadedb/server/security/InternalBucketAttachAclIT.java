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

import com.arcadedb.schema.Schema;
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
 * The per-type grants of a group follow the files a type keeps for its own use - the paired bucket of its EXTERNAL values and the
 * edge-list buckets of its vertex buckets - as they follow its primary buckets. A user allowed to change the schema, but denied a
 * type, cannot hand those files to another type, so the grants that apply to them stay the denied type's.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class InternalBucketAttachAclIT extends BaseGraphServerTest {
  private static final String SCOPED_USER = "internal-bucket-scoped-user";
  private static final String SCOPED_PWD  = "internalbucket1";
  private static final String GROUP       = "internalBucketScoped";
  private static final String SECRET_DOC  = "SecretRecords";
  private static final String SECRET_NODE = "SecretNodes";
  private static final String PUBLIC_DOC  = "PublicRecords";

  private String lastResponseBody = "";

  @AfterEach
  void dropScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
  }

  @Test
  void theExternalBucketOfADeniedTypeStaysUnderItsGrantsWhenAnAttachIsRefused() throws Exception {
    createSecretTypes();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);
    final String externalBucket = SECRET_DOC + "_0_ext";

    assertThat(sql(auth, "query", "SELECT FROM bucket:" + externalBucket)).isEqualTo(403);

    assertThat(sql(auth, "command", "ALTER TYPE " + PUBLIC_DOC + " BUCKET +" + externalBucket))
        .as("a bucket another type keeps for its own use is not attached: %s", lastResponseBody).isNotEqualTo(200);

    // ANY LATER SCHEMA CHANGE REBUILDS THE PER-FILE MAP FROM THE TYPES
    command(0, "CREATE DOCUMENT TYPE Unrelated");
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + externalBucket)).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM " + PUBLIC_DOC)).isEqualTo(200);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(PUBLIC_DOC).hasBucket(externalBucket)).isFalse();
  }

  @Test
  void theEdgeListBucketsOfADeniedTypeStayUnderItsGrantsWhenAnAttachIsRefused() throws Exception {
    createSecretTypes();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);
    final String outBucket = SECRET_NODE + "_0_out_edges";
    final String inBucket = SECRET_NODE + "_0_in_edges";

    assertThat(sql(auth, "query", "SELECT FROM bucket:" + outBucket)).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + inBucket)).isEqualTo(403);

    assertThat(sql(auth, "command", "ALTER TYPE " + PUBLIC_DOC + " BUCKET +" + outBucket)).as(lastResponseBody).isNotEqualTo(200);
    assertThat(sql(auth, "command", "ALTER TYPE " + PUBLIC_DOC + " BUCKET +" + inBucket)).as(lastResponseBody).isNotEqualTo(200);

    command(0, "CREATE DOCUMENT TYPE Unrelated");
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + outBucket)).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + inBucket)).isEqualTo(403);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(PUBLIC_DOC).hasBucket(outBucket)).isFalse();
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(PUBLIC_DOC).hasBucket(inBucket)).isFalse();
  }

  @Test
  void aNewTypeIsRefusedTheInternalBucketsOfADeniedType() throws Exception {
    createSecretTypes();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    assertThat(sql(auth, "command", "CREATE DOCUMENT TYPE TakenExternal BUCKET " + SECRET_DOC + "_0_ext")).as(lastResponseBody)
        .isNotEqualTo(200);
    assertThat(sql(auth, "command", "CREATE DOCUMENT TYPE TakenEdgeList BUCKET " + SECRET_NODE + "_0_out_edges")).as(lastResponseBody)
        .isNotEqualTo(200);

    final Schema schema = getServerDatabase(0, getDatabaseName()).getSchema();
    assertThat(schema.existsType("TakenExternal")).isFalse();
    assertThat(schema.existsType("TakenEdgeList")).isFalse();
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + SECRET_DOC + "_0_ext")).isEqualTo(403);
    assertThat(sql(auth, "query", "SELECT FROM bucket:" + SECRET_NODE + "_0_out_edges")).isEqualTo(403);
  }

  @Test
  void aBucketOfNoTypeStillAttachesToAType() throws Exception {
    createSecretTypes();
    final String auth = basicAuth(SCOPED_USER, SCOPED_PWD);

    // ALTER TYPE ... BUCKET +name creates the bucket when no bucket has that name, and a bucket of no type is attached as before
    assertThat(sql(auth, "command", "ALTER TYPE " + PUBLIC_DOC + " BUCKET +FreeBucket")).as(lastResponseBody).isEqualTo(200);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType(PUBLIC_DOC).hasBucket("FreeBucket")).isTrue();
    assertThat(sql(auth, "query", "SELECT FROM bucket:FreeBucket")).isEqualTo(200);
  }

  private void createSecretTypes() throws Exception {
    command(0, "CREATE DOCUMENT TYPE " + SECRET_DOC);
    command(0, "CREATE PROPERTY " + SECRET_DOC + ".blob STRING");
    command(0, "ALTER PROPERTY " + SECRET_DOC + ".blob EXTERNAL true");
    command(0, "INSERT INTO " + SECRET_DOC + " SET blob = 'topsecret'");
    command(0, "CREATE VERTEX TYPE " + SECRET_NODE);
    command(0, "CREATE EDGE TYPE SecretEdge");
    command(0, "CREATE VERTEX " + SECRET_NODE + " SET name = 'a'");
    command(0, "CREATE VERTEX " + SECRET_NODE + " SET name = 'b'");
    command(0, "CREATE EDGE SecretEdge FROM (SELECT FROM " + SECRET_NODE + " WHERE name = 'a') TO (SELECT FROM " + SECRET_NODE
        + " WHERE name = 'b')");
    command(0, "CREATE DOCUMENT TYPE " + PUBLIC_DOC);
    createScopedUser();
  }

  private void createScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put(GROUP,
        new JSONObject().put("access", new JSONArray().put("updateSchema"))
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord").put("deleteRecord")))
                .put(SECRET_DOC, new JSONObject().put("access", new JSONArray()))
                .put(SECRET_NODE, new JSONObject().put("access", new JSONArray()))));
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
