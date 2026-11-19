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
package com.arcadedb.server.http;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12436: DDL statements that fail on a caller's mistake answered HTTP 500 "Internal error", and one of them crashed with a
 * raw {@code NullPointerException}. The HTTP API reference lists 400 for a statement the server refuses to run and reserves 500
 * for a failure of the server itself. Every refusal here is a 400 with the message that names the problem, as the issue expects:
 * a bad value in the statement, a bucket a type owns, and records or samples that break the constraint being declared, whether
 * it is a missing MANDATORY/NOTNULL or a value over a MAX (which already answered 400).
 */
class Issue12436DdlCallerMistakesAreClientErrorsTest extends BaseGraphServerTest {

  @Test
  void aBucketNamedByIdDoesNotCrashTheStatement() throws Exception {
    final Response unknown = command("CREATE DOCUMENT TYPE WithBuckets12436 BUCKET 999999");

    assertThat(unknown.status).as(unknown.body).isEqualTo(400);
    assertThat(unknown.body).as("no Java crash reaches the client").doesNotContain("NullPointerException");
    assertThat(command("SELECT FROM schema:types WHERE name = 'WithBuckets12436'").body).as("the type is not created")
        .contains("\"result\":[]");
  }

  @Test
  void aBareWordAsAnAttributeValueIsRefusedWithAMessage() throws Exception {
    setUpType();

    final Response mandatory = command("CREATE PROPERTY T12436.bad STRING (mandatory maybe)");
    assertThat(mandatory.status).as(mandatory.body).isEqualTo(400);
    assertThat(mandatory.body).contains("field reference");

    final Response regexp = command("CREATE PROPERTY T12436.bad2 STRING (regexp abc)");
    assertThat(regexp.status).as(regexp.body).isEqualTo(400);
    assertThat(regexp.body).contains("field reference");
  }

  @Test
  void aBoundThatCannotBeReadIsRefusedWithAMessage() throws Exception {
    setUpType();

    final Response response = command("ALTER PROPERTY T12436.x MIN 'abc'");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("cannot be read as a number");
  }

  @Test
  void aConstraintTheStoredRecordsBreakIsRefusedAndThePropertyIsNotCreated() throws Exception {
    setUpType();

    final Response response = command("CREATE PROPERTY T12436.m STRING (mandatory true)");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).as("the constraint is named for production mode").contains("Constraint violated (MANDATORY)");
    assertThat(response.body).contains("violates it").contains("Fix or delete the non-conforming records first");
    assertThat(command("SELECT FROM schema:types WHERE name = 'T12436'").body).as("the property is not created")
        .doesNotContain("\"name\":\"m\"");
  }

  @Test
  void aBucketATypeOwnsCannotBeDropped() throws Exception {
    setUpType();

    final Response response = command("DROP BUCKET T12436_0");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("because used by type 'T12436'");
    assertThat(command("SELECT count(@rid) AS n FROM T12436").body).as("the records are still there").contains("\"n\":1");
  }

  /** Control: a value constraint the stored records break (#12037) already answered 400 and is not changed by this fix. */
  @Test
  void aMaxTheStoredValuesBreakStillAnswers400() throws Exception {
    setUpType();

    final Response response = command("ALTER PROPERTY T12436.x MAX 0");

    assertThat(response.status).as(response.body).isEqualTo(400);
  }

  /** The same refusal on a TIMESERIES type: a value that breaks the bound is the caller's mistake (400). */
  @Test
  void aMaxTheStoredSamplesBreakIsAnswered400() throws Exception {
    for (final String sql : new String[] { "CREATE TIMESERIES TYPE M12436 TIMESTAMP ts TAGS (h STRING) FIELDS (v LONG) SHARDS 1",
        "INSERT INTO M12436 SET ts = 1000, h = 'a', v = 10" })
      assertThat(command(sql).status).as(sql).isEqualTo(200);

    final Response response = command("ALTER PROPERTY M12436.v MAX 5");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("violates it");
  }

  /** A MANDATORY the stored samples lack is the same 400 as the value bound above, and as the one over document records. */
  @Test
  void aMandatoryTheStoredSamplesLackIsAnswered400() throws Exception {
    for (final String sql : new String[] { "CREATE TIMESERIES TYPE S12436 TIMESTAMP ts TAGS (h STRING) FIELDS (v LONG) SHARDS 1",
        "INSERT INTO S12436 SET ts = 1000, h = 'a'" })
      assertThat(command(sql).status).as(sql).isEqualTo(200);

    final Response response = command("ALTER PROPERTY S12436.v MANDATORY true");

    assertThat(response.status).as(response.body).isEqualTo(400);
    assertThat(response.body).contains("violates it").contains("Constraint violated (MANDATORY)");
  }

  @Test
  void aValidPropertyIsStillCreated() throws Exception {
    setUpType();

    final Response response = command("CREATE PROPERTY T12436.ok STRING (mandatory false)");

    assertThat(response.status).as(response.body).isEqualTo(200);
  }

  private void setUpType() throws Exception {
    for (final String sql : new String[] { "CREATE DOCUMENT TYPE T12436", "CREATE PROPERTY T12436.x STRING",
        "INSERT INTO T12436 SET x = 'a'" })
      assertThat(command(sql).status).as(sql).isEqualTo(200);
  }

  private Response command(final String command) throws Exception {
    final HttpURLConnection connection =
        (HttpURLConnection) new URI(getServerHttpUrl("/api/v1/command/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    final String credentials =
        Base64.getEncoder().encodeToString(("root:" + StaticBaseServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    connection.setRequestProperty("Authorization", "Basic " + credentials);
    connection.setDoOutput(true);
    final byte[] payload = new JSONObject().put("language", "sql").put("command", command).toString().getBytes(StandardCharsets.UTF_8);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(payload);
    }
    try {
      final int status = connection.getResponseCode();
      final InputStream in = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
      final String body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private record Response(int status, String body) {
  }
}
