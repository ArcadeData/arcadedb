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
package com.arcadedb.server;

import com.arcadedb.remote.RemoteAlreadyExistsException;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10073: creating a user whose name is taken answered 403 "Security error", the status a non-root caller gets, so
 * a client could not tell "this user already exists" from "you may not do this" without matching on the `detail` text.
 * The contract declares 409, and 409 is what an application provisioning a user per tenant needs to treat the conflict
 * as success.
 * <p>
 * One test per way in: the REST route and the `create user` server command, each driven against a real server. The
 * non-root test is the other half of the contract: a 403 must still mean "not authorized", or the fix would have moved
 * the ambiguity rather than removed it.
 */
class Issue10073DuplicateUserAnswers409IT extends BaseGraphServerTest {

  private static final String PASSWORD = "a-long-password";

  @Test
  void aTakenNameOnTheRestRouteAnswers409AndLeavesTheFirstUserAlone() throws Exception {
    final String name = "dup_rest_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final HttpURLConnection second = open("/api/v1/server/users", basicAuth(), toJson(name, "another-long-password"));
    try {
      assertThat(second.getResponseCode()).isEqualTo(409);
      assertThat(readError(second)).contains("already exists").contains(name);
    } finally {
      second.disconnect();
    }

    // The conflict must not have replaced the first user's credentials
    assertThat(getServer(0).getSecurity().authenticate(name, PASSWORD, null)).isNotNull();
  }

  @Test
  void aTakenNameOnTheServerCommandAnswers409() throws Exception {
    final String name = "dup_command_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final HttpURLConnection second = open("/api/v1/server", basicAuth(),
        new JSONObject().put("command", "create user " + toJson(name, PASSWORD)).toString());
    try {
      assertThat(second.getResponseCode()).isEqualTo(409);
      assertThat(readError(second)).contains("already exists");
    } finally {
      second.disconnect();
    }
  }

  @Test
  void aNonRootCallerStillGets403EvenForATakenName() throws Exception {
    final String name = "dup_nonroot_user";
    assertThat(postUser(name, PASSWORD, basicAuth())).isEqualTo(201);

    final String asThatUser = "Basic " + Base64.getEncoder().encodeToString((name + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    assertThat(postUser(name, PASSWORD, asThatUser)).isEqualTo(403);
    assertThat(postUser("dup_nonroot_other", PASSWORD, asThatUser)).isEqualTo(403);
  }

  /**
   * The Java remote client used to surface the duplicate as a {@code SecurityException}, the type it gives a 403. It
   * arrives as a conflict instead, with its own type ({@link RemoteAlreadyExistsException}, issue #10075), so a caller can
   * tell it apart from a refusal without matching on the message.
   */
  @Test
  void theRemoteClientReportsATakenNameAsAnAlreadyExistsConflict() {
    final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(0), "root", DEFAULT_PASSWORD_FOR_TESTS);
    remote.createUser("dup_remote_user", PASSWORD, List.of());

    assertThatThrownBy(() -> remote.createUser("dup_remote_user", PASSWORD, List.of()))
        .isInstanceOf(RemoteAlreadyExistsException.class)
        .isNotInstanceOf(SecurityException.class)
        .hasMessageContaining("already exists");
  }

  private static String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  private int postUser(final String name, final String password, final String authorization) throws Exception {
    final HttpURLConnection connection = open("/api/v1/server/users", authorization, toJson(name, password));
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static String toJson(final String name, final String password) {
    return new JSONObject().put("name", name).put("password", password).toString();
  }

  private HttpURLConnection open(final String path, final String authorization, final String body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", authorization);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(body.getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();
    return connection;
  }
}
