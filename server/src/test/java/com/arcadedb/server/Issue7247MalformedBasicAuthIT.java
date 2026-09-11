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

import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7247. The {@code catch (Exception)} around the Authorization header discarded the cause of every failure
 * it saw, and the reason it could not simply log one is that a mistyped header is CLIENT-triggerable: a stack
 * trace per request would be a log flood an anonymous caller controls.
 * <p>
 * A header that cannot be Base64-decoded is now answered where it happens, exactly as a header that decodes to
 * something other than {@code user:password} already was - 403, no exception raised. What still reaches the catch
 * is an internal fault, which is why that arm can now log its cause and chain it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7247MalformedBasicAuthIT extends BaseGraphServerTest {

  @Test
  void aBasicHeaderThatIsNotBase64Is403() throws Exception {
    assertThat(statusForAuthorizationHeader("Basic !!!not-base64!!!")).isEqualTo(403);
  }

  @Test
  void aBasicHeaderWithNoPayloadIs403() throws Exception {
    assertThat(statusForAuthorizationHeader("Basic")).isEqualTo(403);
  }

  @Test
  void aBasicHeaderDecodingToSomethingElseIsStill403() throws Exception {
    // The pre-existing sibling of the two above: valid Base64 that is not 'user:password'.
    assertThat(statusForAuthorizationHeader(
        "Basic " + Base64.getEncoder().encodeToString("nocolon".getBytes()))).isEqualTo(403);
  }

  /**
   * The control: without it every assertion above would also pass against a server that refused the request for
   * an unrelated reason, which is exactly what a stray process on port 2480 produces.
   */
  @Test
  void aWellFormedBasicHeaderStillAuthenticates() throws Exception {
    assertThat(statusForAuthorizationHeader("Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))).isEqualTo(200);
  }

  private int statusForAuthorizationHeader(final String authorization) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:2480/api/v1/query/graph/sql/select%201").openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", authorization);
    try {
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
