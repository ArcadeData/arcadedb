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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11875: the 400 for an unknown line-protocol precision names the accepted values in production mode too, where the
 * 'detail' of a mapped exception is concealed.
 */
class Issue11875LineProtocolPrecisionProductionIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_MODE.setValue("production");
  }

  @Test
  void unknownPrecisionNamesTheAcceptedValuesInProductionMode() throws Exception {
    command(0, "CREATE TIMESERIES TYPE precisionprod TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");

    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort(0) + "/api/v1/ts/graph/write?precision=sec").toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setRequestProperty("Content-Type", "text/plain");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write("precisionprod,host=a v=1 1700000000".getBytes(StandardCharsets.UTF_8));
      }

      assertThat(connection.getResponseCode()).isEqualTo(400);
      final JSONObject json = new JSONObject(FileUtils.readStreamAsString(connection.getErrorStream(), "utf8"));
      assertThat(json.getString("error")).contains("'sec'").contains("ns, us (or u), ms or s");
    } finally {
      connection.disconnect();
    }

    assertThat(executeCommand(0, "sql", "SELECT FROM precisionprod").getJSONObject("result").getJSONArray("records").length())
        .isZero();
  }
}
