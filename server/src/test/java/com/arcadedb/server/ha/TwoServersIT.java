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
 */
package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.logging.Level;

public class TwoServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void checkQuery() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + +serverIndex + "/api/v1/query/graph/sql/select%20from%20V1%20limit%201").openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkRecordLoading() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph/" + BaseGraphServerTest.root.getIdentity().toString().substring(1)).openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.connect();

      try {
        final String response = readResponse(connection);

        LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);

        Assertions.assertEquals(200, connection.getResponseCode());

        Assertions.assertEquals("OK", connection.getResponseMessage());

        Assertions.assertTrue(response.contains("V1"));

      } finally {
        connection.disconnect();
      }
    });
  }

  @Test
  public void checkRecordCreate() throws Exception {
    testEachServer((serverIndex) -> {
      HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/document/graph").openConnection();

      connection.setRequestMethod("POST");

      final String payload = "{\"@type\":\"Person\",\"name\":\"Jay\",\"surname\":\"Miner\",\"age\":69}";

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      connection.setDoOutput(true);

      connection.connect();

      PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()));
      pw.write(payload);
      pw.close();

      try {
        final String response = readResponse(connection);

        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());

        LogManager.instance().log(this, Level.INFO, "TEST: Response: %s", null, response);

        Assertions.assertTrue(response.contains("#"));

      } finally {
        connection.disconnect();
      }
    });

  }
}
