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

import java.net.*;
import java.util.*;
import java.util.logging.*;

public class HTTP2ServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void propagationOfSchema() throws Exception {
    // CREATE THE SCHEMA ON BOTH SERVER, ONE TYPE PER SERVER
    testEachServer((serverIndex) -> {
      final HttpURLConnection initialConnection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      try {

        initialConnection.setRequestMethod("POST");
        initialConnection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        formatPost(initialConnection, "sql", "create vertex type VertexType" + serverIndex, new HashMap<>());
        initialConnection.connect();

        final String response = readResponse(initialConnection);

        LogManager.instance().log(this, Level.INFO, "Response: %s", null, response);

        Assertions.assertEquals(200, initialConnection.getResponseCode());
        Assertions.assertEquals("OK", initialConnection.getResponseMessage());
        Assertions.assertTrue(response.contains("VertexType" + serverIndex), "Type " + (("VertexType" + serverIndex) + " not found on server " + serverIndex));

      } finally {
        initialConnection.disconnect();
      }
    });

    Thread.sleep(1000);

    // CHECK THE SCHEMA HAS BEEN PROPAGATED
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      try {
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
        formatPost(connection, "sql", "select from VertexType" + serverIndex, new HashMap<>());
        connection.connect();

        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());

      } finally {
        connection.disconnect();
      }
    });

  }

}
