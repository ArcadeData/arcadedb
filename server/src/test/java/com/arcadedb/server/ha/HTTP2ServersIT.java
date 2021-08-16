/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.ha;

import com.arcadedb.log.LogManager;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

public class HTTP2ServersIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  public void propagationOfSchema() throws Exception {
    // CREATE THE SCHEMA ON BOTH SERVER, ONE TYPE PER SERVER
    testEachServer((serverIndex) -> {
      final HttpURLConnection initialConnection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/command/graph").openConnection();
      try {

        initialConnection.setRequestMethod("POST");
        initialConnection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
        formatPost(initialConnection, "sql", "create vertex type VertexType" + serverIndex, new HashMap<>());
        initialConnection.connect();

        final String response = readResponse(initialConnection);

        LogManager.instance().log(this, Level.INFO, "Response: ", null, response);

        Assertions.assertEquals(200, initialConnection.getResponseCode());
        Assertions.assertEquals("OK", initialConnection.getResponseMessage());
        Assertions.assertTrue(response.contains("VertexType" + serverIndex));

      } finally {
        initialConnection.disconnect();
      }
    });

    Thread.sleep(1000);

    // CHECK THE SCHEMA HAS BEEN PROPAGATED
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + "/command/graph").openConnection();

      try {
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString("root:root".getBytes()));
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
