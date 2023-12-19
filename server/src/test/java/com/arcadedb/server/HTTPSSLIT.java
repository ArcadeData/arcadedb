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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.*;
import java.security.cert.*;
import java.util.*;
import java.util.logging.*;

public class HTTPSSLIT extends BaseGraphServerTest {
  public HTTPSSLIT() {
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration configuration) {
    configuration.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "src/test/resources/keystore.pkcs12");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "sos0nmzWniR0");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "src/test/resources/truststore.jks");
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "nphgDK7ugjGR");
  }

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void testServerInfo() throws Exception {
    testEachServer((serverIndex) -> {
      final ContextConfiguration configuration = getServer(serverIndex).getConfiguration();

      System.setProperty("javax.net.ssl.keyStoreType", "PKCS12");
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");
      System.setProperty("javax.net.ssl.keyStore", configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_KEYSTORE));
      System.setProperty("javax.net.ssl.trustStore", configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE));
      System.setProperty("javax.net.ssl.keyStorePassword", configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD));
      System.setProperty("javax.net.ssl.trustStorePassword", configuration.getValueAsString(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD));
      System.setProperty("jsse.enableSNIExtension", "false");

      final HttpsURLConnection connection = (HttpsURLConnection) URI.create("https://localhost:249" + serverIndex + "/api/v1/server").toURL().openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      try {
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCertificates, new java.security.SecureRandom());
        connection.setSSLSocketFactory(sc.getSocketFactory());
        connection.connect();
        final String response = readResponse(connection);
        LogManager.instance().log(this, Level.FINE, "Response: ", null, response);
        Assertions.assertEquals(200, connection.getResponseCode());
        Assertions.assertEquals("OK", connection.getResponseMessage());
      } finally {
        connection.disconnect();
      }
    });
  }

  TrustManager[] trustAllCertificates = new TrustManager[] { new X509TrustManager() {
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null; // Not relevant.
    }

    @Override
    public void checkClientTrusted(X509Certificate[] certs, String authType) {
      // Do nothing. Just allow them all.
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String authType) {
      // Do nothing. Just allow them all.
    }
  } };
}
