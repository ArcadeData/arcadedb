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
package com.arcadedb.redis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for Redis wire-protocol TLS support: when arcadedb.redis.tls is enabled the listener
 * accepts only TLS connections, so the AUTH credentials are encrypted in transit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisTlsIT extends BaseGraphServerTest {

  private static final int    DEF_PORT            = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final String KEYSTORE_PASSWORD   = "testPassword123";
  private static final String TRUSTSTORE_PASSWORD = "testPassword123";
  private static Path         keystorePath;
  private static Path         truststorePath;
  private static Path         tempDir;

  @BeforeAll
  static void generateCertificates() throws Exception {
    tempDir = Files.createTempDirectory("redis-tls-test");
    keystorePath = tempDir.resolve("keystore.pkcs12");
    truststorePath = tempDir.resolve("truststore.jks");

    new ProcessBuilder("keytool", "-genkeypair", "-alias", "redis-test", "-keyalg", "RSA", "-keysize", "2048",
        "-validity", "365", "-dname", "CN=localhost, O=ArcadeDB Test, L=Test, ST=Test, C=US",
        "-keystore", keystorePath.toString(), "-storepass", KEYSTORE_PASSWORD, "-storetype", "PKCS12")
        .redirectErrorStream(true).start().waitFor();

    final Path certPath = tempDir.resolve("redis-test.cer");
    new ProcessBuilder("keytool", "-exportcert", "-alias", "redis-test", "-keystore", keystorePath.toString(),
        "-storepass", KEYSTORE_PASSWORD, "-file", certPath.toString())
        .redirectErrorStream(true).start().waitFor();

    new ProcessBuilder("keytool", "-importcert", "-alias", "redis-test", "-keystore", truststorePath.toString(),
        "-storepass", TRUSTSTORE_PASSWORD, "-storetype", "JKS", "-file", certPath.toString(), "-noprompt")
        .redirectErrorStream(true).start().waitFor();

    Files.deleteIfExists(certPath);
  }

  @AfterAll
  static void cleanupCertificates() throws Exception {
    if (keystorePath != null)
      Files.deleteIfExists(keystorePath);
    if (truststorePath != null)
      Files.deleteIfExists(truststorePath);
    if (tempDir != null)
      Files.deleteIfExists(tempDir);
  }

  @Test
  void tlsConnectionAuthenticatesAndRunsCommands() throws Exception {
    final JedisClientConfig config = DefaultJedisClientConfig.builder()
        .ssl(true)
        .sslSocketFactory(buildClientSslSocketFactory())
        .build();

    try (final Jedis jedis = new Jedis(new HostAndPort("localhost", DEF_PORT), config)) {
      assertThat(jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");

      jedis.set("tlsKey", "tlsValue");
      assertThat(jedis.get("tlsKey")).isEqualTo("tlsValue");
    }
  }

  private SSLSocketFactory buildClientSslSocketFactory() throws Exception {
    final KeyStore trustStore = KeyStore.getInstance("JKS");
    try (final InputStream in = Files.newInputStream(truststorePath)) {
      trustStore.load(in, TRUSTSTORE_PASSWORD.toCharArray());
    }
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    final SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(null, tmf.getTrustManagers(), null);
    return ctx.getSocketFactory();
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
    GlobalConfiguration.REDIS_TLS.setValue(true);
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration configuration) {
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, keystorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.REDIS_TLS.setValue(false);
    super.endTest();
  }
}
