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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scaffolding of the tests of the Redis listener with {@code arcadedb.redis.tls} on and a pre-authentication deadline: a
 * self-signed key store and trust store generated with {@code keytool}, handed to the server through the shared
 * {@code arcadedb.ssl.*} settings, and a TLS client.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
abstract class RedisTlsPreAuthTestBase extends BaseRedisServerTest {

  private static final String STORE_PASSWORD = "testPassword123";
  private static Path         keystorePath;
  private static Path         truststorePath;
  private static Path         tempDir;

  private Socket clientSocket;

  /** The pre-authentication deadline the server runs with. */
  abstract long deadlineMs();

  @BeforeAll
  static void generateCertificates() throws Exception {
    tempDir = Files.createTempDirectory("redis-tls-deadline-test");
    keystorePath = tempDir.resolve("keystore.pkcs12");
    truststorePath = tempDir.resolve("truststore.jks");
    final Path certPath = tempDir.resolve("redis-test.cer");

    keytool("-genkeypair", "-alias", "redis-test", "-keyalg", "RSA", "-keysize", "2048", "-validity", "365", "-dname",
        "CN=localhost, O=ArcadeDB Test, L=Test, ST=Test, C=US", "-keystore", keystorePath.toString(), "-storepass", STORE_PASSWORD,
        "-storetype", "PKCS12");
    keytool("-exportcert", "-alias", "redis-test", "-keystore", keystorePath.toString(), "-storepass", STORE_PASSWORD, "-file",
        certPath.toString());
    keytool("-importcert", "-alias", "redis-test", "-keystore", truststorePath.toString(), "-storepass", STORE_PASSWORD,
        "-storetype", "JKS", "-file", certPath.toString(), "-noprompt");

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

  /** A client whose TLS handshake is done. It is closed through its plain socket by {@link #endTest()}. */
  protected SSLSocket connectTls() throws Exception {
    final KeyStore trustStore = KeyStore.getInstance("JKS");
    try (final InputStream in = Files.newInputStream(truststorePath)) {
      trustStore.load(in, STORE_PASSWORD.toCharArray());
    }
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    final SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, tmf.getTrustManagers(), null);

    clientSocket = new Socket();
    clientSocket.setReceiveBufferSize(4_096);
    clientSocket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), getServerRedisPort()), 10_000);
    final SSLSocket client = (SSLSocket) context.getSocketFactory()
        .createSocket(clientSocket, "localhost", clientSocket.getPort(), true);
    client.setSoTimeout(10_000);
    client.startHandshake();
    return client;
  }

  protected static void send(final Socket socket, final String raw) throws IOException {
    socket.getOutputStream().write(raw.getBytes(StandardCharsets.ISO_8859_1));
    socket.getOutputStream().flush();
  }

  protected static String readLine(final Socket socket) throws IOException {
    final StringBuilder sb = new StringBuilder();
    int prev = -1;
    while (true) {
      final int b = socket.getInputStream().read();
      if (b == -1)
        throw new IOException("Unexpected EOF while reading a line, read so far: '" + sb + "'");
      if (prev == '\r' && b == '\n') {
        sb.setLength(sb.length() - 1);
        return sb.toString();
      }
      sb.append((char) b);
      prev = b;
    }
  }

  private static void keytool(final String... arguments) throws Exception {
    final String[] command = new String[arguments.length + 1];
    command[0] = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    System.arraycopy(arguments, 0, command, 1, arguments.length);
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    process.getInputStream().readAllBytes();
    assertThat(process.waitFor()).as("keytool %s", arguments[0]).isZero();
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
    GlobalConfiguration.REDIS_TLS.setValue(true);
    GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.setValue(deadlineMs());
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration configuration) {
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, keystorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      if (clientSocket != null)
        clientSocket.close();
    } catch (final IOException e) {
      // IGNORE IT: THE TEST IS OVER
    }
    try {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      GlobalConfiguration.REDIS_TLS.setValue(false);
      GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.reset();
    } finally {
      super.endTest();
    }
  }
}
