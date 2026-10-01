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
package com.arcadedb.postgres;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Shared scaffolding of the Postgres TLS integration tests (issue #8840): a self-signed key store and trust store
 * generated with {@code keytool}, handed to the server through the shared {@code arcadedb.ssl.*} settings, and the
 * TLS mode under test set by the subclass.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class PostgresTlsTestBase extends PostgresWireProtocolTestBase {
  static final  int    SSL_REQUEST_CODE = 80877103;
  private static final String STORE_PASSWORD = "testPassword123";
  private static Path keystorePath;
  private static Path truststorePath;
  private static Path tempDir;

  @BeforeAll
  static void generateCertificates() throws Exception {
    tempDir = Files.createTempDirectory("postgres-tls-test");
    keystorePath = tempDir.resolve("keystore.pkcs12");
    truststorePath = tempDir.resolve("truststore.jks");
    final Path certPath = tempDir.resolve("postgres-test.cer");

    keytool("-genkeypair", "-alias", "postgres-test", "-keyalg", "RSA", "-keysize", "2048", "-validity", "365", "-dname",
        "CN=localhost, O=ArcadeDB Test, L=Test, ST=Test, C=US", "-keystore", keystorePath.toString(), "-storepass", STORE_PASSWORD,
        "-storetype", "PKCS12");
    keytool("-exportcert", "-alias", "postgres-test", "-keystore", keystorePath.toString(), "-storepass", STORE_PASSWORD, "-file",
        certPath.toString());
    keytool("-importcert", "-alias", "postgres-test", "-keystore", truststorePath.toString(), "-storepass", STORE_PASSWORD,
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

  private static void keytool(final String... args) throws Exception {
    final String[] command = new String[args.length + 1];
    command[0] = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
    System.arraycopy(args, 0, command, 1, args.length);
    final Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    process.getInputStream().readAllBytes();
    if (process.waitFor() != 0)
      throw new IllegalStateException("keytool failed: " + String.join(" ", args));
  }

  /** The value of {@code arcadedb.postgres.ssl} the server under test runs with. */
  protected abstract String tlsMode();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.POSTGRES_SSL.setValue(tlsMode());
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, keystorePath.toString());
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, STORE_PASSWORD);
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststorePath.toString());
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.POSTGRES_SSL.setValue("DISABLED");
    super.endTest();
  }

  /** Connects with the PostgreSQL JDBC driver. {@code sslmode=require} encrypts without verifying the certificate. */
  protected Connection connect(final boolean ssl) throws Exception {
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    if (ssl)
      properties.setProperty("sslmode", "require");
    else
      properties.setProperty("sslmode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  protected int selectOne(final Connection connection) throws Exception {
    try (final Statement st = connection.createStatement(); final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
      rs.next();
      return rs.getInt("one");
    }
  }

  /** A raw client socket that fails the test, instead of hanging it, when the server stops answering. */
  protected Socket newSocket() throws Exception {
    final Socket socket = new Socket("localhost", getServerPostgresPort());
    socket.setSoTimeout(15_000);
    return socket;
  }

  /** The next byte the server sends, or -1 when it closed the connection (an abrupt reset counts as closed). */
  protected int readOrClosed(final InputStream in) throws Exception {
    try {
      return in.read();
    } catch (final SocketException e) {
      return -1;
    }
  }

  /** Sends an SSLRequest on a raw socket and returns the single byte the server answers with. */
  protected byte sslRequestAnswer(final Socket socket) throws Exception {
    final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    out.writeInt(8);
    out.writeInt(SSL_REQUEST_CODE);
    out.flush();
    return new DataInputStream(socket.getInputStream()).readByte();
  }

  /** Layers a client TLS session, trusting anything, over a socket that has just been answered {@code S}. */
  protected SSLSocket startClientTls(final Socket socket) throws Exception {
    final SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, new TrustManager[] { new X509TrustManager() {
      @Override
      public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
      }

      @Override
      public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    } }, null);
    final SSLSocket sslSocket = (SSLSocket) context.getSocketFactory().createSocket(socket, "localhost", socket.getPort(), true);
    sslSocket.startHandshake();
    return sslSocket;
  }
}
