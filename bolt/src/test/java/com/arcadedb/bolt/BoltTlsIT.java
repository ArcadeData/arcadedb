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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.test.BaseGraphServerTest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for BOLT TLS support.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltTlsIT extends BaseGraphServerTest {

  private static final String KEYSTORE_PASSWORD  = "testPassword123";
  private static final String TRUSTSTORE_PASSWORD = "testPassword123";
  private static Path         keystorePath;
  private static Path         truststorePath;
  private static Path         tempDir;

  @BeforeAll
  static void generateCertificates() throws Exception {
    tempDir = Files.createTempDirectory("bolt-tls-test");

    // Use keytool to generate a self-signed certificate and keystore
    keystorePath = tempDir.resolve("keystore.pkcs12");
    truststorePath = tempDir.resolve("truststore.jks");

    // Generate keystore with self-signed cert
    final Process keytoolGen = new ProcessBuilder(
        "keytool", "-genkeypair",
        "-alias", "bolt-test",
        "-keyalg", "RSA",
        "-keysize", "2048",
        "-validity", "365",
        "-dname", "CN=localhost, O=ArcadeDB Test, L=Test, ST=Test, C=US",
        "-keystore", keystorePath.toString(),
        "-storepass", KEYSTORE_PASSWORD,
        "-storetype", "PKCS12"
    ).redirectErrorStream(true).start();
    keytoolGen.waitFor();

    // Export the certificate
    final Path certPath = tempDir.resolve("bolt-test.cer");
    final Process keytoolExport = new ProcessBuilder(
        "keytool", "-exportcert",
        "-alias", "bolt-test",
        "-keystore", keystorePath.toString(),
        "-storepass", KEYSTORE_PASSWORD,
        "-file", certPath.toString()
    ).redirectErrorStream(true).start();
    keytoolExport.waitFor();

    // Import into truststore
    final Process keytoolImport = new ProcessBuilder(
        "keytool", "-importcert",
        "-alias", "bolt-test",
        "-keystore", truststorePath.toString(),
        "-storepass", TRUSTSTORE_PASSWORD,
        "-storetype", "JKS",
        "-file", certPath.toString(),
        "-noprompt"
    ).redirectErrorStream(true).start();
    keytoolImport.waitFor();

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

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.BOLT_SSL.setValue("OPTIONAL");
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
    GlobalConfiguration.BOLT_SSL.setValue("DISABLED");
    super.endTest();
  }

  @Test
  void tlsConnectionWithSelfSignedCert() {
    // bolt+ssc:// means TLS with self-signed certificate (no verification)
    try (final Driver driver = GraphDatabase.driver(
        "bolt+ssc://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().build())) {
      driver.verifyConnectivity();
    }
  }

  @Test
  void tlsConnectionCanRunQuery() {
    try (final Driver driver = GraphDatabase.driver(
        "bolt+ssc://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().build())) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final org.neo4j.driver.Result result = session.run("RETURN 1 AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").asLong()).isEqualTo(1L);
      }
    }
  }

  @Test
  void plaintextConnectionInOptionalMode() {
    // In OPTIONAL mode, plaintext bolt:// should also work
    try (final Driver driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      driver.verifyConnectivity();
    }
  }

  @Test
  void plaintextConnectionCanRunQueryInOptionalMode() {
    try (final Driver driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build())) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final org.neo4j.driver.Result result = session.run("RETURN 42 AS answer");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("answer").asLong()).isEqualTo(42L);
      }
    }
  }

}
