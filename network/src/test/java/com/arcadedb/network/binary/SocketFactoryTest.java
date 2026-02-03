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
package com.arcadedb.network.binary;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SocketFactoryTest {

  @TempDir
  Path tempDir;

  @Test
  void instanceCreatesFactory() {
    final ContextConfiguration config = new ContextConfiguration();
    final SocketFactory factory = SocketFactory.instance(config);

    assertThat(factory).isNotNull();
  }

  @Test
  void createSocketWithoutSSL() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, false);

    final SocketFactory factory = SocketFactory.instance(config);
    final Socket socket = factory.createSocket();

    assertThat(socket).isNotNull();
    assertThat(socket.isClosed()).isFalse();
    socket.close();
  }

  @Test
  void createSocketWithSSLMissingKeystore() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "/nonexistent/keystore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "password");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "/nonexistent/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "password");

    final SocketFactory factory = SocketFactory.instance(config);

    assertThatThrownBy(factory::createSocket)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Failed to create ssl context");
  }

  @Test
  void createSocketWithSSLEmptyKeystorePassword() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "/some/keystore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "/some/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "password");

    final SocketFactory factory = SocketFactory.instance(config);

    assertThatThrownBy(factory::createSocket)
        .isInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Please provide a keystore password");
  }

  @Test
  void createSocketWithSSLEmptyTruststorePassword() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, "/some/keystore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, "password");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, "/some/truststore.jks");
    config.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "");

    final SocketFactory factory = SocketFactory.instance(config);

    assertThatThrownBy(factory::createSocket)
        .isInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Please provide a truststore password");
  }

  @Test
  void getAsStreamFromFilePath() throws Exception {
    // Create a temporary file
    final File tempFile = tempDir.resolve("test.txt").toFile();
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      fos.write("test content".getBytes());
    }

    final InputStream stream = SocketFactory.getAsStream(tempFile.getAbsolutePath());
    assertThat(stream).isNotNull();

    final byte[] content = stream.readAllBytes();
    assertThat(new String(content)).isEqualTo("test content");
    stream.close();
  }

  @Test
  void getAsStreamFromNonExistentPath() {
    assertThatThrownBy(() -> SocketFactory.getAsStream("/nonexistent/path/file.txt"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Could not load resource");
  }

  @Test
  void getAsStreamFromRelativePath() throws Exception {
    // Create a temp file with a relative path test
    final File tempFile = tempDir.resolve("relative-test.txt").toFile();
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      fos.write("relative content".getBytes());
    }

    final InputStream stream = SocketFactory.getAsStream(tempFile.getAbsolutePath());
    assertThat(stream).isNotNull();
    stream.close();
  }
}
