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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12974: the version of the frontend protocol is the one thing the startup packet announces, and a client that
 * announced a version the server does not speak was served anyway: the server asked for the password and ran the session
 * as if nothing had been said about the version. PostgreSQL refuses such a packet with a fatal error naming the version
 * the client asked for and the one the server speaks, and so does this one.
 */
class PostgresProtocolVersionIT extends PostgresWireProtocolTestBase {
  /** The version PostgreSQL 3.0, the one this server speaks. */
  private static final int SUPPORTED_VERSION = 196608;
  /** A version nobody speaks: PostgreSQL 2.0. */
  private static final int UNSUPPORTED_VERSION = 131072;
  /** A value that is not a version at all. */
  private static final int NOT_A_VERSION       = 123456789;

  @Test
  void aClientThatAnnouncesTheSupportedVersionIsServed() throws Exception {
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, SUPPORTED_VERSION);

      // The password request: the handshake went on with the version the client announced.
      assertThat(in.readUnsignedByte()).isEqualTo('R');
      assertThat(in.readInt()).isEqualTo(8);
      assertThat(in.readInt()).isEqualTo(3);
    }
  }

  @Test
  void aClientThatAnnouncesAnUnsupportedVersionIsRefused() throws Exception {
    final Map<String, String> error = startupError(UNSUPPORTED_VERSION);

    assertThat(error.get("S")).as("the severity of the refusal").isEqualTo("FATAL");
    assertThat(error.get("C")).as("the SQLSTATE of the refusal").isEqualTo("08P01");
    assertThat(error.get("M")).as("the message of the refusal").isEqualTo("unsupported frontend protocol 2.0: server supports 3.0");
  }

  @Test
  void aClientThatAnnouncesAValueThatIsNotAVersionIsRefused() throws Exception {
    final Map<String, String> error = startupError(NOT_A_VERSION);

    assertThat(error.get("S")).as("the severity of the refusal").isEqualTo("FATAL");
    assertThat(error.get("C")).as("the SQLSTATE of the refusal").isEqualTo("08P01");
    assertThat(error.get("M")).as("the message of the refusal")
        .isEqualTo("unsupported frontend protocol 1883.52501: server supports 3.0");
  }

  @Test
  void theServerKeepsServingClientsAfterARefusedVersion() throws Exception {
    startupError(UNSUPPORTED_VERSION);

    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      try (final ResultSet rs = statement.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt("one")).isEqualTo(1);
      }
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /**
   * The ErrorResponse the server answers a startup packet that announces an unsupported version with.
   */
  private Map<String, String> startupError(final int protocolVersion) throws Exception {
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, protocolVersion);

      assertThat(in.readUnsignedByte()).as("the type of the answer").isEqualTo('E');
      final int length = in.readInt();
      final byte[] body = new byte[length - 4];
      in.readFully(body);

      final Map<String, String> fields = new LinkedHashMap<>();
      int pos = 0;
      while (pos < body.length && body[pos] != 0) {
        final char field = (char) body[pos++];
        int end = pos;
        while (end < body.length && body[end] != 0)
          end++;
        fields.put(String.valueOf(field), new String(body, pos, end - pos, StandardCharsets.UTF_8));
        pos = end + 1;
      }
      return fields;
    }
  }

  /**
   * The startup packet of the PostgreSQL protocol: the length, the version, and the {@code name\0value\0} parameters.
   */
  private void sendStartupMessage(final DataOutputStream out, final int protocolVersion) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "user");
    writeCString(body, "root");
    writeCString(body, "database");
    writeCString(body, getDatabaseName());
    body.write(0);

    final byte[] bodyBytes = body.toByteArray();
    out.writeInt(4 + 4 + bodyBytes.length);
    out.writeInt(protocolVersion);
    out.write(bodyBytes);
    out.flush();
  }

  private Socket openSocket() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 5_000);
    socket.setSoTimeout(30_000);
    return socket;
  }
}
