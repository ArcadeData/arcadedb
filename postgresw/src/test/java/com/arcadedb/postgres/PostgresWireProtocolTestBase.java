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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;

import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Shared scaffolding for the integration tests that start the Postgres plugin, whether they speak the PostgreSQL
 * wire protocol directly over a raw socket (so that malformed or adversarial messages - declared-but-undelivered
 * lengths, unsupported type codes, etc. - can be crafted byte-for-byte) or go through the JDBC driver.
 * <p>
 * Every such test must connect to {@link #getServerPostgresPort()}, never to a hardcoded 5432 nor to the configured
 * {@link GlobalConfiguration#POSTGRES_PORT}: the server is started on {@link #TEST_POSTGRES_PORT_RANGE} and binds
 * the first port of it that is free (issue #8142).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class PostgresWireProtocolTestBase extends BaseGraphServerTest {
  /**
   * The ports a test server may listen on for the Postgres protocol, the counterpart of the {@code 2480-2489}
   * default of {@code arcadedb.server.httpIncomingPort}: a port already taken - by a second concurrent build, or by
   * anything else - is skipped, and {@link #getServerPostgresPort()} reports the one chosen (issue #8142).
   * <p>
   * The range deliberately leaves out the production default 5432. A developer's own PostgreSQL usually listens
   * there on {@code localhost} only, and on macOS a listener binding {@code 0.0.0.0} (with the {@code SO_REUSEADDR}
   * a JDK server socket sets) SUCCEEDS next to it: the test server would then start cleanly while every connection
   * to {@code localhost:5432} reached the stranger, which reads as an authentication failure, not a port conflict.
   */
  static final String TEST_POSTGRES_PORT_RANGE = "5433-5442";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
    // After resetAll(), which super.setTestConfiguration() runs: set earlier, or from a system property or an
    // environment variable, the value would be discarded before the server reads it.
    GlobalConfiguration.POSTGRES_PORT.setValue(TEST_POSTGRES_PORT_RANGE);
  }

  /**
   * The Postgres port the first server ACTUALLY bound, which is not necessarily the first of
   * {@link #TEST_POSTGRES_PORT_RANGE}.
   */
  protected int getServerPostgresPort() {
    return getServerPostgresPort(0);
  }

  /**
   * The Postgres port server {@code serverIndex} ACTUALLY bound, the counterpart of
   * {@link BaseGraphServerTest#getServerHttpPort(int)}.
   */
  protected int getServerPostgresPort(final int serverIndex) {
    return getServerPostgresPort(getServer(serverIndex));
  }

  /**
   * @throws IllegalStateException when the server is not started or does not run the Postgres plugin, because there
   *                               is no port to answer with and a guess would reintroduce the defect this method
   *                               exists to remove
   */
  static int getServerPostgresPort(final ArcadeDBServer server) {
    if (server != null)
      for (final ServerPlugin plugin : server.getPlugins())
        if (plugin instanceof PostgresProtocolPlugin postgres && postgres.getPort() > 0)
          return postgres.getPort();
    throw new IllegalStateException("The Postgres plugin is not listening: it has not bound a port, so there is none to address");
  }

  /**
   * {@code jdbc:postgresql://localhost:<the port the server actually bound>/<the test database>}.
   */
  protected String getServerPostgresJdbcUrl() {
    return "jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + getDatabaseName();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
    super.endTest();
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

  static void sendStartupMessage(final DataOutputStream out, final String user, final String database) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "user");
    writeCString(body, user);
    writeCString(body, "database");
    writeCString(body, database);
    body.write(0);

    final byte[] bodyBytes = body.toByteArray();
    out.writeInt(4 + 4 + bodyBytes.length);
    out.writeInt(196608); // protocol version 3.0
    out.write(bodyBytes);
    out.flush();
  }

  static void sendPasswordMessage(final DataOutputStream out, final String password) throws Exception {
    final byte[] pwBytes = password.getBytes(StandardCharsets.UTF_8);
    out.writeByte('p');
    out.writeInt(4 + pwBytes.length + 1);
    out.write(pwBytes);
    out.writeByte(0);
    out.flush();
  }

  static void writeCString(final ByteArrayOutputStream out, final String s) {
    out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    out.write(0);
  }

  static void readMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    in.skipNBytes(length - 4);
  }

  static void readMessageOfType(final DataInputStream in, final char expectedType) throws Exception {
    while (true) {
      final int type = in.readUnsignedByte();
      final int length = in.readInt();
      in.skipNBytes(length - 4);
      if (type == expectedType)
        return;
    }
  }

  static int readMessageType(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    in.skipNBytes(length - 4);
    return type;
  }
}
