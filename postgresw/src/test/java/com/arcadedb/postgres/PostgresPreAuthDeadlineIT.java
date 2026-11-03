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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The PostgreSQL listener inherits the pre-authentication deadline of the gate every binary listener takes its connections
 * from: a client that trickles its startup message one byte at a time is never idle, so the handshake read timeout never fires
 * for it, and it held its pre-authentication slot for as long as it liked. It is now closed once the deadline from the accept
 * passes, and the listener keeps serving.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresPreAuthDeadlineIT extends PostgresWireProtocolTestBase {

  private static final long DEADLINE_MS = 2_000;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.setValue(DEADLINE_MS);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.reset();
    } finally {
      super.endTest();
    }
  }

  @Test
  void aStartupMessageTrickledByteByByteIsClosedAtTheDeadline() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 5_000);
      socket.setSoTimeout(300);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      // a plausible startup message header: 1000 bytes declared, protocol 3.0, then the body one byte at a time
      out.writeInt(1_000);
      out.writeInt(196608);
      out.flush();

      // a hang detector, not a latency bound: the deadline is 2 s and the reaper ticks every second
      final long giveUpAt = System.currentTimeMillis() + 30_000;
      boolean closed = false;
      while (!closed && System.currentTimeMillis() < giveUpAt) {
        try {
          out.write('u');
          out.flush();
          closed = socket.getInputStream().read() == -1;
        } catch (final SocketTimeoutException e) {
          // nothing answered, still open: send the next byte
        } catch (final SocketException e) {
          closed = true;
        }
      }
      if (!closed)
        fail("the server kept a connection that never authenticated open past its pre-authentication deadline");
    }

    // the listener is unharmed
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT 1")) {
      assertThat(resultSet.next()).isTrue();
      assertThat(resultSet.getInt(1)).isEqualTo(1);
    }
  }

  private Connection openJdbcConnection() throws IOException {
    try {
      Class.forName("org.postgresql.Driver");
      final Properties properties = new Properties();
      properties.setProperty("user", "root");
      properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
      properties.setProperty("ssl", "false");
      properties.setProperty("sslMode", "disable");
      properties.setProperty("preferQueryMode", "simple");
      return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }
}
