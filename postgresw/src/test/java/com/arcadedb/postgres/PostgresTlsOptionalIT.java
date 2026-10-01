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

import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8840: with {@code arcadedb.postgres.ssl=OPTIONAL} the client chooses, and a client that negotiates TLS
 * through the SSLRequest handshake (the JDBC driver at {@code sslmode=require}, {@code psql}, Spark) is served.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresTlsOptionalIT extends PostgresTlsTestBase {
  @Override
  protected String tlsMode() {
    return "OPTIONAL";
  }

  @Test
  void jdbcSslmodeRequireIsServed() throws Exception {
    try (final Connection connection = connect(true)) {
      assertThat(selectOne(connection)).isEqualTo(1);
    }
  }

  @Test
  void plaintextClientIsStillServed() throws Exception {
    try (final Connection connection = connect(false)) {
      assertThat(selectOne(connection)).isEqualTo(1);
    }
  }

  @Test
  void serverAnswersSAndTheStartupRunsInsideTls() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerPostgresPort())) {
      assertThat(sslRequestAnswer(socket)).isEqualTo((byte) 'S');

      try (final SSLSocket tls = startClientTls(socket)) {
        assertThat(tls.getSession().getProtocol()).startsWith("TLS");

        final DataOutputStream out = new DataOutputStream(tls.getOutputStream());
        final DataInputStream in = new DataInputStream(tls.getInputStream());
        sendStartupMessage(out, "root", getDatabaseName());
        // THE SERVER ASKS FOR A CLEAR-TEXT PASSWORD: 'R', length 8, code 3
        assertThat(in.readByte()).isEqualTo((byte) 'R');
        assertThat(in.readInt()).isEqualTo(8);
        assertThat(in.readInt()).isEqualTo(3);
      }
    }
  }

  @Test
  void aSecondSslRequestInsideTlsClosesTheConnection() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerPostgresPort())) {
      assertThat(sslRequestAnswer(socket)).isEqualTo((byte) 'S');

      try (final SSLSocket tls = startClientTls(socket)) {
        final DataOutputStream out = new DataOutputStream(tls.getOutputStream());
        out.writeInt(8);
        out.writeInt(SSL_REQUEST_CODE);
        out.flush();
        assertThat(tls.getInputStream().read()).isEqualTo(-1);
      }
    }
  }
}
