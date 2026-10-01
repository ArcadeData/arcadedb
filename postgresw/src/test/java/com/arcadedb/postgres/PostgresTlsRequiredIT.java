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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.sql.Connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8840: with {@code arcadedb.postgres.ssl=REQUIRED} a plaintext startup is refused before any credential can
 * be sent, while TLS clients are served.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresTlsRequiredIT extends PostgresTlsTestBase {
  @Override
  protected String tlsMode() {
    return "REQUIRED";
  }

  @Test
  void jdbcSslmodeRequireIsServed() throws Exception {
    try (final Connection connection = connect(true)) {
      assertThat(selectOne(connection)).isEqualTo(1);
    }
  }

  @Test
  void plaintextJdbcClientIsRefused() {
    assertThatThrownBy(() -> connect(false).close()).hasMessageContaining("SSL connection is required");
  }

  @Test
  void plaintextStartupGetsAnErrorResponseAndNoPasswordRequest() throws Exception {
    try (final Socket socket = newSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());

      assertThat(in.readByte()).isEqualTo((byte) 'E');
      final int length = in.readInt();
      final byte[] body = new byte[length - 4];
      in.readFully(body);
      assertThat(new String(body)).contains("SSL connection is required");
      assertThat(in.read()).isEqualTo(-1);
    }
  }

  @Test
  void plaintextCancelRequestIsStillAcceptedAndNeverGetsTheFatalError() throws Exception {
    try (final Socket socket = newSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      out.writeInt(16);
      out.writeInt(80877102); // CANCEL REQUEST
      out.writeInt(123456);
      out.writeInt(654321);
      out.flush();
      // THE SERVER CLOSES WITHOUT ANSWERING: NO ERRORRESPONSE
      assertThat(readOrClosed(socket.getInputStream())).isEqualTo(-1);
    }
  }
}
