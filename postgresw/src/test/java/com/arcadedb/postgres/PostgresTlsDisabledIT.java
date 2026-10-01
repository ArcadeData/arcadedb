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

import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8840: the default, {@code arcadedb.postgres.ssl=DISABLED}, keeps the behavior of releases without TLS: an
 * SSLRequest is answered {@code N} and the client continues in plaintext.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresTlsDisabledIT extends PostgresTlsTestBase {
  @Override
  protected String tlsMode() {
    return "DISABLED";
  }

  @Test
  void sslRequestIsAnsweredN() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerPostgresPort())) {
      assertThat(sslRequestAnswer(socket)).isEqualTo((byte) 'N');
    }
  }

  @Test
  void plaintextClientIsServed() throws Exception {
    try (final Connection connection = connect(false)) {
      assertThat(selectOne(connection)).isEqualTo(1);
    }
  }

  @Test
  void sslmodeRequireClientCannotConnect() {
    assertThatThrownBy(() -> connect(true).close()).isInstanceOf(SQLException.class);
  }
}
