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
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Shared scaffolding for integration tests that speak the PostgreSQL wire protocol directly over a raw
 * socket rather than through the JDBC driver, so that malformed or adversarial messages (declared-but-
 * undelivered lengths, unsupported type codes, etc.) can be crafted byte-for-byte.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class PostgresWireProtocolTestBase extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
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
