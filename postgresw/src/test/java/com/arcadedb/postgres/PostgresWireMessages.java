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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Builders and readers for the PostgreSQL wire messages the extended query protocol is made of, for the tests
 * that speak it over a raw socket rather than through the JDBC driver.
 * <p>
 * Kept apart from {@link PostgresWireProtocolTestBase} on purpose: a test class that already carries its own
 * private copy of one of these helpers cannot also inherit a package-private one of the same signature, so
 * putting them in the base class would force every existing wire-protocol test to be rewritten at once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PostgresWireMessages {

  private PostgresWireMessages() {
  }

  record WireMessage(char type, byte[] body) {
  }

  static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length + 1);
    out.write(queryBytes);
    out.writeByte(0);
    out.flush();
  }

  /**
   * Parse ('P') declaring a prepared statement with no parameter types - enough for the parameter-less
   * statements these tests pipeline.
   */
  static void sendParse(final DataOutputStream out, final String statementName, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, statementName);
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0
    sendMessage(out, 'P', body);
  }

  /**
   * Bind ('B') creating a portal from an already-parsed statement, with no parameters and no requested result
   * formats.
   */
  static void sendBind(final DataOutputStream out, final String portalName, final String statementName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, statementName);
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(0); // int16 numParamValues = 0
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0
    sendMessage(out, 'B', body);
  }

  /**
   * Describe ('D') of a bound portal ({@code 'P'}) or of a prepared statement ({@code 'S'}).
   */
  static void sendDescribe(final DataOutputStream out, final char describeType, final String name) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(describeType);
    writeCString(body, name);
    sendMessage(out, 'D', body);
  }

  /**
   * Close ('C') of a bound portal ({@code 'P'}) or of a prepared statement ({@code 'S'}).
   */
  static void sendClose(final DataOutputStream out, final char closeType, final String name) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(closeType);
    writeCString(body, name);
    sendMessage(out, 'C', body);
  }

  static void sendExecute(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0); // int32 row limit = 0 (no limit)
    sendMessage(out, 'E', body);
  }

  static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  static WireMessage readWireMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] body = new byte[length - 4];
    in.readFully(body);
    return new WireMessage((char) type, body);
  }

  /**
   * Reads messages until (and including) the next {@code ReadyForQuery}, so a whole round trip - or a whole
   * pipeline terminated by one Sync - can be inspected at once.
   */
  static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  static char readyForQueryStatusOf(final List<WireMessage> messages) {
    final WireMessage last = messages.get(messages.size() - 1);
    assertThat(last.type()).isEqualTo('Z');
    return (char) last.body()[0];
  }

  static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }

  /**
   * Parses an {@code ErrorResponse}'s {@code code letter -> null-terminated value} fields (e.g. {@code 'M'} for
   * the message, {@code 'C'} for the SQLSTATE code) - the format {@code writeError()} produces.
   */
  static Map<Character, String> errorFields(final WireMessage message) {
    assertThat(message.type()).isEqualTo('E');
    final Map<Character, String> fields = new LinkedHashMap<>();
    final byte[] body = message.body();
    int i = 0;
    while (i < body.length && body[i] != 0) {
      final char code = (char) body[i++];
      final int start = i;
      while (body[i] != 0)
        i++;
      fields.put(code, new String(body, start, i - start, StandardCharsets.UTF_8));
      i++; // skip this field's terminator
    }
    return fields;
  }

  /**
   * Runs {@code SHOW <name>} over the simple-query protocol and returns the one text value its single DataRow carries
   * (issue #8217: a SET is session-scoped, so the connection that ran it is where its effect is read back).
   */
  static String show(final DataOutputStream out, final DataInputStream in, final String name) throws Exception {
    sendSimpleQuery(out, "SHOW " + name);
    return firstDataRowValue(readUntilReadyForQuery(in));
  }

  /**
   * The SQLSTATE of the first {@code ErrorResponse} among {@code messages}.
   */
  static String sqlStateOf(final List<WireMessage> messages) {
    return errorFields(messages.stream().filter(m -> m.type() == 'E').findFirst().orElseThrow()).get('C');
  }

  /**
   * The first column of the first DataRow among {@code messages}, as text.
   */
  static String firstDataRowValue(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'D') {
        final ByteBuffer row = ByteBuffer.wrap(message.body());
        row.getShort(); // column count
        final byte[] value = new byte[row.getInt()];
        row.get(value);
        return new String(value, StandardCharsets.UTF_8);
      }
    throw new AssertionError("no DataRow among " + messageTypesOf(messages));
  }

  private static void sendMessage(final DataOutputStream out, final char type, final ByteArrayOutputStream body) throws Exception {
    final byte[] bodyBytes = body.toByteArray();
    out.writeByte(type);
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void writeCString(final ByteArrayOutputStream out, final String s) {
    out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    out.write(0);
  }
}
