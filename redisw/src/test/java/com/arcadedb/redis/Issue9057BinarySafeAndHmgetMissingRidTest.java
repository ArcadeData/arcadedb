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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for:
 * <ul>
 *   <li>#9057 - a bulk string that is not valid UTF-8 came back with every bad byte replaced by U+FFFD.</li>
 *   <li>#9055 - {@code HMGET <db> <missing-rid> ...} wrote the array header, then an error, so the reply was shorter than
 *   announced and desynchronised the connection.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue9057BinarySafeAndHmgetMissingRidTest extends BaseRedisServerTest {

  private static final String USER     = "root";
  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void nonUtf8BytesRoundTripByteForByte() {
    final byte[][] values = {//
        { 0x61, 0x62, 0x63 },//
        { (byte) 0xc3, (byte) 0xa9 },//
        { (byte) 0xff, (byte) 0xfe, (byte) 0x80, 0x61, 0x62, 0x63 },//
        { (byte) 0x80 },//
        { (byte) 0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, (byte) 0xff },//
        // a valid 4-byte sequence followed by stray continuation bytes, and an encoded surrogate (invalid UTF-8)
        { (byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80, (byte) 0x80, (byte) 0xdc },//
        { (byte) 0xed, (byte) 0xb2, (byte) 0x80 },//
        // a truncated multi-byte sequence at the end
        { 0x41, (byte) 0xe2, (byte) 0x82 } };

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth(USER, PASSWORD);
      for (int i = 0; i < values.length; i++) {
        final byte[] key = ("bin" + i).getBytes(StandardCharsets.US_ASCII);
        assertThat(jedis.set(key, values[i])).isEqualTo("OK");
        assertThat(jedis.get(key)).as("value #" + i).isEqualTo(values[i]);
      }
    }
  }

  @Test
  void nonUtf8KeyStillFindsItsValue() {
    final byte[] key = { 'k', (byte) 0xff, (byte) 0x80 };
    final byte[] value = { (byte) 0xfe, 1, 2 };
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth(USER, PASSWORD);
      jedis.set(key, value);
      assertThat(jedis.get(key)).isEqualTo(value);
      assertThat(jedis.exists(key)).isTrue();
      assertThat(jedis.getDel(key)).isEqualTo(value);
      assertThat(jedis.exists(key)).isFalse();
    }
  }

  @Test
  void hmgetOfAMissingRidKeepsItsPlaceAsNull() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateDocumentType("Issue9055Item");
    final String[] existing = new String[1];
    database.transaction(() -> existing[0] = database.newDocument("Issue9055Item").set("n", 1).save().getIdentity().toString());

    final String missing = existing[0].substring(0, existing[0].indexOf(':') + 1) + "99999";

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth(USER, PASSWORD);
      assertThat(jedis.hmget(getDatabaseName(), existing[0], missing)).hasSize(2).satisfies(l -> {
        assertThat(l.get(0)).contains("\"n\":1");
        assertThat(l.get(1)).isNull();
      });
      final List<String> missingFirst = jedis.hmget(getDatabaseName(), missing, existing[0]);
      assertThat(missingFirst).hasSize(2);
      assertThat(missingFirst.get(0)).isNull();
      assertThat(missingFirst.get(1)).contains("\"n\":1");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }

    // the reply must also keep the pipelined PING in its own reply
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final String auth = "*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$" + PASSWORD.length() + "\r\n" + PASSWORD + "\r\n";
      final String hmget = "*4\r\n$5\r\nHMGET\r\n$" + getDatabaseName().length() + "\r\n" + getDatabaseName() + "\r\n$" + missing.length() + "\r\n"
          + missing + "\r\n$" + existing[0].length() + "\r\n" + existing[0] + "\r\n";
      socket.getOutputStream().write((auth + hmget + "*1\r\n$4\r\nPING\r\n").getBytes(StandardCharsets.UTF_8));
      socket.getOutputStream().flush();
      assertThat(line(socket)).isEqualTo("+OK");
      assertThat(line(socket)).isEqualTo("*2");
      assertThat(line(socket)).isEqualTo("$-1");
      final String size = line(socket);
      assertThat(size).startsWith("$");
      line(socket); // the JSON body
      assertThat(line(socket)).isEqualTo("+PONG");
    }
  }

  private static String line(final Socket socket) throws IOException {
    final InputStream in = socket.getInputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    int c;
    while ((c = in.read()) != -1) {
      if (c == '\n')
        break;
      if (c != '\r')
        out.write(c);
    }
    return out.toString(StandardCharsets.UTF_8);
  }
}
