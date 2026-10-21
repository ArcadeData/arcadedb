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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The replies of a pipelined batch are written to the channel's buffer and flushed only when the next read would block,
 * instead of one flush per command. These tests pin what that must not change: every reply arrives, in the order of the
 * commands, whether the batch arrives in one write, in pieces, or followed by the client closing its side.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisPipeliningTest extends BaseRedisServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void pipelinedCommandsAreAnsweredInOrderIncludingAnError() {
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
      final Pipeline pipeline = jedis.pipelined();
      pipeline.set("pipe:a", "1");
      pipeline.incr("pipe:a");
      pipeline.set("pipe:text", "abc");
      // an error in the middle of the batch is answered in its place and does not stop the commands after it
      pipeline.incr("pipe:text");
      pipeline.get("pipe:a");
      pipeline.exists("pipe:a");
      pipeline.getDel("pipe:text");
      final List<Object> replies = pipeline.syncAndReturnAll();

      assertThat(replies).hasSize(7);
      assertThat(replies.get(0)).isEqualTo("OK");
      assertThat(replies.get(1)).isEqualTo(2L);
      assertThat(replies.get(2)).isEqualTo("OK");
      assertThat(replies.get(3)).isInstanceOf(JedisDataException.class);
      assertThat(replies.get(4)).isEqualTo("2");
      assertThat(replies.get(5)).isEqualTo(true);
      assertThat(replies.get(6)).isEqualTo("abc");

      // the connection is still in step after the batch
      assertThat(jedis.get("pipe:a")).isEqualTo("2");
    }
  }

  @Test
  void aLongPipelineGetsEveryReply() {
    final int count = 20_000;
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
      final Pipeline pipeline = jedis.pipelined();
      for (int i = 0; i < count; i++)
        pipeline.set("pipe:long:" + i, "v" + i);
      for (int i = 0; i < count; i++)
        pipeline.getDel("pipe:long:" + i);
      final List<Object> replies = pipeline.syncAndReturnAll();

      assertThat(replies).hasSize(2 * count);
      for (int i = 0; i < count; i++) {
        assertThat(replies.get(i)).isEqualTo("OK");
        assertThat(replies.get(count + i)).isEqualTo("v" + i);
      }
    }
  }

  @Test
  void repliesQueuedBeforeTheClientClosesItsSideAreStillSent() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      out.write(command("AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS));
      out.write(command("SET", "pipe:eof", "x"));
      out.write(command("GETDEL", "pipe:eof"));
      out.write(command("PING"));
      out.flush();
      // the end of the input is the last read the server makes: the replies it holds must have left before it
      socket.shutdownOutput();

      assertThat(readUntilEndOfStream(socket.getInputStream())).isEqualTo("+OK\r\n+OK\r\n$1\r\nx\r\n+PONG\r\n");
    }
  }

  @Test
  void aCommandSplitAcrossWritesAndALargeValueAreReadWhole() throws Exception {
    // larger than the 32KB input buffer and than the reply buffer the connection keeps, so both spill over
    final String large = "v".repeat(100_000);
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      final InputStream in = socket.getInputStream();
      out.write(command("AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS));
      out.flush();
      assertThat(readExactly(in, "+OK\r\n".length())).isEqualTo("+OK\r\n");

      final byte[] set = command("SET", "pipe:large", large);
      final int half = set.length / 2;
      out.write(set, 0, 7);
      out.flush();
      Thread.sleep(100);
      out.write(set, 7, half - 7);
      out.flush();
      Thread.sleep(100);
      out.write(set, half, set.length - half);
      out.flush();
      assertThat(readExactly(in, "+OK\r\n".length())).isEqualTo("+OK\r\n");

      out.write(command("GETDEL", "pipe:large"));
      out.flush();
      final String expected = "$" + large.length() + "\r\n" + large + "\r\n";
      assertThat(readExactly(in, expected.length())).isEqualTo(expected);
    }
  }

  @Test
  void lengthTokensKeepTheirParsingRules() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      out.write(command("AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS));
      // an explicit plus sign and a 10-digit token with leading zeros are lengths Integer.parseInt always accepted
      out.write("*+1\r\n$0000000004\r\nPING\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
      socket.shutdownOutput();
      assertThat(readUntilEndOfStream(socket.getInputStream())).isEqualTo("+OK\r\n+PONG\r\n");
    }

    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      // a length beyond an int is refused like any malformed one, naming its text
      out.write("*1\r\n$99999999999\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
      final String reply = readUntilEndOfStream(socket.getInputStream());
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).contains("bulk length '99999999999'");
    }
  }

  @Test
  void repliesQueuedBeforeAProtocolErrorAreSentBeforeIt() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      out.write(command("AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS));
      out.write(command("SET", "pipe:before-error", "v"));
      out.write(command("GETDEL", "pipe:before-error"));
      // the server answers a protocol error and closes: what it queued for the commands before must leave first
      out.write("*1\r\n$99999999999\r\n".getBytes(StandardCharsets.US_ASCII));
      out.flush();
      final String reply = readUntilEndOfStream(socket.getInputStream());
      assertThat(reply).startsWith("+OK\r\n+OK\r\n$1\r\nv\r\n-ERR");
    }
  }

  /**
   * One batch mixing replies that go through the connection's buffer (ASCII), around its 256KB bound, and replies written on
   * their own (non-ASCII, larger than the bound): they must all arrive whole and in the order of the commands.
   */
  @Test
  void repliesKeepTheirOrderAcrossTheBufferAndDirectWrites() throws Exception {
    final String nearTheBound = "n".repeat(262_100);
    // "$262133\r\n" + value + "\r\n" is exactly 262144 bytes, the bound itself
    final String atTheBound = "a".repeat(262_133);
    final String overTheBound = "o".repeat(300_000);
    final String nonAscii = "città-ü-€";
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      jedis.auth("root", DEFAULT_PASSWORD_FOR_TESTS);
      jedis.set("pipe:near", nearTheBound);
      jedis.set("pipe:at", atTheBound);
      jedis.set("pipe:over", overTheBound);
      jedis.set("pipe:utf8", nonAscii);
      jedis.set("pipe:small", "s");

      final Pipeline pipeline = jedis.pipelined();
      pipeline.get("pipe:small");
      pipeline.get("pipe:near");
      pipeline.get("pipe:utf8");
      pipeline.get("pipe:small");
      pipeline.get("pipe:over");
      pipeline.get("pipe:near");
      pipeline.get("pipe:small");
      pipeline.get("pipe:at");
      pipeline.get("pipe:small");
      final List<Object> replies = pipeline.syncAndReturnAll();

      assertThat(replies).containsExactly("s", nearTheBound, nonAscii, "s", overTheBound, nearTheBound, "s", atTheBound, "s");
    }
  }

  private static byte[] command(final String... parts) {
    final StringBuilder resp = new StringBuilder("*").append(parts.length).append("\r\n");
    for (final String part : parts)
      resp.append('$').append(part.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(part).append("\r\n");
    return resp.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String readUntilEndOfStream(final InputStream in) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final byte[] buffer = new byte[4096];
    for (int read = in.read(buffer); read > -1; read = in.read(buffer))
      bytes.write(buffer, 0, read);
    return bytes.toString(StandardCharsets.UTF_8);
  }

  private static String readExactly(final InputStream in, final int length) throws Exception {
    return new String(in.readNBytes(length), StandardCharsets.UTF_8);
  }
}
