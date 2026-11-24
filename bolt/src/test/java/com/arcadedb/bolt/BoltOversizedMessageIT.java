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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * A request that is larger than the configured message size (or a WebSocket frame larger than the configured frame size)
 * is refused before its payload is read, so the stream is no longer aligned on a message boundary: the unread bytes would
 * be taken for the start of the next request. The connection must therefore end, never go on reading from the same stream.
 * <p>
 * Each test sends one oversized message whose payload is laid out as complete requests (RESET, RUN, PULL) and checks that
 * the server ends the connection without answering them, and that the statement they carry did not run.
 *
 * @author lvca
 */
public class BoltOversizedMessageIT extends BaseBoltServerTest {
  private static final int MESSAGE_LIMIT   = 4096;
  private static final int FRAME_LIMIT     = 4096;
  private static final int READ_TIMEOUT_MS = 10_000;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE.setValue(MESSAGE_LIMIT);
    GlobalConfiguration.BOLT_WEBSOCKET_MAX_FRAME_SIZE.setValue(FRAME_LIMIT);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.BOLT_MAX_MESSAGE_SIZE.reset();
    GlobalConfiguration.BOLT_WEBSOCKET_MAX_FRAME_SIZE.reset();
    super.endTest();
  }

  @Test
  void aMessageOverTheSizeLimitEndsTheConnectionAndItsBytesAreNotRead() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.setReadTimeout(READ_TIMEOUT_MS);

      final ByteArrayOutputStream carried = new ByteArrayOutputStream();
      for (final byte[] request : requests("ChunkedMarker"))
        carried.write(chunked(request));
      bolt.sendRaw(oversizedChunk(carried.toByteArray()));

      assertTheConnectionEnds(() -> bolt.readSummary().signature());
    }

    assertNothingWasCreated("ChunkedMarker");
  }

  @Test
  void aWebSocketFrameOverTheSizeLimitEndsTheConnectionAndItsBytesAreNotRead() throws Exception {
    try (final WebSocketBolt ws = new WebSocketBolt(getServerBoltPort())) {
      ws.authenticate(getDatabaseName());

      final ByteArrayOutputStream carried = new ByteArrayOutputStream();
      for (final byte[] request : requests("FramedMarker"))
        carried.write(unmaskedFrame(chunked(request)));
      ws.sendRaw(oversizedFrame(carried.toByteArray()));

      assertTheConnectionEnds(ws::nextSignature);
    }

    assertNothingWasCreated("FramedMarker");
  }

  /** What a message just under the limit still does: it is served, in as many chunks as it takes. */
  @Test
  void aMessageWithinTheSizeLimitIsStillServed() {
    final String value = "v".repeat(MESSAGE_LIMIT / 2);
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS));
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThat(session.run("RETURN $v AS v", Map.of("v", value)).single().get("v").asString()).isEqualTo(value);
    }
  }

  private void assertNothingWasCreated(final String label) throws IOException {
    // A new connection: the server is still serving everybody else
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("MATCH (n:" + label + ") RETURN count(n) AS c");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.pull(-1, -1);
      assertThat(bolt.readSummary().records()).containsExactly(List.of(0L));
    }
  }

  /**
   * The server may answer the oversized message once, with a FAILURE, and then it ends the connection: whatever else it
   * sends means it read the payload as requests, and a read that times out means it left the connection open.
   */
  private static void assertTheConnectionEnds(final SignatureReader reader) throws IOException {
    try {
      while (true)
        assertThat(reader.next()).as("the server went on serving the connection after the size limit").isEqualTo(BoltMessage.FAILURE);
    } catch (final SocketTimeoutException e) {
      fail("the server kept the connection open after the size limit");
    } catch (final EOFException | SocketException e) {
      // The connection ended
    }
  }

  /** RESET, then a RUN of a CREATE, then the PULL that would fetch it: three requests, unframed. */
  private static List<byte[]> requests(final String label) throws IOException {
    final PackStreamWriter reset = new PackStreamWriter();
    reset.writeStructureHeader(BoltMessage.RESET, 0);

    final PackStreamWriter run = new PackStreamWriter();
    run.writeStructureHeader(BoltMessage.RUN, 3);
    run.writeString("CREATE (:" + label + " {origin: 'stream'})");
    run.writeMap(Map.of());
    run.writeMap(Map.of());

    final PackStreamWriter pull = new PackStreamWriter();
    pull.writeStructureHeader(BoltMessage.PULL, 1);
    pull.writeMap(Map.<String, Object>of("n", -1L));

    return List.of(reset.toByteArray(), run.toByteArray(), pull.toByteArray());
  }

  private static byte[] chunked(final byte[] message) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    new BoltChunkedOutput(bytes).writeMessage(message);
    return bytes.toByteArray();
  }

  /** One chunk that declares more than the limit, with {@code carried} at the start of its payload, then the terminator. */
  private static byte[] oversizedChunk(final byte[] carried) throws IOException {
    final int declared = MESSAGE_LIMIT * 2;
    assertThat(carried.length).isLessThan(declared);

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bytes);
    out.writeShort(declared);
    out.write(carried);
    out.write(new byte[declared - carried.length]);
    out.writeShort(0);
    return bytes.toByteArray();
  }

  /** A binary frame header that declares more than the limit, with {@code carried} at the start of its payload. */
  private static byte[] oversizedFrame(final byte[] carried) throws IOException {
    final int declared = FRAME_LIMIT * 2;
    assertThat(carried.length).isLessThan(declared);

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0x82);
    out.writeByte(126);
    out.writeShort(declared);
    out.write(carried);
    out.write(new byte[declared - carried.length]);
    return bytes.toByteArray();
  }

  private static byte[] unmaskedFrame(final byte[] payload) {
    assertThat(payload.length).isLessThan(126);
    final byte[] frame = new byte[payload.length + 2];
    frame[0] = (byte) 0x82;
    frame[1] = (byte) payload.length;
    System.arraycopy(payload, 0, frame, 2, payload.length);
    return frame;
  }

  /** The signature of the next message the server sent. */
  @FunctionalInterface
  private interface SignatureReader {
    byte next() throws IOException;
  }

  /** Just enough of a WebSocket client to carry Bolt: masked binary frames out, whole binary frames in. */
  private static final class WebSocketBolt implements AutoCloseable {
    private final Socket           socket;
    private final DataInputStream  in;
    private final OutputStream     out;
    private final ArrayDeque<Byte> pending = new ArrayDeque<>();

    WebSocketBolt(final int port) throws IOException {
      socket = new Socket("localhost", port);
      socket.setSoTimeout(READ_TIMEOUT_MS);
      in = new DataInputStream(socket.getInputStream());
      out = socket.getOutputStream();
    }

    void authenticate(final String database) throws IOException {
      final String upgrade = "GET / HTTP/1.1\r\nHost: localhost:" + socket.getPort() + "\r\nUpgrade: websocket\r\n"
          + "Connection: Upgrade\r\nSec-WebSocket-Key: dGVzdC13ZWJzb2NrZXQta2V5IQ==\r\nSec-WebSocket-Version: 13\r\n\r\n";
      sendRaw(upgrade.getBytes(StandardCharsets.UTF_8));
      final StringBuilder response = new StringBuilder();
      while (response.length() < 4 || !response.substring(response.length() - 4).equals("\r\n\r\n"))
        response.append((char) in.readUnsignedByte());
      assertThat(response.toString()).startsWith("HTTP/1.1 101");

      // Bolt magic, then one proposed version: 4.4
      sendFrame(new byte[] { 0x60, 0x60, (byte) 0xB0, 0x17, 0, 0, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
      assertThat(readFrame()).containsExactly(0, 0, 4, 4);

      final PackStreamWriter hello = new PackStreamWriter();
      hello.writeStructureHeader(BoltMessage.HELLO, 1);
      hello.writeMap(Map.of("user_agent", "framing-test/1.0", "scheme", "basic", "principal", "root", "credentials",
          DEFAULT_PASSWORD_FOR_TESTS, "routing", Map.of("db", database)));
      sendFrame(chunked(hello.toByteArray()));
      assertThat(nextSignature()).isEqualTo(BoltMessage.SUCCESS);
    }

    void sendRaw(final byte[] bytes) throws IOException {
      out.write(bytes);
      out.flush();
    }

    /** A masked binary frame, as a client must send it. */
    void sendFrame(final byte[] payload) throws IOException {
      final byte[] maskKey = { 0x12, 0x34, 0x56, 0x78 };
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final DataOutputStream frame = new DataOutputStream(bytes);
      frame.writeByte(0x82);
      if (payload.length < 126)
        frame.writeByte(0x80 | payload.length);
      else {
        frame.writeByte(0x80 | 126);
        frame.writeShort(payload.length);
      }
      frame.write(maskKey);
      for (int i = 0; i < payload.length; i++)
        frame.writeByte(payload[i] ^ maskKey[i % 4]);
      sendRaw(bytes.toByteArray());
    }

    /** The payload of the next frame the server sent (the server does not mask). */
    byte[] readFrame() throws IOException {
      in.readUnsignedByte();
      int length = in.readUnsignedByte() & 0x7F;
      if (length == 126)
        length = in.readUnsignedShort();
      final byte[] payload = new byte[length];
      in.readFully(payload);
      return payload;
    }

    byte nextSignature() throws IOException {
      while (pending.isEmpty()) {
        final BoltChunkedInput chunks = new BoltChunkedInput(new ByteArrayInputStream(readFrame()));
        while (chunks.available() > 0)
          pending.add(chunks.readMessage()[1]);
      }
      return pending.poll();
    }

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
