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

import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.arcadedb.server.BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A BOLT connection already past the handshake and LOGON, exposing the chunked message pair, so a test can
 * assert on the PROTOCOL rather than on whatever a particular driver's fetch-size configuration happens to make
 * it send.
 * <p>
 * Lifted out of {@code BoltStateMachineIT}, which built it for issue #6804 and is still its largest user: the
 * status code a FAILURE carries is now asserted by tests that have nothing to do with the state machine (issue
 * #7874), and a second copy of a handshake would be a second thing to keep in step with the negotiated version
 * list.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BoltWireConnection implements AutoCloseable {
  /** The BOLT listener's default port, which the server tests leave at its default. */
  public static final int DEFAULT_BOLT_PORT = 7687;

  private final Socket            socket;
  private final BoltChunkedOutput out;
  private final BoltChunkedInput  in;

  /** Connects to the default BOLT port and authenticates as root. */
  public BoltWireConnection(final String database) throws IOException {
    this(DEFAULT_BOLT_PORT, database, "bolt-wire-connection/1.0");
  }

  public BoltWireConnection(final int port, final String database, final String userAgent) throws IOException {
    socket = new Socket("localhost", port);

    final OutputStream rawOut = socket.getOutputStream();
    final ByteBuffer handshake = ByteBuffer.allocate(20);
    handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
    handshake.putInt(0x00000405); // v5.4
    handshake.putInt(0x00000404); // v4.4
    handshake.putInt(0x00000003); // v3.0
    handshake.putInt(0x00000000);
    handshake.flip();
    rawOut.write(handshake.array());
    rawOut.flush();

    final DataInputStream rawIn = new DataInputStream(socket.getInputStream());
    final byte[] negotiated = new byte[4];
    rawIn.readFully(negotiated);
    assertThat(negotiated[3]).as("the deferred-auth path needs Bolt 5.x").isEqualTo((byte) 5);

    out = new BoltChunkedOutput(rawOut);
    in = new BoltChunkedInput(socket.getInputStream());

    sendMap(BoltMessage.HELLO, Map.of("user_agent", userAgent, "routing", Map.of("db", database)));
    assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

    logon();
  }

  public void logon() throws IOException {
    sendMap(BoltMessage.LOGON,
        Map.of("scheme", "basic", "principal", "root", "credentials", DEFAULT_PASSWORD_FOR_TESTS));
    assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }

  /** Writes a request whose single field is a map (HELLO, LOGON, BEGIN, PULL, DISCARD). */
  public void sendMap(final byte signature, final Map<String, Object> field) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(field);
    out.writeMessage(writer.toByteArray());
  }

  /** Writes a request with no fields at all (COMMIT, ROLLBACK, RESET, LOGOFF, GOODBYE). */
  public void sendNoFields(final byte signature) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(signature, 0);
    out.writeMessage(writer.toByteArray());
  }

  /** BEGIN on {@code database}, asserting it is accepted. */
  public void begin(final String database) throws IOException {
    sendMap(BoltMessage.BEGIN, Map.of("db", database));
    assertThat(readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }

  /** BEGIN on {@code database} WITHOUT asserting the answer, for a test whose subject is the refusal. */
  public void sendBegin(final String database) throws IOException {
    sendMap(BoltMessage.BEGIN, Map.of("db", database));
  }

  public void run(final String query) throws IOException {
    run(query, Map.of());
  }

  public void run(final String query, final Map<String, Object> extra) throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.RUN, 3);
    writer.writeString(query);
    writer.writeMap(Map.of());
    writer.writeMap(extra);
    out.writeMessage(writer.toByteArray());
  }

  public void pull(final long n, final long qid) throws IOException {
    sendMap(BoltMessage.PULL, streamSelector(n, qid));
  }

  public void discard(final long n, final long qid) throws IOException {
    sendMap(BoltMessage.DISCARD, streamSelector(n, qid));
  }

  private static Map<String, Object> streamSelector(final long n, final long qid) {
    final Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("n", n);
    extra.put("qid", qid);
    return extra;
  }

  /**
   * Reads RECORDs until the summary message (SUCCESS / FAILURE / IGNORED) that closes the exchange.
   */
  public Summary readSummary() throws IOException {
    final List<Object> records = new ArrayList<>();
    while (true) {
      final byte[] response = in.readMessage();
      final byte signature = response[1];
      if (signature == BoltMessage.RECORD) {
        records.add(decodeSingleField(response));
        continue;
      }
      // IGNORED is a zero-field structure, so there is no field to decode - reading one would EOF and bury the
      // real assertion under an IOException.
      if (signature == BoltMessage.IGNORED)
        return new Summary(signature, Map.of(), records);
      return new Summary(signature, asMetadata(decodeSingleField(response)), records);
    }
  }

  /** SUCCESS/FAILURE/RECORD are all single-field structures: skip the two header bytes, read the field. */
  private Object decodeSingleField(final byte[] response) throws IOException {
    final PackStreamReader reader = new PackStreamReader(response);
    reader.readRawByte();
    reader.readRawByte();
    return reader.readValue();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> asMetadata(final Object value) {
    return value instanceof Map ? (Map<String, Object>) value : Map.of();
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }

  public record Summary(byte signature, Map<String, Object> metadata, List<Object> records) {
    /** The Neo4j status code of a FAILURE, or null when this summary is not one. */
    public String code() {
      final Object code = metadata.get("code");
      return code != null ? code.toString() : null;
    }

    public String message() {
      final Object message = metadata.get("message");
      return message != null ? message.toString() : null;
    }
  }
}
