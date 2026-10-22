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
package com.arcadedb.mongo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every door of the MongoDB backend that reaches a database must authorize the request: a legacy OP_QUERY on a collection is
 * handed by the library to {@code MongoBackend.handleQuery}, past every command, and must not read a document for a client
 * that never authenticated. The wire tests send the frame by hand, because every driver speaks OP_MSG and would never reach
 * that door.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBLegacyOpQueryAuthorizationTest extends BaseMongoServerTest {
  private static final String SECRET  = "topsecret-value-42";
  private static final int    OP_QUERY = 2004;

  /**
   * The request-carrying {@link MongoBackend} entry points that resolve no database at all, so they need no authorization.
   * Any other one must refuse an unauthenticated channel: a library upgrade adding one fails here and gets a decision.
   */
  private static final Set<String> READS_NO_DATABASE = Set.of("handleClose", "getCurrentOperations");

  private Channel channel;

  @AfterEach
  void closeTheChannel() {
    if (channel != null)
      channel.close();
  }

  @Test
  void anUnauthenticatedOpQueryOnACollectionReadsNothing() throws Exception {
    seedSecret();

    // REFUSED: THE LIBRARY HAS NO ERROR REPLY ON THIS DOOR, SO THE REFUSAL CLOSES THE CONNECTION
    assertThat(sendOpQuery(getDatabaseName() + ".Secret", emptyDocument())).isNull();
  }

  @Test
  void anUnauthenticatedOpQueryOnTheCommandCollectionIsRefusedToo() throws Exception {
    seedSecret();

    assertThat(sendOpQuery(getDatabaseName() + ".$cmd", findDocument("Secret"))).isNull();
  }

  @Test
  void theHandshakeStaysPublic() throws Exception {
    final String reply = sendOpQuery("admin.$cmd", stringDocument("isMaster", "1"));

    assertThat(reply).isNotNull().contains("ismaster");
    assertThat(reply).doesNotContain("Unauthorized");
  }

  @Test
  void everyDatabaseReachingEntryPointRefusesAnUnauthenticatedChannel() {
    seedSecret();
    final MongoDBBackend backend = new MongoDBBackend(getServer(0), null);
    channel = new EmbeddedChannel();

    final Set<String> requestCarrying = new TreeSet<>();
    for (final Method method : MongoBackend.class.getMethods())
      if (!Modifier.isStatic(method.getModifiers()) && Arrays.stream(method.getParameterTypes())
          .anyMatch(t -> t == Channel.class || t == MongoQuery.class || t == MongoMessage.class))
        requestCarrying.add(method.getName());

    final Set<String> gated = new TreeSet<>(requestCarrying);
    gated.removeAll(READS_NO_DATABASE);
    assertThat(gated).as("a new database-reaching MongoBackend entry point must be authorized and driven here")
        .isEqualTo(Set.of("handleCommand", "handleMessage", "handleQuery"));

    assertThatThrownBy(() -> backend.handleQuery(
        new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + ".Secret", 0, 0, new Document(), null)))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");

    // ON THE ADMIN NAMESPACE TOO: THE LIBRARY RESOLVES THE DATABASE BY NAME, SO "admin" IS NO EXEMPTION ON THIS DOOR
    assertThatThrownBy(() -> backend.handleQuery(
        new MongoQuery(channel, new MessageHeader(1, 0), "admin.Secret", 0, 0, new Document(), null)))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");

    assertThatThrownBy(() -> backend.handleCommand(channel, getDatabaseName(), "find", new Document("find", "Secret")))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");

    final Document message = new Document("find", "Secret");
    message.put("$db", getDatabaseName());
    assertThatThrownBy(() -> backend.handleMessage(new MongoMessage(channel, new MessageHeader(1, 0), message)))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");

    // "local" AND "config" ARE RESOLVED AS DATABASES BY THE LIBRARY: A DATABASE CALLED THAT GETS NO EXEMPTION
    assertThatThrownBy(() -> backend.handleCommand(channel, "local", "find", new Document("find", "Secret")))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");

    // ADMIN COMMANDS THAT LIST DATABASES OR TOUCH ANOTHER CLIENT'S CURSORS NEED A LOGIN; THE HANDSHAKE DOES NOT
    assertThatThrownBy(() -> backend.handleCommand(channel, "admin", "listDatabases", new Document("listDatabases", 1)))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");
    assertThatThrownBy(() -> backend.handleCommand(channel, "admin", "killCursors", new Document("killCursors", "x")))
        .isInstanceOf(MongoServerError.class).hasMessageContaining("requires authentication");
    assertThat(backend.handleCommand(channel, "admin", "isMaster", new Document("isMaster", 1)).get("ismaster")).isEqualTo(
        Boolean.TRUE);
    assertThat(backend.handleCommand(channel, "admin", "ping", new Document("ping", 1)).get("ok")).isNotNull();
  }

  @Test
  void anAuthenticatedOpQueryStillReads() {
    seedSecret();
    final MongoDBBackend backend = new MongoDBBackend(getServer(0), null);
    channel = new EmbeddedChannel();

    final byte[] payload = ("\u0000root\u0000" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8);
    final Document sasl = new Document("saslStart", 1);
    sasl.put("mechanism", "PLAIN");
    sasl.put("payload", new BinData(payload));
    backend.handleCommand(channel, getDatabaseName(), "saslStart", sasl);

    final var result = backend.handleQuery(
        new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + ".Secret", 0, 10, new Document(), null));
    assertThat(result.collectDocuments()).anyMatch(d -> SECRET.equals(d.get("name")));
  }

  private void seedSecret() {
    final Database db = getServerDatabase(0, getDatabaseName());
    if (!db.getSchema().existsType("Secret"))
      db.command("sqlscript", "CREATE DOCUMENT TYPE Secret;\nINSERT INTO Secret SET name = '" + SECRET + "';");
  }

  /**
   * Sends one legacy OP_QUERY frame on a fresh, never authenticated connection and answers the raw reply as text, or null when
   * the server closed the connection instead of replying.
   */
  private String sendOpQuery(final String fullCollectionName, final byte[] query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(intLE(0)); // flags
    body.write(fullCollectionName.getBytes(StandardCharsets.UTF_8));
    body.write(0);
    body.write(intLE(0)); // numberToSkip
    body.write(intLE(10)); // numberToReturn
    body.write(query);

    final ByteArrayOutputStream frame = new ByteArrayOutputStream();
    frame.write(intLE(16 + body.size()));
    frame.write(intLE(7)); // requestID
    frame.write(intLE(0)); // responseTo
    frame.write(intLE(OP_QUERY));
    frame.write(body.toByteArray());

    try (final Socket socket = new Socket("localhost", getServerMongoPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      out.write(frame.toByteArray());
      out.flush();

      final InputStream in = socket.getInputStream();
      final DataInputStream data = new DataInputStream(in);
      final byte[] lengthBytes = new byte[4];
      try {
        data.readFully(lengthBytes);
      } catch (final EOFException e) {
        return null;
      }
      final int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
      final byte[] rest = new byte[length - 4];
      data.readFully(rest);
      return new String(rest, StandardCharsets.ISO_8859_1);
    }
  }

  private static byte[] emptyDocument() {
    return new byte[] { 5, 0, 0, 0, 0 };
  }

  private static byte[] findDocument(final String collection) throws Exception {
    return stringDocument("find", collection);
  }

  /** A BSON document holding one string field. */
  private static byte[] stringDocument(final String key, final String value) throws Exception {
    final byte[] k = key.getBytes(StandardCharsets.UTF_8);
    final byte[] v = value.getBytes(StandardCharsets.UTF_8);
    final ByteArrayOutputStream element = new ByteArrayOutputStream();
    element.write(0x02);
    element.write(k);
    element.write(0);
    element.write(intLE(v.length + 1));
    element.write(v);
    element.write(0);

    final ByteArrayOutputStream document = new ByteArrayOutputStream();
    document.write(intLE(4 + element.size() + 1));
    document.write(element.toByteArray());
    document.write(0);
    return document.toByteArray();
  }

  private static byte[] intLE(final int value) {
    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
