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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What the MongoDB plugin writes to the log when a request is refused or fails: the name of the command and the reason, never the
 * request document, and the text a client sent is shown with printable characters only. The log is read from the JUL loggers the
 * library (through SLF4J) and the plugin write to.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MongoRequestLoggingTest extends BaseMongoServerTest {
  private static final int COMMAND_NOT_FOUND     = 59;
  private static final int UNAUTHORIZED          = 13;
  private static final int AUTHENTICATION_FAILED = 18;
  private static final int OP_MSG                = 2013;

  private final CapturedLog captured = new CapturedLog();
  private final Logger      library  = Logger.getLogger("de.bwaldvogel");
  private final Logger      plugin   = Logger.getLogger("com.arcadedb.mongo");
  private final Logger      root     = Logger.getLogger("");
  private       Level       libraryLevel;
  private       Level       pluginLevel;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  void startCapturing() {
    libraryLevel = library.getLevel();
    pluginLevel = plugin.getLevel();
    // the library at the level of the distribution's logging configuration (its own dump of every request at DEBUG is opt-in), the
    // plugin at every level
    library.setLevel(Level.INFO);
    plugin.setLevel(Level.ALL);
    captured.setLevel(Level.ALL);
    root.addHandler(captured);
  }

  @AfterEach
  @Override
  public void endTest() {
    root.removeHandler(captured);
    library.setLevel(libraryLevel);
    plugin.setLevel(pluginLevel);
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /** An OP_MSG, which every current driver sends once it is connected. */
  @Test
  void aRefusedLoginIsLoggedWithoutTheRequestPayload() throws Exception {
    final String password = "wrong-" + UUID.randomUUID();
    final byte[] payload = ("\u0000root\u0000" + password).getBytes(StandardCharsets.UTF_8);

    final BsonDocument reply = sendOpMsg(new BsonDocument("saslStart", new BsonInt32(1)).append("mechanism", new BsonString("PLAIN"))
        .append("payload", new BsonBinary(payload)).append("$db", new BsonString(getDatabaseName())));

    // the client is told as before
    assertThat(reply.get("ok").asNumber().intValue()).isZero();
    assertThat(reply.get("code").asNumber().intValue()).isEqualTo(AUTHENTICATION_FAILED);
    assertThat(reply.getString("errmsg").getValue()).isEqualTo("Authentication failed");

    // the refused request is still reported at a fine level: an operator can see that a login was refused
    assertThat(captured.text()).contains("saslStart").contains("Authentication failed");
    assertTheLogHoldsNo(password);
  }

  /** The legacy driver logs in with an OP_QUERY on the command collection, the door the library cannot answer with a reply. */
  @Test
  void aRefusedLoginOfTheLegacyDriverIsLoggedWithoutThePassword() {
    final String password = "wrong-" + UUID.randomUUID();
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(), password.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()), credential,
        MongoClientOptions.builder().serverSelectionTimeout(3000).build())) {
      final Throwable thrown = catchThrowable(() -> client.getDatabase(getDatabaseName()).runCommand(new Document("ping", 1)));
      assertThat(thrown).isInstanceOf(MongoSecurityException.class);
    }

    assertThat(captured.text()).contains("Authentication failed");
    assertTheLogHoldsNo(password);
  }

  /** The payload of the login request is not in the log, as a text or as the hex the library prints a binary value with. */
  private void assertTheLogHoldsNo(final String password) {
    final String log = captured.text();
    assertThat(log).doesNotContain(password);
    assertThat(log).doesNotContain(HexFormat.of().withUpperCase().formatHex(password.getBytes(StandardCharsets.UTF_8)));
    assertThat(log).doesNotContain(HexFormat.of().formatHex(password.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void aFailedCommandIsLoggedWithoutTheRequestDocument() {
    final String value = "document-value-" + UUID.randomUUID();
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(),
        DEFAULT_PASSWORD_FOR_TESTS.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());

      final MongoCommandException refused = (MongoCommandException) catchThrowable(
          () -> db.runCommand(new Document("noSuchCommandOfTheTest", 1).append("field", value)));

      // the client is told as before
      assertThat(refused.getErrorCode()).isEqualTo(COMMAND_NOT_FOUND);
    }

    final String log = captured.text();
    assertThat(log).contains("noSuchCommandOfTheTest");
    assertThat(log).doesNotContain(value);
  }

  @Test
  void aCommandNameOfALoggedInClientIsShownWithPrintableCharactersOnly() {
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(),
        DEFAULT_PASSWORD_FOR_TESTS.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());

      final MongoCommandException refused = (MongoCommandException) catchThrowable(
          () -> db.runCommand(new Document("noSuch\nsecond-line\u001b[31m", 1)));

      assertThat(refused.getErrorCode()).isEqualTo(COMMAND_NOT_FOUND);
      assertThat(refused.getErrorMessage()).startsWith("no such command: 'noSuch?").doesNotContain("\n").doesNotContain("\u001b");
    }

    assertThat(captured.records).isNotEmpty();
    for (final LogRecord record : captured.records)
      assertThat(CapturedLog.lines(record)).as("the log record of %s", record.getMessage())
          .noneMatch(line -> line.chars().anyMatch(Character::isISOControl));
  }

  /** What is not an answer to a wrong client (an error without a code) is a fault of the server: logged with its stack trace. */
  @Test
  void aFaultOfTheServerIsLoggedWithItsStackTraceAndWithoutTheRequestDocument() {
    final String value = "document-value-" + UUID.randomUUID();
    final MongoCredential credential = MongoCredential.createPlainCredential("root", getDatabaseName(),
        DEFAULT_PASSWORD_FOR_TESTS.toCharArray());

    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()), credential,
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());

      final MongoCommandException failed = (MongoCommandException) catchThrowable(
          () -> db.runCommand(new Document("killCursors", "x").append("cursors", value)));

      assertThat(failed.getErrorMessage()).startsWith("Unknown error");
    }

    assertThat(captured.records).anySatisfy(record -> {
      assertThat(record.getLevel()).isEqualTo(Level.SEVERE);
      assertThat(record.getMessage()).contains("killCursors");
      assertThat(record.getThrown()).isNotNull();
    });
    assertThat(captured.text()).doesNotContain(value);
  }

  /** A client that did not log in can raise a fault too (an empty request): it is answered, and not reported as SEVERE. */
  @Test
  void aFaultRaisedByAClientThatDidNotLogInIsNotLoggedAsSevere() throws Exception {
    final BsonDocument reply = sendOpMsg(new BsonDocument());

    assertThat(reply.get("ok").asNumber().intValue()).isZero();
    assertThat(reply.getString("errmsg").getValue()).startsWith("Unknown error");

    assertThat(captured.records).isNotEmpty().noneMatch(record -> record.getLevel().intValue() >= Level.SEVERE.intValue());
    assertThat(captured.records).anySatisfy(record -> assertThat(record.getMessage()).contains("failed").contains("Unknown error"));
  }

  @Test
  void aRefusedCommandStillAnswersTheCodeAndTheConnectionStaysUsable() {
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());

      final MongoCommandException refused = (MongoCommandException) catchThrowable(() -> db.runCommand(new Document("find", "x")));
      assertThat(refused.getErrorCode()).isEqualTo(UNAUTHORIZED);
      assertThat(refused.getErrorCodeName()).isEqualTo("Unauthorized");
      assertThat(refused.getErrorMessage()).isEqualTo("Command 'find' requires authentication");

      assertThat(db.runCommand(new Document("ping", 1)).getDouble("ok")).isEqualTo(1.0);
    }
  }

  @Test
  void clientTextInARefusalIsShownWithPrintableCharactersOnly() {
    try (final MongoClient client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build())) {
      final MongoDatabase db = client.getDatabase(getDatabaseName());

      final MongoCommandException command = (MongoCommandException) catchThrowable(
          () -> db.runCommand(new Document("find\nsecond-line\u001b[31m", "x")));
      assertThat(command.getErrorCode()).isEqualTo(UNAUTHORIZED);
      assertThat(command.getErrorMessage()).contains("requires authentication").doesNotContain("\n").doesNotContain("\u001b");

      final MongoCommandException mechanism = (MongoCommandException) catchThrowable(() -> db.runCommand(
          new Document("saslStart", 1).append("mechanism", "MECH\nsecond-line\u001b[31m")
              .append("payload", new Binary(new byte[0]))));
      assertThat(mechanism.getErrorMessage()).contains("Unsupported authentication mechanism").doesNotContain("\n")
          .doesNotContain("\u001b");

      // a mechanism that is not a text is named by its type, not printed
      final MongoCommandException notText = (MongoCommandException) catchThrowable(() -> db.runCommand(
          new Document("saslStart", 1).append("mechanism", new Document("a", "mechanism-value-marker"))
              .append("payload", new Binary(new byte[0]))));
      assertThat(notText.getErrorMessage()).contains("Unsupported authentication mechanism").doesNotContain("mechanism-value-marker");
    }

    assertThat(captured.records).isNotEmpty();
    for (final LogRecord record : captured.records)
      assertThat(CapturedLog.lines(record)).as("the log record of %s", record.getMessage())
          .noneMatch(line -> line.chars().anyMatch(Character::isISOControl));
  }

  /** Sends one OP_MSG on a fresh connection that never authenticated, and answers the reply document. */
  private BsonDocument sendOpMsg(final BsonDocument command) throws IOException {
    final BasicOutputBuffer body = new BasicOutputBuffer();
    new BsonDocumentCodec().encode(new BsonBinaryWriter(body), command, EncoderContext.builder().build());
    final byte[] document = body.toByteArray();

    final ByteBuffer frame = ByteBuffer.allocate(16 + 4 + 1 + document.length).order(ByteOrder.LITTLE_ENDIAN);
    frame.putInt(frame.capacity());
    frame.putInt(1); // requestID
    frame.putInt(0); // responseTo
    frame.putInt(OP_MSG);
    frame.putInt(0); // flags
    frame.put((byte) 0); // a body section
    frame.put(document);

    try (final Socket socket = new Socket("localhost", getServerMongoPort())) {
      socket.setSoTimeout(10_000);
      final OutputStream out = socket.getOutputStream();
      out.write(frame.array());
      out.flush();

      final DataInputStream in = new DataInputStream(socket.getInputStream());
      final byte[] lengthBytes = new byte[4];
      in.readFully(lengthBytes);
      final int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
      final byte[] rest = new byte[length - 4];
      in.readFully(rest);
      // request id, response to, opcode, flags, section kind: 17 bytes ahead of the document
      final ByteBuffer reply = ByteBuffer.wrap(rest, 17, rest.length - 17).slice();
      return new BsonDocumentCodec().decode(new BsonBinaryReader(reply), DecoderContext.builder().build());
    }
  }

  /**
   * Keeps every record the library (at INFO) and the plugin (at every level) publish: the handler sits on the root logger, where
   * every record ends.
   */
  private static final class CapturedLog extends Handler {
    private final List<LogRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public void publish(final LogRecord record) {
      final String name = record.getLoggerName();
      if (name != null && (name.startsWith("de.bwaldvogel") || name.startsWith("com.arcadedb.mongo")))
        records.add(record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }

    /** Everything the records say, the thrown exceptions included, as one text. */
    String text() {
      final StringBuilder all = new StringBuilder();
      for (final LogRecord record : records)
        for (final String line : lines(record))
          all.append(line).append('\n');
      return all.toString();
    }

    /** The message of the record, its parameters and the text of each throwable of its cause chain. */
    static List<String> lines(final LogRecord record) {
      final List<String> lines = new ArrayList<>();
      lines.add(String.valueOf(record.getMessage()));
      if (record.getParameters() != null)
        for (final Object parameter : record.getParameters())
          lines.add(String.valueOf(parameter));
      for (Throwable t = record.getThrown(); t != null; t = t.getCause())
        lines.add(String.valueOf(t));
      return lines;
    }
  }
}
