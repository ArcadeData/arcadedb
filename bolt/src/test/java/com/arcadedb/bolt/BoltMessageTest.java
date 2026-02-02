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

import com.arcadedb.bolt.message.*;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for BOLT message classes covering serialization and parsing.
 */
class BoltMessageTest {

  // ============ SuccessMessage tests ============

  @Test
  void successMessageCreation() throws IOException {
    final Map<String, Object> metadata = Map.of("server", "ArcadeDB", "version", "1.0");
    final SuccessMessage msg = new SuccessMessage(metadata);

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.SUCCESS);
    assertThat(msg.getMetadata()).containsEntry("server", "ArcadeDB");
    assertThat(msg.getMetadata()).containsEntry("version", "1.0");
  }

  @Test
  void successMessageNullMetadata() {
    final SuccessMessage msg = new SuccessMessage(null);
    assertThat(msg.getMetadata()).isEmpty();
  }

  @Test
  void successMessageWriteAndToString() throws IOException {
    final SuccessMessage msg = new SuccessMessage(Map.of("fields", List.of("a", "b")));
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
    assertThat(msg.toString()).contains("SUCCESS");
    assertThat(msg.toString()).contains("fields");
  }

  // ============ FailureMessage tests ============

  @Test
  void failureMessageWithCodeAndMessage() throws IOException {
    final FailureMessage msg = new FailureMessage(BoltErrorCodes.SYNTAX_ERROR, "Invalid query");

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(msg.getCode()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
    assertThat(msg.getMessage()).isEqualTo("Invalid query");
    assertThat(msg.getMetadata()).containsEntry("code", BoltErrorCodes.SYNTAX_ERROR);
  }

  @Test
  void failureMessageWithMetadata() {
    final Map<String, Object> metadata = Map.of("code", "test.code", "message", "test message");
    final FailureMessage msg = new FailureMessage(metadata);

    assertThat(msg.getCode()).isEqualTo("test.code");
    assertThat(msg.getMessage()).isEqualTo("test message");
  }

  @Test
  void failureMessageNullMetadata() {
    final FailureMessage msg = new FailureMessage(null);
    assertThat(msg.getMetadata()).isEmpty();
    assertThat(msg.getCode()).isNull();
    assertThat(msg.getMessage()).isNull();
  }

  @Test
  void failureMessageWriteAndToString() throws IOException {
    final FailureMessage msg = new FailureMessage(BoltErrorCodes.DATABASE_ERROR, "Error");
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
    assertThat(msg.toString()).contains("FAILURE");
    assertThat(msg.toString()).contains(BoltErrorCodes.DATABASE_ERROR);
  }

  // ============ RecordMessage tests ============

  @Test
  void recordMessageCreation() throws IOException {
    final List<Object> data = List.of("Alice", 30L, true);
    final RecordMessage msg = new RecordMessage(data);

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.RECORD);
    assertThat(msg.getData()).containsExactly("Alice", 30L, true);
  }

  @Test
  void recordMessageNullData() {
    final RecordMessage msg = new RecordMessage(null);
    assertThat(msg.getData()).isEmpty();
  }

  @Test
  void recordMessageWriteAndToString() throws IOException {
    final RecordMessage msg = new RecordMessage(List.of(1, 2, 3));
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);

    assertThat(writer.toByteArray()).isNotEmpty();
    assertThat(msg.toString()).contains("RECORD");
  }

  // ============ IgnoredMessage tests ============

  @Test
  void ignoredMessageCreation() throws IOException {
    final IgnoredMessage msg = new IgnoredMessage();

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.IGNORED);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  // ============ Request message tests ============

  @Test
  void helloMessageCreation() throws IOException {
    final Map<String, Object> extra = Map.of(
        "user_agent", "TestDriver/1.0",
        "scheme", "basic",
        "principal", "neo4j",
        "credentials", "password"
    );
    final HelloMessage msg = new HelloMessage(extra);

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.HELLO);
    assertThat(msg.getExtra()).containsKey("user_agent");
    assertThat(msg.getScheme()).isEqualTo("basic");
    assertThat(msg.getPrincipal()).isEqualTo("neo4j");
    assertThat(msg.getCredentials()).isEqualTo("password");
  }

  @Test
  void helloMessageNullExtra() {
    final HelloMessage msg = new HelloMessage(null);
    assertThat(msg.getExtra()).isEmpty();
    assertThat(msg.getScheme()).isNull();
    assertThat(msg.getPrincipal()).isNull();
    assertThat(msg.getCredentials()).isNull();
  }

  @Test
  void helloMessageWriteTo() throws IOException {
    final HelloMessage msg = new HelloMessage(Map.of("user_agent", "test"));
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void runMessageCreation() throws IOException {
    final RunMessage msg = new RunMessage("RETURN 1", Map.of("param", "value"), Map.of("db", "neo4j"));

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.RUN);
    assertThat(msg.getQuery()).isEqualTo("RETURN 1");
    assertThat(msg.getParameters()).containsEntry("param", "value");
    assertThat(msg.getDatabase()).isEqualTo("neo4j");
  }

  @Test
  void runMessageNullParams() {
    final RunMessage msg = new RunMessage("RETURN 1", null, null);
    assertThat(msg.getParameters()).isEmpty();
    assertThat(msg.getExtra()).isEmpty();
    assertThat(msg.getDatabase()).isNull();
  }

  @Test
  void runMessageWriteTo() throws IOException {
    final RunMessage msg = new RunMessage("RETURN 1", Map.of(), Map.of());
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void beginMessageCreation() throws IOException {
    final BeginMessage msg = new BeginMessage(Map.of("db", "testdb"));

    assertThat(msg.getSignature()).isEqualTo(BoltMessage.BEGIN);
    assertThat(msg.getDatabase()).isEqualTo("testdb");
  }

  @Test
  void beginMessageNullExtra() {
    final BeginMessage msg = new BeginMessage(null);
    assertThat(msg.getExtra()).isEmpty();
    assertThat(msg.getDatabase()).isNull();
  }

  @Test
  void beginMessageWriteTo() throws IOException {
    final BeginMessage msg = new BeginMessage(Map.of());
    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void commitMessageCreation() throws IOException {
    final CommitMessage msg = new CommitMessage();
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.COMMIT);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void rollbackMessageCreation() throws IOException {
    final RollbackMessage msg = new RollbackMessage();
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.ROLLBACK);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void goodbyeMessageCreation() throws IOException {
    final GoodbyeMessage msg = new GoodbyeMessage();
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.GOODBYE);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void resetMessageCreation() throws IOException {
    final ResetMessage msg = new ResetMessage();
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.RESET);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void pullMessageCreation() throws IOException {
    final PullMessage msg = new PullMessage(Map.of("n", 100L));
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.PULL);
    assertThat(msg.getN()).isEqualTo(100L);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void pullMessageDefaultN() {
    final PullMessage msg = new PullMessage(null);
    assertThat(msg.getN()).isEqualTo(-1L); // Default unlimited
  }

  @Test
  void discardMessageCreation() throws IOException {
    final DiscardMessage msg = new DiscardMessage(Map.of("n", 50L));
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.DISCARD);
    assertThat(msg.getN()).isEqualTo(50L);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void discardMessageDefaultN() {
    final DiscardMessage msg = new DiscardMessage(null);
    assertThat(msg.getN()).isEqualTo(-1L);
  }

  @Test
  void logonMessageCreation() throws IOException {
    final LogonMessage msg = new LogonMessage(Map.of("scheme", "basic", "principal", "user", "credentials", "pass"));
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.LOGON);
    assertThat(msg.getScheme()).isEqualTo("basic");
    assertThat(msg.getPrincipal()).isEqualTo("user");
    assertThat(msg.getCredentials()).isEqualTo("pass");

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void logonMessageNullAuth() {
    final LogonMessage msg = new LogonMessage(null);
    assertThat(msg.getAuth()).isEmpty();
  }

  @Test
  void logoffMessageCreation() throws IOException {
    final LogoffMessage msg = new LogoffMessage();
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.LOGOFF);

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void routeMessageCreation() throws IOException {
    final RouteMessage msg = new RouteMessage(
        Map.of("region", "us-east"),
        List.of("bookmark1", "bookmark2"),
        "mydb"
    );
    assertThat(msg.getSignature()).isEqualTo(BoltMessage.ROUTE);
    assertThat(msg.getRouting()).containsEntry("region", "us-east");
    assertThat(msg.getBookmarks()).containsExactly("bookmark1", "bookmark2");
    assertThat(msg.getDatabase()).isEqualTo("mydb");

    final PackStreamWriter writer = new PackStreamWriter();
    msg.writeTo(writer);
    assertThat(writer.toByteArray()).isNotEmpty();
  }

  @Test
  void routeMessageNullValues() {
    final RouteMessage msg = new RouteMessage(null, null, null);
    assertThat(msg.getRouting()).isEmpty();
    assertThat(msg.getBookmarks()).isEmpty();
    assertThat(msg.getDatabase()).isNull();
  }

  // ============ BoltMessage.signatureName() tests ============

  @Test
  void signatureNameAllMessages() {
    assertThat(BoltMessage.signatureName(BoltMessage.HELLO)).isEqualTo("HELLO");
    assertThat(BoltMessage.signatureName(BoltMessage.GOODBYE)).isEqualTo("GOODBYE");
    assertThat(BoltMessage.signatureName(BoltMessage.RESET)).isEqualTo("RESET");
    assertThat(BoltMessage.signatureName(BoltMessage.RUN)).isEqualTo("RUN");
    assertThat(BoltMessage.signatureName(BoltMessage.BEGIN)).isEqualTo("BEGIN");
    assertThat(BoltMessage.signatureName(BoltMessage.COMMIT)).isEqualTo("COMMIT");
    assertThat(BoltMessage.signatureName(BoltMessage.ROLLBACK)).isEqualTo("ROLLBACK");
    assertThat(BoltMessage.signatureName(BoltMessage.DISCARD)).isEqualTo("DISCARD");
    assertThat(BoltMessage.signatureName(BoltMessage.PULL)).isEqualTo("PULL");
    assertThat(BoltMessage.signatureName(BoltMessage.SUCCESS)).isEqualTo("SUCCESS");
    assertThat(BoltMessage.signatureName(BoltMessage.RECORD)).isEqualTo("RECORD");
    assertThat(BoltMessage.signatureName(BoltMessage.IGNORED)).isEqualTo("IGNORED");
    assertThat(BoltMessage.signatureName(BoltMessage.FAILURE)).isEqualTo("FAILURE");
    assertThat(BoltMessage.signatureName(BoltMessage.LOGON)).isEqualTo("LOGON");
    assertThat(BoltMessage.signatureName(BoltMessage.LOGOFF)).isEqualTo("LOGOFF");
    assertThat(BoltMessage.signatureName(BoltMessage.ROUTE)).isEqualTo("ROUTE");
  }

  @Test
  void signatureNameUnknown() {
    final String name = BoltMessage.signatureName((byte) 0xFF);
    assertThat(name).startsWith("UNKNOWN");
    assertThat(name).contains("0xff");
  }

  // ============ BoltMessage.parse() tests ============

  @Test
  void parseHelloMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.HELLO, 1);
    writer.writeMap(Map.of("user_agent", "TestDriver"));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(HelloMessage.class);
    assertThat(((HelloMessage) msg).getExtra()).containsEntry("user_agent", "TestDriver");
  }

  @Test
  void parseHelloMessageEmptyFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.HELLO, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(HelloMessage.class);
    assertThat(((HelloMessage) msg).getExtra()).isEmpty();
  }

  @Test
  void parseGoodbyeMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.GOODBYE, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(GoodbyeMessage.class);
  }

  @Test
  void parseResetMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.RESET, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(ResetMessage.class);
  }

  @Test
  void parseRunMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.RUN, 3);
    writer.writeString("RETURN 1");
    writer.writeMap(Map.of("x", 1));
    writer.writeMap(Map.of("db", "neo4j"));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(RunMessage.class);
    final RunMessage run = (RunMessage) msg;
    assertThat(run.getQuery()).isEqualTo("RETURN 1");
    assertThat(run.getParameters()).containsEntry("x", 1L);
    assertThat(run.getDatabase()).isEqualTo("neo4j");
  }

  @Test
  void parseRunMessageMinimalFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.RUN, 1);
    writer.writeString("RETURN 1");

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(RunMessage.class);
    final RunMessage run = (RunMessage) msg;
    assertThat(run.getQuery()).isEqualTo("RETURN 1");
    assertThat(run.getParameters()).isEmpty();
  }

  @Test
  void parseBeginMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.BEGIN, 1);
    writer.writeMap(Map.of("db", "testdb"));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(BeginMessage.class);
    assertThat(((BeginMessage) msg).getDatabase()).isEqualTo("testdb");
  }

  @Test
  void parseBeginMessageEmptyFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.BEGIN, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(BeginMessage.class);
    assertThat(((BeginMessage) msg).getExtra()).isEmpty();
  }

  @Test
  void parseCommitMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.COMMIT, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(BoltMessage.parse(struct)).isInstanceOf(CommitMessage.class);
  }

  @Test
  void parseRollbackMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.ROLLBACK, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(BoltMessage.parse(struct)).isInstanceOf(RollbackMessage.class);
  }

  @Test
  void parsePullMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.PULL, 1);
    writer.writeMap(Map.of("n", 100));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(PullMessage.class);
    assertThat(((PullMessage) msg).getN()).isEqualTo(100L);
  }

  @Test
  void parsePullMessageEmptyFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.PULL, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(PullMessage.class);
  }

  @Test
  void parseDiscardMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.DISCARD, 1);
    writer.writeMap(Map.of("n", 50));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(DiscardMessage.class);
    assertThat(((DiscardMessage) msg).getN()).isEqualTo(50L);
  }

  @Test
  void parseDiscardMessageEmptyFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.DISCARD, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(BoltMessage.parse(struct)).isInstanceOf(DiscardMessage.class);
  }

  @Test
  void parseLogonMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.LOGON, 1);
    writer.writeMap(Map.of("scheme", "basic", "principal", "user", "credentials", "pass"));

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(LogonMessage.class);
    final LogonMessage logon = (LogonMessage) msg;
    assertThat(logon.getScheme()).isEqualTo("basic");
    assertThat(logon.getPrincipal()).isEqualTo("user");
  }

  @Test
  void parseLogonMessageEmptyFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.LOGON, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(BoltMessage.parse(struct)).isInstanceOf(LogonMessage.class);
  }

  @Test
  void parseLogoffMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.LOGOFF, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThat(BoltMessage.parse(struct)).isInstanceOf(LogoffMessage.class);
  }

  @Test
  void parseRouteMessage() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.ROUTE, 3);
    writer.writeMap(Map.of("region", "us-east"));
    writer.writeList(List.of("bm1", "bm2"));
    writer.writeString("mydb");

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(RouteMessage.class);
    final RouteMessage route = (RouteMessage) msg;
    assertThat(route.getRouting()).containsEntry("region", "us-east");
    assertThat(route.getBookmarks()).containsExactly("bm1", "bm2");
    assertThat(route.getDatabase()).isEqualTo("mydb");
  }

  @Test
  void parseRouteMessageMinimalFields() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader(BoltMessage.ROUTE, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();
    final BoltMessage msg = BoltMessage.parse(struct);

    assertThat(msg).isInstanceOf(RouteMessage.class);
    final RouteMessage route = (RouteMessage) msg;
    assertThat(route.getRouting()).isEmpty();
    assertThat(route.getBookmarks()).isEmpty();
    assertThat(route.getDatabase()).isNull();
  }

  @Test
  void parseUnknownSignatureThrowsException() throws IOException {
    final PackStreamWriter writer = new PackStreamWriter();
    writer.writeStructureHeader((byte) 0xFF, 0);

    final PackStreamReader reader = new PackStreamReader(writer.toByteArray());
    final PackStreamReader.StructureValue struct = (PackStreamReader.StructureValue) reader.readValue();

    assertThatThrownBy(() -> BoltMessage.parse(struct))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unknown message signature");
  }

  // ============ Message toString() coverage ============

  @Test
  void allMessagesToString() {
    assertThat(new SuccessMessage(Map.of("key", "value")).toString()).contains("SUCCESS");
    assertThat(new FailureMessage("code", "msg").toString()).contains("FAILURE");
    assertThat(new RecordMessage(List.of(1, 2, 3)).toString()).contains("RECORD");
    assertThat(new IgnoredMessage().toString()).contains("IGNORED");
    assertThat(new HelloMessage(Map.of()).toString()).contains("HELLO");
    assertThat(new GoodbyeMessage().toString()).contains("GOODBYE");
    assertThat(new ResetMessage().toString()).contains("RESET");
    assertThat(new RunMessage("q", Map.of(), Map.of()).toString()).contains("RUN");
    assertThat(new BeginMessage(Map.of()).toString()).contains("BEGIN");
    assertThat(new CommitMessage().toString()).contains("COMMIT");
    assertThat(new RollbackMessage().toString()).contains("ROLLBACK");
    assertThat(new PullMessage(Map.of()).toString()).contains("PULL");
    assertThat(new DiscardMessage(Map.of()).toString()).contains("DISCARD");
    assertThat(new LogonMessage(Map.of()).toString()).contains("LOGON");
    assertThat(new LogoffMessage().toString()).contains("LOGOFF");
    assertThat(new RouteMessage(Map.of(), List.of(), "db").toString()).contains("ROUTE");
  }
}
