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
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The refusals the backend answers to a connection that has not logged in. The OP_MSG door answers them as a reply document, so the
 * library never sees an exception to log. The legacy OP_QUERY door can only throw: the library logs the exception and closes the
 * connection, so the exception carries printable characters only and no stack trace.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class MongoRefusalTextTest extends BaseMongoServerTest {
  private static final String LINE_BREAK_MARKER = "\nsecond-line\u001b[31m";

  private MongoDBBackend  backend;
  private EmbeddedChannel channel;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  void openAChannel() {
    backend = new MongoDBBackend(getServer(0), null);
    channel = new EmbeddedChannel();
  }

  @AfterEach
  @Override
  public void endTest() {
    channel.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aCommandOfAConnectionThatDidNotLogInIsRefusedWithPrintableText() {
    final String command = "find" + LINE_BREAK_MARKER;

    final Throwable refused = catchThrowable(() -> backend.handleCommand(channel, getDatabaseName(), command, new Document(command, "x")));

    assertThat(refused).isInstanceOf(MongoServerError.class);
    assertThat(refused.getMessage()).contains("requires authentication").contains("find?");
    assertThat(refused.getMessage().chars()).noneMatch(Character::isISOControl);
    assertThat(refused.getStackTrace()).as("a refusal is an answer, not a fault").isEmpty();
  }

  @Test
  void aCommandOfTheAdminNamespaceIsRefusedWithPrintableText() {
    final String command = "listDatabases" + LINE_BREAK_MARKER;

    final Throwable refused = catchThrowable(() -> backend.handleCommand(channel, "admin", command, new Document(command, 1)));

    assertThat(refused).isInstanceOf(MongoServerError.class);
    assertThat(refused.getMessage()).contains("requires authentication");
    assertThat(refused.getMessage().chars()).noneMatch(Character::isISOControl);
    assertThat(refused.getStackTrace()).isEmpty();
  }

  @Test
  void anUnsupportedMechanismIsRefusedWithPrintableTextOfALimitedLength() {
    final Document sasl = new Document("saslStart", 1);
    sasl.put("mechanism", "MECH" + LINE_BREAK_MARKER + "x".repeat(MongoWireText.MAX_LENGTH * 4));
    sasl.put("payload", new BinData(new byte[0]));

    final Throwable refused = catchThrowable(() -> backend.handleCommand(channel, getDatabaseName(), "saslStart", sasl));

    assertThat(refused).isInstanceOf(MongoServerError.class);
    assertThat(refused.getMessage()).contains("Unsupported authentication mechanism");
    assertThat(refused.getMessage().chars()).noneMatch(Character::isISOControl);
    assertThat(refused.getMessage().length()).isLessThan(MongoWireText.MAX_LENGTH * 3);
    assertThat(refused.getStackTrace()).isEmpty();
  }

  @Test
  void aRefusedLoginAnswersWithoutAStackTrace() {
    final byte[] payload = "\u0000root\u0000not-the-password".getBytes(StandardCharsets.UTF_8);
    final Document sasl = new Document("saslStart", 1);
    sasl.put("mechanism", "PLAIN");
    sasl.put("payload", new BinData(payload));

    final Throwable refused = catchThrowable(() -> backend.handleCommand(channel, getDatabaseName(), "saslStart", sasl));

    assertThat(refused).isInstanceOf(MongoServerError.class);
    assertThat(refused.getMessage()).contains("Authentication failed");
    assertThat(refused.getStackTrace()).isEmpty();
  }

  @Test
  void aCollectionQueryOfAConnectionThatDidNotLogInIsRefusedWithPrintableText() {
    final Throwable refused = catchThrowable(() -> backend.handleQuery(
        new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + ".coll" + LINE_BREAK_MARKER, 0, 0, new Document(), null)));

    assertThat(refused).isInstanceOf(MongoServerError.class);
    assertThat(refused.getMessage()).contains("requires authentication");
    assertThat(refused.getMessage().chars()).noneMatch(Character::isISOControl);
    assertThat(refused.getStackTrace()).isEmpty();
  }

  @Test
  void anOpMsgIsAnsweredAsAReplyDocumentNotAsAnException() {
    final Document message = new Document("find", "Secret");
    message.put("$db", getDatabaseName());

    final Document reply = backend.handleMessage(new MongoMessage(channel, new MessageHeader(1, 0), message));

    assertThat(reply.get("ok")).isEqualTo(0);
    assertThat(reply.get("code")).isEqualTo(13);
    assertThat(reply.get("codeName")).isEqualTo("Unauthorized");
    assertThat(reply.get("errmsg")).isEqualTo("Command 'find' requires authentication");
    assertThat(reply.get("$err")).isEqualTo("Command 'find' requires authentication");
  }

  @Test
  void anOpMsgWithoutAnyCommandIsAnsweredAsAReplyDocument() {
    final Document reply = backend.handleMessage(new MongoMessage(channel, new MessageHeader(1, 0), new Document()));

    assertThat(reply.get("ok")).isEqualTo(0);
    assertThat(reply.get("code")).as("an error that is not a coded one has no code").isNull();
    assertThat((String) reply.get("errmsg")).startsWith("Unknown error");
  }
}
