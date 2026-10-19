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

import com.arcadedb.database.ProtocolContext;
import com.arcadedb.server.BaseGraphServerTest;
import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.DatabaseResolver;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoMessage;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10390: the module's one {@code ProtocolContext.set("mongo")} sat inside {@code MongoDBDatabaseWrapper.handleCommand},
 * while the bundled backend reaches the same wrapper through other doors on a client request: a {@code dropDatabase}
 * lands on {@code MongoDatabase.drop}, an OP_QUERY on {@code MongoDatabase.handleQuery}. Both ran as
 * {@link ProtocolContext#INTERNAL}, which exempts them from the HA gate that refuses client work on a database whose
 * directory is being replaced from the leader's snapshot.
 * <p>
 * The backend here is the real {@link MongoDBBackend} against a real server's security; only the {@code MongoDatabase} it
 * opens is a recorder, because what matters is the tag the thread carries when the library calls into it.
 */
class Issue10390MongoProtocolContextTest extends BaseGraphServerTest {

  /**
   * The {@link MongoBackend} methods that carry no client request, so the request-carrying filter below does not see
   * them. They are listed so that a library upgrade adding a method in any shape - one taking a cursor id or a bare
   * database name, say - fails here and gets a decision instead of slipping past the filter.
   */
  private static final Set<String> CARRIES_NO_REQUEST = Set.of("dropDatabase", "getServerStatus", "close", "closeCursors",
      "getClock", "enableOplog", "disableOplog", "resolveDatabase", "version");

  /** The request-carrying entry points this class drives, and proves tagged, one test each. */
  private static final Set<String> DRIVEN_HERE = Set.of("handleCommand", "handleQuery", "handleMessage", "handleClose",
      "getCurrentOperations");

  private RecordingBackend backend;
  private Channel          channel;

  @BeforeEach
  void openTheBackend() {
    backend = new RecordingBackend(this);
    channel = new EmbeddedChannel();
    // AN UNTAGGED THREAD READS ProtocolContext.INTERNAL (ITS THREAD-LOCAL DEFAULT): EVERY "RESTORED" ASSERTION BELOW RELIES ON IT
    ProtocolContext.clear();
  }

  @AfterEach
  void closeTheChannel() {
    channel.close();
    ProtocolContext.clear();
  }

  @Test
  void everyRequestCarryingEntryPointIsOverriddenAndDrivenHere() {
    final Set<String> requestCarrying = new TreeSet<>();
    final Map<String, Method> byName = new TreeMap<>();
    for (final Method method : MongoBackend.class.getMethods())
      if (!Modifier.isStatic(method.getModifiers()) && Arrays.stream(method.getParameterTypes())
          .anyMatch(t -> t == Channel.class || t == MongoQuery.class || t == MongoMessage.class)) {
        requestCarrying.add(method.getName());
        byName.put(method.getName(), method);
      }

    assertThat(requestCarrying).as("the library's request-carrying entry points, enumerated from its interface")
        .containsAll(DRIVEN_HERE);

    final Set<String> everyMethod = new TreeSet<>();
    for (final Method method : MongoBackend.class.getMethods())
      if (!Modifier.isStatic(method.getModifiers()))
        everyMethod.add(method.getName());
    final Set<String> accountedFor = new TreeSet<>(requestCarrying);
    accountedFor.addAll(CARRIES_NO_REQUEST);
    assertThat(everyMethod).as("every MongoBackend method is either request-carrying (and tagged or exempted) or listed in "
        + "CARRIES_NO_REQUEST: a new one needs a decision").isEqualTo(accountedFor);

    for (final String name : requestCarrying) {
      assertThat(DRIVEN_HERE).as("a new MongoBackend entry point '%s' must be tagged in MongoDBBackend and driven here",
          name).contains(name);
      // THE SAME SIGNATURE, NOT ONLY THE SAME NAME: AN OVERLOAD WOULD LEAVE THE LIBRARY'S CALL UNTAGGED
      final Method declared = findDeclared(name, byName.get(name).getParameterTypes());
      assertThat(declared).as("MongoDBBackend overrides '%s' to tag the thread", name).isNotNull();
    }
  }

  /** The {@code dropDatabase} door: handled by the backend itself, it lands on {@code MongoDatabase.drop}. */
  @Test
  void aDropDatabaseCommandDropsUnderTheMongoTag() {
    authenticate();
    backend.resolveDatabase(getDatabaseName());

    backend.handleCommand(channel, getDatabaseName(), "dropDatabase", new Document("dropDatabase", 1));

    assertThat(backend.recorded("drop")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).as("the tag is cleared on the way out").isEqualTo(ProtocolContext.INTERNAL);
  }

  /** The same {@code dropDatabase} as an OP_MSG, which every modern driver sends: the nested command gives the tag back. */
  @Test
  void aDropDatabaseMessageDropsUnderTheMongoTag() {
    authenticate();
    backend.resolveDatabase(getDatabaseName());

    final Document document = new Document("dropDatabase", 1);
    document.put("$db", getDatabaseName());
    backend.handleMessage(new MongoMessage(channel, new MessageHeader(1, 0), document));

    assertThat(backend.recorded("drop")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  /** The OP_QUERY door: the backend hands it to {@code MongoDatabase.handleQuery} without any command. */
  @Test
  void anOpQueryReadsUnderTheMongoTag() {
    backend.handleQuery(new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + ".Person", 0, 0, new Document(),
        null));

    assertThat(backend.recorded("handleQuery")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void aCommandOnACollectionRunsUnderTheMongoTag() {
    authenticate();
    backend.handleCommand(channel, getDatabaseName(), "find", new Document("find", "Person"));

    assertThat(backend.recorded("handleCommand")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void aClosingConnectionIsReleasedUnderTheMongoTag() {
    backend.resolveDatabase(getDatabaseName());

    backend.handleClose(channel);

    assertThat(backend.recorded("handleClose")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  /**
   * The {@code renameCollection} door: an admin command, so it enters through {@code handleCommand} with no
   * authentication, and the library resolves the source collection on the user database before anything else. With
   * ArcadeDB's wrapper that lookup answers {@code null}, so the rename stops there with "source namespace does not exist":
   * the lookup must still run tagged, and the failure must not leave the tag behind on the pooled thread.
   */
  @Test
  void aRenameCollectionResolvesUnderTheMongoTagAndAFailureLeavesNoTagBehind() {
    backend.resolveDatabase(getDatabaseName());

    final Document rename = new Document("renameCollection", getDatabaseName() + ".Person");
    rename.put("to", getDatabaseName() + ".People");
    assertThatThrownBy(() -> backend.handleCommand(channel, "admin", "renameCollection", rename)).isInstanceOf(
        MongoServerException.class).hasMessageContaining("source namespace does not exist");

    assertThat(backend.recorded("resolveCollection")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).as("a failed command clears the tag too").isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void aFailedOpQueryLeavesNoTagBehind() {
    assertThatThrownBy(() -> backend.handleQuery(
        new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + "." + RecordingDatabase.FAILING, 0, 0,
            new Document(), null))).isInstanceOf(MongoServerException.class);

    assertThat(backend.recorded("handleQuery")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  /**
   * AbstractMongoBackend answers it with an empty list today and reads nothing, so there is no door to record at: it is
   * tagged anyway, so that a library version that starts reading a database there does not run untagged. What can be
   * observed is that it gives the thread its tag back.
   */
  @Test
  void aCurrentOperationsRequestRestoresTheEnclosingTag() {
    ProtocolContext.set("http");

    backend.getCurrentOperations(new MongoQuery(channel, new MessageHeader(1, 0), "admin.$cmd.sys.inprog", 0, 0,
        new Document(), null));

    assertThat(ProtocolContext.get()).isEqualTo("http");
  }

  /** A caller that already tagged the thread gets its own tag back, not INTERNAL. */
  @Test
  void anEnclosingTagIsRestored() {
    ProtocolContext.set("http");

    backend.handleQuery(new MongoQuery(channel, new MessageHeader(1, 0), getDatabaseName() + ".Person", 0, 0, new Document(),
        null));

    assertThat(backend.recorded("handleQuery")).isEqualTo(MongoDBBackend.MONGO_PROTOCOL);
    assertThat(ProtocolContext.get()).isEqualTo("http");
  }

  private static Method findDeclared(final String name, final Class<?>[] parameterTypes) {
    try {
      return MongoDBBackend.class.getDeclaredMethod(name, parameterTypes);
    } catch (final NoSuchMethodException e) {
      return null;
    }
  }

  private void authenticate() {
    final byte[] payload = ("\u0000root\u0000" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8);
    final Document sasl = new Document("saslStart", 1);
    sasl.put("mechanism", "PLAIN");
    sasl.put("payload", new BinData(payload));
    backend.handleCommand(channel, getDatabaseName(), "saslStart", sasl);
  }

  /** The real backend, opening a database that records the protocol tag each call arrives under. */
  private static final class RecordingBackend extends MongoDBBackend {
    private final Map<String, String> tags = new ConcurrentHashMap<>();

    RecordingBackend(final Issue10390MongoProtocolContextTest test) {
      super(test.getServer(0), null);
    }

    String recorded(final String method) {
      return tags.get(method);
    }

    @Override
    protected MongoDatabase openOrCreateDatabase(final String databaseName) {
      return new RecordingDatabase(databaseName, tags);
    }
  }

  private record RecordingDatabase(String name, Map<String, String> tags) implements MongoDatabase {
    /** A collection whose OP_QUERY fails, to drive the error path of the entry point. */
    static final String FAILING = "Failing";

    private void record(final String method) {
      tags.put(method, ProtocolContext.get());
    }

    @Override
    public String getDatabaseName() {
      return name;
    }

    @Override
    public void handleClose(final Channel channel) {
      record("handleClose");
    }

    @Override
    public Document handleCommand(final Channel channel, final String command, final Document query,
        final DatabaseResolver resolver, final Oplog oplog) {
      record("handleCommand");
      final Document response = new Document("ok", 1.0);
      response.put("cursor", new Document("firstBatch", List.of()));
      return response;
    }

    @Override
    public QueryResult handleQuery(final MongoQuery query) {
      record("handleQuery");
      if (FAILING.equals(query.getCollectionName()))
        throw new MongoServerException("the query failed");
      return new QueryResult();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public MongoCollection<?> createCollectionOrThrowIfExists(final String collectionName, final CollectionOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MongoCollection<?> resolveCollection(final String collectionName, final boolean throwIfNotFound) {
      record("resolveCollection");
      // LIKE MongoDBDatabaseWrapper.resolveCollection
      return null;
    }

    @Override
    public void drop(final Oplog oplog) {
      record("drop");
    }

    @Override
    public void dropCollection(final String collectionName, final Oplog oplog) {
      record("dropCollection");
    }

    @Override
    public void moveCollection(final MongoDatabase other, final MongoCollection<?> collection, final String newName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterCollection(final String collectionName) {
      record("unregisterCollection");
    }
  }
}
