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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.security.ServerSecurity;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code find} answers a first batch and leaves a cursor for the rest, as mongod does, instead of converting every document of the
 * collection before the first one is sent: the answer is the same documents in the same order whatever the cursor options, the first
 * batch converts no more than it holds, and what a cursor holds is released when it is exhausted, killed, abandoned with its
 * connection or idle.
 */
public class MongoFindBatchesTest extends BaseMongoServerTest {
  private static final int    DOCUMENTS = 10_000;
  private static final String LIMITED_USER     = "cursorOwnerOther";
  private static final String LIMITED_PASSWORD = "cursorOwnerOther1";

  private MongoClient               client;
  private MongoCollection<Document> collection;
  private List<Document>            expected;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    getDatabase(0);
    client = newClient("root", DEFAULT_PASSWORD_FOR_TESTS);
    client.getDatabase(getDatabaseName()).createCollection("c");
    collection = client.getDatabase(getDatabaseName()).getCollection("c");
    expected = new ArrayList<>(DOCUMENTS);
    for (int i = 0; i < DOCUMENTS; i++)
      expected.add(new Document("_id", i).append("n", i).append("g", i % 7).append("s", "v" + i));
    collection.insertMany(expected);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security != null && security.getUser(LIMITED_USER) != null)
        security.dropUser(LIMITED_USER);
    } catch (final Exception e) {
      // the server may already be stopped
    }
    if (client != null)
      client.close();
    super.endTest();
  }

  private MongoClient newClient(final String user, final String password) {
    return new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential(user, getDatabaseName(), password.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  private MongoDBDatabaseWrapper wrapper() {
    for (final ServerPlugin plugin : getServer(0).getPlugins())
      if (plugin instanceof MongoDBProtocolPlugin p && p.wrapperOf(getDatabaseName()) != null)
        return p.wrapperOf(getDatabaseName());
    throw new IllegalStateException("no Mongo plugin");
  }

  private Document run(final Document command) {
    return client.getDatabase(getDatabaseName()).runCommand(command);
  }

  private static Document cursorOf(final Document reply) {
    return (Document) reply.get("cursor");
  }

  @SuppressWarnings("unchecked")
  private static List<Document> batchOf(final Document reply, final String key) {
    return (List<Document>) cursorOf(reply).get(key);
  }

  private static long idOf(final Document reply) {
    return ((Number) cursorOf(reply).get("id")).longValue();
  }

  /** What the answer of a find must be, computed from the inserted documents. */
  private List<Document> model(final Predicate<Document> filter, final String sortField, final int direction,
      final int skip, final int limit, final String project) {
    Stream<Document> stream = expected.stream().filter(filter);
    if (sortField != null)
      stream = stream.sorted((a, b) -> direction * Integer.compare(a.getInteger(sortField), b.getInteger(sortField)));
    stream = stream.skip(skip);
    if (limit > 0)
      stream = stream.limit(limit);
    return stream.map(d -> project == null ? d : new Document(project, d.get(project))).collect(Collectors.toList());
  }

  @Test
  void theDocumentsAreTheSameWhateverTheCursorOptions() {
    final int[] skips = { 0, 17, 9_999, 20_000 };
    final int[] limits = { 0, 1, 5, 101, 102, 1_000, 20_000 };
    final Integer[] batchSizes = { null, 1, 7, 101, 5_000 };
    int compared = 0;
    for (final boolean filtered : new boolean[] { false, true })
      for (final boolean sorted : new boolean[] { false, true })
        for (final boolean projected : new boolean[] { false, true })
          for (final int skip : skips)
            for (final int limit : limits) {
              // the batch size is rotated through the combinations: every size meets every other option without 280 x 5 queries
              final Integer batchSize = batchSizes[(compared++) % batchSizes.length];
              final var find = collection.find(filtered ? new Document("g", 3) : new Document());
              if (sorted)
                find.sort(new Document("n", -1));
              if (projected)
                find.projection(new Document("n", 1).append("_id", 0));
              find.skip(skip).limit(limit);
              if (batchSize != null)
                find.batchSize(batchSize);

              final List<Document> got = find.into(new ArrayList<>());
              final List<Document> want = model(d -> !filtered || d.getInteger("g") == 3, sorted ? "n" : null, -1, skip, limit,
                  projected ? "n" : null);
              assertThat(got).as("filtered=%s sorted=%s projected=%s skip=%d limit=%d batchSize=%s", filtered, sorted, projected, skip,
                  limit, batchSize).isEqualTo(want);
            }
    assertThat(compared).isEqualTo(2 * 2 * 2 * skips.length * limits.length);
    assertThat(wrapper().openCursors()).as("every cursor was read to its end").isZero();
  }

  @Test
  void aFindAnswersAFirstBatchOf101DocumentsAndALeftoverCursor() {
    final Document reply = run(new Document("find", "c").append("filter", new Document()));
    assertThat(batchOf(reply, "firstBatch")).hasSize(101);
    assertThat(idOf(reply)).isNotZero();
    assertThat(wrapper().openCursors()).isEqualTo(1);

    // a getMore with no batch size answers the rest in one batch, and the cursor is gone with the last document
    final Document more = run(new Document("getMore", idOf(reply)).append("collection", "c"));
    assertThat(batchOf(more, "nextBatch")).hasSize(DOCUMENTS - 101);
    assertThat(idOf(more)).isZero();
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void theBatchSizeOfTheFindAndOfTheGetMoreAreHonoured() {
    final Document reply = run(new Document("find", "c").append("batchSize", 3));
    assertThat(batchOf(reply, "firstBatch")).extracting(d -> d.get("n")).containsExactly(0, 1, 2);
    final long id = idOf(reply);
    final Document more = run(new Document("getMore", id).append("collection", "c").append("batchSize", 2));
    assertThat(batchOf(more, "nextBatch")).extracting(d -> d.get("n")).containsExactly(3, 4);
    assertThat(idOf(more)).isEqualTo(id);
    assertThat(wrapper().openCursors()).isEqualTo(1);
  }

  @Test
  void aBatchSizeOfZeroOpensTheCursorWithoutAnyDocument() {
    final Document reply = run(new Document("find", "c").append("batchSize", 0));
    assertThat(batchOf(reply, "firstBatch")).isEmpty();
    assertThat(idOf(reply)).isNotZero();
    final Document more = run(new Document("getMore", idOf(reply)).append("collection", "c").append("batchSize", 4));
    assertThat(batchOf(more, "nextBatch")).extracting(d -> d.get("n")).containsExactly(0, 1, 2, 3);
  }

  @Test
  void aNegativeBatchSizeIsRefusedAsMongodRefusesIt() {
    assertThatThrownBy(() -> run(new Document("find", "c").append("batchSize", -1))).isInstanceOfSatisfying(MongoCommandException.class,
        e -> assertThat(e.getErrorCode()).isEqualTo(2));
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void aSingleBatchHoldsTheLimitAndLeavesNoCursor() {
    final Document reply = run(new Document("find", "c").append("limit", 500).append("singleBatch", true));
    assertThat(batchOf(reply, "firstBatch")).hasSize(500);
    assertThat(idOf(reply)).isZero();

    final Document sized = run(new Document("find", "c").append("limit", 500).append("batchSize", 10).append("singleBatch", true));
    assertThat(batchOf(sized, "firstBatch")).hasSize(10);
    assertThat(idOf(sized)).isZero();

    // a negative limit is the legacy spelling of the same request
    final Document legacy = run(new Document("find", "c").append("limit", -250));
    assertThat(batchOf(legacy, "firstBatch")).hasSize(250);
    assertThat(idOf(legacy)).isZero();
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void theLimitIsCountedAcrossTheBatches() {
    final Document reply = run(new Document("find", "c").append("limit", 205).append("batchSize", 100));
    assertThat(batchOf(reply, "firstBatch")).hasSize(100);
    final Document second = run(new Document("getMore", idOf(reply)).append("collection", "c").append("batchSize", 100));
    assertThat(batchOf(second, "nextBatch")).hasSize(100);
    final Document third = run(new Document("getMore", idOf(second)).append("collection", "c").append("batchSize", 100));
    assertThat(batchOf(third, "nextBatch")).hasSize(5);
    assertThat(idOf(third)).as("the limit is reached: no cursor is left").isZero();
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void aFindThatFitsTheFirstBatchLeavesNoCursor() {
    final Document reply = run(new Document("find", "c").append("filter", new Document("g", 3)).append("limit", 50));
    assertThat(batchOf(reply, "firstBatch")).hasSize(50);
    assertThat(idOf(reply)).isZero();
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void aBatchStopsAtTheSizeAReplyMayHave() {
    // 30 documents of 1 MB: the 101 of a first batch would be 100 MB in one reply, past the 48 MB a driver accepts
    final MongoCollection<Document> big = client.getDatabase(getDatabaseName()).getCollection("big");
    final String megabyte = "x".repeat(1_000_000);
    final List<Document> documents = IntStream.range(0, 30).mapToObj(i -> new Document("_id", i).append("blob", megabyte))
        .collect(Collectors.toList());
    big.insertMany(documents);

    final Document reply = run(new Document("find", "big"));
    final int first = batchOf(reply, "firstBatch").size();
    assertThat(first).isBetween(1, 17);
    assertThat(idOf(reply)).isNotZero();
    run(new Document("killCursors", "big").append("cursors", List.of(idOf(reply))));

    assertThat(big.find().sort(new Document("_id", 1)).into(new ArrayList<>())).hasSize(30);
    assertThat(big.find().into(new ArrayList<>())).hasSize(30);
    assertThat(wrapper().openCursors()).isZero();
  }

  @Test
  void killCursorsClosesTheCursor() {
    final Document reply = run(new Document("find", "c").append("batchSize", 5));
    final long id = idOf(reply);
    assertThat(wrapper().openCursors()).isEqualTo(1);

    final Document killed = run(new Document("killCursors", "c").append("cursors", List.of(id, 12345L)));
    assertThat(killed.get("cursorsKilled")).isEqualTo(List.of(id));
    assertThat(killed.get("cursorsNotFound")).isEqualTo(List.of(12345L));
    assertThat(wrapper().openCursors()).isZero();

    assertThatThrownBy(() -> run(new Document("getMore", id).append("collection", "c"))).isInstanceOfSatisfying(
        MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(43));
  }

  @Test
  void theConnectionThatLastReadACursorTakesItAwayWhenItCloses() {
    // a cursor opened and left behind on a connection of its own
    try (final MongoClient other = newClient("root", DEFAULT_PASSWORD_FOR_TESTS)) {
      final Document reply = other.getDatabase(getDatabaseName()).runCommand(new Document("find", "c").append("batchSize", 5));
      assertThat(idOf(reply)).isNotZero();
      assertThat(wrapper().openCursors()).isEqualTo(1);
    }
    // the server notices the closed socket on its own thread
    for (int i = 0; i < 200 && wrapper().openCursors() > 0; i++)
      sleepBriefly();
    assertThat(wrapper().openCursors()).as("the cursor went with its connection").isZero();
  }

  @Test
  void aCursorBelongsToTheUserAndTheCollectionItWasOpenedOn() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", LIMITED_USER).put("password", security.encodePassword(LIMITED_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));

    final long id = idOf(run(new Document("find", "c").append("batchSize", 5)));

    try (final MongoClient other = newClient(LIMITED_USER, LIMITED_PASSWORD)) {
      final var db = other.getDatabase(getDatabaseName());
      assertThatThrownBy(() -> db.runCommand(new Document("getMore", id).append("collection", "c"))).isInstanceOfSatisfying(
          MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(43));
      final Document killed = db.runCommand(new Document("killCursors", "c").append("cursors", List.of(id)));
      assertThat(killed.get("cursorsKilled")).isEqualTo(List.of());
    }
    assertThat(wrapper().openCursors()).as("the other user could neither read nor close it").isEqualTo(1);

    assertThatThrownBy(() -> run(new Document("getMore", id).append("collection", "another"))).isInstanceOfSatisfying(
        MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(13));
    assertThat(batchOf(run(new Document("getMore", id).append("collection", "c").append("batchSize", 2)), "nextBatch")).hasSize(2);
  }

  @Test
  void aCursorIdleForTooLongIsClosed() {
    final MongoDBDatabaseWrapper wrapper = wrapper();
    final long id = idOf(run(new Document("find", "c").append("batchSize", 5)));
    assertThat(wrapper.openCursors()).isEqualTo(1);

    wrapper.purgeIdleCursors(System.nanoTime());
    assertThat(wrapper.openCursors()).as("just used").isEqualTo(1);

    wrapper.purgeIdleCursors(System.nanoTime() + MongoDBDatabaseWrapper.CURSOR_IDLE_NANOS + 1_000_000_000L);
    assertThat(wrapper.openCursors()).isZero();
    assertThatThrownBy(() -> run(new Document("getMore", id).append("collection", "c"))).isInstanceOf(MongoCommandException.class);
  }

  @Test
  void aCursorOpenedWithNoCursorTimeoutIsNotClosedForBeingIdle() {
    final MongoDBDatabaseWrapper wrapper = wrapper();
    final long kept = idOf(run(new Document("find", "c").append("batchSize", 5).append("noCursorTimeout", true)));
    final long reaped = idOf(run(new Document("find", "c").append("batchSize", 5)));
    assertThat(wrapper.openCursors()).isEqualTo(2);

    wrapper.purgeIdleCursors(System.nanoTime() + MongoDBDatabaseWrapper.CURSOR_IDLE_NANOS + 1_000_000_000L);
    assertThat(wrapper.openCursors()).isEqualTo(1);
    assertThat(batchOf(run(new Document("getMore", kept).append("collection", "c").append("batchSize", 1)), "nextBatch")).hasSize(1);
    assertThatThrownBy(() -> run(new Document("getMore", reaped).append("collection", "c"))).isInstanceOf(MongoCommandException.class);
  }

  @Test
  void aBatchSizeOfAnotherTypeOrANegativeOneOnAGetMoreIsRefused() {
    assertThatThrownBy(() -> run(new Document("find", "c").append("batchSize", "ten"))).isInstanceOfSatisfying(MongoCommandException.class,
        e -> assertThat(e.getErrorCode()).isEqualTo(14));
    assertThat(wrapper().openCursors()).isZero();

    final long id = idOf(run(new Document("find", "c").append("batchSize", 5)));
    assertThatThrownBy(() -> run(new Document("getMore", id).append("collection", "c").append("batchSize", -3))).isInstanceOfSatisfying(
        MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(2));
    assertThat(batchOf(run(new Document("getMore", id).append("collection", "c").append("batchSize", 2)), "nextBatch")).as(
        "the refused getMore did not move the cursor").extracting(d -> d.get("n")).containsExactly(5, 6);
  }

  @Test
  void twoCursorsOnTheSameCollectionAdvanceIndependently() {
    final long first = idOf(run(new Document("find", "c").append("batchSize", 2)));
    final long second = idOf(run(new Document("find", "c").append("filter", new Document("g", 1)).append("batchSize", 2)));
    assertThat(first).isNotEqualTo(second);
    assertThat(batchOf(run(new Document("getMore", first).append("collection", "c").append("batchSize", 2)), "nextBatch")).extracting(
        d -> d.get("n")).containsExactly(2, 3);
    assertThat(batchOf(run(new Document("getMore", second).append("collection", "c").append("batchSize", 2)), "nextBatch")).extracting(
        d -> d.get("n")).containsExactly(15, 22);
    assertThat(batchOf(run(new Document("getMore", first).append("collection", "c").append("batchSize", 1)), "nextBatch")).extracting(
        d -> d.get("n")).containsExactly(4);
    assertThat(wrapper().openCursors()).isEqualTo(2);
  }

  @Test
  void aClientCannotHoardMoreThanTheCursorCap() {
    final MongoDBDatabaseWrapper wrapper = wrapper();
    final long first = idOf(run(new Document("find", "c").append("batchSize", 1)));
    for (int i = 0; i < MongoDBDatabaseWrapper.MAX_OPEN_CURSORS; i++)
      run(new Document("find", "c").append("batchSize", 1));
    assertThat(wrapper.openCursors()).isEqualTo(MongoDBDatabaseWrapper.MAX_OPEN_CURSORS);
    assertThatThrownBy(() -> run(new Document("getMore", first).append("collection", "c"))).as("the idlest one made room")
        .isInstanceOf(MongoCommandException.class);
  }

  @Test
  void aDroppedCollectionTakesItsCursorsWithIt() {
    run(new Document("find", "c").append("batchSize", 5));
    assertThat(wrapper().openCursors()).isEqualTo(1);
    wrapper().dropCollection("c", null);
    assertThat(wrapper().openCursors()).isZero();
  }

  private static void sleepBriefly() {
    try {
      Thread.sleep(25L);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
