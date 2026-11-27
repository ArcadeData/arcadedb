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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityDatabaseUser.ACCESS;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The commands of the MongoDB wire protocol that answer about a collection (its documents, how many of them there are, how many
 * indexes it has) check what the caller may do on it first, as the SQL statements do: a caller who may not read the collection, or
 * who may not change the schema where the command is a schema change, is refused whatever the collection holds, and gets nothing
 * of it in the answer. A {@code createIndexes} names at least one index, as {@code mongod} requires.
 * <p>
 * {@code count}, {@code find} and {@code aggregate} are refused by the engine when the bucket of the collection is counted or
 * opened, empty or not: the test of them pins that, it is the {@code createIndexes} ones that ask for the checks themselves.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoCommandAccessOnCollectionTest {
  private static final String PATH          = "target/databases/MongoCommandAccessOnCollectionTest";
  private static final int    UNAUTHORIZED  = 13;
  private static final int    BAD_VALUE     = 2;
  private static final int    MISSING_FIELD = 40414;
  private static final String FILLED        = "Filled";
  private static final String EMPTY         = "Empty";
  private static final String INDEXED       = "Indexed";
  private static final String MISSING       = "Missing";

  private DatabaseFactory        factory;
  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE " + FILLED);
    database.command("sql", "CREATE DOCUMENT TYPE " + EMPTY);
    database.command("sql", "CREATE DOCUMENT TYPE " + INDEXED);
    database.transaction(() -> {
      for (int k = 1; k <= 3; k++)
        database.newDocument(FILLED).set("k", k).save();
    });
    wrapper = new MongoDBDatabaseWrapper(database, null, null);

    // no user is bound: the fixture is made as the embedded caller, which nothing refuses
    new MongoDBCollectionWrapper(database, INDEXED).insertDocuments(List.of(new Document("_id", 1).append("k", 1)));
    command("createIndexes", index(INDEXED, "k"));
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(null);
    while (database.isTransactionActive())
      database.rollback();
    MongoDBCollectionWrapper.forgetIdIndexes(database);
    database.drop();
    factory.close();
  }

  @Test
  void aUserWhoMayNotReadTheCollectionIsRefusedByTheCommandsThatReadIt() {
    final SecurityDatabaseUser noRead = user(false, ACCESS.CREATE_RECORD, ACCESS.UPDATE_RECORD, ACCESS.DELETE_RECORD);
    for (final String collection : List.of(FILLED, EMPTY, INDEXED)) {
      assertRefused(noRead, collection + " count", () -> command("count", new Document("count", collection)));
      assertRefused(noRead, collection + " count with a filter",
          () -> command("count", new Document("count", collection).append("query", new Document("k", 1))));
      assertRefused(noRead, collection + " find", () -> command("find", new Document("find", collection)));
      assertRefused(noRead, collection + " find with a filter",
          () -> command("find", new Document("find", collection).append("filter", new Document("k", 1))));
      assertRefused(noRead, collection + " aggregate", () -> command("aggregate", aggregate(collection)));
      assertRefused(noRead, collection + " aggregate with a match",
          () -> command("aggregate", new Document("aggregate", collection).append("pipeline", List.of(new Document("$match", new Document("k", 1))))
              .append("cursor", new Document())));
    }
  }

  @Test
  void aUserWhoMayNotReadTheCollectionIsRefusedByTheIndexCommand() {
    // it may change the schema, and still not read the collection
    final SecurityDatabaseUser schemaOnly = user(true);
    final int indexes = indexCount(INDEXED);
    for (final String collection : List.of(FILLED, EMPTY, INDEXED)) {
      assertRefused(schemaOnly, collection + " with no list", () -> command("createIndexes", new Document("createIndexes", collection)));
      assertRefused(schemaOnly, collection + " with an empty list",
          () -> command("createIndexes", new Document("createIndexes", collection).append("indexes", List.of())));
      assertRefused(schemaOnly, collection + " with an index", () -> command("createIndexes", index(collection, "k")));
    }
    assertThat(indexCount(INDEXED)).as("nothing was built").isEqualTo(indexes);
    assertThat(database.getSchema().getType(FILLED).getAllIndexes(false)).isEmpty();
  }

  @Test
  void aUserWhoMayNotChangeTheSchemaIsRefusedByTheIndexCommandWhateverItAsks() {
    final SecurityDatabaseUser reader = user(false, ACCESS.READ_RECORD);
    for (final String collection : List.of(FILLED, EMPTY, INDEXED, MISSING)) {
      assertRefused(reader, collection + " with no list", () -> command("createIndexes", new Document("createIndexes", collection)));
      assertRefused(reader, collection + " with an empty list",
          () -> command("createIndexes", new Document("createIndexes", collection).append("indexes", List.of())));
      assertRefused(reader, collection + " with an index", () -> command("createIndexes", index(collection, "k")));
      assertRefused(reader, collection + " with a unique index", () -> command("createIndexes", unique(collection, "k")));
    }
    assertThat(database.getSchema().existsType(MISSING)).as("no collection was created").isFalse();
  }

  @Test
  void theIndexCommandNamesAtLeastOneIndex() {
    final SecurityDatabaseUser full = user(true, ACCESS.values());
    final int indexes = indexCount(INDEXED);
    for (final String collection : List.of(FILLED, INDEXED, MISSING)) {
      assertThat(refusedWith(full, () -> command("createIndexes", new Document("createIndexes", collection)))).as(collection + " with no list")
          .isEqualTo(MISSING_FIELD);
      assertThat(refusedWith(full, () -> command("createIndexes", new Document("createIndexes", collection).append("indexes", List.of()))))
          .as(collection + " with an empty list").isEqualTo(BAD_VALUE);
    }
    assertThat(database.getSchema().existsType(MISSING)).as("a refused command creates no collection").isFalse();
    assertThat(indexCount(INDEXED)).isEqualTo(indexes);
  }

  @Test
  void aUserWhoMayReadIsAnswered() {
    final SecurityDatabaseUser reader = user(false, ACCESS.READ_RECORD);

    assertThat(as(reader, () -> command("count", new Document("count", FILLED))).get("n")).isEqualTo(3);
    assertThat(as(reader, () -> command("count", new Document("count", FILLED).append("query", new Document("k", 2)))).get("n")).isEqualTo(1);
    assertThat(as(reader, () -> command("count", new Document("count", EMPTY))).get("n")).isEqualTo(0);
    assertThat(firstBatch(as(reader, () -> command("find", new Document("find", FILLED))))).hasSize(3);
    assertThat(firstBatch(as(reader, () -> command("find", new Document("find", EMPTY))))).isEmpty();
    assertThat(firstBatch(as(reader, () -> command("aggregate", aggregate(FILLED))))).hasSize(3);
    assertThat(firstBatch(as(reader, () -> command("aggregate", aggregate(EMPTY))))).isEmpty();
  }

  @Test
  void aUserWhoMayReadAndChangeTheSchemaBuildsIndexesAndIsTold() {
    final SecurityDatabaseUser user = user(true, ACCESS.READ_RECORD);

    final Document created = as(user, () -> command("createIndexes", index(FILLED, "k")));
    assertThat(created.get("ok")).isEqualTo(1.0);
    assertThat(created.get("createdCollectionAutomatically")).isEqualTo(false);
    // the _id index every collection reports is counted, built or not
    final int before = (Integer) created.get("numIndexesBefore");
    assertThat(before).isEqualTo(1);
    assertThat(created.get("numIndexesAfter")).isEqualTo(before + 1);

    // an index that exists costs nothing
    final Document again = as(user, () -> command("createIndexes", index(FILLED, "k")));
    assertThat(again.get("numIndexesBefore")).isEqualTo(before + 1);
    assertThat(again.get("numIndexesAfter")).isEqualTo(before + 1);

    // a collection of the command's own making is not one that was there to be read
    final Document made = as(user, () -> command("createIndexes", index(MISSING, "k")));
    assertThat(made.get("createdCollectionAutomatically")).isEqualTo(true);
    assertThat(database.getSchema().existsType(MISSING)).isTrue();
  }

  @Test
  void aCollectionThatDoesNotExistIsAnsweredAsBefore() {
    final SecurityDatabaseUser noRead = user(false);

    assertThat(as(noRead, () -> command("count", new Document("count", MISSING))).get("n")).isEqualTo(0);
    assertThat(firstBatch(as(noRead, () -> command("find", new Document("find", MISSING))))).isEmpty();
    assertThat(firstBatch(as(noRead, () -> command("aggregate", aggregate(MISSING))))).isEmpty();
    assertThat(database.getSchema().existsType(MISSING)).isFalse();
  }

  @Test
  void aCallWithNoUserBoundIsNotRefused() {
    assertThat(command("count", new Document("count", FILLED)).get("n")).isEqualTo(3);
    assertThat(firstBatch(command("find", new Document("find", FILLED)))).hasSize(3);
    assertThat(command("createIndexes", index(FILLED, "k")).get("ok")).isEqualTo(1.0);
  }

  private Document command(final String name, final Document document) {
    return wrapper.handleCommand(null, name, document, null, null);
  }

  private int indexCount(final String collection) {
    return database.getSchema().getType(collection).getAllIndexes(false).size();
  }

  @SuppressWarnings("unchecked")
  private static List<Document> firstBatch(final Document reply) {
    return (List<Document>) ((Document) reply.get("cursor")).get("firstBatch");
  }

  private static Document aggregate(final String collection) {
    return new Document("aggregate", collection).append("pipeline", List.of()).append("cursor", new Document());
  }

  private static Document index(final String collection, final String field) {
    return new Document("createIndexes", collection).append("indexes", List.of(new Document("key", new Document(field, 1)).append("name", field + "_1")));
  }

  private static Document unique(final String collection, final String field) {
    return new Document("createIndexes", collection).append("indexes",
        List.of(new Document("key", new Document(field, 1)).append("name", field + "_unique").append("unique", true)));
  }

  private void assertRefused(final SecurityDatabaseUser user, final String what, final Runnable command) {
    final int code = refusedWith(user, () -> {
      command.run();
      return null;
    });
    assertThat(code).as(what + ": the command ran for a user who may not run it").isEqualTo(UNAUTHORIZED);
  }

  /** The code the command is refused with, or 0 when it answered. */
  private int refusedWith(final SecurityDatabaseUser user, final Supplier<?> command) {
    bind(user);
    try {
      command.get();
    } catch (final MongoServerError e) {
      return e.getCode();
    } catch (final SecurityException e) {
      fail("a refusal must reach the client as a coded error, not as " + e);
    } finally {
      bind(null);
    }
    return 0;
  }

  private <T> T as(final SecurityDatabaseUser user, final Supplier<T> work) {
    bind(user);
    try {
      return work.get();
    } finally {
      bind(null);
    }
  }

  private void bind(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser user(final boolean schema, final ACCESS... records) {
    return new AccessUser(records.length == 0 ? EnumSet.noneOf(ACCESS.class) : EnumSet.copyOf(List.of(records)), schema);
  }

  /** A user granted the record accesses it is given, and the schema one only when it says so. */
  private record AccessUser(Set<ACCESS> records, boolean schema) implements SecurityDatabaseUser {
    @Override
    public String getName() {
      return "limited";
    }

    @Override
    public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
      return schema && access == DATABASE_ACCESS.UPDATE_SCHEMA;
    }

    @Override
    public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
      return records.contains(access);
    }

    @Override
    public boolean requestAccessOnType(final String typeName, final ACCESS access) {
      return records.contains(access);
    }

    @Override
    public long getResultSetLimit() {
      return -1L;
    }

    @Override
    public long getReadTimeout() {
      return -1L;
    }
  }
}
