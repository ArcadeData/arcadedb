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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #11502 (#11449, #11445, #11444): the MongoDB wire replies diverged from {@code mongod}'s.
 * <ul>
 *   <li>{@code count} of a collection that does not exist answered {@code NamespaceNotFound} instead of {@code n:0}, and the count
 *   of a collection past 2^31 documents wrapped, being narrowed to an int;</li>
 *   <li>an unsupported command or update operator was an uncoded error that named neither the command nor the operator;</li>
 *   <li>{@code nModified} counted every matched document, including those the update left as they were;</li>
 *   <li>a document a declared constraint refuses failed the whole command, rolling back the other documents of the batch, where
 *   {@code mongod} reports it as an entry of {@code writeErrors} and keeps the others.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11502MongoWireRepliesTest extends BaseMongoServerTest {
  private MongoClient               client;
  private MongoDatabase             mongoDatabase;
  private MongoCollection<Document> collection;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    mongoDatabase = client.getDatabase(getDatabaseName());
    mongoDatabase.createCollection("doc");
    collection = mongoDatabase.getCollection("doc");
    for (int i = 1; i <= 3; i++)
      collection.insertOne(new Document("_id", i).append("a", 1).append("b", "x"));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  private Database serverDatabase() {
    return getServerDatabase(0, getDatabaseName());
  }

  @Nested
  class CountOfAMissingCollection {
    @Test
    void theCountCommandAnswersZero() {
      final Document reply = mongoDatabase.runCommand(new Document("count", "nosuch"));
      assertThat(reply.get("n", Number.class).longValue()).isZero();
      assertThat(reply.get("ok", Number.class).doubleValue()).isEqualTo(1.0);

      final Document filtered = mongoDatabase.runCommand(new Document("count", "nosuch").append("query", new Document("a", 1)));
      assertThat(filtered.get("n", Number.class).longValue()).isZero();
    }

    @Test
    void theDriverHelpersAnswerZero() {
      final MongoCollection<Document> missing = mongoDatabase.getCollection("nosuch");
      assertThat(missing.count()).isZero();
      assertThat(missing.estimatedDocumentCount()).isZero();
      assertThat(missing.countDocuments()).isZero();
      assertThat(serverDatabase().getSchema().existsType("nosuch")).isFalse();
    }

    @Test
    void anInvalidFilterIsStillAnErrorOnAMissingCollection() {
      final MongoCommandException e = catchThrowableOfType(MongoCommandException.class,
          () -> mongoDatabase.runCommand(new Document("count", "nosuch").append("query", new Document("a", new Document("$foo", 1)))));
      assertThat(e).isNotNull();
    }

    @Test
    void anExistingCollectionIsStillCounted() {
      final Document reply = mongoDatabase.runCommand(new Document("count", "doc"));
      assertThat(reply.get("n")).isEqualTo(3);
      assertThat(collection.estimatedDocumentCount()).isEqualTo(3L);
    }

    @Test
    void skipAndLimitBoundTheCount() {
      assertThat(count(new Document("skip", 1))).isEqualTo(2);
      assertThat(count(new Document("limit", 2))).isEqualTo(2);
      assertThat(count(new Document("skip", 1).append("limit", 1))).isEqualTo(1);
      assertThat(count(new Document("skip", 5))).isEqualTo(0);
      assertThat(count(new Document("skip", 1).append("query", new Document("b", "x")))).isEqualTo(2);
    }

    @Test
    void aSkipOrLimitThatIsNoNumberIsRefused() {
      for (final String option : List.of("skip", "limit")) {
        final MongoCommandException e = catchThrowableOfType(MongoCommandException.class,
            () -> mongoDatabase.runCommand(new Document("count", "doc").append(option, "1")));
        assertThat(e).as(option).isNotNull();
        assertThat(e.getErrorCode()).as(option).isEqualTo(14);
      }
    }

    private Object count(final Document options) {
      final Document command = new Document("count", "doc");
      command.putAll(options);
      return mongoDatabase.runCommand(command).get("n");
    }
  }

  /**
   * A test cannot store 2^31 documents: the database the wrappers run against answers a type count past it, everything else is the
   * real database.
   */
  @Nested
  class CountPastAnInt {
    @Test
    void theCountReplyCarriesTheWholeCount() {
      final Database database = countingAs(3_000_000_000L);
      assertThat(countReply(database)).isEqualTo(3_000_000_000L);

      final MongoDBCollectionWrapper doc = new MongoDBCollectionWrapper(database, "doc");
      assertThat(doc.countDocuments(null, 0, 0)).isEqualTo(3_000_000_000L);
      // the library's interface answers an int: it is clamped, never wrapped
      assertThat(doc.count()).isEqualTo(Integer.MAX_VALUE);
      assertThat(doc.count(null, 0, -1)).isEqualTo(Integer.MAX_VALUE);
    }

    /** SQL SKIP and LIMIT narrow their bound to an int: the bounds of an unfiltered count are applied to the long count instead. */
    @Test
    void skipAndLimitPastAnIntAreNotNarrowed() {
      final MongoDBCollectionWrapper doc = new MongoDBCollectionWrapper(countingAs(4_000_000_000L), "doc");
      assertThat(doc.countDocuments(null, 2_500_000_000L, 0)).isEqualTo(1_500_000_000L);
      assertThat(doc.countDocuments(null, 0, 3_000_000_000L)).isEqualTo(3_000_000_000L);
      assertThat(doc.countDocuments(null, 500_000_000L, 3_000_000_000L)).isEqualTo(3_000_000_000L);
      assertThat(doc.countDocuments(null, 5_000_000_000L, 0)).isZero();
    }

    @Test
    void aCountThatFitsAnIntStaysAnInt() {
      assertThat(countReply(countingAs(Integer.MAX_VALUE))).isEqualTo(Integer.MAX_VALUE);
    }

    /**
     * The {@code n} the {@code count} command of collection {@code doc} replies, run on the wrapper directly. The wrapper speaks the
     * library's BSON document, whose simple name the driver's takes in this class, hence the qualified name.
     */
    private Object countReply(final Database database) {
      return new MongoDBDatabaseWrapper(database, null, null)
          .handleCommand(null, "count", new de.bwaldvogel.mongo.bson.Document("count", "doc"), null, null).get("n");
    }

    private Database countingAs(final long count) {
      final Database real = serverDatabase();
      return (Database) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { DatabaseInternal.class },
          (proxy, method, args) -> {
            if ("countType".equals(method.getName()))
              return count;
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  @Nested
  class UnsupportedCommandsAndOperators {
    @Test
    void anUnknownCommandIsCommandNotFound() {
      final MongoCommandException e = catchThrowableOfType(MongoCommandException.class,
          () -> mongoDatabase.runCommand(new Document("foo11502", 1)));
      assertThat(e).isNotNull();
      assertThat(e.getErrorCode()).isEqualTo(59);
      assertThat(e.getErrorCodeName()).isEqualTo("CommandNotFound");
      assertThat(e.getErrorMessage()).contains("no such command: 'foo11502'");
    }

    @Test
    void aMongoModifierArcadeDbLacksIsNotImplementedAndNamed() {
      for (final Document filter : List.of(new Document("_id", 1), new Document())) {
        final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
            () -> collection.updateOne(filter, new Document("$push", new Document("tags", "z"))));
        assertThat(e).as("filter %s", filter).isNotNull();
        assertThat(e.getError().getCode()).isEqualTo(238);
        assertThat(e.getError().getMessage()).contains("$push");
      }
      assertThat(collection.find(eq("_id", 1)).first()).doesNotContainKey("tags");
    }

    @Test
    void anUpsertWithAModifierArcadeDbLacksIsNotImplemented() {
      final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
          () -> collection.updateOne(eq("_id", 99), new Document("$setOnInsert", new Document("a", 5)), new UpdateOptions().upsert(true)));
      assertThat(e).isNotNull();
      assertThat(e.getError().getCode()).isEqualTo(238);
      assertThat(e.getError().getMessage()).contains("$setOnInsert");
      assertThat(collection.countDocuments(eq("_id", 99))).isZero();
    }

    @Test
    void anUnknownModifierFailsToParse() {
      final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
          () -> collection.updateOne(eq("_id", 1), new Document("$foo", new Document("a", 2))));
      assertThat(e).isNotNull();
      assertThat(e.getError().getCode()).isEqualTo(9);
      assertThat(e.getError().getMessage()).contains("Unknown modifier: $foo");
    }
  }

  @Nested
  class ModifiedCount {
    @Test
    void anUpdateThatChangesNothingModifiesNothing() {
      final AtomicInteger written = new AtomicInteger();
      serverDatabase().getSchema().getType("doc").getEvents()
          .registerListener((AfterRecordUpdateListener) record -> written.incrementAndGet());

      assertModified(update(new Document("_id", 1), new Document("$set", new Document("a", 1)), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("$inc", new Document("a", 0)), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("a", 1).append("b", "x"), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("$unset", new Document("nope", "")), false), 1, 0);
      assertModified(update(new Document(), new Document("$set", new Document("b", "x")), true), 3, 0);
      assertModified(update(new Document(), new Document("$set", new Document("a", 1)), false), 1, 0);
      assertModified(update(new Document("b", "x"), new Document("$set", new Document("b", "x")), true), 3, 0);

      assertThat(collection.updateOne(new Document("a", 1), new Document("$set", new Document("a", 1))).getModifiedCount()).isZero();
      assertThat(collection.updateOne(new Document("a", 1), new Document("$unset", new Document("nope", ""))).getModifiedCount()).isZero();
      // nothing was written, so nothing was told it was
      assertThat(written.get()).isZero();
    }

    @Test
    void anUpdateThatChangesSomeDocumentsCountsThoseOnly() {
      collection.updateOne(eq("_id", 2), new Document("$set", new Document("a", 5)));

      assertModified(update(new Document(), new Document("$set", new Document("a", 5)), true), 3, 2);
      assertModified(update(new Document("b", "x"), new Document("$inc", new Document("a", 1)), true), 3, 3);
      assertThat(collection.countDocuments(eq("a", 6))).isEqualTo(3L);
    }

    @Test
    void aChangeOfTypeOrOfANestedValueIsAModification() {
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("a", 1L)), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("n.x", 1)), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("n.x", 1)), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("n.x", 2)), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("$unset", new Document("n.y", "")), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("a", null)), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("a", 1).append("b", "x"), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("a", 1).append("b", "x"), false), 1, 0);
    }

    /** A document whose fields are reordered is a different document on mongod: written, counted, and stored in the new order. */
    @Test
    void reorderingTheFieldsOfAnEmbeddedDocumentIsAModification() {
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("e", new Document("x", 1).append("y", 2))), false), 1, 1);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("e", new Document("x", 1).append("y", 2))), false), 1, 0);
      assertModified(update(new Document("_id", 1), new Document("$set", new Document("e", new Document("y", 2).append("x", 1))), false), 1, 1);
      assertThat(collection.find(eq("_id", 1)).first().get("e", Document.class).keySet()).containsExactly("y", "x");
    }

    private Document update(final Document q, final Document u, final boolean multi) {
      return mongoDatabase.runCommand(
          new Document("update", "doc").append("updates", List.of(new Document("q", q).append("u", u).append("multi", multi))));
    }

    private void assertModified(final Document reply, final int n, final int nModified) {
      assertThat(reply.get("n", Number.class).intValue()).as("n of %s", reply).isEqualTo(n);
      assertThat(reply.get("nModified", Number.class).intValue()).as("nModified of %s", reply).isEqualTo(nModified);
    }
  }

  @Nested
  class ConstraintViolations {
    @BeforeEach
    void declareConstraint() {
      final DocumentType type = serverDatabase().getSchema().getOrCreateDocumentType("v");
      type.createProperty("age", Type.INTEGER).setMax("10");
    }

    @Test
    void anInsertOneIsAWriteError() {
      final MongoCollection<Document> v = mongoDatabase.getCollection("v");
      final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
          () -> v.insertOne(new Document("_id", 1).append("age", 99)));
      assertThat(e).isNotNull();
      assertThat(e.getError().getCode()).isEqualTo(121);
      assertThat(e.getError().getMessage()).contains("age");
      assertThat(v.countDocuments()).isZero();
    }

    @Test
    void anOrderedInsertManyKeepsTheDocumentsBeforeTheRefusedOne() {
      final MongoCollection<Document> v = mongoDatabase.getCollection("v");
      final MongoBulkWriteException e = catchThrowableOfType(MongoBulkWriteException.class, () -> v.insertMany(
          List.of(new Document("_id", 10).append("age", 1), new Document("_id", 11).append("age", 99), new Document("_id", 12).append("age", 2))));
      assertThat(e).isNotNull();
      assertThat(e.getWriteErrors()).hasSize(1);
      assertThat(e.getWriteErrors().getFirst().getIndex()).isEqualTo(1);
      assertThat(e.getWriteErrors().getFirst().getCode()).isEqualTo(121);
      assertThat(v.countDocuments()).isEqualTo(1L);
      assertThat(v.countDocuments(eq("_id", 10))).isEqualTo(1L);
    }

    @Test
    void anUnorderedInsertManyKeepsEveryValidDocument() {
      final MongoCollection<Document> v = mongoDatabase.getCollection("v");
      final MongoBulkWriteException e = catchThrowableOfType(MongoBulkWriteException.class, () -> v.insertMany(
          List.of(new Document("_id", 20).append("age", 1), new Document("_id", 21).append("age", 99), new Document("_id", 22).append("age", 2)),
          new InsertManyOptions().ordered(false)));
      assertThat(e).isNotNull();
      assertThat(e.getWriteErrors()).hasSize(1);
      assertThat(e.getWriteErrors().getFirst().getIndex()).isEqualTo(1);
      assertThat(e.getWriteErrors().getFirst().getCode()).isEqualTo(121);
      assertThat(v.countDocuments()).isEqualTo(2L);
    }

    @Test
    void anUpdateOneIsAWriteErrorAndLeavesTheDocument() {
      final MongoCollection<Document> v = mongoDatabase.getCollection("v");
      v.insertOne(new Document("_id", 40).append("age", 5));

      final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
          () -> v.updateOne(eq("_id", 40), new Document("$set", new Document("age", 99))));
      assertThat(e).isNotNull();
      assertThat(e.getError().getCode()).isEqualTo(121);
      assertThat(v.find(eq("_id", 40)).first().getInteger("age")).isEqualTo(5);
    }

    @Test
    void theStatementsOfAnUpdateAreReportedOneByOne() {
      final MongoCollection<Document> v = mongoDatabase.getCollection("v");
      for (int i = 1; i <= 3; i++)
        v.insertOne(new Document("_id", i).append("age", 1));

      final Document unordered = mongoDatabase.runCommand(new Document("update", "v").append("ordered", false).append("updates",
          List.of(statement(1, 2), statement(2, 99), statement(3, 3))));
      assertThat(unordered.get("ok", Number.class).doubleValue()).isEqualTo(1.0);
      assertThat(unordered.get("n", Number.class).intValue()).isEqualTo(2);
      assertThat(unordered.get("nModified", Number.class).intValue()).isEqualTo(2);
      final List<Document> errors = unordered.getList("writeErrors", Document.class);
      assertThat(errors).hasSize(1);
      assertThat(errors.getFirst().get("index", Number.class).intValue()).isEqualTo(1);
      assertThat(errors.getFirst().get("code", Number.class).intValue()).isEqualTo(121);
      assertThat(v.find(eq("_id", 1)).first().getInteger("age")).isEqualTo(2);
      assertThat(v.find(eq("_id", 2)).first().getInteger("age")).isEqualTo(1);
      assertThat(v.find(eq("_id", 3)).first().getInteger("age")).isEqualTo(3);

      final Document ordered = mongoDatabase.runCommand(
          new Document("update", "v").append("updates", List.of(statement(1, 4), statement(2, 99), statement(3, 5))));
      assertThat(ordered.get("n", Number.class).intValue()).isEqualTo(1);
      assertThat(ordered.get("nModified", Number.class).intValue()).isEqualTo(1);
      assertThat(ordered.getList("writeErrors", Document.class)).hasSize(1);
      assertThat(v.find(eq("_id", 1)).first().getInteger("age")).isEqualTo(4);
      assertThat(v.find(eq("_id", 3)).first().getInteger("age")).isEqualTo(3);
    }

    /**
     * An upsert whose filter is refused before any statement runs (the upsert prepares the {@code _id} index up front) is still the
     * statement's error: reported at its index, the other statements kept.
     */
    @Test
    void aRefusedUpsertFilterIsAWriteErrorOfItsStatement() {
      final Document reply = mongoDatabase.runCommand(new Document("update", "doc").append("ordered", false).append("updates", List.of(
          new Document("q", new Document("_id", 1)).append("u", new Document("$set", new Document("a", 7))),
          new Document("q", new Document("a", new Document("$foo11502", 1))).append("u", new Document("$set", new Document("a", 8)))
              .append("upsert", true),
          new Document("q", new Document("_id", 2)).append("u", new Document("$set", new Document("a", 9))))));

      assertThat(reply.get("nModified", Number.class).intValue()).isEqualTo(2);
      final List<Document> errors = reply.getList("writeErrors", Document.class);
      assertThat(errors).hasSize(1);
      assertThat(errors.getFirst().get("index", Number.class).intValue()).isEqualTo(1);
      assertThat(collection.find(eq("_id", 1)).first().getInteger("a")).isEqualTo(7);
      assertThat(collection.find(eq("_id", 2)).first().getInteger("a")).isEqualTo(9);
      assertThat(collection.countDocuments()).isEqualTo(3L);
    }

    /** The text of a refusal reaches the client in production mode too: it names the kind of the stored value, never the value. */
    @Test
    void aPathBelowAStoredScalarDoesNotEchoTheValue() {
      collection.updateOne(eq("_id", 1), new Document("$set", new Document("s", "secret-11502")));

      final MongoWriteException e = catchThrowableOfType(MongoWriteException.class,
          () -> collection.updateOne(eq("_id", 1), new Document("$set", new Document("s.x", 1))));
      assertThat(e).isNotNull();
      assertThat(e.getError().getCode()).isEqualTo(28);
      assertThat(e.getError().getMessage()).contains("'x'").contains("String").doesNotContain("secret-11502");
    }

    private Document statement(final int id, final int age) {
      return new Document("q", new Document("_id", id)).append("u", new Document("$set", new Document("age", age)));
    }
  }
}
