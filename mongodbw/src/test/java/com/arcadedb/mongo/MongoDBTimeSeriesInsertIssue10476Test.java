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
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10476 (follow-up of #10464): {@code insertOne} into a TIMESERIES collection failed with {@code NamespaceNotFound} on a
 * collection that exists, because the insert tried to put the {@code _id} index every MongoDB collection has on a type that
 * stores samples in its own engine, not in records. The samples are now appended through the type's engine, as SQL
 * {@code INSERT INTO Ev SET ...} does, and what cannot be stored is refused with an error naming the type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MongoDBTimeSeriesInsertIssue10476Test extends BaseMongoServerTest {
  private static final long T0 = 1792396800000L;

  private MongoClient               client;
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
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE)");

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    collection = client.getDatabase(getDatabaseName()).getCollection("Ev");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  @Test
  void insertOneAppendsTheSample() {
    collection.insertOne(new Document("ts", T0).append("k", "c").append("v", 4.0));

    final List<Sample> samples = samples();
    assertThat(samples).containsExactly(new Sample(T0, "c", 4.0));
    assertThat(collection.countDocuments()).isEqualTo(1);
    assertThat(collection.find(new Document("k", "c")).into(new ArrayList<>())).hasSize(1);
  }

  @Test
  void insertOneReadsADateAsTheInstant() {
    collection.insertOne(new Document("ts", new Date(T0 + 5)).append("k", "d").append("v", 1.5));

    assertThat(samples()).containsExactly(new Sample(T0 + 5, "d", 1.5));
  }

  @Test
  void insertManyAppendsEverySampleAndCountsThem() {
    final List<Document> batch = new ArrayList<>();
    for (int i = 0; i < 25; i++)
      batch.add(new Document("ts", T0 + i * 1000L).append("k", i % 2 == 0 ? "even" : "odd").append("v", (double) i));

    collection.insertMany(batch);

    assertThat(samples()).hasSize(25);
    assertThat(collection.countDocuments(new Document("k", "odd"))).isEqualTo(12);
  }

  @Test
  void insertOnATimeSeriesTypeLeavesNoIdIndex() {
    collection.insertOne(new Document("ts", T0).append("k", "c").append("v", 4.0));

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.getSchema().getType("Ev").getAllIndexes(true)).isEmpty();
  }

  @Test
  void anUndeclaredKeyIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.insertOne(new Document("ts", T0).append("k", "c").append("v", 4.0).append("misspelled", 1)))
        .isInstanceOf(MongoWriteException.class)
        .satisfies(e -> {
          final MongoWriteException write = (MongoWriteException) e;
          assertThat(write.getError().getCode()).isNotEqualTo(26);
          assertThat(write.getError().getMessage()).contains("Ev").contains("misspelled");
        });

    assertThat(samples()).isEmpty();
  }

  @Test
  void aValueOfTheWrongKindIsRefusedNamingTheColumn() {
    assertThatThrownBy(() -> collection.insertOne(new Document("ts", T0).append("k", "c").append("v", "not a number")))
        .isInstanceOf(MongoWriteException.class)
        .satisfies(e -> {
          final MongoWriteException write = (MongoWriteException) e;
          assertThat(write.getError().getCode()).isNotEqualTo(26);
          assertThat(write.getError().getMessage()).contains("v");
        });

    assertThat(samples()).isEmpty();
  }

  @Test
  void anExplicitIdThatIsNotAnObjectIdIsRefusedInsteadOfLost() {
    assertThatThrownBy(() -> collection.insertOne(new Document("_id", "mine").append("ts", T0).append("k", "c").append("v", 4.0)))
        .isInstanceOf(MongoWriteException.class)
        .satisfies(e -> assertThat(((MongoWriteException) e).getError().getMessage()).contains("Ev").contains("_id"));

    assertThat(samples()).isEmpty();
  }

  @Test
  void anObjectIdIsAcceptedAndNotStored() {
    collection.insertOne(new Document("_id", new ObjectId()).append("ts", T0).append("k", "c").append("v", 4.0));

    assertThat(samples()).containsExactly(new Sample(T0, "c", 4.0));
  }

  @Test
  void anOrderedBatchStopsAtTheFirstRefusedSample() {
    final List<Document> batch = List.of(//
        new Document("ts", T0).append("k", "a").append("v", 1.0), //
        new Document("ts", T0 + 1).append("k", "a").append("v", "bad"), //
        new Document("ts", T0 + 2).append("k", "a").append("v", 3.0));

    assertThatThrownBy(() -> collection.insertMany(batch)).isInstanceOf(MongoBulkWriteException.class).satisfies(e -> {
      final MongoBulkWriteException bulk = (MongoBulkWriteException) e;
      assertThat(bulk.getWriteErrors()).hasSize(1);
      assertThat(bulk.getWriteErrors().getFirst().getIndex()).isEqualTo(1);
      assertThat(bulk.getWriteResult().getInsertedCount()).isEqualTo(1);
    });

    assertThat(samples()).containsExactly(new Sample(T0, "a", 1.0));
  }

  @Test
  void anUnorderedBatchStoresTheSamplesAroundTheRefusedOne() {
    final List<Document> batch = List.of(//
        new Document("ts", T0).append("k", "a").append("v", 1.0), //
        new Document("ts", T0 + 1).append("k", "a").append("v", "bad"), //
        new Document("ts", T0 + 2).append("k", "a").append("v", 3.0));

    assertThatThrownBy(() -> collection.insertMany(batch, new InsertManyOptions().ordered(false))).isInstanceOf(
        MongoBulkWriteException.class).satisfies(e -> {
      final MongoBulkWriteException bulk = (MongoBulkWriteException) e;
      assertThat(bulk.getWriteErrors()).hasSize(1);
      assertThat(bulk.getWriteErrors().getFirst().getIndex()).isEqualTo(1);
      assertThat(bulk.getWriteResult().getInsertedCount()).isEqualTo(2);
    });

    assertThat(samples()).containsExactlyInAnyOrder(new Sample(T0, "a", 1.0), new Sample(T0 + 2, "a", 3.0));
  }

  @Test
  void aCollectionThatIsNotATimeSeriesTypeIsStillCreatedByTheFirstInsert() {
    final MongoCollection<Document> plain = client.getDatabase(getDatabaseName()).getCollection("Plain10476");
    plain.insertOne(new Document("a", 1));

    assertThat(plain.countDocuments()).isEqualTo(1);
  }

  /** The numbers a driver sends in each of the BSON types a DOUBLE field can be written with. */
  @Test
  void everyNumericTypeOfBsonIsStoredInADoubleField() {
    collection.insertOne(new Document("ts", T0).append("k", "int").append("v", 3));
    collection.insertOne(new Document("ts", T0 + 1).append("k", "long").append("v", 4L));
    collection.insertOne(new Document("ts", T0 + 2).append("k", "double").append("v", 5.5));
    collection.insertOne(new Document("ts", T0 + 3).append("k", "decimal").append("v", new Decimal128(new BigDecimal("6.25"))));

    assertThat(samples()).containsExactly(new Sample(T0, "int", 3.0), new Sample(T0 + 1, "long", 4.0),
        new Sample(T0 + 2, "double", 5.5), new Sample(T0 + 3, "decimal", 6.25));
  }

  /**
   * A value of a BSON type a column cannot hold - a nested document, an array, binary data - is the document's fault, reported as
   * a write error that names the column, and not a failure of the command.
   */
  @Test
  void aValueOfATypeTheColumnCannotHoldIsRefusedAsTheDocumentsNotAsAFailedCommand() {
    final List<Document> documents = List.of(new Document("ts", T0).append("k", "c").append("v", new Document("nested", 1)),
        new Document("ts", T0 + 1).append("k", "c").append("v", List.of(1.0, 2.0)),
        new Document("ts", T0 + 2).append("k", List.of("a", "b")).append("v", 1.0),
        new Document("ts", T0 + 3).append("k", new Document("nested", "tag")).append("v", 1.0),
        new Document("ts", T0 + 4).append("k", "c").append("v", new Binary(new byte[] { 1, 2, 3 })),
        new Document("ts", T0 + 5).append("k", "fine").append("v", 9.0));

    assertThatThrownBy(() -> collection.insertMany(documents, new InsertManyOptions().ordered(false))).isInstanceOf(
        MongoBulkWriteException.class).satisfies(e -> {
      final MongoBulkWriteException bulk = (MongoBulkWriteException) e;
      assertThat(bulk.getWriteErrors()).extracting(error -> error.getIndex()).containsExactly(0, 1, 2, 3, 4);
      assertThat(bulk.getWriteErrors()).allSatisfy(error -> assertThat(error.getCode()).isNotEqualTo(26));
      assertThat(bulk.getWriteResult().getInsertedCount()).isEqualTo(1);
    });

    assertThat(samples()).containsExactly(new Sample(T0 + 5, "fine", 9.0));
  }

  /**
   * The values the engine cannot read as the column's type - text for a number, a boolean or a document for the timestamp, a
   * timestamp that is not a date, a sample without its timestamp - are each a write error of their own document, with the index
   * of that document, and the reply counts only the samples that were stored.
   */
  @Test
  void aValueOfTheWrongTypeIsAWriteErrorOfItsDocumentAndTheReplyCountsWhatWasStored() {
    final List<Document> documents = List.of(new Document("ts", T0).append("k", "a").append("v", 1.0),
        new Document("ts", T0 + 1).append("k", "a").append("v", "not a number"),
        new Document("ts", "not a date").append("k", "a").append("v", 2.0),
        new Document("ts", true).append("k", "a").append("v", 3.0),
        new Document("ts", new Document("when", T0)).append("k", "a").append("v", 4.0),
        new Document("k", "a").append("v", 5.0),
        new Document("ts", T0 + 6).append("k", "a").append("v", 6.0));

    assertThatThrownBy(() -> collection.insertMany(documents, new InsertManyOptions().ordered(false))).isInstanceOf(
        MongoBulkWriteException.class).satisfies(e -> {
      final MongoBulkWriteException bulk = (MongoBulkWriteException) e;
      assertThat(bulk.getWriteErrors()).extracting(error -> error.getIndex()).containsExactly(1, 2, 3, 4, 5);
      assertThat(bulk.getWriteErrors()).allSatisfy(error -> assertThat(error.getCode()).isNotEqualTo(26));
      assertThat(bulk.getWriteResult().getInsertedCount()).isEqualTo(2);
    });

    assertThat(samples()).containsExactly(new Sample(T0, "a", 1.0), new Sample(T0 + 6, "a", 6.0));
  }

  /** A refusal of a sample is the document's; a fault of the code or of the disk is the server's and fails the command. */
  @Test
  void onlyTheRefusalsOfASampleAreBlamedOnTheDocument() {
    final MongoServerError refused = MongoTimeSeriesInserts.refusal(new CommandExecutionException("property 'x' is not a declared column"));
    assertThat(refused.getCode()).isEqualTo(ErrorCode.BadValue.getValue());
    assertThat(refused.getMessage()).contains("not a declared column");

    for (final Throwable fault : List.of(new IOException("disk"), new NullPointerException(), new IllegalStateException("closed"),
        new ClassCastException(), new IndexOutOfBoundsException()))
      assertThatThrownBy(() -> MongoTimeSeriesInserts.refusal(new CommandExecutionException("Error appending", fault))).as(
          fault.getClass().getSimpleName()).isInstanceOf(CommandExecutionException.class);

    // a value the type refuses is the document's, with the code MongoDB gives a bad value
    for (final RuntimeException invalid : List.of(new ValidationException("v is mandatory"), new IllegalArgumentException("bad v"))) {
      final MongoServerError error = MongoTimeSeriesInserts.refusal(invalid);
      assertThat(error.getCode()).as(invalid.getClass().getSimpleName()).isEqualTo(ErrorCode.BadValue.getValue());
    }

    // a refusal of our own is returned as it is
    final MongoServerError own = new MongoServerError(ErrorCode.BadValue, "no _id");
    assertThat(MongoTimeSeriesInserts.refusal(own)).isSameAs(own);

    // the default of an untyped refusal of the engine is the document's, unless it wraps one of the faults listed above: a cause
    // the list does not know is a refused sample, not a failed command
    final MongoServerError unlisted = MongoTimeSeriesInserts.refusal(
        new CommandExecutionException("Error appending", new UnsupportedOperationException("column")));
    assertThat(unlisted.getCode()).isEqualTo(ErrorCode.BadValue.getValue());

    // an arithmetic error is the engine's own category and is not decided by that default
    final ArithmeticErrorException overflow = new ArithmeticErrorException("integer overflow");
    assertThatThrownBy(() -> MongoTimeSeriesInserts.refusal(overflow)).isSameAs(overflow);

    // what is not the document's fails the command: a conflict to retry, a timeout, a lack of permission, a schema that changed
    for (final RuntimeException transientOrNot : List.of(new ConcurrentModificationException("page changed"),
        new TimeoutException("shard busy"), new SecurityException("not allowed"), new SchemaException("type dropped")))
      assertThatThrownBy(() -> MongoTimeSeriesInserts.refusal(transientOrNot)).as(transientOrNot.getClass().getSimpleName())
          .isSameAs(transientOrNot);
  }

  private List<Sample> samples() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final List<Sample> samples = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT ts, k, v FROM Ev ORDER BY ts")) {
      while (rs.hasNext()) {
        final var row = rs.next();
        samples.add(new Sample(epochMillis(row.getProperty("ts")), row.getProperty("k"),
            ((Number) row.getProperty("v")).doubleValue()));
      }
    }
    return samples;
  }

  private static long epochMillis(final Object timestamp) {
    // SQL reads the stored milliseconds as the UTC date and time they name
    if (timestamp instanceof LocalDateTime local)
      return local.toInstant(ZoneOffset.UTC).toEpochMilli();
    if (timestamp instanceof Date date)
      return date.getTime();
    return ((Number) timestamp).longValue();
  }

  private record Sample(long ts, String k, double v) {
  }
}
