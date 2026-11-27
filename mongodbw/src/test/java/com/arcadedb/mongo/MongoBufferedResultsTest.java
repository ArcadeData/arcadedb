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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the MongoDB read paths hold in memory while they answer: the rows of the {@code mongo} query language are read as the caller
 * consumes them, the buffers of a legacy query, of a sort and of an aggregation are counted against the heap limits every other query
 * engine honours, and a query that keeps little still runs over a collection larger than those limits. Drives the database wrapper.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoBufferedResultsTest {
  private static final String PATH    = "target/databases/MongoBufferedResultsTest";
  private static final int    RECORDS = 600;
  private static final long   CAP     = 100L;
  /** The documents of a collection a scan cannot read ahead whole: it prefetches up to 1,024 records per bucket. */
  private static final int    MANY    = 10_000;

  private DatabaseFactory        factory;
  private Database               database;
  private MongoDBDatabaseWrapper wrapper;
  private final AtomicLong       reads = new AtomicLong();

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Item BUCKETS 4");
    final String payload = "p".repeat(8 * 1024);
    database.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        database.newDocument("Item").set("k", k).set("payload", payload).save();
    });
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
  }

  @AfterEach
  void tearDown() {
    while (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  // ---- the rows of the mongo query language

  @Test
  void theQueryLanguageReadsRowsAsTheyAreConsumed() {
    createMany();
    countReads("Many");

    try (final ResultSet rows = wrapper.query("{ collection: 'Many', query: {} }")) {
      assertThat(reads.get()).as("nothing is read before the first row is asked for").isLessThan(MANY / 2);
      for (int i = 0; i < 5; i++) {
        assertThat(rows.hasNext()).isTrue();
        assertThat(rows.next().<Integer>getProperty("k")).isNotNull();
      }
      assertThat(reads.get()).as("five rows read ahead a batch, not the collection").isLessThan(MANY / 2);
    }
    assertThat(reads.get()).isLessThan(MANY / 2);
  }

  @Test
  void theQueryLanguageStillAnswersEveryRowInOrder() {
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rows = wrapper.query("{ collection: 'Item', query: { $orderBy: { k: -1 } } }")) {
      while (rows.hasNext())
        ks.add(rows.next().getProperty("k"));
    }
    assertThat(ks).hasSize(RECORDS).startsWith(RECORDS, RECORDS - 1, RECORDS - 2).endsWith(3, 2, 1);

    int count = 0;
    try (final ResultSet rows = wrapper.query("{ collection: 'Item', numberToSkip: 10, numberToReturn: 25, query: {} }")) {
      while (rows.hasNext()) {
        final Result row = rows.next();
        assertThat(row.<Integer>getProperty("k")).isNotNull();
        ++count;
      }
    }
    assertThat(count).isEqualTo(25);
  }

  @Test
  void theQueryLanguageSortIsRefusedPastTheElementCap() {
    cap(CAP);
    assertThatThrownBy(() -> wrapper.query("{ collection: 'Item', query: { $orderBy: { k: 1 } } }")).hasRootCauseInstanceOf(
        HeapLimitExceededException.class);

    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rows = wrapper.query("{ collection: 'Item', query: { k: { $lte: 50 }, $orderBy: { k: -1 } } }")) {
      while (rows.hasNext())
        ks.add(rows.next().getProperty("k"));
    }
    assertThat(ks).as("a sort within the cap answers").hasSize(50).startsWith(50, 49).endsWith(2, 1);
  }

  // ---- the legacy query

  @Test
  void aLegacyQueryAnsweringMoreThanTheElementCapIsRefused() {
    cap(CAP);
    assertThatThrownBy(() -> wrapper.handleQuery(legacyQuery(0))).hasRootCauseInstanceOf(HeapLimitExceededException.class);
    assertThatThrownBy(() -> wrapper.handleQuery(legacyQuery(RECORDS))).hasRootCauseInstanceOf(HeapLimitExceededException.class);

    assertThat(wrapper.handleQuery(legacyQuery(50)).collectDocuments()).as("a query within the cap answers").hasSize(50);
  }

  @Test
  void aLegacyQueryHandsBackWhatItCharged() {
    cap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    assertThat(wrapper.handleQuery(legacyQuery(0)).collectDocuments()).hasSize(RECORDS);

    assertThat(QueryHeapBudget.getReservedBytes()).as("the budget is whole again once the answer is built").isLessThanOrEqualTo(baseline);
  }

  // ---- the find command

  @Test
  void aSortedFindWithoutALimitIsRefusedPastTheElementCap() {
    cap(CAP);
    assertThatThrownBy(() -> find(new Document("find", "Item").append("sort", new Document("k", 1)))).hasRootCauseInstanceOf(
        HeapLimitExceededException.class);
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aSortedFindWithALimitLargerThanTheCollectionIsRefusedPastTheElementCap() {
    cap(CAP);
    assertThatThrownBy(() -> find(new Document("find", "Item").append("sort", new Document("k", 1)).append("limit", 1_000_000))).hasRootCauseInstanceOf(
        HeapLimitExceededException.class);
    assertThat(wrapper.openCursors()).isZero();

    final Document reply = find(new Document("find", "Item").append("sort", new Document("k", -1)).append("limit", 40));
    assertThat(ksOf(firstBatch(reply))).as("a sort whose limit fits the cap answers").hasSize(40).startsWith(RECORDS, RECORDS - 1);
  }

  @Test
  void aSortedFindKeepsItsBufferChargedWhileItsCursorIsOpen() {
    cap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    final Document reply = find(new Document("find", "Item").append("sort", new Document("k", 1)).append("batchSize", 10));
    final long id = cursorId(reply);
    assertThat(id).isNotZero();
    assertThat(QueryHeapBudget.getReservedBytes()).as("the sorted rows the cursor holds are counted").isGreaterThan(baseline);

    final List<Object> killed = new ArrayList<>();
    final List<Object> notFound = new ArrayList<>();
    wrapper.killCursors(List.of(id), killed, notFound);
    assertThat(killed).hasSize(1);
    assertThat(QueryHeapBudget.getReservedBytes()).as("closing the cursor gives it back").isLessThanOrEqualTo(baseline);
  }

  @Test
  void aBoundedSortChargesTheRowsItKeepsWhenBetterOnesTakeThePlaceOfTheFirst() {
    cap(-1L);
    // the rows grow with their key: the first ones a descending sort holds are the smallest, the ones it ends with the largest
    database.command("sql", "CREATE DOCUMENT TYPE Growing BUCKETS 1");
    database.transaction(() -> {
      for (int k = 1; k <= 20; k++)
        database.newDocument("Growing").set("k", k).set("payload", "g".repeat(k * 20_000)).save();
    });
    final long baseline = QueryHeapBudget.getReservedBytes();

    final Document reply = find(
        new Document("find", "Growing").append("sort", new Document("k", -1)).append("limit", 3).append("batchSize", 1));
    assertThat(ksOf(firstBatch(reply))).containsExactly(20);
    final long id = cursorId(reply);
    assertThat(id).as("two rows are left to ask for").isNotZero();
    assertThat(QueryHeapBudget.getReservedBytes()).as("the three largest rows are held, not the three first ones").isGreaterThanOrEqualTo(
        baseline + QueryHeapTracker.MIN_CHUNK_BYTES);

    wrapper.killCursors(List.of(id), new ArrayList<>(), new ArrayList<>());
    assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
  }

  @Test
  void theBinaryValuesOfTheDocumentsAreCountedAgainstTheBudget() {
    cap(-1L);
    database.command("sql", "CREATE DOCUMENT TYPE Blobs BUCKETS 1");
    final byte[] megabyte = new byte[1024 * 1024];
    database.transaction(() -> {
      for (int k = 1; k <= 32; k++)
        database.newDocument("Blobs").set("k", k).set("data", megabyte).save();
    });

    // a budget of 8 MB: 32 documents of one megabyte each are far past it, and are counted by what they hold
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(8L);
    try {
      final MongoQuery all = new MongoQuery(null, null, database.getName() + ".Blobs", 0, 0, new Document(), null);
      assertThatThrownBy(() -> wrapper.handleQuery(all)).hasRootCauseInstanceOf(HeapLimitExceededException.class);
      assertThatThrownBy(() -> aggregate("Blobs", new Document("$match", new Document("k", new Document("$gt", 0))))).hasRootCauseInstanceOf(
          HeapLimitExceededException.class);

      final MongoQuery few = new MongoQuery(null, null, database.getName() + ".Blobs", 0, 3, new Document(), null);
      assertThat(wrapper.handleQuery(few).collectDocuments()).as("a few of them fit the budget").hasSize(3);
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }
  }

  @Test
  void aFindWithoutASortStreamsPastTheElementCap() {
    cap(CAP);
    final Document first = find(new Document("find", "Item").append("batchSize", 50));
    final List<Integer> ks = new ArrayList<>(ksOf(firstBatch(first)));
    long id = cursorId(first);
    while (id != 0L) {
      final Document more = wrapper.getMore(new Document("getMore", id).append("collection", "Item").append("batchSize", 50));
      ks.addAll(ksOf(nextBatch(more)));
      id = cursorId(more);
    }
    assertThat(ks).hasSize(RECORDS);
    assertThat(wrapper.openCursors()).isZero();
  }

  // ---- the aggregate command

  @Test
  void anAggregationReadsItsInputAsTheStagesConsumeIt() {
    createMany();
    countReads("Many");
    assertThat(firstBatch(aggregate("Many", new Document("$limit", 5)))).hasSize(5);
    assertThat(reads.get()).as("a limit stage stops the read").isLessThan(MANY / 2);

    reads.set(0);
    assertThat(
        firstBatch(aggregate("Many", new Document("$match", new Document("k", new Document("$gt", 0))), new Document("$limit", 5)))).hasSize(
        5);
    assertThat(reads.get()).as("so does a limit after a match").isLessThan(MANY / 2);

    final List<Document> counted = firstBatch(aggregate("Many", new Document("$count", "n")));
    assertThat(((Number) counted.getFirst().get("n")).intValue()).as("a count over the collection still answers").isEqualTo(MANY);
  }

  @Test
  void anAggregationAnsweringMoreThanTheElementCapIsRefused() {
    cap(CAP);
    assertThatThrownBy(() -> aggregate(new Document("$match", new Document("k", new Document("$gt", 0))))).hasRootCauseInstanceOf(
        HeapLimitExceededException.class);
    assertThatThrownBy(() -> aggregate(new Document("$project", new Document("k", 1)))).hasRootCauseInstanceOf(HeapLimitExceededException.class);

    assertThat(firstBatch(aggregate(new Document("$match", new Document("k", new Document("$gt", RECORDS - 50)))))).as(
        "an answer within the cap is served").hasSize(50);
  }

  @Test
  void anAggregationSortIsRefusedPastTheElementCap() {
    cap(CAP);
    assertThatThrownBy(() -> aggregate(new Document("$sort", new Document("k", -1)), new Document("$limit", 3))).hasRootCauseInstanceOf(
        HeapLimitExceededException.class);

    assertThat(ksOf(firstBatch(aggregate(new Document("$match", new Document("k", new Document("$lte", 50))),
        new Document("$sort", new Document("k", -1)), new Document("$limit", 3))))).as("a sort within the cap answers").containsExactly(50, 49, 48);
  }

  @Test
  void anAggregationThatKeepsLittleRunsOverMoreThanTheElementCap() {
    cap(CAP);
    final List<Document> counted = firstBatch(aggregate(new Document("$match", new Document("k", new Document("$gt", 0))),
        new Document("$count", "n")));
    assertThat(counted).hasSize(1);
    assertThat(((Number) counted.getFirst().get("n")).intValue()).isEqualTo(RECORDS);

    final List<Document> grouped = firstBatch(
        aggregate(new Document("$group", new Document("_id", null).append("n", new Document("$sum", 1)))));
    assertThat(grouped).hasSize(1);
    assertThat(((Number) grouped.getFirst().get("n")).intValue()).isEqualTo(RECORDS);
  }

  @Test
  void anAggregationHandsBackWhatItCharged() {
    cap(-1L);
    final long baseline = QueryHeapBudget.getReservedBytes();

    assertThat(firstBatch(aggregate(new Document("$sort", new Document("k", 1))))).hasSize(RECORDS);

    assertThat(QueryHeapBudget.getReservedBytes()).as("the budget is whole again once the answer is built").isLessThanOrEqualTo(baseline);
  }

  // ---- helpers

  private void cap(final long elements) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, elements);
  }

  private void createMany() {
    database.command("sql", "CREATE DOCUMENT TYPE Many BUCKETS 4");
    database.transaction(() -> {
      for (int k = 1; k <= MANY; k++)
        database.newDocument("Many").set("k", k).save();
    });
  }

  private void countReads(final String type) {
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    database.getSchema().getType(type).getEvents().registerListener(listener);
  }

  private MongoQuery legacyQuery(final int numberToReturn) {
    return new MongoQuery(null, null, database.getName() + ".Item", 0, numberToReturn, new Document(), null);
  }

  private Document find(final Document command) {
    return wrapper.handleCommand(null, "find", command, null, null);
  }

  private Document aggregate(final Document... stages) {
    return aggregate("Item", stages);
  }

  private Document aggregate(final String collection, final Document... stages) {
    return wrapper.handleCommand(null, "aggregate",
        new Document("aggregate", collection).append("pipeline", List.of(stages)).append("cursor", new Document()), null, null);
  }

  private static long cursorId(final Document reply) {
    return ((Number) ((Document) reply.get("cursor")).get("id")).longValue();
  }

  @SuppressWarnings("unchecked")
  private static List<Document> firstBatch(final Document reply) {
    return (List<Document>) ((Document) reply.get("cursor")).get("firstBatch");
  }

  @SuppressWarnings("unchecked")
  private static List<Document> nextBatch(final Document reply) {
    return (List<Document>) ((Document) reply.get("cursor")).get("nextBatch");
  }

  private static List<Integer> ksOf(final List<Document> documents) {
    final List<Integer> ks = new ArrayList<>(documents.size());
    for (final Document document : documents)
      ks.add(((Number) document.get("k")).intValue());
    return ks;
  }
}
