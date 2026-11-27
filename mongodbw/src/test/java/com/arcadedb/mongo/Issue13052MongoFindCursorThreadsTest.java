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
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13052: the batches of a cursor are asked by requests that run on the Netty thread of whatever connection sends them, so the
 * iterator a {@code find} opened on one thread is advanced by a {@code getMore} on another, under the {@link DatabaseContext} (user,
 * transaction) of that other thread. A type scan, the SQL result set of a filtered find and a sort are each read by two threads in turn
 * and must answer every document once, in order.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13052MongoFindCursorThreadsTest {
  private static final String PATH    = "target/databases/Issue13052MongoFindCursorThreadsTest";
  private static final int    RECORDS = 500;

  private DatabaseFactory        factory;
  private Database               database;
  private MongoDBDatabaseWrapper wrapper;
  private ExecutorService        first;
  private ExecutorService        second;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Item BUCKETS 4");
    database.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        database.newDocument("Item").set("k", k).set("g", k % 5).save();
    });
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
    first = Executors.newSingleThreadExecutor();
    second = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    first.shutdownNow();
    second.shutdownNow();
    database.drop();
    factory.close();
  }

  @Test
  void aTypeScanIsReadByBatchesOfTwoThreads() throws Exception {
    // the buckets are scanned one after the other: every document once is what is asked, in the order of the scan
    assertThat(readAlternating(new Document("find", "Item").append("batchSize", 30))).containsExactlyInAnyOrderElementsOf(ks(1, RECORDS));
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aFilteredSqlResultSetIsReadByBatchesOfTwoThreads() throws Exception {
    final List<Integer> expected = new ArrayList<>();
    for (int k = 1; k <= RECORDS; k++)
      if (k % 5 == 3)
        expected.add(k);
    assertThat(readAlternating(new Document("find", "Item").append("filter", new Document("g", 3)).append("batchSize", 11)))
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aSortedAnswerIsReadByBatchesOfTwoThreads() throws Exception {
    final List<Integer> expected = new ArrayList<>(ks(1, RECORDS));
    Collections.reverse(expected);
    assertThat(readAlternating(new Document("find", "Item").append("sort", new Document("k", -1)).append("batchSize", 40)))
        .isEqualTo(expected);
    assertThat(wrapper.openCursors()).isZero();
  }

  /** The find runs on one thread; every batch after it on the other and the first, alternately. */
  private List<Integer> readAlternating(final Document find) throws ExecutionException, InterruptedException {
    final Document reply = on(first, () -> wrapper.handleCommand(null, "find", find, null, null));
    final List<Integer> ks = new ArrayList<>(ksOf(batch(reply, "firstBatch")));
    long id = cursorId(reply);
    assertThat(id).isNotZero();
    for (int turn = 0; id != 0L; turn++) {
      final long cursor = id;
      final Document more = on(turn % 2 == 0 ? second : first,
          () -> wrapper.getMore(new Document("getMore", cursor).append("collection", "Item").append("batchSize", 1_000)));
      ks.addAll(ksOf(batch(more, "nextBatch")));
      id = cursorId(more);
    }
    return ks;
  }

  /** Runs {@code work} on the thread of {@code executor} as a request: the user is bound into the context of that thread. */
  private <T> T on(final ExecutorService executor, final Supplier<T> work) throws ExecutionException, InterruptedException {
    final Future<T> future = executor.submit(() -> {
      DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user());
      try {
        return work.get();
      } finally {
        DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()).setCurrentUser(null);
      }
    });
    return future.get();
  }

  private static List<Integer> ks(final int from, final int to) {
    final List<Integer> ks = new ArrayList<>();
    for (int k = from; k <= to; k++)
      ks.add(k);
    return ks;
  }

  private static long cursorId(final Document reply) {
    return ((Number) ((Document) reply.get("cursor")).get("id")).longValue();
  }

  @SuppressWarnings("unchecked")
  private static List<Document> batch(final Document reply, final String key) {
    return (List<Document>) ((Document) reply.get("cursor")).get(key);
  }

  private static List<Integer> ksOf(final List<Document> documents) {
    final List<Integer> ks = new ArrayList<>(documents.size());
    for (final Document document : documents)
      ks.add(((Number) document.get("k")).intValue());
    return ks;
  }

  private static SecurityDatabaseUser user() {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "reader";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
