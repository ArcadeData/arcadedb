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
import com.arcadedb.query.ProfiledLimitScope;
import com.arcadedb.security.SecurityDatabaseUser;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13052 meets issue #13087: a {@code find} that is served from a cursor answers at most the {@code resultSetLimit} of the
 * groups of its user in all, whatever the batches it is spread over, and a batch that fails is answered with the code the failure
 * has, as it would have been by the {@code find}. Drives the database wrapper, with a user bound as the server binds the one of a
 * request.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13052MongoFindCursorQuotaTest {
  private static final String PATH    = "target/databases/Issue13052MongoFindCursorQuotaTest";
  private static final int    RECORDS = 200;

  private DatabaseFactory        factory;
  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void setUp() {
    // A SECURITY MANAGER, OR THE DATABASE READS NO QUOTA FROM THE CURRENT USER AT ALL
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE Item BUCKETS 4");
    database.transaction(() -> {
      for (int k = 1; k <= RECORDS; k++)
        database.newDocument("Item").set("k", k).save();
    });
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    while (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  @Test
  void theQuotaIsCountedAcrossTheBatchesOfACursor() {
    bindUser(user(150L, -1L));

    final Document first = find(new Document("find", "Item").append("batchSize", 100));
    assertThat(firstBatch(first)).hasSize(100);
    final long id = cursorId(first);
    assertThat(id).as("50 documents are still to be answered").isNotZero();
    assertThat(wrapper.openCursors()).isEqualTo(1);

    final Document second = getMore(id, 100);
    assertThat(nextBatch(second)).hasSize(50);
    assertThat(cursorId(second)).as("the quota is reached: no cursor is left").isZero();
    assertThat(wrapper.openCursors()).isZero();
    assertThat(ProfiledLimitScope.isActive()).as("nothing is left on the thread").isFalse();
  }

  @Test
  void aFirstBatchOfTheDefaultSizeIsCutAtTheQuotaToo() {
    bindUser(user(150L, -1L));
    final Document first = find(new Document("find", "Item"));
    assertThat(firstBatch(first)).as("the 101 of a first batch").hasSize(101);
    final Document more = getMore(cursorId(first), 0);
    assertThat(nextBatch(more)).hasSize(49);
    assertThat(cursorId(more)).isZero();

    bindUser(user(20L, -1L));
    final Document capped = find(new Document("find", "Item"));
    assertThat(firstBatch(capped)).hasSize(20);
    assertThat(cursorId(capped)).as("the quota fits the first batch: no cursor").isZero();
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aSortedAndFilteredCursorAnswersTheTopOfTheOrderUnderTheQuota() {
    bindUser(user(25L, -1L));
    final Document first = find(new Document("find", "Item").append("filter", new Document("k", new Document("$gt", 0)))
        .append("sort", new Document("k", -1)).append("batchSize", 10));
    final List<Integer> ks = new ArrayList<>(ksOf(firstBatch(first)));
    long id = cursorId(first);
    while (id != 0L) {
      final Document more = getMore(id, 10);
      ks.addAll(ksOf(nextBatch(more)));
      id = cursorId(more);
    }
    assertThat(ks).as("the order is the one of the whole collection, the quota cuts its end").hasSize(25).startsWith(200, 199, 198)
        .endsWith(177, 176);
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aLimitBelowTheQuotaStaysInForceAcrossTheBatches() {
    bindUser(user(150L, -1L));
    final Document first = find(new Document("find", "Item").append("limit", 30).append("batchSize", 20));
    assertThat(firstBatch(first)).hasSize(20);
    final Document more = getMore(cursorId(first), 20);
    assertThat(nextBatch(more)).hasSize(10);
    assertThat(cursorId(more)).isZero();

    // a legacy negative limit is one batch, cut at the quota when it is the tighter
    final Document legacy = find(new Document("find", "Item").append("limit", -500));
    assertThat(firstBatch(legacy)).hasSize(150);
    assertThat(cursorId(legacy)).isZero();
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aQuotaOfZeroAnswersNoDocumentAndLeavesNoCursorButStillRefusesAMalformedFilter() {
    bindUser(user(0L, -1L));
    final Document reply = find(new Document("find", "Item"));
    assertThat(firstBatch(reply)).isEmpty();
    assertThat(cursorId(reply)).isZero();
    assertThat(wrapper.openCursors()).isZero();

    assertThatThrownBy(() -> find(new Document("find", "Item").append("filter", new Document("$and", 1)))).isInstanceOf(
        MongoServerException.class);
    assertThat(wrapper.openCursors()).isZero();
  }

  @Test
  void aBatchThatFailsIsAnsweredWithTheCodeOfItsFailureAndTheCursorIsGone() throws InterruptedException {
    // A READ TIMEOUT BOUNDS THE SCAN FROM THE MOMENT THE find OPENS IT: THE BATCH THAT COMES AFTER IT FAILS WITH A TimeoutException
    bindUser(user(-1L, 1_500L));
    final Document first = find(new Document("find", "Item").append("batchSize", 10));
    final long id = cursorId(first);
    assertThat(id).isNotZero();
    assertThat(wrapper.openCursors()).isEqualTo(1);

    Thread.sleep(1_800L);

    assertThatThrownBy(() -> getMore(id, 10)).isInstanceOfSatisfying(MongoServerError.class, e -> {
      assertThat(e.getCode()).as("MaxTimeMSExpired, not an uncoded error").isEqualTo(50);
      assertThat(e.getCodeName()).isEqualTo("MaxTimeMSExpired");
    });
    assertThat(wrapper.openCursors()).as("a cursor that failed is not kept").isZero();
    assertThat(wrapper.getMore(new Document("getMore", id).append("collection", "Item"))).as("the library answers CursorNotFound").isNull();
    assertThat(ProfiledLimitScope.isActive()).as("nothing is left on the thread").isFalse();
  }

  /**
   * A cursor parked between two requests pins nothing a close needs: the database closes with the cursor open, and the instance that
   * replaces it (what the backend binds the wrapper to on the next request) takes the cursors of the one before with it.
   */
  @Test
  void aCursorParkedOnADatabaseDoesNotHoldItsCloseAndIsReleasedWhenTheInstanceIsReplaced() {
    bindUser(user(-1L, -1L));
    final long id = cursorId(find(new Document("find", "Item").append("batchSize", 10)));
    assertThat(id).isNotZero();
    assertThat(wrapper.openCursors()).isEqualTo(1);

    database.close();
    assertThat(database.isOpen()).as("the close did not wait for the cursor").isFalse();
    database = factory.open();
    wrapper.bind(database);

    assertThat(wrapper.openCursors()).as("the cursors of the instance replaced are closed").isZero();
    assertThat(wrapper.getMore(new Document("getMore", id).append("collection", "Item"))).as("the library answers CursorNotFound").isNull();
  }

  private Document find(final Document command) {
    return wrapper.handleCommand(null, "find", command, null, null);
  }

  private Document getMore(final long id, final int batchSize) {
    final Document command = new Document("getMore", id).append("collection", "Item");
    if (batchSize > 0)
      command.append("batchSize", batchSize);
    return wrapper.getMore(command);
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

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser user(final long resultSetLimit, final long readTimeout) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "capped";
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
        return resultSetLimit;
      }

      @Override
      public long getReadTimeout() {
        return readTimeout;
      }
    };
  }
}
