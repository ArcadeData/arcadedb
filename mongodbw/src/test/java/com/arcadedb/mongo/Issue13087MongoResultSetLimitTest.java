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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13087: a security group's {@code resultSetLimit} caps the documents a MongoDB command answers, as it caps the rows of a
 * SQL or an openCypher statement, and nothing else. The MongoDB module never applied it as a row cap: an unfiltered {@code find}
 * was bounded only by the truncation of every bucket scan, which #12888 removed, and read every document. Where a command reads
 * through a SQL statement of its own, that statement took the quota as its LIMIT instead, before the command had filtered, sorted,
 * counted or written: a sort ordered the first N candidates, a {@code count} answered at most N, and a {@code deleteMany} or a
 * multi-document update stopped after N documents.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue13087MongoResultSetLimitTest {
  private static final String PATH    = "target/databases/Issue13087MongoResultSetLimitTest";
  private static final int    RECORDS = 40;
  private static final int    QUOTA   = 2;

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
  void aQueryAnswersTheQuota() {
    assertThat(ks("{ collection: 'Item', query: {} }")).as("no user, no quota").hasSize(RECORDS);

    bindUser(quota(QUOTA));
    assertThat(ks("{ collection: 'Item', query: {} }")).hasSize(QUOTA);
    assertThat(ks("{ collection: 'Item', query: { k: { $gt: 0 } } }")).hasSize(QUOTA);
    assertThat(ProfiledLimitScope.isActive()).as("nothing is left on the thread").isFalse();
  }

  @Test
  void aQueryFindsADocumentPastTheQuotaOfItsBucket() {
    bindUser(quota(QUOTA));
    assertThat(ks("{ collection: 'Item', query: { k: { $eq: 40 } } }")).containsExactly(40);
  }

  /** The sort runs over every document that matches, and the quota caps what it answers. */
  @Test
  void aSortRunsOverEveryDocumentAndAnswersTheQuota() {
    bindUser(quota(QUOTA));
    assertThat(ks("{ collection: 'Item', query: { k: { $gt: 0 }, $orderBy: { k: -1 } } }")).containsExactly(40, 39);
    assertThat(ks("{ collection: 'Item', query: { $orderBy: { k: -1 } } }")).containsExactly(40, 39);
  }

  @Test
  void aLimitBelowTheQuotaStaysInForceAndOneAboveItIsCapped() {
    bindUser(quota(QUOTA));
    assertThat(ks("{ collection: 'Item', numberToReturn: 1, query: { k: { $gt: 0 }, $orderBy: { k: -1 } } }")).containsExactly(40);
    assertThat(ks("{ collection: 'Item', numberToReturn: 10, query: { k: { $gt: 0 }, $orderBy: { k: -1 } } }"))
        .containsExactly(40, 39);
    assertThat(ks("{ collection: 'Item', numberToSkip: 39, query: { k: { $gt: 0 }, $orderBy: { k: 1 } } }"))
        .as("the skipped documents are not answered, so they do not count").containsExactly(40);
  }

  @Test
  void aQuotaOfZeroAnswersNoDocument() {
    bindUser(quota(0));
    assertThat(ks("{ collection: 'Item', query: {} }")).isEmpty();
    assertThat(firstBatch(find(new Document("find", "Item")))).isEmpty();
  }

  @Test
  void theFindCommandAnswersTheQuota() {
    assertThat(firstBatch(find(new Document("find", "Item")))).as("no user, no quota").hasSize(RECORDS);

    bindUser(quota(QUOTA));
    assertThat(firstBatch(find(new Document("find", "Item")))).hasSize(QUOTA);
    assertThat(firstBatch(find(new Document("find", "Item").append("limit", 10)))).hasSize(QUOTA);
    assertThat(firstBatch(find(new Document("find", "Item").append("limit", 1)))).hasSize(1);
    assertThat(ksOf(firstBatch(find(new Document("find", "Item").append("filter", gtZero()).append("sort", new Document("k", -1))))))
        .containsExactly(40, 39);
    assertThat(ProfiledLimitScope.isActive()).as("nothing is left on the thread").isFalse();
  }

  /** A count answers one number, computed over every document that matches. */
  @Test
  void aCountRunsOverEveryDocument() {
    bindUser(quota(QUOTA));
    assertThat(wrapper.handleCommand(null, "count", new Document("count", "Item").append("query", gtZero()), null, null).get("n"))
        .isEqualTo(RECORDS);
    assertThat(wrapper.handleCommand(null, "count", new Document("count", "Item"), null, null).get("n")).isEqualTo(RECORDS);
  }

  /** A pipeline reads every document, and the quota caps the documents its last stage answers. */
  @Test
  void anAggregationRunsOverEveryDocumentAndAnswersTheQuota() {
    bindUser(quota(QUOTA));
    final List<Document> grouped = firstBatch(aggregate(new Document("$match", gtZero()),
        new Document("$group", new Document("_id", null).append("n", new Document("$sum", 1)))));
    assertThat(grouped).hasSize(1);
    assertThat(((Number) grouped.getFirst().get("n")).intValue()).isEqualTo(RECORDS);

    assertThat(firstBatch(aggregate(new Document("$match", gtZero())))).hasSize(QUOTA);
    assertThat(ksOf(firstBatch(aggregate(new Document("$sort", new Document("k", -1)))))).containsExactly(40, 39);

    bindUser(null);
    assertThat(firstBatch(aggregate(new Document("$match", gtZero())))).hasSize(RECORDS);
  }

  /** The quota does not limit a write: an update of many documents reaches every one that matches. */
  @Test
  void aMultiDocumentUpdateReachesEveryDocument() {
    bindUser(quota(QUOTA));
    final Document reply = wrapper.handleCommand(null, "update", new Document("update", "Item").append("updates",
        List.of(new Document("q", gtZero()).append("u", new Document("$set", new Document("touched", true))).append("multi", true))),
        null, null);
    assertThat(reply.get("n")).isEqualTo(RECORDS);

    bindUser(null);
    assertThat(count("SELECT count(*) AS c FROM Item WHERE touched = true")).isEqualTo(RECORDS);
  }

  @Test
  void aDeleteOfManyDocumentsReachesEveryDocument() {
    bindUser(quota(QUOTA));
    final Document filtered = wrapper.handleCommand(null, "delete", new Document("delete", "Item").append("deletes",
        List.of(new Document("q", new Document("k", new Document("$gt", 20))).append("limit", 0))), null, null);
    assertThat(filtered.get("n")).isEqualTo(RECORDS - 20);

    final Document all = wrapper.handleCommand(null, "delete",
        new Document("delete", "Item").append("deletes", List.of(new Document("q", new Document()).append("limit", 0))), null, null);
    assertThat(all.get("n")).isEqualTo(20);

    bindUser(null);
    assertThat(count("SELECT count(*) AS c FROM Item")).isZero();
  }

  /** A command run as a part of a statement the quota already caps is not an answer to the user: it reads every document. */
  @Test
  void aQueryRunByACappedStatementIsNotCapped() {
    bindUser(quota(QUOTA));
    ProfiledLimitScope.enter();
    try {
      assertThat(ks("{ collection: 'Item', query: {} }")).hasSize(RECORDS);
    } finally {
      ProfiledLimitScope.exit();
    }
    assertThat(ks("{ collection: 'Item', query: {} }")).as("the next top-level query").hasSize(QUOTA);
  }

  private static Document gtZero() {
    return new Document("k", new Document("$gt", 0));
  }

  private Document find(final Document command) {
    return wrapper.handleCommand(null, "find", command, null, null);
  }

  private Document aggregate(final Document... stages) {
    return wrapper.handleCommand(null, "aggregate",
        new Document("aggregate", "Item").append("pipeline", List.of(stages)).append("cursor", new Document()), null, null);
  }

  @SuppressWarnings("unchecked")
  private static List<Document> firstBatch(final Document reply) {
    return (List<Document>) ((Document) reply.get("cursor")).get("firstBatch");
  }

  private static List<Integer> ksOf(final List<Document> documents) {
    final List<Integer> ks = new ArrayList<>(documents.size());
    for (final Document document : documents)
      ks.add(((Number) document.get("k")).intValue());
    return ks;
  }

  private List<Integer> ks(final String query) {
    final List<Integer> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("mongo", query)) {
      while (rs.hasNext())
        ks.add(rs.next().<Number>getProperty("k").intValue());
    }
    return ks;
  }

  private int count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").intValue();
    }
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private static SecurityDatabaseUser quota(final long resultSetLimit) {
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
        return -1L;
      }
    };
  }
}
