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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class TruncateClassStatementExecutionTest extends TestHelper {

  @SuppressWarnings("unchecked")
  @Test
  void truncateClass() {
    database.begin();

    final Schema schema = database.getSchema();
    final DocumentType testClass = getOrcreateDocumentType(schema);

    final Index index = getOrCreateIndex(testClass);

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(1, 2)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(3, 0)).save();

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(5, 6, 7)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(8, 9, -1)).save();

    final ResultSet result = database.query("sql", "SElect from test_class");
    //    Assertions.assertThat(2).isEqualTo(result.size());

    final Set<Integer> set = new HashSet<Integer>();
    while (result.hasNext()) {
      set.addAll(result.next().getProperty("data"));
    }
    result.close();
    assertThat(set.containsAll(Arrays.asList(5, 6, 7, 8, 9, -1))).isTrue();

    schema.dropType("test_class");
    database.commit();
  }

  @Test
  void truncateVertexClassSubclasses() {
    database.begin();
    database.command("sql", "create document type TestTruncateVertexClassSuperclass");
    database.command("sql", "create document type TestTruncateVertexClassSubclass extends TestTruncateVertexClassSuperclass");

    database.command("sql", "insert into TestTruncateVertexClassSuperclass set name = 'foo'");
    database.command("sql", "insert into TestTruncateVertexClassSubclass set name = 'bar'");

    ResultSet result = database.query("sql", "SElect from TestTruncateVertexClassSuperclass");
    for (int i = 0; i < 2; i++) {
      assertThat(result.hasNext()).isTrue();
      result.next();
    }
    assertThat(result.hasNext()).isFalse();
    result.close();

    database.command("sql", "truncate type TestTruncateVertexClassSuperclass ");
    result = database.query("sql", "SElect from TestTruncateVertexClassSubclass");
    assertThat(result.hasNext()).isTrue();
    result.next();
    assertThat(result.hasNext()).isFalse();
    result.close();

    database.command("sql", "truncate type TestTruncateVertexClassSuperclass polymorphic");
    result = database.query("sql", "SElect from TestTruncateVertexClassSubclass");
    assertThat(result.hasNext()).isFalse();
    result.close();
    database.commit();
  }

  @Test
  void truncateVertexClassSubclassesWithIndex() {
    database.begin();
    database.command("sql", "create document type TestTruncateVertexClassSuperclassWithIndex");
    database.command("sql", "create property TestTruncateVertexClassSuperclassWithIndex.name STRING");
    database.command("sql", "create index on TestTruncateVertexClassSuperclassWithIndex (name) NOTUNIQUE");

    database.command("sql", "create document type TestTruncateVertexClassSubclassWithIndex extends TestTruncateVertexClassSuperclassWithIndex");

    database.command("sql", "insert into TestTruncateVertexClassSuperclassWithIndex set name = 'foo'");
    database.command("sql", "insert into TestTruncateVertexClassSubclassWithIndex set name = 'bar'");

    database.command("sql", "truncate type TestTruncateVertexClassSubclassWithIndex");
    database.command("sql", "truncate type TestTruncateVertexClassSuperclassWithIndex polymorphic");
    database.commit();
  }

  @SuppressWarnings("unchecked")
  @Test
  void truncateClassWithCommandCache() {
    database.begin();

    final Schema schema = database.getSchema();
    final DocumentType testClass = getOrcreateDocumentType(schema);

    database.command("sql", "truncate type test_class");

    database.newDocument(testClass.getName()).set("name", "x").set("data", Arrays.asList(1, 2)).save();
    database.newDocument(testClass.getName()).set("name", "y").set("data", Arrays.asList(3, 0)).save();

    ResultSet result = database.query("sql", "SElect from test_class");
    assertThat(toList(result).size()).isEqualTo(2);

    result.close();
    database.command("sql", "truncate type test_class");

    result = database.query("sql", "SElect from test_class");
    assertThat(toList(result).size()).isEqualTo(0);
    result.close();

    schema.dropType("test_class");

    database.commit();
  }

  private List<Result> toList(final ResultSet input) {
    final List<Result> result = new ArrayList<>();
    while (input.hasNext()) {
      result.add(input.next());
    }
    return result;
  }

  private Index getOrCreateIndex(final DocumentType testClass) {
    if (database.getSchema().existsIndex("test_class_by_data"))
      return database.getSchema().getIndexByName("test_class_by_data");

    testClass.createProperty("data", Type.LIST);
    return testClass.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "data");
  }

  private DocumentType getOrcreateDocumentType(final Schema schema) {
    final DocumentType testClass;
    if (schema.existsType("test_class")) {
      testClass = schema.getType("test_class");
    } else {
      testClass = schema.createDocumentType("test_class");
    }
    return testClass;
  }

  // ---- #6220: TRUNCATE joins the caller's transaction ----
  //
  // Regression tests for issue #6220: {@code TRUNCATE TYPE} and {@code TRUNCATE BUCKET} committed the caller's
  // transaction from the inside - {@code schema.dropIndex()} per index before a single record was touched, then
  // {@code commit(); begin()} once per {@code arcadedb.truncateBatchSize} records - so an explicit {@code ROLLBACK}
  // recovered at most whatever the last, uncommitted batch held: nothing at all on an indexed type.
  // <p>
  // The other half of the same fix: with no transaction active the statement now opens its own, where before every
  // delete failed with "Transaction not begun" and the batched drop/rebuild fast path could therefore only ever run on
  // a caller's transaction - the one place it must not.

  /** Regression test for issue #6220. */
  @Test
  void rollbackPutsBackAnUnindexedTypeSpanningManyBatches() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      for (int i = 0; i < 1100; i++)
        database.command("sql", "INSERT INTO T SET n = ?", i);
    });

    assertThat(database.countType("T", false)).isEqualTo(1100L);

    database.begin();
    database.command("sql", "TRUNCATE TYPE `T` UNSAFE").close();
    database.rollback();

    assertThat(database.countType("T", false)).isEqualTo(1100L);
  }

  /** Regression test for issue #6220. */
  @Test
  void rollbackPutsBackAnIndexedType() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.n INTEGER");
    database.command("sql", "CREATE INDEX ON U(n) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO U SET n = ?", i);
    });

    assertThat(database.countType("U", false)).isEqualTo(5L);

    database.begin();
    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE `U` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional"))
          .as("the statement must report which path it took").isTrue();
    }
    database.rollback();

    assertThat(database.countType("U", false)).isEqualTo(5L);
    assertThat(database.getSchema().getType("U").getAllIndexes(true)).isNotEmpty();
    assertThat(database.query("sql", "SELECT FROM U WHERE n = 3").stream().count()).isEqualTo(1L);
  }

  /** Regression test for issue #6220. */
  @Test
  void commitStillRemovesEverything() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 100);

    database.command("sql", "CREATE DOCUMENT TYPE V2");
    database.command("sql", "CREATE PROPERTY V2.n INTEGER");
    database.command("sql", "CREATE INDEX ON V2(n) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 350; i++)
        database.command("sql", "INSERT INTO V2 SET n = ?", i);
    });

    database.begin();
    database.command("sql", "TRUNCATE TYPE `V2` UNSAFE").close();
    database.commit();

    assertThat(database.countType("V2", false)).isEqualTo(0L);
    assertThat(database.query("sql", "SELECT FROM V2 WHERE n = 3").stream().count()).isEqualTo(0L);

    database.transaction(() -> database.command("sql", "INSERT INTO V2 SET n = 3"));
    assertThat(database.query("sql", "SELECT FROM V2 WHERE n = 3").stream().count()).isEqualTo(1L);
  }

  /**
   * Regression test for issue #6220.
   * <p>
   * The shape the issue is really about: reload a staging type in one unit of work. When the insert half fails, the
   * old rows must still be there.
   */
  @Test
  void aFailedReloadLeavesTheOldRowsInPlace() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE Staging");
    database.command("sql", "CREATE PROPERTY Staging.n INTEGER");
    database.command("sql", "CREATE INDEX ON Staging(n) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.command("sql", "INSERT INTO Staging SET n = ?", i);
    });

    assertThatThrownBy(() -> database.transaction(() -> {
      database.command("sql", "TRUNCATE TYPE `Staging` UNSAFE").close();
      database.command("sql", "INSERT INTO Staging SET n = 1000").close();
      throw new IllegalStateException("the reload failed halfway");
    })).isInstanceOf(IllegalStateException.class);

    assertThat(database.countType("Staging", false)).isEqualTo(40L);
    assertThat(database.query("sql", "SELECT FROM Staging WHERE n = 1000").stream().count()).isZero();
    for (int i = 0; i < 40; i++)
      assertThat(database.query("sql", "SELECT FROM Staging WHERE n = ?", i).stream().count()).isEqualTo(1L);
  }

  /**
   * Regression test for issue #6220.
   * <p>
   * With no transaction active the statement owns one: it must open it (the deletes used to fail with "Transaction
   * not begun", which made this whole path unreachable), take the drop/rebuild fast path, and hand the caller's
   * thread back without a transaction on it.
   */
  @Test
  void truncateWithNoActiveTransactionOwnsOneAndRebuildsTheIndexes() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE W");
    database.command("sql", "CREATE PROPERTY W.n INTEGER");
    database.command("sql", "CREATE INDEX ON W(n) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO W SET n = ?", i);
    });

    try (final ResultSet rs = database.command("sql", "TRUNCATE TYPE `W` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional"))
          .as("a truncate that owns its transaction commits it: a rollback cannot put the records back").isFalse();
    }

    assertThat(database.isTransactionActive()).as("TRUNCATE must not leave its own transaction open").isFalse();
    assertThat(database.countType("W", false)).isZero();
    assertThat(database.getSchema().getType("W").getAllIndexes(true)).isNotEmpty();
    assertThat(database.getSchema().getIndexByName("W[n]").countEntries()).isZero();

    // the recreated unique index accepts every key back
    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO W SET n = ?", i);
    });
    assertThat(database.countType("W", false)).isEqualTo(35L);
  }

  /**
   * Regression test for issue #6220.
   * <p>
   * The deletes now belong to the caller's transaction, so they must also see what that transaction has written but
   * not yet committed - a truncate that only cleared the on-disk rows would leave the ones inserted a statement ago.
   */
  @Test
  void truncateAlsoRemovesRecordsTheSameTransactionJustInserted() {
    database.command("sql", "CREATE DOCUMENT TYPE Fresh");
    database.command("sql", "CREATE PROPERTY Fresh.n INTEGER");
    database.command("sql", "CREATE INDEX ON Fresh(n) UNIQUE");

    database.transaction(() -> database.command("sql", "INSERT INTO Fresh SET n = 1"));

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Fresh SET n = 2").close();
      database.command("sql", "TRUNCATE TYPE `Fresh` UNSAFE").close();
      database.command("sql", "INSERT INTO Fresh SET n = 1").close();
    });

    assertThat(database.countType("Fresh", false)).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM Fresh WHERE n = 1").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM Fresh WHERE n = 2").stream().count()).isZero();
  }

  /**
   * Regression test for issue #6220.
   * <p>
   * The fast path drops the indexes with a committed schema change, so whatever it does about the failure it must
   * not be able to end with the type missing an index it had: nothing later can undo that drop.
   * <p>
   * The rollback of the statement's own transaction does not, and cannot, take the rebuilt indexes with it -
   * {@code TypeIndexBuilder.create()} goes through {@code LocalSchema.recordFileChanges}, which persists the schema
   * itself and wraps each per-bucket build in its own {@code joinCurrent=false} transaction. This test is what says
   * so out loud: it asserts the surviving records, the index that indexes them, and their agreement, none of which
   * would hold if the rollback reached either the drop or the rebuild.
   */
  @Test
  void aFailedTruncateOnTheFastPathStillLeavesTheTypeWithItsIndexes() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE Boom");
    database.command("sql", "CREATE PROPERTY Boom.n INTEGER");
    database.command("sql", "CREATE INDEX ON Boom(n) UNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO Boom SET n = ?", i);
    });

    final AtomicInteger deletes = new AtomicInteger();
    final BeforeRecordDeleteListener boom = (final Record record) -> {
      if (deletes.incrementAndGet() == 25)
        throw new RuntimeException("simulated mid-truncate failure");
      return true;
    };
    database.getSchema().getType("Boom").getEvents().registerListener(boom);
    try {
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE `Boom` UNSAFE"))
          .hasMessageContaining("simulated mid-truncate failure");
    } finally {
      database.getSchema().getType("Boom").getEvents().unregisterListener(boom);
    }

    assertThat(database.isTransactionActive()).as("a failed truncate leaves no transaction behind").isFalse();
    assertThat(database.getSchema().getType("Boom").getAllIndexes(true))
        .as("the dropped index is committed, so a failure must never leave the type without it").isNotEmpty();

    // Whatever survived the truncate, the index and the buckets must agree about it: every surviving record is
    // reachable through the index, and the index points at nothing that is gone. Bounded on both sides so the
    // agreement cannot be the vacuous 0 == 0: the committed batches really did delete, and the uncommitted one
    // really did come back.
    final long surviving = database.countType("Boom", false);
    assertThat(surviving).as("the committed batches deleted, the last uncommitted one was rolled back")
        .isGreaterThan(0L).isLessThan(35L);
    assertThat(database.getSchema().getIndexByName("Boom[n]").countEntries()).isEqualTo(surviving);
    for (final Result r : database.query("sql", "SELECT n FROM Boom").stream().toList())
      assertThat(database.query("sql", "SELECT FROM Boom WHERE n = ?", r.<Integer>getProperty("n")).stream().count())
          .as("record n=%s is reachable through the rebuilt index", r.<Integer>getProperty("n")).isEqualTo(1L);

    // and a retry now completes the truncate cleanly through the same path
    database.command("sql", "TRUNCATE TYPE `Boom` UNSAFE").close();
    assertThat(database.countType("Boom", false)).isZero();
    database.transaction(() -> database.command("sql", "INSERT INTO Boom SET n = 1"));
    assertThat(database.query("sql", "SELECT FROM Boom WHERE n = 1").stream().count()).isEqualTo(1L);
  }

  /** Regression test for issue #6220. */
  @Test
  void rollbackPutsBackAPolymorphicTruncate() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE DOCUMENT TYPE Derived EXTENDS Base");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Base SET n = 1");
      database.command("sql", "INSERT INTO Derived SET n = 2");
    });

    database.begin();
    database.command("sql", "TRUNCATE TYPE `Base` POLYMORPHIC UNSAFE").close();
    assertThat(database.countType("Base", true)).as("the deletes are visible inside the transaction").isZero();
    database.rollback();

    assertThat(database.countType("Base", true)).isEqualTo(2L);
    assertThat(database.countType("Derived", false)).isEqualTo(1L);
  }

  /**
   * Regression test for issue #6220.
   * <p>
   * The bucket statement's own-transaction path has the same close-out to get right as the type one, and a bucket
   * owns no indexes to soften a mistake: a failure mid-scan must keep the committed batches, put the uncommitted one
   * back, report the failure and leave no transaction on the caller's thread.
   */
  @Test
  void aFailedTruncateBucketOnTheFastPathKeepsTheCommittedBatchesAndReportsTheFailure() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE BoomBucket BUCKETS 1");
    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO BoomBucket SET n = ?", i);
    });

    final String bucketName = database.getSchema().getType("BoomBucket").getBuckets(false).getFirst().getName();

    final AtomicInteger deletes = new AtomicInteger();
    final BeforeRecordDeleteListener boom = (final Record record) -> {
      if (deletes.incrementAndGet() == 25)
        throw new RuntimeException("simulated mid-truncate-bucket failure");
      return true;
    };
    database.getSchema().getType("BoomBucket").getEvents().registerListener(boom);
    try {
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE"))
          .hasMessageContaining("simulated mid-truncate-bucket failure");
    } finally {
      database.getSchema().getType("BoomBucket").getEvents().unregisterListener(boom);
    }

    assertThat(database.isTransactionActive()).as("a failed truncate leaves no transaction behind").isFalse();
    assertThat(database.countBucket(bucketName))
        .as("the committed batches deleted, the last uncommitted one was rolled back")
        .isGreaterThan(0L).isLessThan(35L);

    // and a retry now clears it
    database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE").close();
    assertThat(database.countBucket(bucketName)).isZero();
  }

  /** Regression test for issue #6220. */
  @Test
  void truncateBucketRollbackPutsBackTheRecords() {
    database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, 10);

    database.command("sql", "CREATE DOCUMENT TYPE B BUCKETS 1");
    database.transaction(() -> {
      for (int i = 0; i < 35; i++)
        database.command("sql", "INSERT INTO B SET n = ?", i);
    });

    final String bucketName = database.getSchema().getType("B").getBuckets(false).getFirst().getName();

    database.begin();
    database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE").close();
    database.rollback();

    assertThat(database.countBucket(bucketName)).isEqualTo(35L);

    // ...and with no transaction of its own to break, TRUNCATE BUCKET still clears it
    try (final ResultSet rs = database.command("sql", "TRUNCATE BUCKET `" + bucketName + "` UNSAFE")) {
      assertThat(rs.next().<Boolean>getProperty("transactional")).isFalse();
    }
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(database.countBucket(bucketName)).isZero();
  }

  // ---- #7668: TRUNCATE of a heavyweight edge type with a LIGHTWEIGHT subtype ----
  //
  // Regression test for issue #7668: {@code TRUNCATE TYPE} on a heavyweight edge type that merely has a LIGHTWEIGHT
  // subtype used to route through {@code EdgeType.holdsLightweightEdges()} to the batched, index-unaware
  // {@code DELETE FROM} path meant only for a genuinely lightweight type - re-introducing the #4352 tombstone hazard
  // on the heavyweight type's own index, and never actually reaching the lightweight subtype's edges (which have no
  // bucket for a record-based delete to find).

  /** Regression test for issue #7668. */
  @Test
  void truncatePolymorphicClearsHeavyweightIndexAndLightweightSubtypeEdges() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE PROPERTY Follows.since STRING");
    database.command("sql", "CREATE INDEX ON Follows(since) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE CloseFollows EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
      database.lookupByRID(p3, true).asVertex().modify().newEdge("CloseFollows", p4);
    } finally {
      database.commit();
    }

    assertCount("Follows", 2);
    assertIndexSize("Follows[since]", 1);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .isEqualTo(1);

    // Fingerprint the index's underlying LSM-Tree files: the safe path (drop the index, clear the records, rebuild
    // an empty one) replaces them, while the buggy path - a bucket-scan delete with the index left live - keeps
    // deleting into the very same files. This is what actually distinguishes the two paths: at this small scale
    // both leave the index correctly sized, so only the file identity tells them apart (issue #7668).
    final List<Integer> indexFileIdsBeforeTruncate = List.copyOf(((TypeIndex) database.getSchema().getIndexByName("Follows[since]")).getFileIds());

    // outside a transaction: this is the fast, own-transaction path that used to be skipped for the whole tree
    // whenever ANY type in it was lightweight (issue #7668)
    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE");

    assertCount("Follows", 0);
    // the index must be empty, not just unreachable through a bucket scan - reusing the same key must succeed
    assertIndexSize("Follows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("Follows[since]")).getFileIds())
        .as("Follows has its own index and properties: it must take the index-drop/rebuild path, not the raw "
            + "batched delete meant only for a type with no index to protect (issue #7668)")
        .isNotEqualTo(indexFileIdsBeforeTruncate);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .as("the lightweight subtype's edge has no bucket of its own: only the walk-based DELETE FROM reaches it")
        .isEqualTo(0);

    // the recreated index must accept the same key again (used to fail with a stale-tombstone duplicate-key error,
    // issue #4352, if the heavyweight part were ever routed through the raw batched delete instead)
    database.begin();
    try {
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("Follows", 1);
    assertIndexSize("Follows[since]", 1);
  }

  /**
   * Regression test for issue #7668.
   * <p>
   * Symmetric shape: the LIGHTWEIGHT type is the root and the heavyweight, indexed type is the subtype. A type's
   * own LIGHTWEIGHT flag is independent of its parent's, so this hierarchy is just as legal as the one above and
   * exercises the same routing decision from the other side.
   */
  @Test
  void truncatePolymorphicClearsLightweightRootAndHeavyweightSubtypeIndex() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE StrongKnows EXTENDS Knows");
    database.command("sql", "CREATE PROPERTY StrongKnows.since STRING");
    database.command("sql", "CREATE INDEX ON StrongKnows(since) UNIQUE");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Knows", p2);
      database.lookupByRID(p3, true).asVertex().modify().newEdge("StrongKnows", p4).set("since", "2020").save();
    } finally {
      database.commit();
    }

    assertCount("Knows", 2);
    assertIndexSize("StrongKnows[since]", 1);

    final List<Integer> indexFileIdsBeforeTruncate = List.copyOf(((TypeIndex) database.getSchema().getIndexByName("StrongKnows[since]")).getFileIds());

    database.command("sql", "TRUNCATE TYPE Knows POLYMORPHIC UNSAFE");

    assertCount("Knows", 0);
    assertIndexSize("StrongKnows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("StrongKnows[since]")).getFileIds())
        .as("StrongKnows has its own index and properties: it must take the index-drop/rebuild path even though "
            + "its LIGHTWEIGHT parent Knows has none (issue #7668)")
        .isNotEqualTo(indexFileIdsBeforeTruncate);
    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Knows"))
        .isEqualTo(0);

    database.begin();
    try {
      database.lookupByRID(p3, true).asVertex().modify().newEdge("StrongKnows", p4).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("StrongKnows", 1);
    assertIndexSize("StrongKnows[since]", 1);
  }

  /**
   * Regression test for issue #7668.
   * <p>
   * A LIGHTWEIGHT type sandwiched between two heavyweight, indexed ones (CodeRabbit review on this PR): {@code
   * getSubTypes()} returns direct subtypes only, so an index-collection walk that stops at depth one would miss
   * VeryCloseFollows' index entirely while the record-backed scan beneath it - which walks the full subtype tree
   * regardless of depth - deletes its records anyway, reintroducing the #4352 tombstone hazard for exactly the
   * type this fix exists to protect, two levels down instead of one.
   */
  @Test
  void truncatePolymorphicClearsAnIndexTwoLevelsBelowALightweightIntermediateType() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE CloseFollows EXTENDS Follows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE VeryCloseFollows EXTENDS CloseFollows");
    database.command("sql", "CREATE PROPERTY VeryCloseFollows.since STRING");
    database.command("sql", "CREATE INDEX ON VeryCloseFollows(since) UNIQUE");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("VeryCloseFollows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }

    assertCount("VeryCloseFollows", 1);
    assertIndexSize("VeryCloseFollows[since]", 1);

    final List<Integer> indexFileIdsBeforeTruncate =
        List.copyOf(((TypeIndex) database.getSchema().getIndexByName("VeryCloseFollows[since]")).getFileIds());

    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE");

    assertCount("VeryCloseFollows", 0);
    assertIndexSize("VeryCloseFollows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("VeryCloseFollows[since]")).getFileIds())
        .as("VeryCloseFollows' index is two levels below the LIGHTWEIGHT type collectTruncationScope routed "
            + "through: collectIndexDefinitions must recurse the same full depth to find and protect it")
        .isNotEqualTo(indexFileIdsBeforeTruncate);

    database.begin();
    try {
      database.lookupByRID(p1, true).asVertex().modify().newEdge("VeryCloseFollows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("VeryCloseFollows", 1);
    assertIndexSize("VeryCloseFollows[since]", 1);
  }

  /**
   * Regression test for issue #7668.
   * <p>
   * The {@code transactional == true} branch (code review): every other test in this class runs
   * {@code TRUNCATE TYPE ... POLYMORPHIC UNSAFE} with no transaction open, exercising only
   * {@code truncateInOwnTransaction()} joined with {@code truncateLightweightEdgeTypes(db, false, ...)}. Inside an
   * open transaction the statement instead joins it via {@code truncateInCallerTransaction()} and
   * {@code truncateLightweightEdgeTypes(db, true, ...)}, which is new code this PR added and had no coverage of its
   * own. Both combinations must (a) apply within the transaction before it is closed, and (b) fully undo on
   * ROLLBACK, mirroring what the #6220 tests above already pin for the plain, non-mixed case.
   */
  @Test
  void truncateInsideAnOpenTransactionAppliesBeforeCommitAndFullyUndoesOnRollback() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE PROPERTY Follows.since STRING");
    database.command("sql", "CREATE INDEX ON Follows(since) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE CloseFollows EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
      database.lookupByRID(p3, true).asVertex().modify().newEdge("CloseFollows", p4);
    } finally {
      database.commit();
    }

    assertCount("Follows", 2);
    assertIndexSize("Follows[since]", 1);

    // ROLLBACK first: both the heavyweight record/index and the lightweight edge must come back.
    database.begin();
    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE").close();
    assertCount("Follows", 0);
    assertIndexSize("Follows[since]", 0);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .as("applied inside the open transaction, before it is closed").isEqualTo(0);
    database.rollback();

    assertCount("Follows", 2);
    assertIndexSize("Follows[since]", 1);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .as("a ROLLBACK of the caller's transaction must undo the lightweight DELETE FROM the same way it undoes "
            + "the heavyweight per-record deletes (issue #6220)")
        .isEqualTo(1);

    // Then COMMIT: both must be gone for good, with the index still usable (per-record delete, not drop/rebuild).
    database.begin();
    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE").close();
    database.commit();

    assertCount("Follows", 0);
    assertIndexSize("Follows[since]", 0);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows")).isEqualTo(0);

    database.begin();
    try {
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("Follows", 1);
    assertIndexSize("Follows[since]", 1);
  }

  private void assertCount(final String typeName, final long expected) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM " + typeName)) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(expected);
    }
  }

  private void assertIndexSize(final String indexName, final long expected) {
    final RangeIndex index = (RangeIndex) database.getSchema().getIndexByName(indexName);
    long count = 0;
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    assertThat(count).as("Index '%s' size", indexName).isEqualTo(expected);
  }

  // ---- #7919: non-POLYMORPHIC TRUNCATE guard ----
  //
  // Regression test for issue #7919: {@code TRUNCATE TYPE}'s non-POLYMORPHIC guard read the CONFLATED
  // {@code EdgeType.holdsLightweightEdges()}, which answers true when the type <b>or anything below it</b> is
  // lightweight. So a heavyweight edge type that merely has a lightweight descendant had a plain
  // {@code TRUNCATE TYPE Follows} refused, with a message that is false for that shape - it claimed the type has "no
  // bucket of its own", when a record-backed root has exactly that, and a non-polymorphic truncate would clear it and
  // never touch the lightweight subtype.
  // <p>
  // Two more lines in the same method are pinned here: the safety count keyed on the same conflated flag (which would
  // have run an unconditionally-polymorphic {@code count(*)} for a non-polymorphic truncate and refused on records
  // outside the scope), and the per-subtype loop, which counted the ROOT on every iteration instead of the subtype,
  // so the check meant to refuse a polymorphic truncate over a non-empty subtype never looked at a subtype at all.

  /** Regression test for issue #7919. */
  @Test
  void nonPolymorphicTruncateOfAHeavyweightRootWithALightweightSubtypeIsAllowed() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE FollowsLite EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
      database.lookupByRID(p3, true).asVertex().modify().newEdge("FollowsLite", p4);
    } finally {
      database.commit();
    }

    assertThat(database.countType("Follows", false)).isEqualTo(1);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite")).isEqualTo(1);

    // Used to be refused outright: Follows is record-backed, so it has a bucket of its own to scope this to.
    database.command("sql", "TRUNCATE TYPE Follows UNSAFE").close();

    assertThat(database.countType("Follows", false)).as("the root's own bucket is cleared").isEqualTo(0);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite"))
        .as("a non-polymorphic truncate must not reach into the lightweight subtype")
        .isEqualTo(1);
  }

  /**
   * Regression test for issue #7919.
   * <p>
   * The count at the heart of the same guard. A non-polymorphic truncate of a heavyweight root must be gated on the
   * root's own records: routing it through the {@code count(*)} push-down, which is unconditionally polymorphic,
   * would refuse an empty root because a subtype below it holds edges the statement is not going to touch.
   */
  @Test
  void nonPolymorphicTruncateOfAnEmptyHeavyweightRootIsNotRefusedForItsLightweightSubtypesEdges() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE FollowsLite EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("FollowsLite", p2);
    } finally {
      database.commit();
    }

    // No UNSAFE: the safety count must see the root's own zero records, not the subtree's one edge.
    database.command("sql", "TRUNCATE TYPE Follows").close();

    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite"))
        .as("out of scope for a non-polymorphic truncate, so still there")
        .isEqualTo(1);
  }

  /**
   * Regression test for issue #7919.
   * <p>
   * The shape the guard exists for is still refused: a LIGHTWEIGHT root has no bucket, so a non-polymorphic
   * TRUNCATE could only be served by the unconditionally-polymorphic {@code DELETE FROM}, which would reach into
   * the subtype the caller did not ask for.
   */
  @Test
  void nonPolymorphicTruncateOfALightweightRootWithASubtypeIsStillRefused() {
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE StrongKnows EXTENDS Knows");

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Knows"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("without POLYMORPHIC because it is a LIGHTWEIGHT edge type with a subtype")
        .hasMessageContaining("TRUNCATE TYPE Knows POLYMORPHIC");
  }

  /**
   * Regression test for issue #7919.
   * <p>
   * A lightweight type with no subtype at all has no ambiguity to refuse, whichever way POLYMORPHIC was spelled.
   */
  @Test
  void nonPolymorphicTruncateOfALightweightLeafStillWorks() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Knows", p2);
    } finally {
      database.commit();
    }

    database.command("sql", "TRUNCATE TYPE Knows UNSAFE").close();

    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Knows")).isEqualTo(0);
  }

  /**
   * Regression test for issue #7919.
   * <p>
   * The per-subtype loop (line :122 of the issue). Reaching it needs a root whose OWN guard does not throw first,
   * which means a root that is neither a vertex nor an edge type - a DOCUMENT root with a graph subtype under it,
   * the "multiple inheritance" case the loop's own comment names. With the root counted instead of the subtype, the
   * loop saw the root's zero and let a polymorphic truncate of a non-empty subtype through without the UNSAFE
   * keyword it was written to demand.
   * <p>
   * This fixture used to build the hierarchy around a type it created called {@code E}, because the root guard
   * asked {@code isSubTypeOf("E")} by NAME and that was the only way to make it fire at all. It fires on the type
   * now (issue #8042), so the arrangement that used to be a way IN to the loop became a way past it: the root's own
   * guard answers first for a graph root, whatever its subtypes hold.
   */
  @Test
  void thePerSubtypeSafetyCountLooksAtTheSubtypeNotTheRoot() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE EDGE TYPE Sub EXTENDS Base");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Sub", p2).save();
    } finally {
      database.commit();
    }

    assertThat(database.countType("Base", false)).as("the root itself holds nothing").isZero();
    assertThat(database.countType("Sub", false)).isEqualTo(1);

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes (Sub)");

    // still there: the refusal must have stopped the truncate, not merely reported on it afterwards
    assertThat(database.countType("Sub", false)).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC UNSAFE").close();
    assertThat(database.countType("Sub", false)).isZero();
  }

  /**
   * Regression test for issue #7919.
   * <p>
   * The other half of the same typo: an EMPTY subtype under a non-empty root was reported BY NAME as the reason for
   * a refusal, because the count that triggered it was the root's. Same document-root shape as the test above, for
   * the same reason (issue #8042).
   */
  @Test
  void anEmptySubtypeIsNotBlamedForTheRootsRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE EDGE TYPE Sub EXTENDS Base");

    database.transaction(() -> database.command("sql", "INSERT INTO Base SET name = 'b1'").close());

    assertThat(database.countType("Base", false)).isEqualTo(1);
    assertThat(database.countType("Sub", false)).isZero();

    // Base is a document type, so its own guard does not refuse; the loop must not refuse on Sub's behalf either,
    // because Sub is empty.
    database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC").close();

    assertThat(database.countType("Base", true)).isZero();
  }

  // ---- #8042: TRUNCATE refuses a non-empty graph type without UNSAFE ----
  //
  // https://github.com/ArcadeData/arcadedb/issues/8042
  // <p>
  // {@code TruncateTypeStatement.executeDDL} refuses to truncate a non-empty graph type unless {@code UNSAFE} is
  // given, and that refusal is the only thing between {@code TRUNCATE TYPE Person} and the silent emptying of a live
  // vertex type. It could not fire: the test was {@code typez.isSubTypeOf("V")}/{@code isSubTypeOf("E")}, which
  // matches by NAME - this type's own, then recursively its super types' - and ArcadeDB has no implicit root types
  // called {@code V} and {@code E} (that is an OrientDB inheritance). {@code CREATE VERTEX TYPE Person} builds a
  // {@code VertexType} with an EMPTY super-type list, so for every vertex and edge type a user actually creates both
  // tests answered false and the truncate proceeded with the documented {@code UNSAFE} escape hatch never demanded.
  // <p>
  // The hierarchies below deliberately contain NO type called {@code V} or {@code E}, which is what the #7919
  // regression test's own hierarchy relies on and why it went on passing over a dead check.

  /** Regression test for issue #8042. */
  @Test
  void aNonEmptyVertexTypeIsRefusedWithoutUnsafe() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Person").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("UNSAFE");

    assertThat(count("Person")).isEqualTo(1);
  }

  /** Regression test for issue #8042. */
  @Test
  void aNonEmptyEdgeTypeIsRefusedWithoutUnsafe() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE Knows").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Knows").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("UNSAFE");

    assertThat(count("Knows")).isEqualTo(1);
    assertThat(count("Person")).isEqualTo(2);
  }

  /** Regression test for issue #8042. */
  @Test
  void unsafeStillTruncatesAVertexType() {
    // THE ESCAPE HATCH THE MESSAGE NAMES HAS TO KEEP WORKING - THAT IS WHAT MAKES THE REFUSAL ACCEPTABLE
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    database.command("sql", "TRUNCATE TYPE Person UNSAFE").close();

    assertThat(count("Person")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void anEmptyVertexTypeIsStillTruncatedWithoutUnsafe() {
    // THE GUARD IS KEYED ON recs > 0: AN EMPTY TYPE HAS NOTHING TO LOSE AND MUST NOT START BEING REFUSED
    database.command("sql", "CREATE VERTEX TYPE Person").close();

    database.command("sql", "TRUNCATE TYPE Person").close();

    assertThat(count("Person")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void aNonEmptyDocumentTypeIsStillTruncatedWithoutUnsafe() {
    // THE GUARD IS ABOUT GRAPH TYPES ONLY: A PLAIN DOCUMENT TYPE HAS NO EDGES TO STRAND
    database.command("sql", "CREATE DOCUMENT TYPE Doc").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'").close());

    database.command("sql", "TRUNCATE TYPE Doc").close();

    assertThat(count("Doc")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void aPolymorphicTruncateIsRefusedForANonEmptyVertexSubtype() {
    // THE HALF #7919 REWROTE: THE COUNT IT CORRECTED FEEDS THIS CHECK, AND THE CHECK WAS DEAD. NO TYPE IN THIS
    // HIERARCHY IS CALLED V, WHICH IS EXACTLY WHAT #7919's OWN TEST RELIED ON
    database.command("sql", "CREATE VERTEX TYPE VBase").close();
    database.command("sql", "CREATE VERTEX TYPE VSub EXTENDS VBase").close();
    database.command("sql", "CREATE VERTEX TYPE VGrandSub EXTENDS VSub").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VGrandSub SET x = 1").close());

    // THE ROOT'S OWN GUARD ANSWERS FIRST HERE, BECAUSE A POLYMORPHIC countType() ALREADY SEES THE GRANDCHILD'S
    // RECORD. THAT IS THE POINT: BEFORE THE FIX NEITHER THIS GUARD NOR THE PER-SUBTYPE LOOP BELOW IT COULD FIRE AT
    // ALL AND THE WHOLE SUBTREE WAS EMPTIED IN SILENCE
    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE VBase POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes");

    assertThat(count("VGrandSub")).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE VBase POLYMORPHIC UNSAFE").close();
    assertThat(count("VGrandSub")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void theSubtypeLoopNamesTheNonEmptyVertexSubtypeUnderADocumentRoot() {
    // THE ONE SHAPE THAT REACHES THE PER-SUBTYPE LOOP #7919 REWROTE: A DOCUMENT ROOT PASSES ITS OWN GUARD (IT IS
    // NEITHER A VertexType NOR AN EdgeType, WHATEVER ITS POLYMORPHIC COUNT), SO THE LOOP IS WHAT HAS TO REFUSE ON
    // BEHALF OF THE GRAPH SUBTYPE UNDER IT - AND IT NAMES IT
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VChild EXTENDS DocRoot").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VChild SET x = 1").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VChild");

    assertThat(count("VChild")).isEqualTo(1);
  }

  /** Regression test for issue #8042. */
  @Test
  void aGraphLeafIsFoundBehindADocumentTypeIntermediateNode() {
    // getSubTypes() answers DIRECT children only, so a guard that walks one level down is blind to
    // DocRoot -> DocMiddle -> VLeaf: neither the root nor DocMiddle is a graph type, so neither names a reason to
    // refuse, while the polymorphic scanType() below happily deletes VLeaf's records. The scope of a POLYMORPHIC
    // truncate is the whole subtree, so the guard has to read the whole subtree (CodeRabbit on PR #8094).
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS DocMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VLeaf SET x = 1").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VLeaf");

    assertThat(count("VLeaf")).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC UNSAFE").close();
    assertThat(count("VLeaf")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void anEdgeLeafIsFoundBehindTwoDocumentTypeIntermediateNodes() {
    // The same shape one level deeper, and on the edge arm, so the recursion is pinned rather than a single extra
    // getSubTypes() hop.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocLower EXTENDS DocMiddle").close();
    database.command("sql", "CREATE EDGE TYPE ELeaf EXTENDS DocLower").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE ELeaf FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("ELeaf");

    assertThat(count("ELeaf")).isEqualTo(1);
  }

  /** Regression test for issue #8042. */
  @Test
  void anEmptyGraphAncestorIsNotBlamedForItsDescendantsRecords() {
    // A chain of TWO graph levels under a non-graph root. Counting each descendant polymorphically charged VLeaf's
    // record to the empty VMiddle above it, and the walk reaches VMiddle first, so the refusal named the one type
    // in the subtree that holds nothing (CodeRabbit on PR #8094). Refusing was right; naming VMiddle was not.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS VMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VLeaf SET x = 1").close());

    assertThat(count("VMiddle")).as("the intermediate holds nothing of its own").isEqualTo(1);
    assertThat(database.countType("VMiddle", false)).isZero();

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VLeaf")
        .hasMessageNotContaining("VMiddle");
  }

  /** Regression test for issue #8042. */
  @Test
  void anEmptyLightweightAncestorIsNotBlamedForItsSubtypesEdges() {
    // The lightweight twin of the case above. A LIGHTWEIGHT edge allocates no record, so countType() answers 0 for
    // it however many edges it holds (#7477) and only a count(*) walk can see them - but count(*) is
    // unconditionally polymorphic, so the empty LWAnc absorbed LWSub's edge and, being reached first, was named
    // (CodeRabbit on PR #8094). A record-backed sibling is in the same subtree to pin that the two count paths
    // agree on which type to blame.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE LWAnc EXTENDS DocRoot LIGHTWEIGHT").close();
    database.command("sql", "CREATE EDGE TYPE LWSub EXTENDS LWAnc LIGHTWEIGHT").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE LWSub FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThat(count("LWAnc")).as("polymorphically the ancestor sees its subtype's edge").isEqualTo(1);

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("LWSub")
        .hasMessageNotContaining("LWAnc");
  }

  /** Regression test for issue #8042. */
  @Test
  void anEmptyLightweightAncestorWithARecordBackedSubtypeNamesTheRecordBackedOne() {
    // Same shape, but the non-empty subtype is RECORD-BACKED under a lightweight ancestor: a type's own LIGHTWEIGHT
    // flag is independent of its parent's, so this is a legal hierarchy and the two count paths have to agree on
    // which type to name.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE LWAnc EXTENDS DocRoot LIGHTWEIGHT").close();
    database.command("sql", "CREATE EDGE TYPE HeavySub EXTENDS LWAnc").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE HeavySub FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("HeavySub")
        .hasMessageNotContaining("LWAnc");
  }

  /** Regression test for issue #8042. */
  @Test
  void anEmptyGraphLeafBehindADocumentIntermediateIsNotBlamed() {
    // The recursion must not start refusing a subtree that holds nothing: only a non-empty graph descendant is a
    // reason to demand UNSAFE.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS DocMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO DocRoot SET x = 1").close());

    database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close();

    assertThat(count("DocRoot")).isZero();
  }

  /** Regression test for issue #8042. */
  @Test
  void truncateTypeAndTruncateBucketNowAgreeOnTheSameVertexType() {
    // THE SHORTEST STATEMENT OF THE WHOLE FINDING: TRUNCATE BUCKET REFUSED WHERE TRUNCATE TYPE DID NOT, ON THE SAME
    // DATA, WITH THE SAME MESSAGE TEMPLATE
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 1").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    final String bucketName = database.getSchema().getType("Person").getBuckets(false).getFirst().getName();

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET `" + bucketName + "`").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not empty vertex bucket");
    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Person").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not empty vertex classes");

    assertThat(count("Person")).isEqualTo(1);
  }

  private long count(final String typeName) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM `" + typeName + "`").nextIfAvailable()
        .getProperty("c")).longValue();
  }
}
