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
package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The create and update grants of a type are checked before the record's own constraints are evaluated and before any
 * before-create / before-update listener or trigger runs, the order the delete follows. The user below resolves permissions as the
 * server does: the grant map is keyed on the type's involved buckets, every other file id is default-allow.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RecordWriteGrantOrderTest {
  private static final String PATH = "target/databases/RecordWriteGrantOrderTest";

  private DatabaseFactory factory;
  private Database        database;
  private DocumentType    gated;
  private AtomicInteger   typeCreateEvents;
  private AtomicInteger   databaseCreateEvents;
  private AtomicInteger   typeUpdateEvents;
  private AtomicInteger   databaseUpdateEvents;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createDocumentType("Audit");
    database.getSchema().createDocumentType("Other");

    gated = database.getSchema().createDocumentType("Gated");
    gated.createProperty("code", Type.STRING).setMandatory(true).setRegexp("[A-Z]{3}");
    gated.createProperty("size", Type.INTEGER).setMin("1");

    typeCreateEvents = new AtomicInteger();
    databaseCreateEvents = new AtomicInteger();
    typeUpdateEvents = new AtomicInteger();
    databaseUpdateEvents = new AtomicInteger();

    gated.getEvents().registerListener((BeforeRecordCreateListener) record -> {
      typeCreateEvents.incrementAndGet();
      return true;
    });
    gated.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      typeUpdateEvents.incrementAndGet();
      return true;
    });
    // the trigger below writes an Audit record: only the records of the gated type are counted
    database.getEvents().registerListener((BeforeRecordCreateListener) record -> {
      if (record instanceof Document document && "Gated".equals(document.getTypeName()))
        databaseCreateEvents.incrementAndGet();
      return true;
    });
    database.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      if (record instanceof Document document && "Gated".equals(document.getTypeName()))
        databaseUpdateEvents.incrementAndGet();
      return true;
    });

    database.command("sql", "CREATE TRIGGER GatedAudit BEFORE CREATE ON TYPE Gated EXECUTE SQL \"INSERT INTO Audit SET what = 'seen'\"")
        .close();
  }

  @AfterEach
  void tearDown() {
    if (database == null)
      return;
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  @Test
  void aRefusedCreateAnswersTheGrantBeforeTheConstraints() {
    refuse(gated, SecurityDatabaseUser.ACCESS.CREATE_RECORD, SecurityDatabaseUser.ACCESS.READ_RECORD);

    // the mandatory property is missing and the other values break their declared limits
    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Gated").set("size", 0).save())))
        .isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Gated").set("code", "no").save())))
        .isInstanceOf(SecurityException.class);

    // a valid record is refused the same way
    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Gated").set("code", "ABC").save())))
        .isInstanceOf(SecurityException.class);

    assertThat(typeCreateEvents.get()).isZero();
    assertThat(databaseCreateEvents.get()).isZero();
  }

  @Test
  void aRefusedCreateRunsNoTrigger() {
    refuse(gated, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    // the caller owns the transaction and commits it after the refusal
    database.begin();
    assertThat(catchThrowable(() -> database.newDocument("Gated").set("code", "ABC").save())).isInstanceOf(SecurityException.class);
    database.commit();

    assertThat(database.countType("Audit", false)).isZero();
    assertThat(database.countType("Gated", false)).isZero();
  }

  @Test
  void aRefusedSqlInsertAnswersTheGrantBeforeTheConstraints() {
    refuse(gated, SecurityDatabaseUser.ACCESS.CREATE_RECORD, SecurityDatabaseUser.ACCESS.READ_RECORD);

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO Gated SET size = 0").close())))
        .isInstanceOf(SecurityException.class);

    assertThat(typeCreateEvents.get()).isZero();
    assertThat(databaseCreateEvents.get()).isZero();
  }

  @Test
  void aRefusedCreateInANamedBucketAnswersTheGrantFirst() {
    final String gatedBucket = gated.getBuckets(false).getFirst().getName();
    refuse(gated, SecurityDatabaseUser.ACCESS.CREATE_RECORD, SecurityDatabaseUser.ACCESS.READ_RECORD);

    // a record of the type itself, with a value its constraints refuse
    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Gated").set("size", 0).save(gatedBucket))))
        .isInstanceOf(SecurityException.class);

    // a record of another type in a bucket that is not its own: the answer is the grant, not the owner of the bucket
    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Other").save(gatedBucket))))
        .isInstanceOf(SecurityException.class);

    assertThat(typeCreateEvents.get()).isZero();
    assertThat(databaseCreateEvents.get()).isZero();
  }

  @Test
  void aGrantedCreateStillEvaluatesTheConstraintsAndRunsTheListeners() {
    refuse(gated, SecurityDatabaseUser.ACCESS.UPDATE_RECORD, SecurityDatabaseUser.ACCESS.DELETE_RECORD);

    assertThat(catchThrowable(() -> database.transaction(() -> database.newDocument("Gated").set("size", 0).save())))
        .isInstanceOf(ValidationException.class);
    assertThat(typeCreateEvents.get()).isZero();

    database.transaction(() -> database.newDocument("Gated").set("code", "ABC").set("size", 3).save());

    assertThat(typeCreateEvents.get()).isEqualTo(1);
    assertThat(databaseCreateEvents.get()).isEqualTo(1);
    assertThat(database.countType("Gated", false)).isEqualTo(1);
    assertThat(database.countType("Audit", false)).isEqualTo(1);
  }

  @Test
  void aRefusedUpdateAnswersTheGrantBeforeTheConstraints() {
    final RID rid = createGatedRecord();
    typeUpdateEvents.set(0);
    databaseUpdateEvents.set(0);

    refuse(gated, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);

    database.begin();
    final MutableDocument invalid = rid.asDocument().modify();
    invalid.remove("code");
    invalid.set("size", 0);
    assertThat(catchThrowable(invalid::save)).isInstanceOf(SecurityException.class);

    final MutableDocument valid = rid.asDocument().modify();
    valid.set("size", 5);
    assertThat(catchThrowable(valid::save)).isInstanceOf(SecurityException.class);
    database.rollback();

    assertThat(typeUpdateEvents.get()).isZero();
    assertThat(databaseUpdateEvents.get()).isZero();
    assertThat(rid.asDocument().getInteger("size")).isEqualTo(3);
  }

  @Test
  void aRefusedSqlUpdateAnswersTheGrantBeforeTheConstraints() {
    createGatedRecord();
    typeUpdateEvents.set(0);
    databaseUpdateEvents.set(0);

    refuse(gated, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "UPDATE Gated SET size = 0").close())))
        .isInstanceOf(SecurityException.class);

    assertThat(typeUpdateEvents.get()).isZero();
    assertThat(databaseUpdateEvents.get()).isZero();
  }

  @Test
  void aGrantedUpdateStillEvaluatesTheConstraintsAndRunsTheListeners() {
    final RID rid = createGatedRecord();
    typeUpdateEvents.set(0);
    databaseUpdateEvents.set(0);

    refuse(gated, SecurityDatabaseUser.ACCESS.CREATE_RECORD, SecurityDatabaseUser.ACCESS.DELETE_RECORD);

    database.begin();
    final MutableDocument invalid = rid.asDocument().modify();
    invalid.set("size", 0);
    assertThat(catchThrowable(invalid::save)).isInstanceOf(ValidationException.class);
    database.rollback();
    assertThat(typeUpdateEvents.get()).isZero();

    database.transaction(() -> rid.asDocument().modify().set("size", 5).save());

    assertThat(typeUpdateEvents.get()).isEqualTo(1);
    assertThat(databaseUpdateEvents.get()).isEqualTo(1);
    assertThat(rid.asDocument().getInteger("size")).isEqualTo(5);
  }

  @Test
  void aRefusedLightweightEdgeAnswersTheGrantBeforeTheExistenceOfTheEdge() {
    database.getSchema().createVertexType("Node");
    final DocumentType follows = database.getSchema().buildEdgeType().withName("Follows").withLightweight(true).withUnique(true)
        .create();

    final RID[] nodes = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").save();
      final MutableVertex b = database.newVertex("Node").save();
      a.newEdge("Follows", b);
      nodes[0] = a.getIdentity();
      nodes[1] = b.getIdentity();
    });

    // a second edge between the same pair is a duplicate for a user allowed to create it...
    assertThat(catchThrowable(() -> database.transaction(() -> nodes[0].asVertex().modify().newEdge("Follows", nodes[1]))))
        .isInstanceOf(DuplicatedKeyException.class);

    // ...and a refusal of the grant for one that is not
    refuse(follows, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    assertThat(catchThrowable(() -> database.transaction(() -> nodes[0].asVertex().modify().newEdge("Follows", nodes[1]))))
        .isInstanceOf(SecurityException.class);

    assertThat(nodes[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "Follows")).isEqualTo(1);
  }

  @Test
  void aRefusedEdgeToAMissingVertexAnswersTheGrant() {
    database.getSchema().createVertexType("Node");
    final DocumentType knows = database.getSchema().buildEdgeType().withName("Knows").withBidirectional(false).create();

    final RID[] nodes = new RID[1];
    database.transaction(() -> nodes[0] = database.newVertex("Node").save().getIdentity());
    final RID missing = RID.create(database, nodes[0].getBucketId(), 500L);

    refuse(knows, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    assertThat(catchThrowable(() -> database.transaction(() -> nodes[0].asVertex().modify().newEdge("Knows", missing))))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void aUserWithoutAnyRefusalReadsAndWritesAsBefore() {
    final RID rid = createGatedRecord();

    refuse(database.getSchema().getType("Other"), SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    // the refusal of another type does not reach this one
    database.transaction(() -> rid.asDocument().modify().set("size", 7).save());
    assertThat(rid.asDocument().getInteger("size")).isEqualTo(7);

    try (final ResultSet resultSet = database.query("sql", "SELECT FROM Gated")) {
      assertThat(resultSet.stream().count()).isEqualTo(1);
    }
  }

  private RID createGatedRecord() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Gated").set("code", "ABC").set("size", 3).save().getIdentity());
    typeCreateEvents.set(0);
    databaseCreateEvents.set(0);
    return rid[0];
  }

  /** Binds a user refused {@code refused} on the type's involved buckets: every other file id and every other access is allowed. */
  private void refuse(final DocumentType type, final SecurityDatabaseUser.ACCESS... refused) {
    final Set<Integer> gatedFiles = new HashSet<>();
    for (final Bucket bucket : type.getInvolvedBuckets())
      gatedFiles.add(bucket.getFileId());
    assertThat(gatedFiles).isNotEmpty();

    final Set<SecurityDatabaseUser.ACCESS> refusedAccess = EnumSet.noneOf(SecurityDatabaseUser.ACCESS.class);
    for (final SecurityDatabaseUser.ACCESS access : refused)
      refusedAccess.add(access);

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "limited";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !gatedFiles.contains(fileId) || !refusedAccess.contains(access);
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
    });
  }
}
